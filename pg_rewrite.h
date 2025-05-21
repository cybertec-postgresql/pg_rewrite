/*------------------------------------------------------------
 *
 * pg_rewrite.h
 *     Tools for maintenance that requires table rewriting.
 *
 * Copyright (c) 2023, Cybertec PostgreSQL International GmbH
 *
 *------------------------------------------------------------
 */

#include <sys/time.h>

#include "c.h"
#include "postgres.h"
#include "fmgr.h"
#include "miscadmin.h"

#include "access/genam.h"
#include "access/heapam.h"
#include "access/relscan.h"
#include "access/xlog_internal.h"
#include "access/xact.h"
#include "catalog/pg_class.h"
#include "nodes/execnodes.h"
#include "postmaster/bgworker.h"
#include "replication/logical.h"
#include "replication/origin.h"
#include "utils/inval.h"
#include "utils/resowner.h"
#include "utils/snapmgr.h"

typedef enum
{
	CHANGE_INSERT,
	CHANGE_UPDATE_OLD,
	CHANGE_UPDATE_NEW,
	CHANGE_DELETE
} ConcurrentChangeKind;

typedef struct ConcurrentChange
{
	/* See the enum above. */
	ConcurrentChangeKind kind;

	/*
	 * The actual tuple.
	 *
	 * The tuple data follows the ConcurrentChange structure. Before use make
	 * sure the tuple is correctly aligned (ConcurrentChange can be stored as
	 * bytea) and that tuple->t_data is fixed.
	 */
	HeapTupleData tup_data;
} ConcurrentChange;

typedef struct DecodingOutputState
{
	/* The relation whose changes we're decoding. */
	Oid			relid;

	/*
	 * Decoded changes are stored here. Although we try to avoid excessive
	 * batches, it can happen that the changes need to be stored to disk. The
	 * tuplestore does this transparently.
	 */
	Tuplestorestate *tstore;

	/* The current number of changes in tstore. */
	double		nchanges;

	/*
	 * Descriptor to store the ConcurrentChange structure serialized (bytea).
	 * We can't store the tuple directly because tuplestore only supports
	 * minimum tuple and we may need to transfer OID system column from the
	 * output plugin. Also we need to transfer the change kind, so it's better
	 * to put everything in the structure than to use 2 tuplestores "in
	 * parallel".
	 */
	TupleDesc	tupdesc_change;

	/*
	 * Tuple descriptor needed process the concurrent data changes.
	 */
	TupleDesc	tupdesc_src;

	/* Slot to retrieve data from tstore. */
	TupleTableSlot *tsslot;

	/*
	 * WAL records having this origin have been created by the initial load
	 * and should not be decoded.
	 */
	RepOriginId rorigin;

	ResourceOwner resowner;
} DecodingOutputState;

/* The WAL segment being decoded. */
extern XLogSegNo rewrite_current_segment;

extern void _PG_init(void);

/* Progress tracking. */
typedef struct TaskProgress
{
	/* Tuples inserted during the initial load. */
	int64		ins_initial;

	/*
	 * Tuples inserted, updated and deleted after the initial load (i.e.
	 * during the catch-up phase).
	 */
	int64		ins;
	int64		upd;
	int64		del;
} TaskProgress;

/*
 * The new implementation, which delegates the execution to a background
 * worker (as opposed to the PG executor).
 *
 * Arguments are passed to the worker via this structure, located in the
 * shared memory.
 */
typedef struct WorkerTask
{
	/* Connection info. */
	Oid		dbid;
	Oid		roleid;

	/* Worker that performs the task both sets and clears this field. */
	pid_t		pid;

	/* See the comments of pg_rewrite_exit_if_requested(). */
	bool	exit_requested;

	/* The progress is only valid if the dbid is valid. */
	TaskProgress	progress;

	/*
	 * Use this when setting / clearing the fields above. Once dbid is set,
	 * the task belongs to the backend that set it, so the other fields may be
	 * assigned w/o the lock.
	 */
	slock_t		mutex;

	/* The tables to work on. */
	NameData	relschema;
	NameData	relname;
	NameData	relname_new;
	NameData	relschema_dst;
	NameData	relname_dst;

	/*
	 * Space for the worker to send an error message to the backend.
	 *
	 * XXX Note that later messages overwrite the earlier ones, so only the
	 * last message is received. Is it worth using a queue instead?
	 */
#define	MAX_ERR_MSG_LEN	1024
	char		msg[MAX_ERR_MSG_LEN];

	/* Detailed error message. */
	char		msg_detail[MAX_ERR_MSG_LEN];

	int		elevel;

	/*
	 * Should rewrite_table() return w/o waiting for the worker's exit? If
	 * this flag is set, the worker is responsible for releasing the
	 * task. Otherwise the worker must not release the task because the
	 * backend might be interested in 'msg' and 'msg_detail'.
	 */
	bool	nowait;
} WorkerTask;

#define		MAX_TASKS	8

/* Each backend stores here the pointer to its task in the shared memory. */
extern WorkerTask *MyWorkerTask;

/*
 * Like AttrMap in PG core, but here we add an array of expressions to coerce
 * the input values to output ones. (A new name is needed as it's hard to
 * avoid inclusion of the in-core structure.)
 */
typedef struct AttrMapExt
{
	AttrNumber *attnums;
	int			maplen;
	bool	dropped_attr;	/* Has outer or inner descriptor a dropped
							 * attribute? */
	Node	**coerceExprs;	/* Non-NULL field tells how to convert the input
							 * value to the output data type. NULL indicates
							 * that no conversion is needed. */
} AttrMapExt;

/*
 * Like TupleConversionMap in PG core, but here we add an array of expressions
 * to coerce the input values to output ones. (A new name is needed as it's
 * hard to avoid inclusion of the in-core structure.)
 */
typedef struct TupleConversionMapExt
{
	TupleDesc	indesc;			/* tupdesc for source rowtype */
	TupleDesc	outdesc;		/* tupdesc for result rowtype */
	AttrMapExt    *attrMap;		/* indexes of input fields, or 0 for null */
	Datum	   *invalues;		/* workspace for deconstructing source */
	bool	   *inisnull;
	Datum	   *outvalues;		/* workspace for constructing result */
	bool	   *outisnull;
	ExprState	**coerceExprs;	/* See AttrMapExt */
	EState		*estate;		/* Executor state used to evaluate
								 * coerceExprs. */
	TupleTableSlot *in_slot;	/* Slot to store the input tuple for
								 * coercion. */
} TupleConversionMapExt;

/*
 * Hash table to cache partition-specific information.
 */
typedef struct PartitionEntry
{
	Oid			part_oid;		/* key */
	Relation	ident_index;

	/*
	 * Slot (TTSOpsHeapTuple) to apply data changes to the partition.
	 */
	TupleTableSlot *slot;

	/*
	 * Slot to retrieve tuples from the partition. Separate from 'slot_ind'
	 * because it has to be TTSOpsBufferHeapTuple.
	 */
	TupleTableSlot *slot_ind;

	/* This should make insertions into partitions more efficient. */
	BulkInsertState bistate;

	/*
	 * Map to convert tuples that match the partitioned table so they match
	 * this partition.
	 */
	TupleConversionMapExt	*conv_map;

	char		status;			/* used by simplehash */
} PartitionEntry;

#define SH_PREFIX partitions
#define SH_ELEMENT_TYPE PartitionEntry
#define SH_KEY_TYPE Oid
#define SH_KEY part_oid
#define SH_HASH_KEY(tb, key) (key)
#define SH_EQUAL(tb, a, b) ((a) == (b))
#define SH_SCOPE static inline
#define SH_DECLARE
#define SH_DEFINE
#include "lib/simplehash.h"

extern PGDLLEXPORT void rewrite_worker_main(Datum main_arg);

extern void pg_rewrite_exit_if_requested(void);

/*
 * Use function names distinct from those in pg_squeeze, in case both
 * extensions are installed.
 */
extern bool pg_rewrite_process_concurrent_changes(EState *estate,
												  ModifyTableState *mtstate,
												  struct PartitionTupleRouting *proute,
												  LogicalDecodingContext *ctx,
												  XLogRecPtr end_of_wal,
												  ScanKey ident_key,
												  int ident_key_nentries,
												  Relation ident_index,
												  TupleTableSlot *slot_dst_ind,
												  LOCKMODE lock_held,
												  partitions_hash *partitions,
												  TupleConversionMapExt *conv_map,
												  struct timeval *must_complete);
extern bool pg_rewrite_decode_concurrent_changes(LogicalDecodingContext *ctx,
												 XLogRecPtr end_of_wal,
												 struct timeval *must_complete);
extern HeapTuple convert_tuple_for_dest_table(HeapTuple tuple,
											  TupleConversionMapExt *conv_map);
extern void _PG_output_plugin_init(OutputPluginCallbacks *cb);
extern PartitionEntry *get_partition_entry(partitions_hash *partitions,
										   Oid part_oid);;
extern HeapTuple pg_rewrite_execute_attr_map_tuple(HeapTuple tuple,
												   TupleConversionMapExt *map);
