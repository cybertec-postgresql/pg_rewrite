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

	/* Tuple descriptor needed to update indexes. */
	TupleDesc	tupdesc;

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

/*
 * Subset of fields of pg_class, plus the necessary info on attributes. It
 * represents either the source relation or a composite type of the source
 * relation's attribute.
 */
typedef struct PgClassCatInfo
{
	/* pg_class(oid) */
	Oid			relid;

	/*
	 * pg_class(xmin)
	 */
	TransactionId xmin;

	/* Array of pg_attribute(xmin). (Dropped columns are here too.) */
	TransactionId *attr_xmins;
	int16		relnatts;
} PgClassCatInfo;

/*
 * Information on source relation index, used to build the index on the
 * transient relation. To avoid repeated retrieval of the pg_index fields we
 * also add pg_class(xmin) and pass the same structure to
 * check_catalog_changes().
 */
typedef struct IndexCatInfo
{
	Oid			oid;			/* pg_index(indexrelid) */
	NameData	relname;		/* pg_class(relname) */
	Oid			reltablespace;	/* pg_class(reltablespace) */
	TransactionId xmin;			/* pg_index(xmin) */
	TransactionId pg_class_xmin;	/* pg_class(xmin) of the index (not the
									 * parent relation) */
} IndexCatInfo;

/*
 * If the source relation has attribute(s) of composite type, we need to check
 * for changes of those types.
 */
typedef struct TypeCatInfo
{
	Oid			oid;			/* pg_type(oid) */
	TransactionId xmin;			/* pg_type(xmin) */

	/*
	 * The pg_class entry whose oid == pg_type(typrelid) of this type.
	 */
	PgClassCatInfo rel;
} TypeCatInfo;

/*
 * Information to check whether an "incompatible" catalog change took
 * place. Such a change prevents us from completing processing of the current
 * table.
 */
typedef struct CatalogState
{
	/* The relation whose changes we'll check for. */
	PgClassCatInfo rel;

	/* Copy of pg_class tuple of the source relation. */
	Form_pg_class form_class;

	/* Copy of pg_class tuple descriptor of the source relation. */
	TupleDesc	desc_class;

	/* Per-index info. */
	int			relninds;
	IndexCatInfo *indexes;

	/* Composite types used by the source rel attributes. */
	TypeCatInfo *comptypes;
	/* Size of the array. */
	int			ncomptypes_max;
	/* Used elements of the array. */
	int			ncomptypes;

	/*
	 * Does at least one index have wrong value of indisvalid, indisready or
	 * indislive?
	 */
	bool		invalid_index;

	/* Does the table have primary key index? */
	bool		have_pk_index;
} CatalogState;

/*
 * Hash table to cache partition-specific information.
 */
typedef struct PartitionEntry
{
	Oid			part_oid;		/* key */
	Relation	ident_index;

	/*
	 * Slot to retrieve tuples from identity index. Since we only allow
	 * partitions to have exactly the same attributes as the parent table, it
	 * should work if we used the same slot for all partitions. However it
	 * seems cleaner if separate slots are used.
	 */
	TupleTableSlot *ind_slot;

	/* This should make insertions into partitions more efficient. */
	BulkInsertState bistate;

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

typedef enum WorkerTaskKind
{
	WORKER_TASK_PARTITION
} WorkerTaskKind;

/*
 * The new implementation, which delegates the execution to a background
 * worker (as opposed to the PG executor).
 *
 * Arguments are passed to the worker via this structure, located in the
 * shared memory.
 */
typedef struct WorkerTask
{
	WorkerTaskKind	kind;

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

	/* Space for the worker to send an error message to the backend. */
#define	MAX_ERR_MSG_LEN	1024
	char		msg[MAX_ERR_MSG_LEN];

	/* The rewrite.wait_after_load GUC, for test purposes. */
	/* TODO Consider using injection points instead */
	int		wait_after_load;
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

extern PGDLLEXPORT void rewrite_worker_main(Datum main_arg);

extern void pg_rewrite_exit_if_requested(void);

/*
 * Use function names distinct from those in pg_squeeze, in case both
 * extensions are installed.
 */
extern void pg_rewrite_check_catalog_changes(CatalogState *state,
											 LOCKMODE lock_held);

extern bool pg_rewrite_process_concurrent_changes(EState *estate,
												  ModifyTableState *mtstate,
												  struct PartitionTupleRouting *proute,
												  LogicalDecodingContext *ctx,
												  XLogRecPtr end_of_wal,
												  CatalogState *cat_state,
												  Relation rel_dst,
												  ScanKey ident_key,
												  int ident_key_nentries,
												  Relation ident_index,
												  TupleTableSlot *ind_slot,
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
extern BulkInsertState get_partition_insert_state(partitions_hash *partitions,
												  Oid part_oid);;
extern HeapTuple pg_rewrite_execute_attr_map_tuple(HeapTuple tuple,
												   TupleConversionMapExt *map);
