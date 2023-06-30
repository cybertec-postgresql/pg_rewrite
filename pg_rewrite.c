/*------------------------------------------------------------
 *
 * pg_rewrite.c
 *     Tools for maintenance that requires table rewriting.
 *
 * Copyright (c) 2021-2023, Cybertec PostgreSQL International GmbH
 *
 *------------------------------------------------------------
 */
#include "pg_rewrite.h"

#include "access/heaptoast.h"
#include "access/multixact.h"
#include "access/sysattr.h"
#include "access/tupdesc_details.h"
#if PG_VERSION_NUM >= 150000
#include "access/xloginsert.h"
#endif
#include "access/xlogutils.h"
#include "catalog/catalog.h"
#include "catalog/heap.h"
#include "catalog/index.h"
#include "catalog/namespace.h"
#include "catalog/objectaddress.h"
#include "catalog/objectaccess.h"
#include "catalog/pg_am.h"
#include "catalog/pg_constraint.h"
#include "catalog/pg_control.h"
#include "catalog/pg_extension.h"
#include "catalog/pg_type.h"
#include "catalog/pg_tablespace.h"
#include "catalog/toasting.h"
#include "commands/extension.h"
#include "commands/tablecmds.h"
#include "commands/tablespace.h"
#include "executor/executor.h"
#include "executor/execPartition.h"
#include "lib/stringinfo.h"
#include "nodes/primnodes.h"
#include "nodes/makefuncs.h"
#include "optimizer/optimizer.h"
#include "replication/snapbuild.h"
#include "partitioning/partdesc.h"
#include "storage/bufmgr.h"
#include "storage/freespace.h"
#include "storage/lmgr.h"
#include "storage/proc.h"
#include "storage/smgr.h"
#include "storage/standbydefs.h"
#include "tcop/tcopprot.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/datum.h"
#include "utils/fmgroids.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/syscache.h"
#include "utils/varlena.h"

#if PG_VERSION_NUM < 130000
#error "PostgreSQL version 13 or higher is required"
#endif

PG_MODULE_MAGIC;

#define	REPL_SLOT_BASE_NAME	"pg_rewrite_slot_"
#define	REPL_PLUGIN_NAME	"pg_rewrite"

static void partition_table_internal(PG_FUNCTION_ARGS);
static int	index_cat_info_compare(const void *arg1, const void *arg2);

/* The WAL segment being decoded. */
XLogSegNo	part_current_segment = 0;

/*
 * Information on a single constraint, needed to compare constraints of the
 * source and the destination relations.
 */
typedef struct ConstraintInfo
{
	char		contype;
	bool		convalidated;
	char	   *conbin;
	NameData	conname;
	Bitmapset  *attnos;

	Oid			confrelid;
	int			numfks;
	AttrNumber	conkey[INDEX_MAX_KEYS];
	AttrNumber	confkey[INDEX_MAX_KEYS];
	Oid			pf_eq_oprs[INDEX_MAX_KEYS];
	Oid			pp_eq_oprs[INDEX_MAX_KEYS];
	Oid			ff_eq_oprs[INDEX_MAX_KEYS];
#if PG_VERSION_NUM >= 150000
	int			num_fk_del_set_cols;
	AttrNumber	fk_del_set_cols[INDEX_MAX_KEYS];
#endif
} ConstraintInfo;

static void check_prerequisites(Relation rel);
static LogicalDecodingContext *setup_decoding(Oid relid, TupleDesc tup_desc);
static void decoding_cleanup(LogicalDecodingContext *ctx);
static CatalogState *get_catalog_state(Oid relid);
static void get_pg_class_info(Oid relid, TransactionId *xmin,
							  Form_pg_class *form_p, TupleDesc *desc_p);
static void get_attribute_info(Oid relid, int relnatts,
							   TransactionId **xmins_p,
							   CatalogState *cat_state);
static void cache_composite_type_info(CatalogState *cat_state, Oid typid);
static void get_composite_type_info(TypeCatInfo *tinfo);
static IndexCatInfo *get_index_info(Oid relid, int *relninds,
									bool *found_invalid,
									bool invalid_check_only,
									bool *found_pk);
static void check_tup_descs_match(TupleDesc tupdesc_src, char *tabname_src,
								  TupleDesc tupdesc_dst, char *tabname_dst);
static bool equal_op_expressions(char *expr_str1, char *expr_str2);
static bool is_rel_referenced(Relation rel);
static void compare_constraints(Relation rel1, Relation rel2);
static List *get_relation_constraints(Relation rel, int *ncheck,
									  int *nunique, int *nfk);
static void free_relation_constraints(List *l);
static ConstraintInfo **get_relation_constraints_array(List *all,
													   char contype,
													   int n);
static bool equal_dest_descs(TupleDesc tupdesc1, TupleDesc tupdesc2);
static void report_tupdesc_mismatch(const char *desc,
									char *attname, char *tabname_src,
									bool has_src, char *tabname_dst,
									bool has_dst);
static TupleDesc get_index_tuple_desc(Oid ind_oid);
static ModifyTableState *get_modify_table_state(EState *estate, Relation rel,
												CmdType operation);
static void free_modify_table_state(ModifyTableState *mtstate);
static void check_attribute_changes(CatalogState *cat_state);
static void check_index_changes(CatalogState *state);
static void check_composite_type_changes(CatalogState *cat_state);
static void free_catalog_state(CatalogState *state);
static void check_pg_class_changes(CatalogState *state);
static Snapshot build_historic_snapshot(SnapBuild *builder);
static void perform_initial_load(EState *estate, ModifyTableState *mtstate,
								 struct PartitionTupleRouting *proute,
								 Relation rel_src, Snapshot snap_hist,
								 Relation rel_dst,
								 partitions_hash *partitions,
								 LogicalDecodingContext *ctx,
								 TupleConversionMap *conv_map);
static ScanKey build_identity_key(Relation ident_idx_rel, int *nentries);
static bool perform_final_merge(EState *estate,
								ModifyTableState *mtstate,
								struct PartitionTupleRouting *proute,
								Oid relid_src, Oid *indexes_src, int nindexes,
								Relation rel_dst, ScanKey ident_key,
								int ident_key_nentries,
								CatalogState *cat_state,
								LogicalDecodingContext *ctx,
								partitions_hash *ident_indexes,
								TupleConversionMap *conv_map);
static void close_partitions(partitions_hash *partitions);

/*
 * Should it be checked whether constraints on the destination table match
 * constraints on the source table?
 */
bool		rewrite_check_constraints = true;

/*
 * The maximum time to hold AccessExclusiveLock on the source table during the
 * final processing. Note that it only pg_rewrite_process_concurrent_changes()
 * execution time is included here.
 */
int			rewrite_max_xlock_time = 0;

/*
 * Time (in seconds) to wait after the initial load has completed and before
 * we start decoding of data changes introduced by other transactions. This
 * helps to ensure defined order of steps when we test processing of the
 * concurrent changes.
 */
int			rewrite_wait_after_load = 0;

void
_PG_init(void)
{
	DefineCustomBoolVariable("rewrite.check_constraints",
							 "Should constraints on the destination table be checked?",
							 "Should it be checked whether constraints on the destination table match those "
							 "on the source table?",
							 &rewrite_check_constraints,
							 true,
							 PGC_USERSET,
							 0,
							 NULL, NULL, NULL);

	DefineCustomIntVariable("rewrite.max_xlock_time",
							"The maximum time the processed table may be locked exclusively.",
							"The source table is locked exclusively during the final stage of "
							"processing. If the lock time should exceed this value, the lock is "
							"released and the final stage is retried a few more times.",
							&rewrite_max_xlock_time,
							0, 0, INT_MAX,
							PGC_USERSET,
							GUC_UNIT_MS,
							NULL, NULL, NULL);

	DefineCustomIntVariable("rewrite.wait_after_load",
							"Time to wait after the initial load.",
							"Time to wait after the initial load, so that a concurrent session "
							"can perform data changes before processing goes on. This is useful "
							"for regression tests.",
							&rewrite_wait_after_load,
							0, 0, 10,
							PGC_USERSET,
							GUC_UNIT_S,
							NULL, NULL, NULL);
}

#define REPLORIGIN_NAME_PATTERN	"pg_rewrite_%u"

/*
 * SQL interface to partition one table interactively.
 */
extern Datum partition_table(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(partition_table);
Datum
partition_table(PG_FUNCTION_ARGS)
{
	PG_TRY();
	{
		partition_table_internal(fcinfo);
	}
	PG_CATCH();
	{
		/*
		 * Special effort is needed to release the replication slot because,
		 * unlike other resources, AbortTransaction() does not release it.
		 * While the transaction is aborted if an ERROR is caught in the main
		 * loop of postgres.c, it would not do if the ERROR was trapped at
		 * lower level in the stack. The typical case is that
		 * partition_table() is called from pl/pgsql function.
		 */
		if (MyReplicationSlot != NULL)
			ReplicationSlotRelease();

		/*
		 * There seems to be no automatic cleanup of the origin, so do it
		 * here.
		 */
		if (replorigin_session_origin != InvalidRepOriginId)
		{
#if PG_VERSION_NUM >= 140000
			char		replorigin_name[255];

			snprintf(replorigin_name, sizeof(replorigin_name),
					 REPLORIGIN_NAME_PATTERN, MyDatabaseId);
			replorigin_drop_by_name(replorigin_name, false, true);
#else
			replorigin_drop(replorigin_session_origin, false);
#endif
			replorigin_session_origin = InvalidRepOriginId;
		}

		PG_RE_THROW();
	}
	PG_END_TRY();

	PG_RETURN_VOID();
}

/* PG >= 14 does define this macro. */
#if PG_VERSION_NUM < 140000
#define RelationIsPermanent(relation) \
	((relation)->rd_rel->relpersistence == RELPERSISTENCE_PERMANENT)
#endif

static void
partition_table_internal(PG_FUNCTION_ARGS)
{
	text	   *relname_src_t,
			   *relname_dst,
			   *relname_src_new_t;
	char	   *relname_src,
			   *relname_src_new;
	RangeVar   *relrv;
	Relation	rel_src,
				rel_dst;
	PartitionDesc part_desc;
	Oid			ident_idx_src;
	Oid			relid_src;
	ScanKey		ident_key = NULL;
	int			i,
				ident_key_nentries = 0;
	LogicalDecodingContext *ctx;
	ReplicationSlot *slot;
	Snapshot	snap_hist;
	TupleDesc	tup_desc_src,
				ident_src_tupdesc;
	CatalogState *cat_state;
	XLogRecPtr	end_of_wal;
	XLogRecPtr	xlog_insert_ptr;
	int			nindexes;
	Oid		   *indexes_src = NULL;
	bool		invalid_index = false;
	IndexCatInfo *ind_info;
	bool		source_finalized;
	Relation   *parts_dst;
	partitions_hash *partitions;
	TupleConversionMap *conv_map;
	EState	   *estate;
	ModifyTableState *mtstate;
	struct PartitionTupleRouting *proute;

	if (PG_ARGISNULL(0) || PG_ARGISNULL(1) || PG_ARGISNULL(2))
		ereport(ERROR,
				(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
				 (errmsg("NULL arguments not allowed"))));

	relname_src_t = PG_GETARG_TEXT_PP(0);
	relrv = makeRangeVarFromNameList(textToQualifiedNameList(relname_src_t));
	rel_src = table_openrv(relrv, AccessShareLock);

	relname_src_new_t = PG_GETARG_TEXT_PP(2);
	relrv = makeRangeVarFromNameList(textToQualifiedNameList(relname_src_new_t));

	/*
	 * Technically it's possible to move the source relation to another schema
	 * but don't bother for this version.
	 */
	if (relrv->schemaname || relrv->catalogname)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_NAME),
				 (errmsg("the new source relation name may not be qualified"))));
	relname_src_new = pstrdup(relrv->relname);

	check_prerequisites(rel_src);

	/*
	 * Retrieve the useful info while holding lock on the relation.
	 */
	ident_idx_src = RelationGetReplicaIndex(rel_src);

	/* The table can have PK although the replica identity is FULL. */
	if (ident_idx_src == InvalidOid && rel_src->rd_pkindex != InvalidOid)
		ident_idx_src = rel_src->rd_pkindex;

	relname_src = pstrdup(RelationGetRelationName(rel_src));

	/*
	 * Check if we're ready to capture changes that possibly take place during
	 * the initial load.
	 *
	 * Concurrent DDL causes ERROR in any case, so don't worry about validity
	 * of this test during the next steps.
	 *
	 * Note: we let the plugin do this check on per-change basis, and allow
	 * processing of tables with no identity if only INSERT changes are
	 * decoded. However it seems inconsistent.
	 *
	 * XXX Although ERRCODE_UNIQUE_VIOLATION is no actual "unique violation",
	 * this error code seems to be the best match.
	 * (ERRCODE_TRIGGERED_ACTION_EXCEPTION might be worth consideration as
	 * well.)
	 */
	if (!OidIsValid(ident_idx_src))
		ereport(ERROR,
				(errcode(ERRCODE_UNIQUE_VIOLATION),
				 (errmsg("Table \"%s\" has no identity index",
						 relname_src))));

	relid_src = RelationGetRelid(rel_src);

	/*
	 * Info to initialize the tuplestore we'll use during logical decoding.
	 */
	tup_desc_src = CreateTupleDescCopyConstr(RelationGetDescr(rel_src));

	/*
	 * Get ready for the subsequent calls of
	 * pg_rewrite_check_catalog_changes().
	 *
	 * Not all index changes do conflict with the AccessShareLock - see
	 * get_index_info() for explanation.
	 *
	 * XXX It'd still be correct to start the check a bit later, i.e. just
	 * before CreateInitDecodingContext(), but the gain is not worth making
	 * the code less readable.
	 */
	cat_state = get_catalog_state(relid_src);

	/* Give up if it's clear enough to do so. */
	if (cat_state->invalid_index)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 (errmsg("At least one index is invalid"))));

	/*
	 * The relation shouldn't be locked during the call of setup_decoding(),
	 * otherwise another transaction could write XLOG records before the
	 * slots' data.restart_lsn and we'd have to wait for it to finish. If such
	 * a transaction requested exclusive lock on our relation (e.g. ALTER
	 * TABLE), it'd result in a deadlock.
	 *
	 * We can't keep the lock till the end of transaction anyway - that's why
	 * pg_rewrite_check_catalog_changes() exists.
	 */
	table_close(rel_src, AccessShareLock);

	nindexes = cat_state->relninds;

	/*
	 * Existence of identity index was checked above, so number of indexes and
	 * attributes are both non-zero.
	 */
	Assert(cat_state->form_class->relnatts >= 1);
	Assert(nindexes > 0);

	/* Copy the OIDs into a separate array, for convenient use later. */
	indexes_src = (Oid *) palloc(nindexes * sizeof(Oid));
	for (i = 0; i < nindexes; i++)
		indexes_src[i] = cat_state->indexes[i].oid;

	ctx = setup_decoding(relid_src, tup_desc_src);

	/*
	 * The destination table should not be accessed by anyone during our
	 * processing. We're especially worried about DDLs because those might
	 * make us crash during data insertion. So lock the table in exclusive
	 * mode. Thus the checks for catalog changes we perform below stay valid
	 * until the processing is done.
	 *
	 * This cannot be done before the call of setup_decoding() as the
	 * exclusive lock does assign XID.
	 */
	relname_dst = PG_GETARG_TEXT_PP(1);
	relrv = makeRangeVarFromNameList(textToQualifiedNameList(relname_dst));
	rel_dst = table_openrv(relrv, AccessExclusiveLock);

	/* We're going to distribute data into partitions. */
	if (rel_dst->rd_rel->relkind != RELKIND_PARTITIONED_TABLE)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("\"%s\" is not a partitioned table",
						text_to_cstring(relname_dst))));

	/*
	 * If the destination table is temporary, user probably messed things up
	 * and a lot of data would be lost at the end of the session. Unlogged
	 * table might be o.k. but let's allow only permanent so far.
	 */
	if (!RelationIsPermanent(rel_dst))
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("\"%s\" is not a permanent table",
						text_to_cstring(relname_dst))));

#if PG_VERSION_NUM >= 140000
	part_desc = RelationGetPartitionDesc(rel_dst, true);
#else
	part_desc = RelationGetPartitionDesc(rel_dst);
#endif
	if (part_desc->nparts == 0)
		ereport(ERROR,
				(errmsg("table \"%s\" has no partitions",
						text_to_cstring(relname_dst))));

	/*
	 * It's probably not necessary to lock the partitions in exclusive mode,
	 * but we'll need to open them later. Simply use the exclusive lock
	 * instead of trying to determine the minimum lock level needed.
	 */
	parts_dst = (Relation *) palloc(part_desc->nparts * sizeof(Relation));
	for (i = 0; i < part_desc->nparts; i++)
		parts_dst[i] = table_open(part_desc->oids[i], AccessExclusiveLock);

	/*
	 * Build a "historic snapshot", i.e. one that reflect the table state at
	 * the moment the snapshot builder reached SNAPBUILD_CONSISTENT state.
	 */
	snap_hist = build_historic_snapshot(ctx->snapshot_builder);

	/* The source relation will be needed for the initial load. */
	rel_src = table_open(relid_src, AccessShareLock);

	/*
	 * Check if the source and destination table have compatible type. This is
	 * needed because ExecFindPartition() needs the (root) destination table
	 * tuple.
	 */
	check_tup_descs_match(tup_desc_src, RelationGetRelationName(rel_src),
						  RelationGetDescr(rel_dst),
						  RelationGetRelationName(rel_dst));
	conv_map = convert_tuples_by_position(tup_desc_src,
										  RelationGetDescr(rel_dst),

	/*
	 * XXX Better log message? User shouldn't see this anyway.
	 */
										  "cannot map tuples");

	/*
	 * Check if the source table has all the constraints that the destination
	 * has. Since we copy tuples from the source table w/o performing any
	 * checks, we can only accept constraints that the source table already
	 * enforced.
	 *
	 * Note: we do not check triggers since existence of a (non-constraint)
	 * trigger does not guarantee that it has been executed on all the
	 * existing rows of a table.
	 */
	compare_constraints(rel_src, rel_dst);

	/*
	 * Also check if the source table is referenced by any FK. Since the
	 * destination table is initially empty, user should not be able to create
	 * the corresponding FKs referencing it. A "workaround" to omit the FKs to
	 * the destination table should not be allowed by default.
	 */
	if (rewrite_check_constraints)
	{
		if (is_rel_referenced(rel_src))
			ereport(ERROR,
					(errmsg("table \"%s\" is referenced by a foreign key",
							RelationGetRelationName(rel_src))));
	}

	/*
	 * Store the identity of the source relation, in order to check that of
	 * the destination table partitions.
	 */
	ident_src_tupdesc = get_index_tuple_desc(ident_idx_src);

	/*
	 * Pointers to identity indexes will be looked up by the partition
	 * relation OID.
	 */
	partitions = partitions_create(CurrentMemoryContext, 8, NULL);

	/*
	 * Gather partition information that we'll need later. It happens here
	 * because it's a good opportunity to check the partition tuple
	 * descriptors and identity indexes before the initial load starts. (The
	 * load does not need those indexes, but it'd be unfortunate to find out
	 * incorrect or missing identity index after the initial load has been
	 * performed.)
	 */
	for (i = 0; i < part_desc->nparts; i++)
	{
		Oid			ident_idx_dst;
		Relation	partition = parts_dst[i];
		Relation	ident_idx_rel;
		TupleDesc	ident_dst_tupdesc;
		PartitionEntry *entry;
		bool		found;

		/* Info on partitions. */
		entry = partitions_insert(partitions, RelationGetRelid(partition),
								  &found);
		Assert(!found);

		/*
		 * Identity of the rows of a foreign table is hard to implement for
		 * foreign tables. We'd hit the problem below, but it's clearer to
		 * report the problem this way.
		 */
		if (partition->rd_rel->relkind == RELKIND_FOREIGN_TABLE)
			ereport(ERROR,
					(errmsg("\"%s\" is a foreign table",
							RelationGetRelationName(partition))));

		/*
		 * Check if the destination table and its partitions have compatible
		 * type. This should be o.k. for the user because he is expected to
		 * create the destination table from scratch (i.e. not attaching
		 * existing tables to the destination table). Otherwise we'd have to
		 * do additional tuple mapping.
		 */
		if (!equal_dest_descs(RelationGetDescr(rel_dst), RelationGetDescr(partition)))
			elog(ERROR,
				 "tuple descriptor of partition \"%s\" does not exactly match that of the \"%s\" table",
				 RelationGetRelationName(partition),
				 RelationGetRelationName(rel_dst));

		ident_idx_dst = RelationGetReplicaIndex(partition);
		if (!OidIsValid(ident_idx_dst))
			elog(ERROR, "Identity index missing on partition \"%s\"",
				 RelationGetRelationName(partition));
		ident_idx_rel = index_open(ident_idx_dst, AccessShareLock);

		ident_dst_tupdesc = CreateTupleDescCopy(RelationGetDescr(ident_idx_rel));
		if (!equalTupleDescs(ident_dst_tupdesc, ident_src_tupdesc))
			elog(ERROR,
				 "identity index on partition \"%s\" does not match that on the source table",
				 RelationGetRelationName(partition));
		FreeTupleDesc(ident_dst_tupdesc);

		entry->ident_index = ident_idx_rel;
		entry->ind_slot = table_slot_create(partition, NULL);
		/* Expect many insertions. */
		entry->bistate = GetBulkInsertState();

		/*
		 * Build scan key that we'll use to look for rows to be updated /
		 * deleted during logical decoding.
		 *
		 * As all the partitions have the same identity index, there should
		 * only be a single identity key.
		 */
		if (ident_key == NULL)
			ident_key = build_identity_key(ident_idx_rel,
										   &ident_key_nentries);
	}
	Assert(ident_key_nentries > 0);

	FreeTupleDesc(ident_src_tupdesc);

	/*
	 * We need to know whether that no DDL took place that allows for data
	 * inconsistency. The source relation was unlocked for some time since
	 * last check, so pass NoLock.
	 *
	 * The destination relation needs no check because we've locked it in
	 * exclusive mode at the beginning and will not release the lock until
	 * we're done.
	 */
	pg_rewrite_check_catalog_changes(cat_state, NoLock);

	/* Executor state to determine the target partition. */
	estate = CreateExecutorState();

	/*
	 * XXX Is it ok to leave the CMD_INSERT command for processing of the
	 * concurrent changes, which includes UPDATE and DELETE commands?
	 */
	mtstate = get_modify_table_state(estate, rel_dst, CMD_INSERT);
#if PG_VERSION_NUM >= 140000
	proute = ExecSetupPartitionTupleRouting(estate, rel_dst);
#else
	proute = ExecSetupPartitionTupleRouting(estate, mtstate, rel_dst);
#endif

	/*
	 * The historic snapshot is used to retrieve data w/o concurrent changes.
	 */
	perform_initial_load(estate, mtstate, proute, rel_src, snap_hist, rel_dst,
						 partitions, ctx, conv_map);

	/*
	 * We no longer need to preserve the rows processed during the initial
	 * load from VACUUM. (User is not likely to run VACUUM on a table that we
	 * currently process, but our stale effective_xmin would also restrict
	 * VACUUM on other tables.)
	 */
	slot = ctx->slot;
	SpinLockAcquire(&slot->mutex);
	Assert(TransactionIdIsValid(slot->effective_xmin) &&
		   !TransactionIdIsValid(slot->data.xmin));
	slot->effective_xmin = InvalidTransactionId;
	SpinLockRelease(&slot->mutex);

	/*
	 * The historic snapshot won't be needed anymore.
	 */
	pfree(snap_hist);

	/*
	 * This is rather paranoia than anything else --- perform_initial_load()
	 * uses each snapshot to access different table, and it does not cause
	 * catalog changes.
	 */
	InvalidateSystemCaches();

	/*
	 * Make sure the contents of the destination partitions is visible, for
	 * the sake of concurrent data changes.
	 */
	CommandCounterIncrement();

	/*
	 * As we'll need to take exclusive lock later, release the shared one.
	 *
	 * Note: PG core code shouldn't actually participate in such a deadlock,
	 * as it (supposedly) does not raise lock level. Nor should concurrent
	 * call of the partition_table() function participate in the deadlock,
	 * because it should have failed much earlier when creating an existing
	 * logical replication slot again. Nevertheless, these circumstances still
	 * don't justify generally bad practice.
	 *
	 * (As we haven't changed the catalog entry yet, there's no need to send
	 * invalidation messages.)
	 */
	table_close(rel_src, AccessShareLock);

	/*
	 * Flush all WAL records inserted so far (possibly except for the last
	 * incomplete page, see GetInsertRecPtr), to minimize the amount of data
	 * we need to flush while holding exclusive lock on the source table.
	 */
	xlog_insert_ptr = GetInsertRecPtr();
	XLogFlush(xlog_insert_ptr);

	/*
	 * During regression tests, wait until the other transactions performed
	 * their data changes so that we can process them.
	 *
	 * Since this should only be used for tests, don't bother using
	 * WaitLatch().
	 */
	if (rewrite_wait_after_load > 0)
	{
		LOCKTAG		tag;
		Oid			extension_id;
		LockAcquireResult lock_res PG_USED_FOR_ASSERTS_ONLY;

		extension_id = get_extension_oid("pg_rewrite", false);
		SET_LOCKTAG_OBJECT(tag, MyDatabaseId, ExtensionRelationId,
						   extension_id, 0);
		lock_res = LockAcquire(&tag, ExclusiveLock, false, false);
		Assert(lock_res == LOCKACQUIRE_OK);

		/*
		 * Misuse lock on our extension to let the concurrent backend(s) check
		 * that we're exactly here.
		 */
		pg_usleep(rewrite_wait_after_load * 1000000L);
		LockRelease(&tag, ExclusiveLock, false);
	}

	/*
	 * Since we'll do some more changes, all the WAL records flushed so far
	 * need to be decoded for sure.
	 */
#if PG_VERSION_NUM >= 150000
	end_of_wal = GetFlushRecPtr(NULL);
#else
	end_of_wal = GetFlushRecPtr();
#endif

	/*
	 * Decode and apply the data changes that occurred while the initial load
	 * was in progress. The XLOG reader should continue where setup_decoding()
	 * has left it.
	 *
	 * Even if the amount of concurrent changes of our source table might not
	 * be significant, both initial load and index build could have produced
	 * many XLOG records that we need to read. Do so before requesting
	 * exclusive lock on the source relation.
	 */
	pg_rewrite_process_concurrent_changes(estate,
										  mtstate,
										  proute,
										  ctx,
										  end_of_wal,
										  cat_state,
										  rel_dst,
										  ident_key,
										  ident_key_nentries,
										  NoLock,
										  partitions,
										  conv_map,
										  NULL);

	/*
	 * This (supposedly cheap) special check should avoid one particular
	 * deadlock scenario: another transaction, performing index DDL
	 * concurrenly (e.g. DROP INDEX CONCURRENTLY) committed change of
	 * indisvalid, indisready, ... and called WaitForLockers() before we
	 * unlocked both source table and its indexes above. WaitForLockers()
	 * waits till the end of the holding (our) transaction as opposed to the
	 * end of our locks, and the other transaction holds (non-exclusive) lock
	 * on both relation and index. In this situation we'd cause deadlock by
	 * requesting exclusive lock. We should recognize this scenario by
	 * checking pg_index alone.
	 */
	ind_info = get_index_info(relid_src, NULL, &invalid_index, true, NULL);
	if (invalid_index)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_IN_USE),
				 errmsg("Concurrent change of index detected")));
	else
		pfree(ind_info);

	/*
	 * Try a few times to perform the stage that requires exclusive lock on
	 * the source relation.
	 *
	 * XXX Not sure the number of attempts should be configurable. If it fails
	 * several times, admin should either increase partition_max_xlock_time or
	 * disable it.
	 */
	source_finalized = false;
	for (i = 0; i < 4; i++)
	{
		if (perform_final_merge(estate, mtstate, proute,
								relid_src, indexes_src, nindexes,
								rel_dst, ident_key, ident_key_nentries,
								cat_state, ctx, partitions, conv_map))
		{
			source_finalized = true;
			break;
		}
		else
			elog(DEBUG1,
				 "pg_rewrite exclusive lock on table %u had to be released.",
				 relid_src);
	}
	if (!source_finalized)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_IN_USE),
				 errmsg("pg_rewrite: \"max_xlock_time\" prevented partitioning from completion")));

	/*
	 * Done with decoding.
	 *
	 * XXX decoding_cleanup() frees tup_desc_src, although we've used it not
	 * only for the decoding.
	 */
	decoding_cleanup(ctx);
	ReplicationSlotRelease();

	pfree(ident_key);

	/* Rename the tables. */
	RenameRelationInternal(relid_src, relname_src_new, false, false);

	/*
	 * The new relation will be renamed to the old one, so make sure renaming
	 * of the old one is visible, otherwise there's a conflict.
	 */
	CommandCounterIncrement();

	/*
	 * While the source should have been left locked (in the
	 * AccessExclusiveLock mode) by perform_final_merge(), we keep the same
	 * lock for rel_dst since we opened it first time.
	 */
	RenameRelationInternal(RelationGetRelid(rel_dst), relname_src, false,
						   false);

	/*
	 * Everything done for destination table, so close it.
	 *
	 * Note that RenameRelationInternal() locked it in the exclusive mode too
	 * and didn't unlock. We can perform one more unlocking explicitly, but
	 * it'll happen on transaction commit anyway.
	 *
	 * As for the source relation, the lock acquired by perform_final_merge()
	 * will be released at the end of transaction - no need to deal with it
	 * here.
	 */
	table_close(rel_dst, AccessExclusiveLock);
	for (i = 0; i < part_desc->nparts; i++)
		table_close(parts_dst[i], AccessExclusiveLock);

	pfree(parts_dst);
	close_partitions(partitions);

	if (nindexes > 0)
		pfree(indexes_src);

	/* State not needed anymore. */
	free_catalog_state(cat_state);

	ExecCleanupTupleRouting(mtstate, proute);
	pfree(proute);
	free_modify_table_state(mtstate);
	FreeExecutorState(estate);
	if (conv_map)
		free_conversion_map(conv_map);
}

static int
index_cat_info_compare(const void *arg1, const void *arg2)
{
	IndexCatInfo *i1 = (IndexCatInfo *) arg1;
	IndexCatInfo *i2 = (IndexCatInfo *) arg2;

	if (i1->oid > i2->oid)
		return 1;
	else if (i1->oid < i2->oid)
		return -1;
	else
		return 0;
}

/*
 * Raise error if the relation is not eligible for partitioning or any adverse
 * conditions exist.
 *
 * Some of the checks may be redundant (e.g. heap_open() checks relkind) but
 * its safer to have them all listed here.
 */
static void
check_prerequisites(Relation rel)
{
	Form_pg_class form = RelationGetForm(rel);

	/* Check the relation first. */
	if (form->relkind == RELKIND_PARTITIONED_TABLE)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("cannot process partitioned table")));

	if (form->relkind != RELKIND_RELATION)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("\"%s\" is not a table",
						RelationGetRelationName(rel))));

	if (!RelationIsPermanent(rel))
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("\"%s\" is not a permanent table",
						RelationGetRelationName(rel))));

	if (form->relisshared)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("\"%s\" is shared relation",
						RelationGetRelationName(rel))));

	if (RelationIsMapped(rel))
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("\"%s\" is mapped relation",
						RelationGetRelationName(rel))));

	/*
	 * There's no urgent need to process catalog tables.
	 *
	 * Should this limitation be relaxed someday, consider if we need to write
	 * xl_heap_rewrite_mapping records. (Probably not because the whole
	 * "decoding session" takes place within a call of partition_table() and
	 * our catalog checks should not allow for a concurrent rewrite that could
	 * make snapmgr.c:tuplecid_data obsolete. Furthermore, such a rewrite
	 * would have to take place before perform_initial_load(), but this is
	 * called before any transactions could have been decoded, so tuplecid
	 * should still be empty anyway.)
	 */
	if (RelationGetRelid(rel) < FirstNormalObjectId)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("\"%s\" is not user relation",
						RelationGetRelationName(rel))));

	/*
	 * While AFTER trigger should not be an issue (to generate an event must
	 * have got XID assigned, causing setup_decoding() to fail later), open
	 * cursor might be. See comments of the function for details.
	 */
	CheckTableNotInUse(rel, "partition_table()");
}

/*
 * This function is much like pg_create_logical_replication_slot() except that
 * the new slot is neither released (if anyone else could read changes from
 * our slot, we could miss changes other backends do while we copy the
 * existing data into temporary table), nor persisted (it's easier to handle
 * crash by restarting all the work from scratch).
 *
 * XXX Even though CreateInitDecodingContext() does not set state to
 * RS_PERSISTENT, it does write the slot to disk. We rely on
 * RestoreSlotFromDisk() to delete ephemeral slots during startup. (Both ERROR
 * and FATAL should lead to cleanup even before the cluster goes down.)
 */
static LogicalDecodingContext *
setup_decoding(Oid relid, TupleDesc tup_desc)
{
	StringInfo	buf;
	LogicalDecodingContext *ctx;
	DecodingOutputState *dstate;
	MemoryContext oldcontext;

	/* check_permissions() "inlined", as logicalfuncs.c does not export it. */
	if (!superuser() && !has_rolreplication(GetUserId()))
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 (errmsg("must be superuser or replication role to use replication slots"))));

	CheckLogicalDecodingRequirements();

	/* Make sure there's no conflict with the SPI and its contexts. */
	oldcontext = MemoryContextSwitchTo(TopTransactionContext);

	/*
	 * In order to be able to run partition_table() in each database
	 * independently, thus the slot name should be database-specific.
	 */
	buf = makeStringInfo();
	appendStringInfoString(buf, REPL_SLOT_BASE_NAME);
	appendStringInfo(buf, "%u", MyDatabaseId);
#if PG_VERSION_NUM >= 140000
	ReplicationSlotCreate(buf->data, true, RS_EPHEMERAL, false);
#else
	ReplicationSlotCreate(buf->data, true, RS_EPHEMERAL);
#endif

	/*
	 * Neither prepare_write nor do_write callback nor update_progress is
	 * useful for us.
	 *
	 * Regarding the value of need_full_snapshot, we pass true to protect its
	 * data from VACUUM. Otherwise the historical snapshot we use for the
	 * initial load could miss some data. (Unlike logical decoding, we need
	 * the historical snapshot for non-catalog tables.)
	 */
	ctx = CreateInitDecodingContext(REPL_PLUGIN_NAME,
									NIL,
									true,
									InvalidXLogRecPtr,
									XL_ROUTINE(.page_read = read_local_xlog_page,
											   .segment_open = wal_segment_open,
											   .segment_close = wal_segment_close),
									NULL, NULL, NULL);

	/*
	 * We don't have control on setting fast_forward, so at least check it.
	 */
	Assert(!ctx->fast_forward);

	DecodingContextFindStartpoint(ctx);

	/* Some WAL records should have been read. */
	Assert(ctx->reader->EndRecPtr != InvalidXLogRecPtr);

	XLByteToSeg(ctx->reader->EndRecPtr, part_current_segment,
				wal_segment_size);

	/*
	 * Setup structures to store decoded changes.
	 */
	dstate = palloc0(sizeof(DecodingOutputState));
	dstate->relid = relid;
	dstate->tstore = tuplestore_begin_heap(false, false,
										   maintenance_work_mem);
	dstate->tupdesc = tup_desc;

	/* Initialize the descriptor to store the changes ... */
	dstate->tupdesc_change = CreateTemplateTupleDesc(1);

	TupleDescInitEntry(dstate->tupdesc_change, 1, NULL, BYTEAOID, -1, 0);
	/* ... as well as the corresponding slot. */
	dstate->tsslot = MakeSingleTupleTableSlot(dstate->tupdesc_change,
											  &TTSOpsMinimalTuple);

	dstate->resowner = ResourceOwnerCreate(CurrentResourceOwner,
										   "logical decoding");

	MemoryContextSwitchTo(oldcontext);

	ctx->output_writer_private = dstate;
	return ctx;
}

static void
decoding_cleanup(LogicalDecodingContext *ctx)
{
	DecodingOutputState *dstate;

	dstate = (DecodingOutputState *) ctx->output_writer_private;

	ExecDropSingleTupleTableSlot(dstate->tsslot);
	FreeTupleDesc(dstate->tupdesc_change);
	FreeTupleDesc(dstate->tupdesc);
	tuplestore_end(dstate->tstore);

	FreeDecodingContext(ctx);
}

/*
 * Retrieve the catalog state to be passed later to
 * pg_rewrite_check_catalog_changes.
 *
 * Caller is supposed to hold (at least) AccessShareLock on the relation.
 */
static CatalogState *
get_catalog_state(Oid relid)
{
	CatalogState *result;

	result = (CatalogState *) palloc0(sizeof(CatalogState));
	result->rel.relid = relid;

	/*
	 * pg_class(xmin) helps to ensure that the "user_catalog_option" wasn't
	 * turned off and on. On the other hand it might restrict some concurrent
	 * DDLs that would be safe as such.
	 */
	get_pg_class_info(relid, &result->rel.xmin, &result->form_class,
					  &result->desc_class);

	result->rel.relnatts = result->form_class->relnatts;

	/*
	 * We might want to avoid the check if relhasindex is false, but
	 * index_update_stats() updates this field in-place. (Currently it should
	 * not change from "true" to "false", but let's be cautious anyway.)
	 */
	result->indexes = get_index_info(relid, &result->relninds,
									 &result->invalid_index, false,
									 &result->have_pk_index);

	/* If any index is "invalid", no more catalog information is needed. */
	if (result->invalid_index)
		return result;

	if (result->form_class->relnatts > 0)
		get_attribute_info(relid, result->form_class->relnatts,
						   &result->rel.attr_xmins, result);

	return result;
}

/*
 * For given relid retrieve pg_class(xmin). Also set *form and *desc if valid
 * pointers are passed.
 */
static void
get_pg_class_info(Oid relid, TransactionId *xmin, Form_pg_class *form_p,
				  TupleDesc *desc_p)
{
	HeapTuple	tuple;
	Form_pg_class form_class;
	Relation	rel;
	SysScanDesc scan;
	ScanKeyData key[1];

	/*
	 * ScanPgRelation() would do most of the work below, but relcache.c does
	 * not export it.
	 */
	rel = table_open(RelationRelationId, AccessShareLock);

	ScanKeyInit(&key[0],
				Anum_pg_class_oid,
				BTEqualStrategyNumber,
				F_OIDEQ,
				ObjectIdGetDatum(relid));
	scan = systable_beginscan(rel, ClassOidIndexId, true, NULL, 1, key);
	tuple = systable_getnext(scan);

	/*
	 * As the relation might not be locked by some callers, it could have
	 * disappeared.
	 */
	if (!HeapTupleIsValid(tuple))
	{
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_TABLE),
				 (errmsg("Table no longer exists"))));
	}

	/* Invalid relfilenode indicates mapped relation. */
	form_class = (Form_pg_class) GETSTRUCT(tuple);
	if (form_class->relfilenode == InvalidOid)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 (errmsg("Mapped relation cannot be partitioned"))));

	*xmin = HeapTupleHeaderGetXmin(tuple->t_data);

	if (form_p)
	{
		*form_p = (Form_pg_class) palloc(CLASS_TUPLE_SIZE);
		memcpy(*form_p, form_class, CLASS_TUPLE_SIZE);
	}

	if (desc_p)
		*desc_p = CreateTupleDescCopy(RelationGetDescr(rel));

	systable_endscan(scan);
	table_close(rel, AccessShareLock);
}

/*
 * Retrieve array of pg_attribute(xmin) values for given relation, ordered by
 * attnum. (The ordering is not essential but lets us do some extra sanity
 * checks.)
 *
 * If cat_state is passed and the attribute is of a composite type, make sure
 * it's cached in ->comptypes.
 */
static void
get_attribute_info(Oid relid, int relnatts, TransactionId **xmins_p,
				   CatalogState *cat_state)
{
	Relation	rel;
	ScanKeyData key[2];
	SysScanDesc scan;
	HeapTuple	tuple;
	TransactionId *result;
	int			n = 0;

	rel = table_open(AttributeRelationId, AccessShareLock);

	ScanKeyInit(&key[0], Anum_pg_attribute_attrelid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(relid));
	/* System columns should not be ALTERed. */
	ScanKeyInit(&key[1],
				Anum_pg_attribute_attnum,
				BTGreaterStrategyNumber, F_INT2GT,
				Int16GetDatum(0));
	scan = systable_beginscan(rel, AttributeRelidNumIndexId, true, NULL,
							  2, key);
	result = (TransactionId *) palloc(relnatts * sizeof(TransactionId));
	while ((tuple = systable_getnext(scan)) != NULL)
	{
		Form_pg_attribute form;
		int			i;

		Assert(HeapTupleIsValid(tuple));
		form = (Form_pg_attribute) GETSTRUCT(tuple);
		Assert(form->attnum > 0);

		/* AttributeRelidNumIndexId index ensures ordering. */
		i = form->attnum - 1;
		Assert(i == n);

		/*
		 * Caller should hold at least AccesShareLock on the owning relation,
		 * supposedly no need for repalloc(). (elog() rather than Assert() as
		 * it's not difficult to break this assumption during future coding.)
		 */
		if (n++ > relnatts)
			elog(ERROR, "Relation %u has too many attributes", relid);

		result[i] = HeapTupleHeaderGetXmin(tuple->t_data);

		/*
		 * Gather composite type info if needed.
		 */
		if (cat_state != NULL &&
			get_typtype(form->atttypid) == TYPTYPE_COMPOSITE)
			cache_composite_type_info(cat_state, form->atttypid);
	}
	Assert(relnatts == n);
	systable_endscan(scan);
	table_close(rel, AccessShareLock);
	*xmins_p = result;
}


/*
 * Make sure that information on a type that caller has recognized as
 * composite type is cached in cat_state.
 */
static void
cache_composite_type_info(CatalogState *cat_state, Oid typid)
{
	int			i;
	bool		found = false;
	TypeCatInfo *tinfo;

	/* Check if we already have this type. */
	for (i = 0; i < cat_state->ncomptypes; i++)
	{
		tinfo = &cat_state->comptypes[i];

		if (tinfo->oid == typid)
		{
			found = true;
			break;
		}
	}

	if (found)
		return;

	/* Extend the comptypes array if necessary. */
	if (cat_state->ncomptypes == cat_state->ncomptypes_max)
	{
		if (cat_state->ncomptypes_max == 0)
		{
			Assert(cat_state->comptypes == NULL);
			cat_state->ncomptypes_max = 2;
			cat_state->comptypes = (TypeCatInfo *)
				palloc(cat_state->ncomptypes_max * sizeof(TypeCatInfo));
		}
		else
		{
			cat_state->ncomptypes_max *= 2;
			cat_state->comptypes = (TypeCatInfo *)
				repalloc(cat_state->comptypes,
						 cat_state->ncomptypes_max * sizeof(TypeCatInfo));
		}
	}

	tinfo = &cat_state->comptypes[cat_state->ncomptypes];
	tinfo->oid = typid;
	get_composite_type_info(tinfo);

	cat_state->ncomptypes++;
}

/*
 * Retrieve information on a type that caller has recognized as composite
 * type. tinfo->oid must be initialized.
 */
static void
get_composite_type_info(TypeCatInfo *tinfo)
{
	Relation	rel;
	ScanKeyData key[1];
	SysScanDesc scan;
	HeapTuple	tuple;
	Form_pg_type form_type;
	Form_pg_class form_class;

	Assert(tinfo->oid != InvalidOid);

	/* Find the pg_type tuple. */
	rel = table_open(TypeRelationId, AccessShareLock);

	ScanKeyInit(&key[0],
				Anum_pg_type_oid,
				BTEqualStrategyNumber,
				F_OIDEQ,
				ObjectIdGetDatum(tinfo->oid));
	scan = systable_beginscan(rel, TypeOidIndexId, true, NULL, 1, key);
	tuple = systable_getnext(scan);
	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "composite type %u not found", tinfo->oid);

	form_type = (Form_pg_type) GETSTRUCT(tuple);
	Assert(form_type->typtype == TYPTYPE_COMPOSITE);

	/* Initialize the structure. */
	tinfo->xmin = HeapTupleHeaderGetXmin(tuple->t_data);

	/*
	 * Retrieve the pg_class tuple that represents the composite type, as well
	 * as the corresponding pg_attribute tuples.
	 */
	tinfo->rel.relid = form_type->typrelid;
	get_pg_class_info(form_type->typrelid, &tinfo->rel.xmin, &form_class,
					  NULL);
	if (form_class->relnatts > 0)
		get_attribute_info(form_type->typrelid, form_class->relnatts,
						   &tinfo->rel.attr_xmins, NULL);
	else
		tinfo->rel.attr_xmins = NULL;
	tinfo->rel.relnatts = form_class->relnatts;

	pfree(form_class);
	systable_endscan(scan);
	table_close(rel, AccessShareLock);
}

/*
 * Retrieve pg_class(oid) and pg_class(xmin) for each index of given
 * relation.
 *
 * If at least one index appears to be problematic in terms of concurrency,
 * *found_invalid receives true and retrieval of index information ends
 * immediately.
 *
 * If invalid_check_only is true, return after having verified that all
 * indexes are valid.
 *
 * Note that some index DDLs can commit while this function is called from
 * get_catalog_state(). If we manage to see these changes, our result includes
 * them and they'll affect the destination table. If any such change gets
 * committed later and we miss it, it'll be identified as disruptive by
 * pg_rewrite_check_catalog_changes(). After all, there should be no dangerous
 * race conditions.
 */
static IndexCatInfo *
get_index_info(Oid relid, int *relninds, bool *found_invalid,
			   bool invalid_check_only, bool *found_pk)
{
	Relation	rel,
				rel_idx;
	ScanKeyData key[1];
	SysScanDesc scan;
	HeapTuple	tuple;
	IndexCatInfo *result;
	int			i,
				n = 0;
	int			relninds_max = 4;
	Datum	   *oids_d;
	int16		oidlen;
	bool		oidbyval;
	char		oidalign;
	ArrayType  *oids_a;
	bool		mismatch;

	*found_invalid = false;
	if (found_pk)
		*found_pk = false;

	/*
	 * Open both pg_class and pg_index catalogs at once, so that we have a
	 * consistent view in terms of invalidation. Otherwise we might get
	 * different snapshot for each. Thus, in-progress index changes that do
	 * not conflict with AccessShareLock on the parent table could trigger
	 * false alarms later in pg_rewrite_check_catalog_changes().
	 */
	rel = table_open(RelationRelationId, AccessShareLock);
	rel_idx = table_open(IndexRelationId, AccessShareLock);

	ScanKeyInit(&key[0], Anum_pg_index_indrelid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(relid));
	scan = systable_beginscan(rel_idx, IndexIndrelidIndexId, true, NULL, 1,
							  key);

	result = (IndexCatInfo *) palloc(relninds_max * sizeof(IndexCatInfo));
	while ((tuple = systable_getnext(scan)) != NULL)
	{
		Form_pg_index form;
		IndexCatInfo *res_entry;

		form = (Form_pg_index) GETSTRUCT(tuple);

		/*
		 * First, perform the simple checks that can make the next work
		 * unnecessary.
		 */
		if (!form->indisvalid || !form->indisready || !form->indislive)
		{
			*found_invalid = true;
			break;
		}

		res_entry = (IndexCatInfo *) &result[n++];
		res_entry->oid = form->indexrelid;
		res_entry->xmin = HeapTupleHeaderGetXmin(tuple->t_data);
		if (found_pk && form->indisprimary)
			*found_pk = true;

		/*
		 * Unlike get_attribute_info(), we can't receive the expected number
		 * of entries from caller.
		 */
		if (n == relninds_max)
		{
			relninds_max *= 2;
			result = (IndexCatInfo *)
				repalloc(result, relninds_max * sizeof(IndexCatInfo));
		}
	}
	systable_endscan(scan);
	table_close(rel_idx, AccessShareLock);

	/* Return if invalid index was found or ... */
	if (*found_invalid)
	{
		table_close(rel, AccessShareLock);
		return result;
	}
	/* ... caller is not interested in anything else.  */
	if (invalid_check_only)
	{
		table_close(rel, AccessShareLock);
		return result;
	}

	/*
	 * Enforce sorting by OID, so that the entries match the result of the
	 * following scan using OID index.
	 */
	qsort(result, n, sizeof(IndexCatInfo), index_cat_info_compare);

	if (relninds)
		*relninds = n;
	if (n == 0)
	{
		table_close(rel, AccessShareLock);
		return result;
	}

	/*
	 * Now retrieve the corresponding pg_class(xmax) values.
	 *
	 * Here it seems reasonable to construct an array of OIDs of the pg_class
	 * entries of the indexes and use amsearcharray function of the index.
	 */
	oids_d = (Datum *) palloc(n * sizeof(Datum));
	for (i = 0; i < n; i++)
		oids_d[i] = ObjectIdGetDatum(result[i].oid);
	get_typlenbyvalalign(OIDOID, &oidlen, &oidbyval, &oidalign);
	oids_a = construct_array(oids_d, n, OIDOID, oidlen, oidbyval, oidalign);
	pfree(oids_d);

	ScanKeyInit(&key[0],
				Anum_pg_class_oid,
				BTEqualStrategyNumber,
				F_OIDEQ,
				PointerGetDatum(oids_a));
	key[0].sk_flags |= SK_SEARCHARRAY;
	scan = systable_beginscan(rel, ClassOidIndexId, true, NULL, 1, key);
	i = 0;
	mismatch = false;
	while ((tuple = systable_getnext(scan)) != NULL)
	{
		IndexCatInfo *res_item;
		Form_pg_class form_class;
		char	   *namestr;

		if (i == n)
		{
			/* Index added concurrently? */
			mismatch = true;
			break;
		}
		res_item = &result[i++];
		res_item->pg_class_xmin = HeapTupleHeaderGetXmin(tuple->t_data);
		form_class = (Form_pg_class) GETSTRUCT(tuple);
		namestr = NameStr(form_class->relname);
		Assert(strlen(namestr) < NAMEDATALEN);
		strcpy(NameStr(res_item->relname), namestr);

		res_item->reltablespace = form_class->reltablespace;
	}
	if (i < n)
		mismatch = true;

	if (mismatch)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_IN_USE),
				 errmsg("Concurrent change of index detected")));

	systable_endscan(scan);
	table_close(rel, AccessShareLock);
	pfree(oids_a);

	return result;
}

/*
 * Check if the tuple descriptors are at least as similar that tupconvert.c
 * can handle conversion from tupdesc_src to tupdesc_dst.
 *
 * Derived from equalTupleDescs(), therefore check that function in PG core
 * when adapting the code to the next PG version.
 */
static void
check_tup_descs_match(TupleDesc tupdesc_src, char *tabname_src,
					  TupleDesc tupdesc_dst, char *tabname_dst)
{
	int			n_src = tupdesc_src->natts;
	int			n_dst = tupdesc_dst->natts;
	AttrNumber *valid_src,
			   *valid_dst;
	AttrNumber	nvalid_src,
				nvalid_dst;
	int			i;

	/* Gather non-dropped columns. */
	valid_src = (AttrNumber *) palloc(n_src * sizeof(AttrNumber));
	nvalid_src = 0;
	for (i = 0; i < n_src; i++)
	{
		Form_pg_attribute attr = TupleDescAttr(tupdesc_src, i);

		if (!attr->attisdropped)
			valid_src[nvalid_src++] = i;
	}

	valid_dst = (AttrNumber *) palloc(n_dst * sizeof(AttrNumber));
	nvalid_dst = 0;
	for (i = 0; i < n_dst; i++)
	{
		Form_pg_attribute attr = TupleDescAttr(tupdesc_dst, i);

		if (!attr->attisdropped)
			valid_dst[nvalid_dst++] = i;
	}

	if (nvalid_src != nvalid_dst)
		ereport(ERROR,
				(errcode(ERRCODE_DATATYPE_MISMATCH),
				 (errmsg("relation \"%s\" has different number of columns than relation \"%s\"",
						 tabname_src, tabname_dst))));

	for (i = 0; i < nvalid_src; i++)
	{
		Form_pg_attribute attr1 = TupleDescAttr(tupdesc_src, valid_src[i]);
		Form_pg_attribute attr2 = TupleDescAttr(tupdesc_dst, valid_dst[i]);

		/*
		 * XXX Is it worth the effort to allow the tupdesc_dst to have
		 * different ordering of columns from tupdesc_src? The user might want
		 * to take the opportunity and reduce padding. If allowing different
		 * order, adjust compare_constraints().
		 */
		if (strcmp(NameStr(attr1->attname), NameStr(attr2->attname)) != 0)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_COLUMN_REFERENCE),
					 errmsg("no match found for the \"%s\" column of the \"%s\" table",
							NameStr(attr1->attname), tabname_src),
					 errhint("the source and destination table should have the same column ordering")));
		if (attr1->atttypid != attr2->atttypid ||
			attr1->attlen != attr2->attlen ||
			attr1->attndims != attr2->attndims ||
			attr1->atttypmod != attr2->atttypmod ||
			attr1->attbyval != attr2->attbyval ||
			attr1->attalign != attr2->attalign)
			ereport(ERROR,
					(errcode(ERRCODE_DATATYPE_MISMATCH),
					 errmsg("data type of the \"%s\" column of the \"%s\" table does not match the data type of the \"%s\" column of the \"%s\" table",
							NameStr(attr1->attname),
							tabname_src,
							NameStr(attr2->attname),
							tabname_dst)));

		if (attr1->attstorage != attr2->attstorage)
			ereport(ERROR,
					(errcode(ERRCODE_DATATYPE_MISMATCH),
					 errmsg("storage of the \"%s\" column of the \"%s\" table does not match the storage of the \"%s\" column of the \"%s\" table",
							NameStr(attr1->attname),
							tabname_src,
							NameStr(attr2->attname),
							tabname_dst)));
#if PG_VERSION_NUM >= 140000
		if (attr1->attcompression != attr2->attcompression)
			ereport(ERROR,
					(errcode(ERRCODE_DATATYPE_MISMATCH),
					 errmsg("compression of the \"%s\" column of the \"%s\" table does not match the compression of the \"%s\" column of the \"%s\" table",
							NameStr(attr1->attname),
							tabname_src,
							NameStr(attr2->attname),
							tabname_dst)));
#endif

		/*
		 * XXX Allow the case where the source table does have NOT NULL while
		 * destination does not?
		 */
		if (attr1->attnotnull != attr2->attnotnull)
			report_tupdesc_mismatch("NOT NULL constraint",
									NameStr(attr1->attname),
									tabname_src,
									attr1->attnotnull,
									tabname_dst,
									attr2->attnotnull);

		/*
		 * XXX Allow the case where the source table does not have the default
		 * value destination does?
		 */
		if (attr1->atthasdef != attr2->atthasdef)
			report_tupdesc_mismatch("DEFAULT value",
									NameStr(attr1->attname),
									tabname_src,
									attr1->atthasdef,
									tabname_dst,
									attr2->atthasdef);

		/*
		 * Change of the way value is generated between the source and the
		 * destination table should not make an *existing* source tuple
		 * incompatible with the destination table, however if attidentity or
		 * attgenerated is different, user might end up being unable to insert
		 * new rows into the destination table (because the new expression can
		 * generate values that e.g. break CHECK constraint). That should be
		 * easy for the user to fix, but it's also easy for us to check.
		 */
		if (attr1->attidentity != attr2->attidentity ||
			attr1->attgenerated != attr2->attgenerated)
			ereport(ERROR,
					(errmsg("value of the \"%s\" column of the \"%s\" table is generated in a different way than the value for the \"%s\" table",
							NameStr(attr1->attname), tabname_src,
							tabname_dst)));

		/*
		 * Collation needs to match so that the identity indexes on both
		 * source and destination table can have the same definition.
		 */
		if (attr1->attcollation != attr2->attcollation)
			ereport(ERROR,
					(errcode(ERRCODE_COLLATION_MISMATCH),
					 errmsg("collation of the \"%s\" column of the \"%s\" table does not match the collation of the \"%s\" column of the \"%s\" table",
							NameStr(attr1->attname),
							tabname_src,
							NameStr(attr2->attname),
							tabname_dst)));
	}

	if (!rewrite_check_constraints)
		return;

	if (tupdesc_src->constr != NULL)
	{
		TupleConstr *constr_src = tupdesc_src->constr;
		TupleConstr *constr_dst = tupdesc_dst->constr;
		int			n;

		if (constr_dst == NULL)
			report_tupdesc_mismatch("constraints",
									NULL,
									tabname_src,
									true,
									tabname_dst,
									false);

		/*
		 * Given the check of attnotnull above, Assert() would make sense here
		 * but there's none in the equalTupleDescs() function in PG core
		 */
		if (constr_src->has_not_null != constr_dst->has_not_null)
			report_tupdesc_mismatch("NOT NULL constraint",
									NULL,
									tabname_src,
									constr_src->has_not_null,
									tabname_dst,
									constr_dst->has_not_null);
		/* Likewise, Assert() would make sense here. */
		if (constr_src->has_generated_stored != constr_dst->has_generated_stored)
			report_tupdesc_mismatch("generated stored column",
									NULL,
									tabname_src,
									constr_src->has_generated_stored,
									tabname_dst,
									constr_dst->has_generated_stored);

		/*
		 * XXX It should be o.k. to allow the destination table to have extra
		 * CHECK expressions.
		 */
		n = constr_src->num_defval;
		if (n != (int) constr_dst->num_defval)
			ereport(ERROR,
					(errmsg("table \"%s\" has different number of DEFAULT expressions than table \"%s\"",
							tabname_src, tabname_dst)));

		n = constr_src->num_check;
		if (n != (int) constr_dst->num_check)
			ereport(ERROR,
					(errmsg("table \"%s\" has a different set of CHECK constraints than table \"%s\"",
							tabname_src, tabname_dst)));

		/*
		 * Similarly, we rely here on the ConstrCheck entries being sorted by
		 * name. If there are duplicate names, the outcome of the comparison
		 * is uncertain, but that should not happen.
		 */
		for (i = 0; i < n; i++)
		{
			ConstrCheck *check1 = constr_src->check + i;
			ConstrCheck *check2 = constr_dst->check + i;

			if (!(equal_op_expressions(check1->ccbin, check2->ccbin) &&
				  check1->ccvalid == check2->ccvalid &&
				  check1->ccnoinherit == check2->ccnoinherit))
				ereport(ERROR,
						(errmsg("table \"%s\" has a different set of CHECK constraints than table \"%s\"",
								tabname_src, tabname_dst)));
		}
	}
	else if (tupdesc_dst->constr != NULL)
		report_tupdesc_mismatch("constraints",
								NULL,
								tabname_src,
								false,
								tabname_dst,
								true);

}

/* Compare expressions for which string comparison would not work.*/
static bool
equal_op_expressions(char *expr_str1, char *expr_str2)
{
	OpExpr	   *expr1 = (OpExpr *) stringToNode(expr_str1);
	OpExpr	   *expr2 = (OpExpr *) stringToNode(expr_str1);

	return equal(expr1, expr2);
}

/* Returns true iff the constraints should be considered equal. */
typedef bool (*constraint_comparator) (ConstraintInfo *c1, ConstraintInfo *c2,
									   char kind);

static void
compare_constraints_internal(Relation rel1, List *constrs1, int n1,
							 Relation rel2, List *constrs2, int n2,
							 char kind, constraint_comparator callback)
{
	ConstraintInfo **constrs1_a = NULL;
	ConstraintInfo **constrs2_a = NULL;
	bool	   *matched = NULL;
	int			i;
	char	   *kind_str = NULL;

	constrs1_a = get_relation_constraints_array(constrs1, kind, n1);
	if (n2 > 0)
	{
		constrs2_a = get_relation_constraints_array(constrs2, kind, n2);
		matched = (bool *) palloc0(n2 * sizeof(bool));
	}

	switch (kind)
	{
		case CONSTRAINT_CHECK:
			kind_str = "CHECK";
			break;

		case CONSTRAINT_UNIQUE:
			kind_str = "UNIQUE";
			break;

		case CONSTRAINT_FOREIGN:
			kind_str = "FOREIGN KEY";
			break;

		default:
			Assert(false);
	}

	/*
	 * For each item of constrs1 check if a matching item exists in constrs2.
	 */
	for (i = 0; i < n1; i++)
	{
		ConstraintInfo *c1 = constrs1_a[i];
		int			j;
		bool		found = false;

		for (j = 0; j < n2; j++)
		{
			ConstraintInfo *c2 = constrs2_a[j];

			if ((*callback) (c1, c2, kind))
			{
				found = true;
				matched[j] = true;
				break;
			}
		}

		if (!found)
		{
			ereport(ERROR,
					(errmsg("relation \"%s\" has %s constraint \"%s\" for which relation \"%s\" has no matching constraint",
							RelationGetRelationName(rel1),
							kind_str,
							NameStr(c1->conname),
							RelationGetRelationName(rel2))));
		}
	}

	/*
	 * Are there any constraints left in constrs2 for which there's no match
	 * in constrs1?
	 */
	for (i = 0; i < n2; i++)
	{
		if (!matched[i])
		{
			ConstraintInfo *c2 = constrs2_a[i];

			ereport(ERROR,
					(errmsg("relation \"%s\" has %s constraint \"%s\" for which relation \"%s\" has no matching constraint",
							RelationGetRelationName(rel2),
							kind_str,
							NameStr(c2->conname),
							RelationGetRelationName(rel1))));
		}
	}

	if (n2 > 0)
	{
		pfree(constrs2_a);
		pfree(matched);
	}
	pfree(constrs1_a);
}

static bool
check_comparator(ConstraintInfo *c1, ConstraintInfo *c2, char kind)
{
	Assert(kind == CONSTRAINT_CHECK);

	return (c1->convalidated == c2->convalidated &&
			equal_op_expressions(c1->conbin, c2->conbin));
}

static bool
unique_comparator(ConstraintInfo *c1, ConstraintInfo *c2, char kind)
{
	Assert(kind == CONSTRAINT_UNIQUE);

	return bms_equal(c1->attnos, c2->attnos);
}

static bool
fk_comparator(ConstraintInfo *c1, ConstraintInfo *c2, char kind)
{
	int			i;

	Assert(kind == CONSTRAINT_FOREIGN);

	if (c1->confrelid != c2->confrelid)
		return false;

	Assert(c1->numfks > 0 && c2->numfks > 0);
	if (c1->numfks != c2->numfks)
		return false;

	Assert(c1->numfks <= INDEX_MAX_KEYS);
	for (i = 0; i < c1->numfks; i++)
	{
		if (c1->conkey[i] != c2->conkey[i])
			return false;

		if (c1->confkey[i] != c2->confkey[i])
			return false;

		if (c1->pf_eq_oprs[i] != c2->pf_eq_oprs[i])
			return false;

		if (c1->pp_eq_oprs[i] != c2->pp_eq_oprs[i])
			return false;

		if (c1->ff_eq_oprs[i] != c2->ff_eq_oprs[i])
			return false;
	}

#if PG_VERSION_NUM >= 150000
	if (c1->num_fk_del_set_cols != c2->num_fk_del_set_cols)
		return false;

	for (i = 0; i < c1->num_fk_del_set_cols; i++)
	{
		if (c1->fk_del_set_cols[i] != c2->fk_del_set_cols[i])
			return false;
	}
#endif

	return true;
}

/*
 * Check if any FK constraint that references given relation.
 */
static bool
is_rel_referenced(Relation rel)
{
	Oid			relid = RelationGetRelid(rel);
	Relation	conrel;
	SysScanDesc conscan;
	ScanKeyData skey;
	HeapTuple	htup;
	bool		result = false;

	/*
	 * Prepare to scan pg_constraint for entries having confrelid = this rel.
	 */
	ScanKeyInit(&skey,
				Anum_pg_constraint_confrelid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(relid));

	conrel = table_open(ConstraintRelationId, AccessShareLock);
	conscan = systable_beginscan(conrel, InvalidOid, false,
								 NULL, 1, &skey);

	while (HeapTupleIsValid(htup = systable_getnext(conscan)))
	{
		Form_pg_constraint constraint PG_USED_FOR_ASSERTS_ONLY;

		constraint = (Form_pg_constraint) GETSTRUCT(htup);
		Assert(constraint->contype == CONSTRAINT_FOREIGN);
		result = true;
		break;
	}
	systable_endscan(conscan);
	table_close(conrel, AccessShareLock);

	return result;
}

/*
 * Compare the constraints on the two relations. ERROR if one relation has a
 * constraint that the other does not have.
 */
static void
compare_constraints(Relation rel1, Relation rel2)
{
	Bitmapset  *pk1,
			   *pk2;
	List	   *constrs1,
			   *constrs2;
	int			nunique1,
				nunique2;
	int			ncheck1,
				ncheck2;
	int			nfk1,
				nfk2;

	/*
	 * Even the check of the PK may be skipped w/o breaking the functionality.
	 * The tables can still have compatible identity indexes, but that's
	 * checked elsewhere.
	 */
	if (!rewrite_check_constraints)
		return;

	/* First, check the PKs - relation cache can be used here. */
	pk1 = RelationGetIndexAttrBitmap(rel1, INDEX_ATTR_BITMAP_PRIMARY_KEY);
	pk2 = RelationGetIndexAttrBitmap(rel2, INDEX_ATTR_BITMAP_PRIMARY_KEY);
	if (!bms_equal(pk1, pk2))
		ereport(ERROR,
				(errmsg("the source and destination relations have different primary key")));
	bms_free(pk1);
	bms_free(pk2);

	constrs1 = get_relation_constraints(rel1, &ncheck1, &nunique1, &nfk1);
	constrs2 = get_relation_constraints(rel2, &ncheck2, &nunique2, &nfk2);

	/* Second, the CHECK constraints. */
	if (ncheck1 > 0 || ncheck2 > 0)
		compare_constraints_internal(rel1, constrs1, ncheck1,
									 rel2, constrs2, ncheck2,
									 CONSTRAINT_CHECK,
									 check_comparator);

	/* Third, the UNIQUE constraints. */
	if (nunique1 > 0 || nunique2 > 0)
		compare_constraints_internal(rel1, constrs1, nunique1,
									 rel2, constrs2, nunique2,
									 CONSTRAINT_UNIQUE,
									 unique_comparator);

	/*
	 * Fourth, FKs.
	 *
	 * Currently we only check the FK side because the extension is not
	 * supposed to be used to make the PK table partitioned: the partitioned
	 * table needs to be initially empty so it can hardly be referenced by any
	 * non-empty FK. And if all the FK tables are empty, there's effectively
	 * no constraint.
	 */
	if (nfk1 > 0 || nfk2 > 0)
		compare_constraints_internal(rel1, constrs1, nfk1,
									 rel2, constrs2, nfk2,
									 CONSTRAINT_FOREIGN,
									 fk_comparator);

	/* EXCLUSION constraints are not supported on partitioned tables. */

	free_relation_constraints(constrs1);
	free_relation_constraints(constrs2);
}

/*
 * Get a list of constraints, except for CHECK.
 *
 * Besides the list, returns number of constraints of each kind.
 */
static List *
get_relation_constraints(Relation rel, int *ncheck, int *nunique, int *nfk)
{
	List	   *result = NIL;
	Oid			relid = RelationGetRelid(rel);
	Relation	conrel;
	SysScanDesc conscan;
	ScanKeyData skey;
	HeapTuple	htup;

	*ncheck = 0;
	*nunique = 0;
	*nfk = 0;

	/* Prepare to scan pg_constraint for entries having conrelid = this rel. */
	ScanKeyInit(&skey,
				Anum_pg_constraint_conrelid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(relid));

	conrel = table_open(ConstraintRelationId, AccessShareLock);
	conscan = systable_beginscan(conrel, ConstraintRelidTypidNameIndexId, true,
								 NULL, 1, &skey);

	while (HeapTupleIsValid(htup = systable_getnext(conscan)))
	{
		Form_pg_constraint constraint = (Form_pg_constraint) GETSTRUCT(htup);
		ConstraintInfo *info;

		info = (ConstraintInfo *) palloc0(sizeof(ConstraintInfo));
		memcpy(NameStr(info->conname), NameStr(constraint->conname),
			   NAMEDATALEN);
		info->contype = constraint->contype;
		if (info->contype == CONSTRAINT_CHECK)
		{
			Datum		val;
			bool		isnull;

			info->convalidated = constraint->convalidated;

			val = fastgetattr(htup,
							  Anum_pg_constraint_conbin,
							  conrel->rd_att, &isnull);
			if (isnull)
				elog(ERROR, "null conbin for relation \"%s\"",
					 RelationGetRelationName(rel));

			/* detoast and convert to cstring */
			info->conbin = TextDatumGetCString(val);

			(*ncheck)++;
		}
		else if (info->contype == CONSTRAINT_UNIQUE)
		{
			Oid			conid;

			info->attnos = get_relation_constraint_attnos(relid,
														  NameStr(constraint->conname),
														  false,
														  &conid);
			Assert(constraint->oid == conid);

			(*nunique)++;
		}
		else if (info->contype == CONSTRAINT_FOREIGN)
		{
			info->confrelid = constraint->confrelid;

			DeconstructFkConstraintRow(htup,
									   &info->numfks,
									   info->conkey,
									   info->confkey,
									   info->pf_eq_oprs,
									   info->pp_eq_oprs,
									   info->ff_eq_oprs
#if PG_VERSION_NUM >= 150000
									   ,&info->num_fk_del_set_cols,
									   info->fk_del_set_cols
#endif
				);
			(*nfk)++;
		}
		else
			continue;

		result = lappend(result, info);
	}

	systable_endscan(conscan);
	table_close(conrel, AccessShareLock);

	return result;
}

static void
free_relation_constraints(List *l)
{
	ListCell   *lc;

	foreach(lc, l)
	{
		ConstraintInfo *info = (ConstraintInfo *) lfirst(lc);

		if (info->conbin)
			pfree(info->conbin);
		if (info->attnos)
			bms_free(info->attnos);
	}
	list_free_deep(l);
}

/*
 * Return an array of pointers to constraints of given kind found in the
 * list. 'n' is the expected number of items.
 */
static ConstraintInfo **
get_relation_constraints_array(List *all, char contype, int n)
{
	ListCell   *lc;
	ConstraintInfo **result,
			  **c;
	int			i = 0;

	c = result = (ConstraintInfo **) palloc(n * sizeof(ConstraintInfo *));

	foreach(lc, all)
	{
		ConstraintInfo *info = (ConstraintInfo *) lfirst(lc);

		if (info->contype == contype)
		{
			*c = info;
			c++;
			i++;
		}
		if (i == n)
			break;
	}

	return result;
}

/*
 * Check if the partition matches the partitioned table enough. The point is
 * that any tuple suitable for the partitioned table must also be suitable for
 * the partition.
 *
 * This is actually equalTupleDescs() copy and pasted from the core, except we
 * ignore the 'tdtypeid' field of the descriptors, as well as 'attislocal' and
 * 'attinhcount' of the attributes.
 */
bool
equal_dest_descs(TupleDesc tupdesc1, TupleDesc tupdesc2)
{
	int			i,
				n;

	if (tupdesc1->natts != tupdesc2->natts)
		return false;

	for (i = 0; i < tupdesc1->natts; i++)
	{
		Form_pg_attribute attr1 = TupleDescAttr(tupdesc1, i);
		Form_pg_attribute attr2 = TupleDescAttr(tupdesc2, i);

		/*
		 * We do not need to check every single field here: we can disregard
		 * attrelid and attnum (which were used to place the row in the attrs
		 * array in the first place).  It might look like we could dispense
		 * with checking attlen/attbyval/attalign, since these are derived
		 * from atttypid; but in the case of dropped columns we must check
		 * them (since atttypid will be zero for all dropped columns) and in
		 * general it seems safer to check them always.
		 *
		 * attcacheoff must NOT be checked since it's possibly not set in both
		 * copies.  We also intentionally ignore atthasmissing, since that's
		 * not very relevant in tupdescs, which lack the attmissingval field.
		 */
		if (strcmp(NameStr(attr1->attname), NameStr(attr2->attname)) != 0)
			return false;
		if (attr1->atttypid != attr2->atttypid)
			return false;
		if (attr1->attstattarget != attr2->attstattarget)
			return false;
		if (attr1->attlen != attr2->attlen)
			return false;
		if (attr1->attndims != attr2->attndims)
			return false;
		if (attr1->atttypmod != attr2->atttypmod)
			return false;
		if (attr1->attbyval != attr2->attbyval)
			return false;
		if (attr1->attalign != attr2->attalign)
			return false;
		if (attr1->attstorage != attr2->attstorage)
			return false;
#if PG_VERSION_NUM >= 140000
		if (attr1->attcompression != attr2->attcompression)
			return false;
#endif
		if (attr1->attnotnull != attr2->attnotnull)
			return false;
		if (attr1->atthasdef != attr2->atthasdef)
			return false;
		if (attr1->attidentity != attr2->attidentity)
			return false;
		if (attr1->attgenerated != attr2->attgenerated)
			return false;
		if (attr1->attisdropped != attr2->attisdropped)
			return false;
		if (attr1->attcollation != attr2->attcollation)
			return false;
		/* variable-length fields are not even present... */
	}

	if (tupdesc1->constr != NULL)
	{
		TupleConstr *constr1 = tupdesc1->constr;
		TupleConstr *constr2 = tupdesc2->constr;

		if (constr2 == NULL)
			return false;
		if (constr1->has_not_null != constr2->has_not_null)
			return false;
		if (constr1->has_generated_stored != constr2->has_generated_stored)
			return false;
		n = constr1->num_defval;
		if (n != (int) constr2->num_defval)
			return false;

		/*
		 * It's not critical if DEFAULT expressions are different, it's not an
		 * actual constraint. Checking that would be rather tricky as the
		 * expression can contain volatile functions.
		 */

		if (constr1->missing)
		{
			if (!constr2->missing)
				return false;
			for (i = 0; i < tupdesc1->natts; i++)
			{
				AttrMissing *missval1 = constr1->missing + i;
				AttrMissing *missval2 = constr2->missing + i;

				if (missval1->am_present != missval2->am_present)
					return false;
				if (missval1->am_present)
				{
					Form_pg_attribute missatt1 = TupleDescAttr(tupdesc1, i);

					if (!datumIsEqual(missval1->am_value, missval2->am_value,
									  missatt1->attbyval, missatt1->attlen))
						return false;
				}
			}
		}
		else if (constr2->missing)
			return false;
		n = constr1->num_check;
		if (n != (int) constr2->num_check)
			return false;

		/*
		 * Similarly, we rely here on the ConstrCheck entries being sorted by
		 * name.  If there are duplicate names, the outcome of the comparison
		 * is uncertain, but that should not happen.
		 */
		for (i = 0; i < n; i++)
		{
			ConstrCheck *check1 = constr1->check + i;
			ConstrCheck *check2 = constr2->check + i;

			if (!(strcmp(check1->ccname, check2->ccname) == 0 &&
				  equal_op_expressions(check1->ccbin, check2->ccbin) &&
				  check1->ccvalid == check2->ccvalid &&
				  check1->ccnoinherit == check2->ccnoinherit))
				return false;
		}
	}
	else if (tupdesc2->constr != NULL)
		return false;
	return true;
}

/*
 * Report mismatch between tuples or, if attname==NULL, between specific
 * columns.
 */
static void
report_tupdesc_mismatch(const char *desc, char *attname,
						char *tabname_src, bool has_src,
						char *tabname_dst, bool has_dst)
{
	char	   *does,
			   *does_not;

	Assert(has_src != has_dst);

	does = has_src ? tabname_src : tabname_dst;
	does_not = has_dst ? tabname_src : tabname_dst;

	/* XXX What's the best errcode here? */
	if (attname)
		ereport(ERROR,
				(errmsg("\"%s\" column of the \"%s\" table does have %s but \"%s\" column of the \"%s\" table does not",
						attname, does, desc, attname, does_not)));
	else
		ereport(ERROR,
				(errmsg("the \"%s\" table does have %s but the \"%s\" table does not",
						does, desc, does_not)));
}

static TupleDesc
get_index_tuple_desc(Oid ind_oid)
{
	Relation	ind_rel;
	TupleDesc	result;

	ind_rel = index_open(ind_oid, AccessShareLock);
	result = CreateTupleDescCopy(RelationGetDescr(ind_rel));
	index_close(ind_rel, AccessShareLock);
	return result;
}

/*
 * Create ModifyTableState and do the minimal initialization so that
 * ExecFindPartition() works.
 */
static ModifyTableState *
get_modify_table_state(EState *estate, Relation rel, CmdType operation)
{
	ModifyTableState *result = makeNode(ModifyTableState);
	ResultRelInfo *rri = makeNode(ResultRelInfo);

	InitResultRelInfo(rri, rel, 0, NULL, 0);

	result->ps.plan = NULL;
	result->ps.state = estate;
	result->operation = operation;
#if PG_VERSION_NUM >= 140000
	result->mt_nrels = 1;
#endif
	result->resultRelInfo = rri;
	result->rootResultRelInfo = rri;
	return result;
}

static void
free_modify_table_state(ModifyTableState *mtstate)
{
	pfree(mtstate->resultRelInfo);
	pfree(mtstate);
}

/*
 * Compare the passed catalog information to the info retrieved using the most
 * recent catalog snapshot. Perform the cheapest checks first, the trickier
 * ones later.
 *
 * lock_held is the *least* mode of the lock held by caller on stat->relid
 * relation since the last check. This information helps to avoid unnecessary
 * checks.
 *
 * We check neither constraint nor trigger related DDLs, those are checked by
 * compare_constraints().
 *
 * Unlike get_catalog_state(), fresh catalog snapshot is used for each catalog
 * scan. That might increase the chance a little bit that concurrent change
 * will be detected in the current call, instead of the following one.
 *
 * (As long as we use xmin columns of the catalog tables to detect changes, we
 * can't use syscache here.)
 *
 * XXX It's worth checking AlterTableGetLockLevel() each time we adopt a new
 * version of PG core.
 *
 * XXX Do we still need everything this function does? Unlike pg_squeeze,
 * where we only deal with a single table, various catalog entries need to be
 * compared between the source and the destination table. Since the
 * destination table is locked exclusively all the time (i.e. its catalog
 * entries should not change), catalog changes on the source side should be
 * detected by comparing the source and destination relation again.
 */
void
pg_rewrite_check_catalog_changes(CatalogState *state, LOCKMODE lock_held)
{
	/*
	 * No DDL should be compatible with this lock mode. (Not sure if this
	 * condition will ever fire.)
	 */
	if (lock_held == AccessExclusiveLock)
		return;

	/*
	 * First the source relation itself.
	 *
	 * Only AccessExclusiveLock guarantees that the pg_class entry hasn't
	 * changed. By lowering this threshold we'd perhaps skip unnecessary check
	 * sometimes (e.g. change of pg_class(relhastriggers) is unimportant), but
	 * we could also miss the check when necessary. It's simply too fragile to
	 * deduce the kind of DDL from lock level, so do this check
	 * unconditionally.
	 */
	check_pg_class_changes(state);

	/*
	 * Index change does not necessarily require lock of the parent relation,
	 * so check indexes unconditionally.
	 */
	check_index_changes(state);

	/*
	 * XXX If any lock level lower than AccessExclusiveLock conflicts with all
	 * commands that change pg_attribute catalog, skip this check if lock_held
	 * is at least that level.
	 */
	check_attribute_changes(state);

	/*
	 * Finally check if any composite type used by the source relation has
	 * changed.
	 */
	if (state->ncomptypes > 0)
		check_composite_type_changes(state);
}

static void
check_pg_class_changes(CatalogState *cat_state)
{
	TransactionId xmin_current;

	get_pg_class_info(cat_state->rel.relid, &xmin_current, NULL, NULL);

	/*
	 * Check if pg_class(xmin) has changed.
	 *
	 * The changes caught here include change of pg_class(relfilenode), which
	 * indicates heap rewriting or TRUNCATE command (or concurrent call of
	 * partition_table(), but that should fail to allocate new replication
	 * slot). (Invalid relfilenode does not change, but mapped relations are
	 * excluded from processing by get_catalog_state().)
	 */
	if (!TransactionIdEquals(xmin_current, cat_state->rel.xmin))
		/* XXX Does more suitable error code exist? */
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_IN_USE),
				 errmsg("Incompatible DDL or heap rewrite performed concurrently")));
}

/*
 * Check if any tuple of pg_attribute of given relation has changed. In
 * addition, if the attribute type is composite, check for its changes too.
 */
static void
check_attribute_changes(CatalogState *cat_state)
{
	TransactionId *attrs_new;
	int			i;

	/*
	 * Since pg_class should have been checked by now, relnatts can only be
	 * zero if it was zero originally, so there's no info to be compared to
	 * the current state.
	 */
	if (cat_state->rel.relnatts == 0)
	{
		Assert(cat_state->rel.attr_xmins == NULL);
		return;
	}

	/*
	 * Check if any row of pg_attribute changed.
	 *
	 * If the underlying type is composite, pg_attribute(xmin) will not
	 * reflect its change, so pass NULL for cat_state to indicate that we're
	 * not interested in type info at the moment. We'll do that later if all
	 * the cheaper tests pass.
	 */
	get_attribute_info(cat_state->rel.relid, cat_state->rel.relnatts,
					   &attrs_new, NULL);
	for (i = 0; i < cat_state->rel.relnatts; i++)
	{
		if (!TransactionIdEquals(cat_state->rel.attr_xmins[i], attrs_new[i]))
			ereport(ERROR,
					(errcode(ERRCODE_OBJECT_IN_USE),
					 errmsg("Table definition changed concurrently")));
	}
	pfree(attrs_new);
}

static void
check_index_changes(CatalogState *cat_state)
{
	IndexCatInfo *inds_new;
	int			relninds_new;
	bool		failed = false;
	bool		invalid_index;
	bool		have_pk_index;

	if (cat_state->relninds == 0)
	{
		Assert(cat_state->indexes == NULL);
		return;
	}

	inds_new = get_index_info(cat_state->rel.relid, &relninds_new,
							  &invalid_index, false, &have_pk_index);

	/*
	 * If this field was set to true, no attention was paid to the other
	 * fields during catalog scans.
	 */
	if (invalid_index)
		failed = true;

	if (!failed && relninds_new != cat_state->relninds)
		failed = true;

	/*
	 * It might be o.k. for the PK index to disappear if the table still has
	 * an unique constraint, but this is too hard to check.
	 */
	if (!failed && cat_state->have_pk_index != have_pk_index)
		failed = true;

	if (!failed)
	{
		int			i;

		for (i = 0; i < cat_state->relninds; i++)
		{
			IndexCatInfo *ind,
					   *ind_new;

			ind = &cat_state->indexes[i];
			ind_new = &inds_new[i];
			if (ind->oid != ind_new->oid ||
				!TransactionIdEquals(ind->xmin, ind_new->xmin) ||
				!TransactionIdEquals(ind->pg_class_xmin,
									 ind_new->pg_class_xmin))
			{
				failed = true;
				break;
			}
		}
	}
	if (failed)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_IN_USE),
				 errmsg("Concurrent change of index detected")));
	pfree(inds_new);
}

static void
check_composite_type_changes(CatalogState *cat_state)
{
	int			i;
	TypeCatInfo *changed = NULL;

	for (i = 0; i < cat_state->ncomptypes; i++)
	{
		TypeCatInfo *tinfo = &cat_state->comptypes[i];
		TypeCatInfo tinfo_new;
		int			j;

		tinfo_new.oid = tinfo->oid;
		get_composite_type_info(&tinfo_new);

		if (!TransactionIdEquals(tinfo->xmin, tinfo_new.xmin) ||
			!TransactionIdEquals(tinfo->rel.xmin, tinfo_new.rel.xmin) ||
			(tinfo->rel.relnatts != tinfo_new.rel.relnatts))
		{
			changed = tinfo;
			break;
		}

		/*
		 * Check the individual attributes of the type relation.
		 *
		 * This should catch ALTER TYPE ... ALTER ATTRIBUTE ... change of
		 * attribute data type, which is currently not allowed if the type is
		 * referenced by any table. Do it yet in this generic way so that we
		 * don't have to care whether any PG restrictions are relaxed in the
		 * future.
		 */
		for (j = 0; j < tinfo->rel.relnatts; j++)
		{
			if (!TransactionIdEquals(tinfo->rel.attr_xmins[j],
									 tinfo_new.rel.attr_xmins[j]))
			{
				changed = tinfo;
				break;
			}
		}

		if (tinfo_new.rel.relnatts > 0)
		{
			Assert(tinfo_new.rel.attr_xmins != NULL);
			pfree(tinfo_new.rel.attr_xmins);
		}

		if (changed != NULL)
			break;
	}

	if (changed != NULL)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_IN_USE),
				 errmsg("Concurrent change of composite type %u detected",
						changed->oid)));
}

static void
free_catalog_state(CatalogState *state)
{
	if (state->form_class)
		pfree(state->form_class);

	if (state->desc_class)
		pfree(state->desc_class);

	if (state->rel.attr_xmins)
		pfree(state->rel.attr_xmins);

	if (state->indexes)
		pfree(state->indexes);

	if (state->comptypes)
	{
		int			i;

		for (i = 0; i < state->ncomptypes; i++)
		{
			TypeCatInfo *tinfo = &state->comptypes[i];

			if (tinfo->rel.attr_xmins)
				pfree(tinfo->rel.attr_xmins);
		}
		pfree(state->comptypes);
	}
	pfree(state);
}

/*
 * Wrapper for SnapBuildInitialSnapshot().
 *
 * We do not have to meet the assertions that SnapBuildInitialSnapshot()
 * contains, nor should we set MyPgXact->xmin.
 */
static Snapshot
build_historic_snapshot(SnapBuild *builder)
{
	Snapshot	result;
	bool		FirstSnapshotSet_save;
	int			XactIsoLevel_save;
	TransactionId xmin_save;

	/*
	 * Fake both FirstSnapshotSet and XactIsoLevel so that the assertions in
	 * SnapBuildInitialSnapshot() don't fire. Otherwise partition_table() has
	 * no reason to apply these values.
	 */
	FirstSnapshotSet_save = FirstSnapshotSet;
	FirstSnapshotSet = false;
	XactIsoLevel_save = XactIsoLevel;
	XactIsoLevel = XACT_REPEATABLE_READ;

	/*
	 * Likewise, fake MyPgXact->xmin so that the corresponding check passes.
	 */
#if PG_VERSION_NUM >= 140000
	xmin_save = MyProc->xmin;
	MyProc->xmin = InvalidTransactionId;
#else
	xmin_save = MyPgXact->xmin;
	MyPgXact->xmin = InvalidTransactionId;
#endif

	/*
	 * Call the core function to actually build the snapshot.
	 */
	result = SnapBuildInitialSnapshot(builder);

	/*
	 * Restore the original values.
	 */
	FirstSnapshotSet = FirstSnapshotSet_save;
	XactIsoLevel = XactIsoLevel_save;
#if PG_VERSION_NUM >= 140000
	MyProc->xmin = xmin_save;
#else
	MyPgXact->xmin = xmin_save;
#endif

	return result;
}

/*
 * Use snap_hist snapshot to get the relevant data from rel_src and insert it
 * into the appropriate partitions of rel_dst.
 *
 * Caller is responsible for opening and locking the source relation.
 */
static void
perform_initial_load(EState *estate, ModifyTableState *mtstate,
					 struct PartitionTupleRouting *proute,
					 Relation rel_src, Snapshot snap_hist, Relation rel_dst,
					 partitions_hash *partitions,
					 LogicalDecodingContext *ctx,
					 TupleConversionMap *conv_map)
{
	int			batch_size,
				batch_max_size;
	Size		tuple_array_size;
	bool		tuple_array_can_expand = true;
	TableScanDesc heap_scan;
	TupleTableSlot *slot_src,
			   *slot_dst;
	HeapTuple  *tuples = NULL;
	MemoryContext load_cxt,
				old_cxt;
	XLogRecPtr	end_of_wal_prev = InvalidXLogRecPtr;
	DecodingOutputState *dstate;
	char		replorigin_name[255];

	/*
	 * The session origin will be used to mark WAL records produced by the
	 * load itself so that they are not decoded.
	 */
	Assert(replorigin_session_origin == InvalidRepOriginId);
	snprintf(replorigin_name, sizeof(replorigin_name),
			 REPLORIGIN_NAME_PATTERN, MyDatabaseId);
	replorigin_session_origin = replorigin_create(replorigin_name);

	/*
	 * Also remember that the WAL records created during the load should not
	 * be decoded later.
	 */
	dstate = (DecodingOutputState *) ctx->output_writer_private;
	dstate->rorigin = replorigin_session_origin;

	heap_scan = table_beginscan(rel_src, snap_hist, 0, (ScanKey) NULL);

	/* Slot to retrieve data from the source table. */
	slot_src = table_slot_create(rel_src, NULL);

	/*
	 * Slot to be passed to ExecFindPartition(). We use a separate slot
	 * because here we want to enforce the TTSOpsHeapTuple because the tuple
	 * we pass to ExecFindPartition() is a HeapTuple for sure (i.e we try to
	 * avoid deforming the tuple when storing it).
	 */
	slot_dst = MakeSingleTupleTableSlot(RelationGetDescr(rel_dst),
										&TTSOpsHeapTuple);

	/*
	 * Store as much data as we can store in memory. The more memory is
	 * available, the fewer iterations.
	 */
	batch_max_size = 1024;
	tuple_array_size = batch_max_size * sizeof(HeapTuple);
	/* The minimum value of maintenance_work_mem is 1024 kB. */
	Assert(tuple_array_size / 1024 < maintenance_work_mem);
	tuples = (HeapTuple *) palloc(tuple_array_size);

	/*
	 * The processing can take many iterations. In case any data manipulation
	 * below leaked, try to defend against out-of-memory conditions by using a
	 * separate memory context.
	 */
	load_cxt = AllocSetContextCreate(CurrentMemoryContext,
									 "pg_rewrite initial load cxt",
									 ALLOCSET_DEFAULT_SIZES);
	old_cxt = MemoryContextSwitchTo(load_cxt);

	while (true)
	{
		HeapTuple	tup_in = NULL;
		int			i;
		Size		data_size = 0;
		XLogRecPtr	end_of_wal;

		for (i = 0;; i++)
		{
			bool		flattened = false;

			/*
			 * Check if the tuple array fits into maintenance_work_mem.
			 *
			 * Since the tuple cannot be put back to the scan, it'd make
			 * things tricky if we involved the current tuple in the
			 * computation. Since the unit of maintenance_work_mem is kB, one
			 * extra tuple shouldn't hurt too much.
			 */
			if (((data_size + tuple_array_size) / 1024)
				>= maintenance_work_mem)
			{
				/*
				 * data_size should still be zero if tup_in is the first item
				 * of the current batch and the array itself should never
				 * exceed maintenance_work_mem. XXX If the condition above is
				 * changed to include the current tuple (i.e. we put the
				 * current tuple aside for the next batch), make sure the
				 * first tuple of a batch is inserted regardless its size. We
				 * cannot shrink the array in favor of actual data in generic
				 * case (i.e. tuple size can in general be bigger than
				 * maintenance_work_mem).
				 */
				Assert(i > 0);

				break;
			}

			/*
			 * Perform the tuple retrieval in the original context so that no
			 * scan data is freed during the cleanup between batches.
			 */
			MemoryContextSwitchTo(old_cxt);
			{
				bool		res;

				res = table_scan_getnextslot(heap_scan,
											 ForwardScanDirection,
											 slot_src);

				if (res)
				{
					bool		shouldFree;

					tup_in = ExecFetchSlotHeapTuple(slot_src, false, &shouldFree);
					/* TTSOpsBufferHeapTuple has .get_heap_tuple != NULL. */
					Assert(!shouldFree);
				}
				else
					tup_in = NULL;
			}
			MemoryContextSwitchTo(load_cxt);

			/*
			 * Ran out of input data?
			 */
			if (tup_in == NULL)
				break;

			/*
			 * Even though special snapshot is used to retrieve values from
			 * TOAST relation (see toast_fetch_datum), we'd better flatten the
			 * tuple and thus retrieve the TOAST while the historic snapshot
			 * is active. One particular reason is that tuptoaster.c does
			 * access catalog.
			 */
			if (HeapTupleHasExternal(tup_in))
			{
				tup_in = toast_flatten_tuple(tup_in,
											 RelationGetDescr(rel_src));
				flattened = true;
			}

			CHECK_FOR_INTERRUPTS();

			/*
			 * Check for a free slot early enough so that the current tuple
			 * can be stored even if the array cannot be reallocated. Do not
			 * try again and again if the tuple array reached the maximum
			 * value.
			 */
			if (i == (batch_max_size - 1) && tuple_array_can_expand)
			{
				int			batch_max_size_new;
				Size		tuple_array_size_new;

				batch_max_size_new = 2 * batch_max_size;
				tuple_array_size_new = batch_max_size_new *
					sizeof(HeapTuple);

				/*
				 * Besides being of valid size, the new array should allow for
				 * storing some data w/o exceeding maintenance_work_mem. XXX
				 * Consider tuning the portion of maintenance_work_mem that
				 * the array can use.
				 */
				if (!AllocSizeIsValid(tuple_array_size_new) ||
					tuple_array_size_new / 1024 >=
					maintenance_work_mem / 16)
					tuple_array_can_expand = false;

				/*
				 * Only expand the array if the current iteration does not
				 * violate maintenance_work_mem.
				 */
				if (tuple_array_can_expand)
				{
					tuples = (HeapTuple *)
						repalloc(tuples, tuple_array_size_new);

					batch_max_size = batch_max_size_new;
					tuple_array_size = tuple_array_size_new;
				}
			}

			if (!flattened)
				tup_in = heap_copytuple(tup_in);

			/* Store the tuple and account for its size. */
			tuples[i] = tup_in;
			data_size += HEAPTUPLESIZE + tup_in->t_len;

			/*
			 * If the tuple array could not be expanded, stop reading for the
			 * current batch.
			 */
			if (i == (batch_max_size - 1))
			{
				/* The current tuple belongs to the current batch. */
				i++;

				break;
			}
		}

		/*
		 * Insert the tuples into the target table.
		 *
		 * pg_rewrite_check_catalog_changes() shouldn't be necessary as long
		 * as the AccessSqhareLock we hold on the source relation does not
		 * allow change of table type. (Should ALTER INDEX take place
		 * concurrently, it does not break the heap insertions. In such a case
		 * we'll find out later that we need to terminate processing of the
		 * current table, but it's probably not worth checking each batch.)
		 */

		/*
		 * Has the previous batch processed all the remaining tuples?
		 *
		 * In theory, the counter might end up zero as a result of overflow.
		 * However in practice 'i' should not overflow because its upper limit
		 * is controlled by 'batch_max_size' which is also of the int data
		 * type, and which in turn should not overflow because value much
		 * lower than INT_MAX will make AllocSizeIsValid(tuple_array_size_new)
		 * return false.
		 */
		if (i == 0)
			break;

		batch_size = i;
		i = 0;
		while (true)
		{
			HeapTuple	tup_out;
			ResultRelInfo *rri;
			List	   *recheck;
			BulkInsertState bistate;

			CHECK_FOR_INTERRUPTS();

			if (i == batch_size)
				tup_out = NULL;
			else
				tup_out = tuples[i++];

			if (tup_out == NULL)
				break;

			/* Convert the tuple if needed. */
			if (conv_map)
				tup_out = convert_tuple_for_dest_table(tup_out, conv_map);
			ExecStoreHeapTuple(tup_out, slot_dst, false);
			/* Find out which partition the tuple belongs to. */
			rri = ExecFindPartition(mtstate, mtstate->rootResultRelInfo,
									proute, slot_dst, estate);

			/*
			 * Insert the tuple into the partition.
			 *
			 * XXX Should this happen outside load_cxt? Currently "bistate" is
			 * a flat object (i.e. it does not point to any memory chunk that
			 * the previous call of table_tuple_insert() might have allocated)
			 * and thus the cleanup between batches should not damage it, but
			 * can't it get more complex in future PG versions?
			 */
			bistate = get_partition_insert_state(partitions,
												 RelationGetRelid(rri->ri_RelationDesc));
			Assert(bistate != NULL);
			table_tuple_insert(rri->ri_RelationDesc, slot_dst,
							   GetCurrentCommandId(true), 0, bistate);

#if PG_VERSION_NUM < 140000
			estate->es_result_relation_info = rri;
#endif

			/* Update indexes. */
			recheck = ExecInsertIndexTuples(
#if PG_VERSION_NUM >= 140000
											rri,
#endif
											slot_dst,
											estate,
#if PG_VERSION_NUM >= 140000
											false,	/* update */
#endif
											false,
											NULL,
											NIL);
			ExecClearTuple(slot_dst);

			/*
			 * If recheck is required, it must have been preformed on the
			 * source relation by now. (All the logical changes we process
			 * here are already committed.)
			 */
			list_free(recheck);
			pfree(tup_out);
		}

		/*
		 * Reached the end of scan when retrieving data from the source table?
		 */
		if (tup_in == NULL)
			break;

		/*
		 * Free possibly-leaked memory.
		 */
		MemoryContextReset(load_cxt);

		/*
		 * Decode the WAL produced by the load, as well as by other
		 * transactions, so that the replication slot can advance and WAL does
		 * not pile up. Of course we must not apply the changes until the
		 * initial load has completed.
		 *
		 * Note that the insertions into the new table shouldn't actually be
		 * decoded, they should be filtered out by their origin.
		 */
#if PG_VERSION_NUM >= 150000
		end_of_wal = GetFlushRecPtr(NULL);
#else
		end_of_wal = GetFlushRecPtr();
#endif
		if (end_of_wal > end_of_wal_prev)
		{
			MemoryContextSwitchTo(old_cxt);
			pg_rewrite_decode_concurrent_changes(ctx, end_of_wal, NULL);
			MemoryContextSwitchTo(load_cxt);
		}
		end_of_wal_prev = end_of_wal;
	}

	/*
	 * At whichever stage the loop broke, the historic snapshot should no
	 * longer be active.
	 */

	/* Cleanup. */
	pfree(tuples);
	table_endscan(heap_scan);
	ExecDropSingleTupleTableSlot(slot_src);
	ExecDropSingleTupleTableSlot(slot_dst);

	/* Drop the replication origin. */
#if PG_VERSION_NUM >= 140000
	replorigin_drop_by_name(replorigin_name, false, true);
#else
	replorigin_drop(replorigin_session_origin, false);
#endif
	replorigin_session_origin = InvalidRepOriginId;

	MemoryContextSwitchTo(old_cxt);
	MemoryContextDelete(load_cxt);

	elog(DEBUG1, "pg_rewrite: the initial load completed");
}

/*
 * Build scan key to process logical changes.
 */
static ScanKey
build_identity_key(Relation ident_idx_rel, int *nentries)
{
	HeapTuple	ht_idx;
	Datum		coldatum;
	bool		colisnull;
	oidvector  *indcollation;
	int			n,
				i;
	ScanKey		result;

	ht_idx = ident_idx_rel->rd_indextuple;
	coldatum = SysCacheGetAttr(INDEXRELID, ht_idx,
							   Anum_pg_index_indcollation, &colisnull);
	Assert(!colisnull);
	indcollation = (oidvector *) DatumGetPointer(coldatum);
	n = RelationGetNumberOfAttributes(ident_idx_rel);
	result = (ScanKey) palloc(sizeof(ScanKeyData) * n);
	for (i = 0; i < n; i++)
	{
		ScanKey		entry;
		Oid			opfamily,
					opcintype,
					opno,
					opcode;

		entry = &result[i];

		opfamily = ident_idx_rel->rd_opfamily[i];
		opcintype = ident_idx_rel->rd_opcintype[i];
		opno = get_opfamily_member(opfamily, opcintype, opcintype,
								   BTEqualStrategyNumber);

		if (!OidIsValid(opno))
			elog(ERROR, "Failed to find = operator for type %u", opcintype);

		opcode = get_opcode(opno);
		if (!OidIsValid(opcode))
			elog(ERROR, "Failed to find = operator for operator %u", opno);

		/* Initialize everything but argument. */
		ScanKeyInit(entry,
					i + 1,
					BTEqualStrategyNumber, opcode,
					(Datum) NULL);
		entry->sk_collation = indcollation->values[i];
	}

	*nentries = n;
	return result;
}

/*
 * Try to perform the final processing of concurrent data changes of the
 * source table, which requires an exclusive lock. The return value tells
 * whether this step succeeded. (If not, caller might want to retry.)
 */
static bool
perform_final_merge(EState *estate,
					ModifyTableState *mtstate,
					struct PartitionTupleRouting *proute,
					Oid relid_src, Oid *indexes_src, int nindexes,
					Relation rel_dst, ScanKey ident_key,
					int ident_key_nentries,
					CatalogState *cat_state,
					LogicalDecodingContext *ctx,
					partitions_hash *partitions,
					TupleConversionMap *conv_map)
{
	Relation	rel_src;
	bool		success;
	XLogRecPtr	xlog_insert_ptr,
				end_of_wal;
	int			i;
	struct timeval t_end;
	struct timeval *t_end_ptr = NULL;
	char		dummy_rec_data = '\0';

	/*
	 * Lock the source table exclusively last time, to finalize the work.
	 *
	 * On pg_repack: before taking the exclusive lock, pg_repack extension is
	 * more restrictive in waiting for other transactions to complete. That
	 * might reduce the likelihood of MVCC-unsafe behavior that PG core admits
	 * in some cases
	 * (https://www.postgresql.org/docs/9.6/static/mvcc-caveats.html) but
	 * can't completely avoid it anyway. On the other hand, pg_rewrite only
	 * waits for completion of transactions which performed write (i.e. do
	 * have XID assigned) - this is a side effect of bringing our replication
	 * slot into consistent state.
	 *
	 * As pg_repack shows, extra effort makes little sense here, because some
	 * other transactions still can start before the exclusive lock on the
	 * source relation is acquired. In particular, if transaction A starts in
	 * this period and commits a change, transaction B can miss it if the next
	 * steps are as follows: 1. transaction B took a snapshot (e.g. it has
	 * REPEATABLE READ isolation level), 2. pg_repack took the exclusive
	 * relation lock and finished its work, 3. transaction B acquired shared
	 * lock and performed its scan. (And of course, waiting for transactions
	 * A, B, ... to complete while holding the exclusive lock can cause
	 * deadlocks.)
	 */
	rel_src = table_open(relid_src, AccessExclusiveLock);

	/*
	 * Lock the indexes too, as ALTER INDEX does not need table lock.
	 *
	 * The locking will succeed even if the index is no longer there. In that
	 * case, ERROR will be raised during the catalog check below.
	 */
	for (i = 0; i < nindexes; i++)
		LockRelationOid(indexes_src[i], AccessExclusiveLock);

	if (rewrite_max_xlock_time > 0)
	{
		int64		usec;
		struct timeval t_start;

		gettimeofday(&t_start, NULL);
		usec = t_start.tv_usec + 1000 * (rewrite_max_xlock_time % 1000);
		t_end.tv_sec = t_start.tv_sec + usec / USECS_PER_SEC;
		t_end.tv_usec = usec % USECS_PER_SEC;
		t_end_ptr = &t_end;
	}

	/*
	 * Check the source relation for DDLs once again. If this check passes, no
	 * DDL can break the process anymore. NoLock must be passed because the
	 * relation was really unlocked for some period since the last check.
	 *
	 * It makes sense to do this immediately after having acquired the
	 * exclusive lock(s), so we don't waste any effort if the source table is
	 * no longer compatible.
	 */
	pg_rewrite_check_catalog_changes(cat_state, NoLock);

	/* Changes of constraints are not allowed too. */
	compare_constraints(rel_src, rel_dst);

	/*
	 * Check if any FK referencing the source table was created while it was
	 * unlocked.
	 *
	 * XXX Given the check is rather expensive (there's no index on the
	 * pg_constraints(confrelid) column), Consider if this check isn't too
	 * paranoid.
	 */
	if (rewrite_check_constraints)
	{
		if (is_rel_referenced(rel_src))
			ereport(ERROR,
					(errmsg("table \"%s\" is referenced by a foreign key",
							RelationGetRelationName(rel_src))));
	}

	/* rel_src cache entry is not needed anymore, but the lock is. */
	table_close(rel_src, NoLock);

	/*
	 * Flush anything we see in WAL, to make sure that all changes committed
	 * while we were creating indexes and waiting for the exclusive lock are
	 * available for decoding. This should not be necessary if all backends
	 * had synchronous_commit set, but we can't rely on this setting.
	 *
	 * Unfortunately, GetInsertRecPtr() may lag behind the actual insert
	 * position, and GetLastImportantRecPtr() points at the start of the last
	 * record rather than at the end. Thus the simplest way to determine the
	 * insert position is to insert a dummy record and use its LSN.
	 *
	 * XXX Consider using GetLastImportantRecPtr() and adding the size of the
	 * last record (plus the total size of all the page headers the record
	 * spans)?
	 */
	XLogBeginInsert();
	XLogRegisterData(&dummy_rec_data, 1);
	xlog_insert_ptr = XLogInsert(RM_XLOG_ID, XLOG_NOOP);
	XLogFlush(xlog_insert_ptr);
#if PG_VERSION_NUM >= 150000
	end_of_wal = GetFlushRecPtr(NULL);
#else
	end_of_wal = GetFlushRecPtr();
#endif

	/*
	 * Process the changes that might have taken place while we were waiting
	 * for the lock.
	 *
	 * AccessExclusiveLock effectively disables catalog checks - we've already
	 * performed them above.
	 */
	success = pg_rewrite_process_concurrent_changes(estate,
													mtstate,
													proute,
													ctx,
													end_of_wal,
													cat_state,
													rel_dst,
													ident_key,
													ident_key_nentries,
													AccessExclusiveLock,
													partitions,
													conv_map,
													t_end_ptr);
	if (!success)
	{
		/* Unlock the relations and indexes. */
		for (i = 0; i < nindexes; i++)
			UnlockRelationOid(indexes_src[i], AccessExclusiveLock);

		UnlockRelationOid(relid_src, AccessExclusiveLock);

		/*
		 * Take time to reach end_of_wal.
		 *
		 * XXX DecodingOutputState may contain some changes. The corner case
		 * that the data_size has already reached maintenance_work_mem so the
		 * first change we decode now will make it spill to disk is too low to
		 * justify calling apply_concurrent_changes() separately.
		 */
		pg_rewrite_process_concurrent_changes(estate,
											  mtstate,
											  proute,
											  ctx,
											  end_of_wal,
											  cat_state,
											  rel_dst,
											  ident_key,
											  ident_key_nentries,
											  AccessExclusiveLock,
											  partitions,
											  conv_map,
											  NULL);
	}

	return success;
}

/*
 * Close the partition identity indexes contained in the hash table and
 * destroy the hash table itself.
 */
static void
close_partitions(partitions_hash *partitions)
{
	partitions_iterator iterator;
	PartitionEntry *entry;

	partitions_start_iterate(partitions, &iterator);
	while ((entry = partitions_iterate(partitions, &iterator)) != NULL)
	{
		index_close(entry->ident_index, AccessShareLock);
		ExecDropSingleTupleTableSlot(entry->ind_slot);
		FreeBulkInsertState(entry->bistate);
	}

	partitions_destroy(partitions);
}

/*
 * Find BulkInsertState for given partition.
 */
BulkInsertState
get_partition_insert_state(partitions_hash *partitions, Oid part_oid)
{
	PartitionEntry *entry;

	entry = partitions_lookup(partitions, part_oid);
	if (entry == NULL)
		elog(ERROR, "bulk insert state not found for partition %u", part_oid);
	Assert(entry->part_oid == part_oid);

	return entry->bistate;
}
