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

#if PG_VERSION_NUM < 130000
#error "PostgreSQL version 13 or higher is required"
#endif

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
#include "commands/dbcommands.h"
#include "commands/extension.h"
#include "commands/tablecmds.h"
#include "commands/tablespace.h"
#include "executor/executor.h"
#include "executor/execPartition.h"
#include "funcapi.h"
#include "lib/stringinfo.h"
#include "nodes/primnodes.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/optimizer.h"
#include "parser/parse_coerce.h"
#include "parser/parse_collate.h"
#include "replication/snapbuild.h"
#include "partitioning/partdesc.h"
#include "storage/bufmgr.h"
#include "storage/freespace.h"
#include "storage/ipc.h"
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
#include "utils/injection_point.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/syscache.h"
#include "utils/varlena.h"

PG_MODULE_MAGIC;

#define	REPL_SLOT_BASE_NAME	"pg_rewrite_slot_"
#define	REPL_PLUGIN_NAME	"pg_rewrite"

static void rewrite_table_impl(char *relschema_src, char *relname_src,
							   char *relname_new, char *relschema_dst,
							   char *relname_dst);
static Relation get_identity_index(Relation rel_dst, Relation rel_src);
static partitions_hash *get_partitions(Relation rel_src, Relation rel_dst,
									   int *nparts,
									   Relation **parts_dst_p,
									   ScanKey *ident_key_p,
									   int *ident_key_nentries);
static int	index_cat_info_compare(const void *arg1, const void *arg2);

/* The WAL segment being decoded. */
XLogSegNo	rewrite_current_segment = 0;

static void worker_shmem_request(void);
static void worker_shmem_startup(void);
static void worker_shmem_shutdown(int code, Datum arg);

static void relation_rewrite_get_args(PG_FUNCTION_ARGS, RangeVar **rv_src_p,
									  RangeVar **rv_src_new_p,
									  RangeVar **rv_dst_p);
static WorkerTask *get_task(int *idx, char *relschema, char *relname,
							bool nowait);
static void initialize_worker(BackgroundWorker *worker, int task_idx);
static void run_worker(BackgroundWorker *worker, WorkerTask *task,
					   bool nowait);

static void check_prerequisites(Relation rel);
static LogicalDecodingContext *setup_decoding(Oid relid);
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
								 TupleConversionMapExt *conv_map);
static ScanKey build_identity_key(Relation ident_idx_rel, int *nentries);
static bool perform_final_merge(EState *estate,
								ModifyTableState *mtstate,
								struct PartitionTupleRouting *proute,
								Oid relid_src, Oid *indexes_src, int nindexes,
								Relation rel_dst, ScanKey ident_key,
								int ident_key_nentries,
								Relation ident_index,
								TupleTableSlot *ind_slot,
								CatalogState *cat_state,
								LogicalDecodingContext *ctx,
								partitions_hash *ident_indexes,
								TupleConversionMapExt *conv_map);
static void close_partitions(partitions_hash *partitions);

static TupleConversionMapExt *convert_tuples_by_name_ext(TupleDesc indesc,
														 TupleDesc outdesc);
static AttrMapExt *make_attrmap_ext(int maplen);
static void free_attrmap_ext(AttrMapExt *map);
static AttrMapExt *build_attrmap_by_name_if_req_ext(TupleDesc indesc,
													TupleDesc outdesc);
static AttrMapExt *build_attrmap_by_name_ext(TupleDesc indesc,
											 TupleDesc outdesc);
static bool check_attrmap_match_ext(TupleDesc indesc, TupleDesc outdesc,
									AttrMapExt *attrMap);
static TupleConversionMapExt *convert_tuples_by_name_attrmap_ext(TupleDesc indesc,
																 TupleDesc outdesc,
																 AttrMapExt *attrMap);
static void free_conversion_map_ext(TupleConversionMapExt *map);

/*
 * The maximum time to hold AccessExclusiveLock on the source table during the
 * final processing. Note that it only pg_rewrite_process_concurrent_changes()
 * execution time is included here.
 */
int			rewrite_max_xlock_time = 0;

#if PG_VERSION_NUM >= 150000
shmem_request_hook_type prev_shmem_request_hook = NULL;
#endif
shmem_startup_hook_type prev_shmem_startup_hook = NULL;

void
_PG_init(void)
{
	if (!process_shared_preload_libraries_in_progress)
		ereport(ERROR,
				(errmsg("pg_rewrite must be loaded via shared_preload_libraries")));

#if PG_VERSION_NUM >= 150000
	prev_shmem_request_hook = shmem_request_hook;
	shmem_request_hook = worker_shmem_request;
#else
	worker_shmem_request();
#endif

	prev_shmem_startup_hook = shmem_startup_hook;
	shmem_startup_hook = worker_shmem_startup;

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
}

#define REPLORIGIN_NAME_PATTERN	"pg_rewrite_%u"

/*
 * The original implementation would certainly fail on PG 16 and higher, due
 * to the commit 240e0dbacd (in the master branch) - this commit makes it
 * impossible to invoke our functionality via the PG executor. It's not worth
 * supporting lower versions of pg_rewrite on lower versions of PG server. We
 * keep the symbol in the library so that the upgrade path works.
 */
extern Datum partition_table(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(partition_table);
Datum
partition_table(PG_FUNCTION_ARGS)
{
	ereport(ERROR, (errmsg("the function is no longer supported"),
					errhint("please run \"ALTER EXTENSION pg_rewrite UPDATE\"")));

	PG_RETURN_VOID();
}

/*
 * Likewise, keep the symbol because the upgrade path to 1.3 (or higher)
 * requires that.
 */
extern Datum partition_table_new(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(partition_table_new);
Datum
partition_table_new(PG_FUNCTION_ARGS)
{
	ereport(ERROR, (errmsg("the function is no longer supported"),
					errhint("please run \"ALTER EXTENSION pg_rewrite UPDATE\"")));

	PG_RETURN_VOID();
}


/* Pointer to task array in the shared memory, available in all backends. */
static WorkerTask	*workerTasks = NULL;

/* Each worker stores here the pointer to its task in the shared memory. */
WorkerTask *MyWorkerTask = NULL;

static void
interrupt_worker(WorkerTask *task)
{
	SpinLockAcquire(&task->mutex);
	task->exit_requested = true;
	SpinLockRelease(&task->mutex);
}

static void
release_task(WorkerTask *task, bool worker)
{
	if (worker)
	{
		SpinLockAcquire(&task->mutex);

		/*
		 * First, handle special case that can happen in regression tests. If
		 * rewrite_table_nowait() gets cancelled before the worker got its
		 * MyDatabaseId assigned, 'task' slot can leak (note that
		 * rewrite_table_nowait() does not release the task in this case). We
		 * can release the task regardless of MyDatabaseId because
		 * pg_rewrite_concurrent.spec should not launch a new worker (and thus
		 * reuse the task) before the existing one exited.
		 */
		if (task->nowait)
			task->dbid = InvalidOid;
		/*
		 * Otherwise, worker must not release the task because the backend can
		 * be interested in its contents.
		 */

		/*
		 * However, the worker always should clear the fields it set.
		 */
		task->pid = InvalidPid;
		task->exit_requested = false;
		SpinLockRelease(&task->mutex);
		return;
	}

	/*
	 * The following should only be performed by the backend, after the worker
	 * has exited.
	 */
	SpinLockAcquire(&task->mutex);
	Assert(OidIsValid(task->dbid));
	task->dbid = InvalidOid;
	SpinLockRelease(&task->mutex);
}

static Size
worker_shmem_size(void)
{
	return MAX_TASKS * sizeof(WorkerTask);
}

static void
worker_shmem_request(void)
{
	/* With lower PG versions this function is called from _PG_init(). */
#if PG_VERSION_NUM >= 150000
	if (prev_shmem_request_hook)
		prev_shmem_request_hook();
#endif	/* PG_VERSION_NUM >= 150000 */

	RequestAddinShmemSpace(worker_shmem_size());
}

static void
worker_shmem_startup(void)
{
	bool		found;

	if (prev_shmem_startup_hook)
		prev_shmem_startup_hook();

	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);

	workerTasks = ShmemInitStruct("pg_rewrite",
								  worker_shmem_size(),
								  &found);
	if (!found)
	{
		int		i;

		for (i = 0; i < MAX_TASKS; i++)
		{
			WorkerTask *task = &workerTasks[i];

			task->dbid = InvalidOid;
			task->roleid = InvalidOid;
			task->pid = InvalidPid;
			task->exit_requested = false;
			SpinLockInit(&task->mutex);
		}
	}

	LWLockRelease(AddinShmemInitLock);
}

static void
worker_shmem_shutdown(int code, Datum arg)
{
	if (MyWorkerTask)
		release_task(MyWorkerTask, true);
}

static void
relation_rewrite_get_args(PG_FUNCTION_ARGS, RangeVar **rv_src_p,
						  RangeVar **rv_src_new_p, RangeVar **rv_dst_p)
{
	text	*rel_src_t, *rel_src_new_t, *rel_dst_t;
	RangeVar	*rv_src, *rv_src_new, *rv_dst;

	rel_src_t = PG_GETARG_TEXT_PP(0);
	rv_src = makeRangeVarFromNameList(textToQualifiedNameList(rel_src_t));

	rel_dst_t = PG_GETARG_TEXT_PP(1);
	rv_dst = makeRangeVarFromNameList(textToQualifiedNameList(rel_dst_t));

	rel_src_new_t = PG_GETARG_TEXT_PP(2);
	rv_src_new = makeRangeVarFromNameList(textToQualifiedNameList(rel_src_new_t));

	if (rv_src->catalogname || rv_dst->catalogname || rv_src_new->catalogname)
		ereport(ERROR,
				(errmsg("relation may only be qualified by schema, not by database")));

	/*
	 * Technically it's possible to move the source relation to another schema
	 * but don't bother for this version.
	 */
	if (rv_src_new->schemaname)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_NAME),
				 (errmsg("the new source relation name may not be qualified"))));

	*rv_src_p = rv_src;
	*rv_src_new_p = rv_src_new;
	*rv_dst_p = rv_dst;
}

/*
 * Find a free task structure and initialize the common fields.
 */
static WorkerTask *
get_task(int *idx, char *relschema, char *relname, bool nowait)
{
	int		i;
	WorkerTask	*task = NULL;
	bool	found = false;

	for (i = 0; i < MAX_TASKS; i++)
	{
		task = &workerTasks[i];
		SpinLockAcquire(&task->mutex);
		if (task->dbid == InvalidOid && task->pid == InvalidPid)
		{
			TaskProgress	*progress = &task->progress;

			/* Make sure that no other backend can use the task. */
			task->dbid = MyDatabaseId;
			progress->ins_initial = 0;
			progress->ins = 0;
			progress->upd = 0;
			progress->del = 0;

			found = true;
		}
		SpinLockRelease(&task->mutex);

		if (found)
			break;
	}
	if (!found)
		ereport(ERROR, (errmsg("too many concurrent tasks in progress")));

	/* Finalize the task. */
	task->roleid = GetUserId();
	task->exit_requested = false;
	if (relschema)
		namestrcpy(&task->relschema, relschema);
	else
		NameStr(task->relschema)[0] = '\0';
	namestrcpy(&task->relname, relname);

	task->msg[0] = '\0';
	task->msg_detail[0] = '\0';

	task->nowait = nowait;

	*idx = i;
	return task;
}

static void
initialize_worker(BackgroundWorker *worker, int task_idx)
{
	char	*dbname;

	worker->bgw_flags = BGWORKER_SHMEM_ACCESS |
		BGWORKER_BACKEND_DATABASE_CONNECTION;
	worker->bgw_start_time = BgWorkerStart_RecoveryFinished;
	worker->bgw_restart_time = BGW_NEVER_RESTART;
	sprintf(worker->bgw_library_name, "pg_rewrite");
	sprintf(worker->bgw_function_name, "rewrite_worker_main");

	/*
	 * XXX The function can throw ERROR but the database should really exist,
	 * so no need to put this code in the PG_TRY block.
	 */
	dbname = get_database_name(MyDatabaseId);
	snprintf(worker->bgw_name, BGW_MAXLEN,
			 "pg_rewrite worker for database %s", dbname);
	snprintf(worker->bgw_type, BGW_MAXLEN, "pg_rewrite worker");

	worker->bgw_main_arg = (Datum) task_idx;
	worker->bgw_notify_pid = MyProcPid;
}

static void
run_worker(BackgroundWorker *worker, WorkerTask *task, bool nowait)
{
	BackgroundWorkerHandle *handle;
	BgwHandleStatus status;
	pid_t		pid;
	char	*msg = NULL;
	char	*msg_detail = NULL;

	/*
	 * Start the worker. Avoid leaking the task if the function ends due to
	 * ERROR.
	 */
	PG_TRY();
	{
		if (!RegisterDynamicBackgroundWorker(worker, &handle))
			ereport(ERROR,
					(errcode(ERRCODE_INSUFFICIENT_RESOURCES),
					 errmsg("could not register background process"),
					 errhint("More details may be available in the server log.")));

		status = WaitForBackgroundWorkerStartup(handle, &pid);
	}
	PG_CATCH();
	{
		/*
		 * It seems possible that the worker is trying to start even if we end
		 * up here - at least when WaitForBackgroundWorkerStartup() got
		 * interrupted.
		 */
		interrupt_worker(task);

		release_task(task, false);
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (status == BGWH_STOPPED)
	{
		/* Work already done? */
		goto done;
	}
	else if (status == BGWH_POSTMASTER_DIED)
	{
		ereport(ERROR,
				(errmsg("could not start background worker because the postmaster died"),
				 errhint("More details may be available in the server log.")));
		/* No need to release the task in the shared memory. */
	}

	/*
	 * WaitForBackgroundWorkerStartup() should not return
	 * BGWH_NOT_YET_STARTED.
	 */
	Assert(status == BGWH_STARTED);

	if (nowait)
		/* The worker should take care of releasing the task. */
		return;

	PG_TRY();
	{
		status = WaitForBackgroundWorkerShutdown(handle);
	}
	PG_CATCH();
	{
		/*
		 * Make sure the worker stops. Interrupt received from the user is the
		 * typical use case.
		 */
		interrupt_worker(task);

		release_task(task, false);
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (status == BGWH_POSTMASTER_DIED)
	{
		ereport(ERROR,
				(errmsg("the postmaster died before the background worker could finish"),
				 errhint("More details may be available in the server log.")));
		/* No need to release the task in the shared memory. */
	}

	/*
	 * WaitForBackgroundWorkerShutdown() should not return anything else.
	 */
	Assert(status == BGWH_STOPPED);

done:
	if (strlen(task->msg) > 0)
		msg = pstrdup(task->msg);
	if (strlen(task->msg_detail) > 0)
		msg_detail = pstrdup(task->msg_detail);

	release_task(task, false);

	/* Report the worker's ERROR in the backend. */
	if (msg)
	{
		if (msg_detail)
			ereport(ERROR, (errmsg("%s", msg),
							errdetail("%s", msg_detail)));
		else
			ereport(ERROR, (errmsg("%s", msg)));
	}

}

/* PG >= 14 does define this macro. */
#if PG_VERSION_NUM < 140000
#define RelationIsPermanent(relation) \
	((relation)->rd_rel->relpersistence == RELPERSISTENCE_PERMANENT)
#endif

/*
 * Start the background worker and wait until it exits.
 */
extern Datum rewrite_table(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(rewrite_table);
Datum
rewrite_table(PG_FUNCTION_ARGS)
{
	RangeVar	*rv_src, *rv_src_new, *rv_dst;
	BackgroundWorker worker;
	WorkerTask	*task;
	int		task_idx;

	relation_rewrite_get_args(fcinfo, &rv_src, &rv_src_new, &rv_dst);

	task = get_task(&task_idx, rv_src->schemaname, rv_src->relname, false);
	Assert(task_idx < MAX_TASKS);

	/* Specify the relation to be processed. */
	if (rv_dst->schemaname)
		namestrcpy(&task->relschema_dst, rv_dst->schemaname);
	else
		NameStr(task->relschema_dst)[0] = '\0';
	namestrcpy(&task->relname_dst, rv_dst->relname);
	namestrcpy(&task->relname_new, rv_src_new->relname);

	initialize_worker(&worker, task_idx);

	run_worker(&worker, task, false);

	PG_RETURN_VOID();
}

/*
 * See pg_rewrite_concurrent.spec for information why this function is needed.
 */
extern Datum rewrite_table_nowait(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(rewrite_table_nowait);
Datum
rewrite_table_nowait(PG_FUNCTION_ARGS)
{
	RangeVar	*rv_src, *rv_src_new, *rv_dst;
	BackgroundWorker worker;
	WorkerTask	*task;
	int		task_idx;

	relation_rewrite_get_args(fcinfo, &rv_src, &rv_src_new, &rv_dst);

	task = get_task(&task_idx, rv_src->schemaname, rv_src->relname, true);
	Assert(task_idx < MAX_TASKS);

	/* Specify the relation to be processed. */
	if (rv_dst->schemaname)
		namestrcpy(&task->relschema_dst, rv_dst->schemaname);
	else
		NameStr(task->relschema_dst)[0] = '\0';
	namestrcpy(&task->relname_dst, rv_dst->relname);
	namestrcpy(&task->relname_new, rv_src_new->relname);

	initialize_worker(&worker, task_idx);

	run_worker(&worker, task, true);

	PG_RETURN_VOID();
}

void
rewrite_worker_main(Datum main_arg)
{
	Datum	arg;
	int		i;
	Oid		dbid, roleid;
	char *relschema, *relname, *relname_new, *relschema_dst,
		*relname_dst;
	WorkerTask	*task;

	/* The worker should do its cleanup when exiting. */
	before_shmem_exit(worker_shmem_shutdown, (Datum) 0);

	/*
	 * The standard handlers for SIGTERM and SIGQUIT are fine, see
	 * bgworker.c.
	 */
	BackgroundWorkerUnblockSignals();

	/* Retrieve task index. */
	Assert(MyBgworkerEntry != NULL);
	arg = MyBgworkerEntry->bgw_main_arg;
	i = DatumGetInt32(arg);
	Assert(i >= 0 && i < MAX_TASKS);

	Assert(MyWorkerTask == NULL);
	task = MyWorkerTask = &workerTasks[i];

	/*
	 * The task should be fully initialized before the backend registers the
	 * worker. Let's copy the arguments so that we have a consistent view -
	 * see the explanation below.
	 */
	relschema = NameStr(task->relschema);
	relschema = *relschema != '\0' ? pstrdup(relschema) : NULL;
	relname = pstrdup(NameStr(task->relname));
	relname_new = pstrdup(NameStr(task->relname_new));

	relschema_dst = NameStr(task->relschema_dst);
	relschema_dst = *relschema_dst != '\0' ? pstrdup(relschema_dst) : NULL;
	relname_dst = pstrdup(NameStr(task->relname_dst));

	/*
	 * Get the information provided by the backend and set our pid.
	 */
	SpinLockAcquire(&MyWorkerTask->mutex);
	dbid = MyWorkerTask->dbid;
	Assert(MyWorkerTask->roleid != InvalidOid);
	roleid = MyWorkerTask->roleid;
	task->pid = MyProcPid;
	SpinLockRelease(&MyWorkerTask->mutex);

	/*
	 * Has the "owning" backend of this worker exited too early?
	 */
	if (!OidIsValid(dbid))
	{
		ereport(DEBUG1,
				(errmsg("task cancelled before the worker could start")));
		return;
	}
	/*
	 * If the backend exits later (w/o waiting for the worker's exit), that
	 * backend's ERRORs (which include interrupts) should make the worker stop
	 * (via interrupt_worker()).
	 */

	BackgroundWorkerInitializeConnectionByOid(dbid, roleid, 0);

	/* Do the actual work. */
	StartTransactionCommand();
	PG_TRY();
	{
		rewrite_table_impl(relschema, relname, relname_new, relschema_dst,
						   relname_dst);
		CommitTransactionCommand();

		/*
		 * In regression tests, use this injection point to check that
		 * the changes are visible by other transactions.
		 */
		INJECTION_POINT("pg_rewrite-after-commit");

	}
	PG_CATCH();
	{
		MemoryContext old_context = CurrentMemoryContext;
		ErrorData	*edata;

		/*
		 * If the backend is not waiting for our exit, make sure the error is
		 * logged.
		 */
		if (MyWorkerTask->nowait)
			PG_RE_THROW();

		HOLD_INTERRUPTS();

		/*
		 * CopyErrorData() requires the context to be different from
		 * ErrorContext.
		 */
		MemoryContextSwitchTo(TopMemoryContext);
		edata = CopyErrorData();
		MemoryContextSwitchTo(old_context);

		/*
		 * The following shouldn't be necessary because the worker isn't going
		 * to do anything else, but cleanup is just a good practice.
		 *
		 * XXX Should we re-throw the error instead of doing the cleanup? Not
		 * sure, the error message would then appear twice in the log.
		 */
		FlushErrorState();

		/* Not done by AbortTransaction(). */
		if (MyReplicationSlot != NULL)
			ReplicationSlotRelease();
		/*
		 * Likewise, there seems to be no automatic cleanup of the origin, so
		 * do it here.  The insertion into the ReplicationOriginRelationId
		 * catalog will be rolled back due to the transaction abort.
		 */
		if (replorigin_session_origin != InvalidRepOriginId)
			replorigin_session_origin = InvalidRepOriginId;

		AbortOutOfAnyTransaction();

		/*
		 * Currently we only copy 'message' and 'detail.More information can
		 * be added if needed. (Ideally we'd use the message queue like
		 * parallel workers do, but the related PG core functions have some
		 * parallel worker specific arguments.)
		 */
		strlcpy(task->msg, edata->message, MAX_ERR_MSG_LEN);
		if (edata->detail && strlen(edata->detail) > 0)
			strlcpy(task->msg_detail, edata->detail, MAX_ERR_MSG_LEN);

		FreeErrorData(edata);
	}
	PG_END_TRY();
}

/*
 * A substitute for CHECK_FOR_INTERRUPRS.
 *
 * procsignal_sigusr1_handler does not support signaling from a backend to a
 * non-parallel worker (see the values of ProcSignalReason), so the worker
 * cannot use CHECK_FOR_INTERRUPTS. Let's use shared memory to tell the worker
 * that it should exit.  (SIGTERM would terminate the worker easily, but due
 * to race conditions we could terminate another backend / worker which
 * already managed to reuse this worker's PID.)
 */
void
pg_rewrite_exit_if_requested(void)
{
	bool	exit_requested;

	SpinLockAcquire(&MyWorkerTask->mutex);
	exit_requested = MyWorkerTask->exit_requested;
	SpinLockRelease(&MyWorkerTask->mutex);

	if (!exit_requested)
		return;

	/*
	 * There seems to be no automatic cleanup of the origin, so do it here.
	 * The insertion into the ReplicationOriginRelationId catalog will be
	 * rolled back due to the transaction abort.
	 */
	if (replorigin_session_origin != InvalidRepOriginId)
		replorigin_session_origin = InvalidRepOriginId;

	/*
	 * Message similar to that in ProcessInterrupts(), but ERROR is
	 * sufficient here. rewrite_worker_main() should catch it.
	 */
	ereport(ERROR,
			(errcode(ERRCODE_ADMIN_SHUTDOWN),
			 errmsg("terminating pg_rewrite background worker due to administrator command")));
}

/*
 * Perform the rewriting.
 *
 * The function is executed by a background worker. We do not catch ERRORs
 * here, they will simply make the worker rollback any transaction and exit.
 */
static void
rewrite_table_impl(char *relschema_src, char *relname_src,
				   char *relname_new, char *relschema_dst,
				   char *relname_dst)
{
	RangeVar   *relrv;
	Relation	rel_src,
				rel_dst;
	Oid			ident_idx_src;
	Oid			relid_src;
	Relation	ident_index = NULL;
	ScanKey		ident_key;
	TupleTableSlot	*ind_slot = NULL;
	int			i,
				ident_key_nentries = 0;
	LogicalDecodingContext *ctx;
	ReplicationSlot *slot;
	Snapshot	snap_hist;
	CatalogState *cat_state;
	XLogRecPtr	end_of_wal;
	XLogRecPtr	xlog_insert_ptr;
	int			nindexes;
	Oid		   *indexes_src = NULL;
	bool		invalid_index = false;
	IndexCatInfo *ind_info;
	bool		source_finalized;
	Relation   *parts_dst = NULL;
	int		nparts;
	partitions_hash *partitions = NULL;
	TupleConversionMapExt *conv_map;
	EState	   *estate;
	ModifyTableState *mtstate;
	struct PartitionTupleRouting *proute = NULL;

	relrv = makeRangeVar(relschema_src, relname_src, -1);
	rel_src = table_openrv(relrv, AccessShareLock);

	check_prerequisites(rel_src);

	/*
	 * Retrieve the useful info while holding lock on the relation.
	 */
	ident_idx_src = RelationGetReplicaIndex(rel_src);

	/* The table can have PK although the replica identity is FULL. */
	if (ident_idx_src == InvalidOid && rel_src->rd_pkindex != InvalidOid)
		ident_idx_src = rel_src->rd_pkindex;

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
	/*
	 * TODO Consider
	 * https://github.com/cybertec-postgresql/pg_squeeze/issues/84 (also
	 * update README).
	 */
	if (!OidIsValid(ident_idx_src))
		ereport(ERROR,
				(errcode(ERRCODE_UNIQUE_VIOLATION),
				 (errmsg("Table \"%s\" has no identity index",
						 relname_src))));

	relid_src = RelationGetRelid(rel_src);

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

	ctx = setup_decoding(relid_src);

	/*
	 * The destination table should not be accessed by anyone during our
	 * processing. We're especially worried about DDLs because those might
	 * make us crash during data insertion. So lock the table in exclusive
	 * mode. Thus the checks for catalog changes we perform below stay valid
	 * until the processing is done.
	 *
	 * This cannot be done before the call of setup_decoding() as the
	 * exclusive lock does assign XID. (setup_decoding() would then wait for
	 * our transaction to complete.)
	 */
	relrv = makeRangeVar(relschema_dst, relname_dst, -1);
	rel_dst = table_openrv(relrv, AccessExclusiveLock);

	/*
	 * If the destination table is temporary, user probably messed things up
	 * and a lot of data would be lost at the end of the session. Unlogged
	 * table might be o.k. but let's allow only permanent so far.
	 */
	if (!RelationIsPermanent(rel_dst))
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("\"%s\" is not a permanent table", relname_dst)));

	/*
	 * Now that we have locked rel_dst, get its tuple descriptor.
	 */
	((DecodingOutputState *) ctx->output_writer_private)->tupdesc =
		RelationGetDescr(rel_dst);

	/*
	 * Build a "historic snapshot", i.e. one that reflect the table state at
	 * the moment the snapshot builder reached SNAPBUILD_CONSISTENT state.
	 */
	snap_hist = build_historic_snapshot(ctx->snapshot_builder);

	/* The source relation will be needed for the initial load. */
	rel_src = table_open(relid_src, AccessShareLock);

	/*
	 * Create a conversion map so that we can handle difference(s) in the
	 * tuple descriptor. Note that a copy is passed to 'indesc' since the map
	 * contains a tuple slot based on this descriptor and since 'rel_src' will
	 * be closed and re-opened (in order to acquire AccessExclusiveLock)
	 * before the last use of 'conv_map'.
	 */
	/*
	 * TODO Say in documentation that the destination table must have the same
	 * constraints as the source. (Also explain how to treat FK constraints.)
	 */
	conv_map = convert_tuples_by_name_ext(CreateTupleDescCopy(RelationGetDescr(rel_src)),
										  RelationGetDescr(rel_dst));

	/*
	 * Are we going to route the data into partitions?
	 */
	if (rel_dst->rd_rel->relkind == RELKIND_PARTITIONED_TABLE)
		partitions = get_partitions(rel_src, rel_dst, &nparts, &parts_dst,
									&ident_key, &ident_key_nentries);
	else
	{
		ident_index = get_identity_index(rel_dst, rel_src);
		ident_key = build_identity_key(ident_index, &ident_key_nentries);
		ind_slot = table_slot_create(rel_dst, NULL);
	}
	Assert(ident_key_nentries > 0);

	/*
	 * We need to know whether no DDL took place that allows for data
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
	if (partitions)
	{
#if PG_VERSION_NUM >= 140000
		proute = ExecSetupPartitionTupleRouting(estate, rel_dst);
#else
		proute = ExecSetupPartitionTupleRouting(estate, mtstate, rel_dst);
#endif
	}

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
     * During testing, wait for another backend to perform concurrent data
     * changes which we will process below.
     */
    INJECTION_POINT("pg_rewrite-before-lock");

	/*
	 * Flush all WAL records inserted so far (possibly except for the last
	 * incomplete page, see GetInsertRecPtr), to minimize the amount of data
	 * we need to flush while holding exclusive lock on the source table.
	 */
	xlog_insert_ptr = GetInsertRecPtr();
	XLogFlush(xlog_insert_ptr);

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
										  ident_index,
										  ind_slot,
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
								ident_index, ind_slot,
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
	RenameRelationInternal(relid_src, relname_new, false, false);

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

	if (partitions)
	{
		close_partitions(partitions);

		for (i = 0; i < nparts; i++)
			table_close(parts_dst[i], AccessExclusiveLock);
		pfree(parts_dst);
	}
	else
	{
		index_close(ident_index, AccessShareLock);
		ExecDropSingleTupleTableSlot(ind_slot);
	}

	if (nindexes > 0)
		pfree(indexes_src);

	/* State not needed anymore. */
	free_catalog_state(cat_state);

	if (proute)
	{
		ExecCleanupTupleRouting(mtstate, proute);
		pfree(proute);
	}
	free_modify_table_state(mtstate);
	FreeExecutorState(estate);
	if (conv_map)
		free_conversion_map_ext(conv_map);
}

/*
 * Check that both relations have matching identity indexes and return the
 * identity index of 'rel_dst'.
 */
static Relation
get_identity_index(Relation rel_dst, Relation rel_src)
{
	Oid			index_dst_oid, index_src_oid;
	Relation	index_dst, index_src;
	TupleDesc	tupdesc_dst, tupdesc_src;
	bool		match = true;

	index_dst_oid = RelationGetReplicaIndex(rel_dst);
	if (!OidIsValid(index_dst_oid))
		elog(ERROR, "Identity index missing on table \"%s\"",
			 RelationGetRelationName(rel_dst));
	index_dst = index_open(index_dst_oid, AccessShareLock);
	tupdesc_dst = RelationGetDescr(index_dst);

	index_src_oid = RelationGetReplicaIndex(rel_src);
	if (!OidIsValid(index_src_oid))
		elog(ERROR, "Identity index missing on table \"%s\"",
			 RelationGetRelationName(rel_src));
	index_src = index_open(index_src_oid, AccessShareLock);
	tupdesc_src = RelationGetDescr(index_src);

	/*
	 * The tuple descriptors might not be equal, since some attributes can
	 * have different types. What should match though is attribute names and
	 * their order.
	 */
	if (tupdesc_src->natts != tupdesc_dst->natts)
		match = false;
	else
	{
		for (int i = 0; i < tupdesc_src->natts; i++)
		{
			Form_pg_attribute att_src = TupleDescAttr(tupdesc_src, i);
			Form_pg_attribute att_dst = TupleDescAttr(tupdesc_dst, i);

			/* Indexes should not have dropped attributes. */
			Assert(!att_src->attisdropped);
			Assert(!att_dst->attisdropped);

			if (strcmp(NameStr(att_src->attname), NameStr(att_dst->attname)) != 0)
			{
				match = false;
				break;
			}
		}
	}

	if (!match)
		elog(ERROR,
			 "identity index on table \"%s\" does not match that on table \"%s\"",
			 RelationGetRelationName(rel_dst), RelationGetRelationName(rel_src));

	index_close(index_src, AccessShareLock);

	return index_dst;
}

/*
 * Retrieve information needed to apply DML commands to partitioned table.
 */
static partitions_hash *
get_partitions(Relation rel_src, Relation rel_dst, int *nparts,
			   Relation **parts_dst_p, ScanKey *ident_key_p,
			   int *ident_key_nentries)
{
	partitions_hash *partitions;
	Relation   *parts_dst;
	ScanKey		ident_key = NULL;
	PartitionDesc part_desc;

#if PG_VERSION_NUM >= 140000
	part_desc = RelationGetPartitionDesc(rel_dst, true);
#else
	part_desc = RelationGetPartitionDesc(rel_dst);
#endif
	if (part_desc->nparts == 0)
		ereport(ERROR,
				(errmsg("table \"%s\" has no partitions",
						RelationGetRelationName(rel_dst))));

	/*
	 * It's probably not necessary to lock the partitions in exclusive mode,
	 * but we'll need to open them later. Simply use the exclusive lock
	 * instead of trying to determine the minimum lock level needed.
	 */
	parts_dst = (Relation *) palloc(part_desc->nparts * sizeof(Relation));
	for (int i = 0; i < part_desc->nparts; i++)
		parts_dst[i] = table_open(part_desc->oids[i],
								  AccessExclusiveLock);

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
	for (int i = 0; i < part_desc->nparts; i++)
	{
		Relation	partition = parts_dst[i];
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

		entry->ident_index = get_identity_index(partition, rel_src);
		entry->ind_slot = table_slot_create(partition, NULL);
		entry->conv_map =
			convert_tuples_by_name_ext(RelationGetDescr(rel_dst),
									   RelationGetDescr(partition));
		/* Expect many insertions. */
		entry->bistate = GetBulkInsertState();

		/*
		 * Build scan key that we'll use to look for rows to be updated /
		 * deleted during logical decoding.
		 *
		 * As all the partitions have the same definition of the identity
		 * index, there should only be a single identity key.
		 */
		if (ident_key == NULL)
		{
			ident_key = build_identity_key(entry->ident_index,
										   ident_key_nentries);
			*ident_key_p = ident_key;
		}
	}

	*nparts = part_desc->nparts;
	*parts_dst_p = parts_dst;

	return partitions;
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
				 errmsg("the source table may not be partitioned")));

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
	CheckTableNotInUse(rel, "rewrite_table()");
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
setup_decoding(Oid relid)
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
	 * In order to be able to run partition_table() for multiple tables at a
	 * time, slot name should contain both database OID and relation OID.
	 */
	buf = makeStringInfo();
	appendStringInfoString(buf, REPL_SLOT_BASE_NAME);
	appendStringInfo(buf, "%u_%u", MyDatabaseId, relid);
#if PG_VERSION_NUM >= 170000
	ReplicationSlotCreate(buf->data, true, RS_EPHEMERAL, false, false, false);
#elif PG_VERSION_NUM >= 140000
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

	XLByteToSeg(ctx->reader->EndRecPtr, rewrite_current_segment,
				wal_segment_size);

	/*
	 * Setup structures to store decoded changes.
	 */
	dstate = palloc0(sizeof(DecodingOutputState));
	dstate->relid = relid;
	dstate->tstore = tuplestore_begin_heap(false, false,
										   maintenance_work_mem);

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
 * Create ModifyTableState and do the minimal initialization so that
 * ExecFindPartition() works.
 */
static ModifyTableState *
get_modify_table_state(EState *estate, Relation rel, CmdType operation)
{
	ModifyTableState *result = makeNode(ModifyTableState);
	ResultRelInfo *rri = makeNode(ResultRelInfo);

	InitResultRelInfo(rri, rel, 0, NULL, 0);
	ExecOpenIndices(rri, false);

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
	ExecCloseIndices(mtstate->resultRelInfo);
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
					 TupleConversionMapExt *conv_map)
{
	int			batch_size,
				batch_max_size;
	Size		tuple_array_size;
	bool		tuple_array_can_expand = true;
	TableScanDesc heap_scan;
	TupleTableSlot *slot_src,
			   *slot_dst;
	HeapTuple  *tuples = NULL;
	BulkInsertState	bistate_nonpart = NULL;
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
	 * If the table is partitioned, each partition has a separate instance of
	 * BulkInsertState. Otherwise we need to allocate one here.
	 */
	if (proute == NULL)
		bistate_nonpart = GetBulkInsertState();

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

			pg_rewrite_exit_if_requested();

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
			Relation	rel_ins;
			List	   *recheck;
			BulkInsertState bistate;

			pg_rewrite_exit_if_requested();

			if (i == batch_size)
				tup_out = NULL;
			else
				tup_out = tuples[i++];

			if (tup_out == NULL)
				break;

			/*
			 * If needed, convert the tuple so it matches the destination
			 * table.
			 */
			if (conv_map)
				tup_out = convert_tuple_for_dest_table(tup_out, conv_map);
			ExecStoreHeapTuple(tup_out, slot_dst, false);

			if (proute)
			{
				PartitionEntry *entry;

				/* Find out which partition the tuple belongs to. */
				rri = ExecFindPartition(mtstate, mtstate->rootResultRelInfo,
										proute, slot_dst, estate);

				rel_ins = rri->ri_RelationDesc;
				entry = get_partition_entry(partitions,
											RelationGetRelid(rri->ri_RelationDesc));
				bistate = entry->bistate;

				/*
				 * Make sure the tuple matches the partition. The typical
				 * problem we address here is that a partition was attached
				 * that has a different order of columns.
				 */
				if (entry->conv_map)
				{
					tup_out = convert_tuple_for_dest_table(tup_out,
														   entry->conv_map);
					ExecClearTuple(slot_dst);
					ExecStoreHeapTuple(tup_out, slot_dst, false);
				}
			}
			else
			{
				/* Non-partitioned table. */
				rri = mtstate->resultRelInfo;
				rel_ins = rel_dst;
				bistate = bistate_nonpart;
			}

			/*
			 * Insert the tuple into the relation (or partition).
			 *
			 * XXX Should this happen outside load_cxt? Currently "bistate" is
			 * a flat object (i.e. it does not point to any memory chunk that
			 * the previous call of table_tuple_insert() might have allocated)
			 * and thus the cleanup between batches should not damage it, but
			 * can't it get more complex in future PG versions?
			 */
			Assert(bistate != NULL);
			table_tuple_insert(rel_ins, slot_dst, GetCurrentCommandId(true),
							   0, bistate);

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
											false, /* noDupErr */
											NULL,  /* specConflict */
											NIL	   /* arbiterIndexes */
#if PG_VERSION_NUM >= 160000
											, false /* onlySummarizing */
#endif
				);
			ExecClearTuple(slot_dst);

			/*
			 * If recheck is required, it must have been preformed on the
			 * source relation by now. (All the logical changes we process
			 * here are already committed.)
			 */
			list_free(recheck);
			pfree(tup_out);

			/* Update the progress information. */
			SpinLockAcquire(&MyWorkerTask->mutex);
			MyWorkerTask->progress.ins_initial++;
			SpinLockRelease(&MyWorkerTask->mutex);
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
	if (bistate_nonpart)
		FreeBulkInsertState(bistate_nonpart);

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
					Relation ident_index,
					TupleTableSlot	*ind_slot,
					CatalogState *cat_state,
					LogicalDecodingContext *ctx,
					partitions_hash *partitions,
					TupleConversionMapExt *conv_map)
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
													ident_index,
													ind_slot,
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
											  ident_index,
											  ind_slot,
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
		if (entry->conv_map)
			free_conversion_map_ext(entry->conv_map);
		FreeBulkInsertState(entry->bistate);
	}

	partitions_destroy(partitions);
}

/*
 * Find hash entry for given partition.
 */
PartitionEntry *
get_partition_entry(partitions_hash *partitions, Oid part_oid)
{
	PartitionEntry *entry;

	entry = partitions_lookup(partitions, part_oid);
	if (entry == NULL)
		elog(ERROR, "bulk insert state not found for partition %u", part_oid);
	Assert(entry->part_oid == part_oid);

	return entry;
}

/*
 * Like make_attrmap() in PG core, but return AttrMapExt.
 */
static AttrMapExt *
make_attrmap_ext(int maplen)
{
	AttrMapExt    *res;

	res = (AttrMapExt *) palloc0(sizeof(AttrMapExt));
	res->maplen = maplen;
	res->attnums = (AttrNumber *) palloc0(sizeof(AttrNumber) * maplen);
	res->coerceExprs = palloc0_array(Node *, maplen);
	return res;
}

static void
free_attrmap_ext(AttrMapExt *map)
{
	pfree(map->attnums);
	pfree(map->coerceExprs);
	pfree(map);
}


/*
 * Like convert_tuples_by_name() in PG core, but try to coerce if the input
 * and output types differ.
 */
static TupleConversionMapExt *
convert_tuples_by_name_ext(TupleDesc indesc,
						   TupleDesc outdesc)
{
	AttrMapExt    *attrMap;

	/* Verify compatibility and prepare attribute-number map */
	attrMap = build_attrmap_by_name_if_req_ext(indesc, outdesc);

	if (attrMap == NULL)
	{
		/* runtime conversion is not needed */
		return NULL;
	}

	return convert_tuples_by_name_attrmap_ext(indesc, outdesc, attrMap);
}

/*
 * Like build_attrmap_by_name_if_req() in PG core, but try to coerce if the
 * input and output types differ.
 */
static AttrMapExt *
build_attrmap_by_name_if_req_ext(TupleDesc indesc,
								 TupleDesc outdesc)
{
	AttrMapExt    *attrMap;

	/* Verify compatibility and prepare attribute-number map */
	attrMap = build_attrmap_by_name_ext(indesc, outdesc);

	/*
	 * Check if the map has a one-to-one match and if there's any coercion.
	 */
	if (check_attrmap_match_ext(indesc, outdesc, attrMap))
	{
		/* Runtime conversion is not needed */
		free_attrmap_ext(attrMap);
		return NULL;
	}

	return attrMap;
}

/*
 * Like build_attrmap_by_name() in PG core but try to coerce if the input and
 * output types differ.
 */
static AttrMapExt *
build_attrmap_by_name_ext(TupleDesc indesc,
						  TupleDesc outdesc)
{
	AttrMapExt    *attrMap;
	int			outnatts;
	int			innatts;
	int			i;
	int			nextindesc = -1;

	outnatts = outdesc->natts;
	innatts = indesc->natts;

	attrMap = make_attrmap_ext(outnatts);
	for (i = 0; i < outnatts; i++)
	{
		Form_pg_attribute outatt = TupleDescAttr(outdesc, i);
		char	   *attname;
		Oid			atttypid;
		int32		atttypmod;
		int			j;

		if (outatt->attisdropped)
			continue;			/* attrMap->attnums[i] is already 0 */
		attname = NameStr(outatt->attname);
		atttypid = outatt->atttypid;
		atttypmod = outatt->atttypmod;

		/*
		 * Now search for an attribute with the same name in the indesc. It
		 * seems likely that a partitioned table will have the attributes in
		 * the same order as the partition, so the search below is optimized
		 * for that case.  It is possible that columns are dropped in one of
		 * the relations, but not the other, so we use the 'nextindesc'
		 * counter to track the starting point of the search.  If the inner
		 * loop encounters dropped columns then it will have to skip over
		 * them, but it should leave 'nextindesc' at the correct position for
		 * the next outer loop.
		 */
		for (j = 0; j < innatts; j++)
		{
			Form_pg_attribute inatt;

			nextindesc++;
			if (nextindesc >= innatts)
				nextindesc = 0;

			inatt = TupleDescAttr(indesc, nextindesc);
			if (inatt->attisdropped)
				continue;
			if (strcmp(attname, NameStr(inatt->attname)) == 0)
			{
				/* Found it, check type */
				if (atttypid != inatt->atttypid || atttypmod != inatt->atttypmod)
				{
					Node	*expr;
					ParseState *pstate = make_parsestate(NULL);

					/*
					 * Can the input attribute be coerced to the output one?
					 *
					 * TODO Hard-wire data types for which we are sure that FK
					 * validation can be skipped.
					 *
					 * XXX Currently we follow ATPrepAlterColumnType() in PG
					 * core - should anything be different?
					 */
					expr = (Node *) makeVar(1, inatt->attnum,
											inatt->atttypid, inatt->atttypmod,
											inatt->attcollation,
											0);
					expr = coerce_to_target_type(pstate,
												 expr, exprType(expr),
												 outatt->atttypid,
												 outatt->atttypmod,
												 COERCION_ASSIGNMENT,
												 COERCE_IMPLICIT_CAST,
												 -1);

					if (expr)
					{
						assign_expr_collations(pstate, expr);
						attrMap->coerceExprs[i] = expr;
					}
					else
						ereport(ERROR,
								(errcode(ERRCODE_DATATYPE_MISMATCH),
								 errmsg("could not convert row type"),
								 errdetail("Attribute \"%s\" of type %s does not match corresponding attribute of type %s.",
										   attname,
										   format_type_be(outdesc->tdtypeid),
										   format_type_be(indesc->tdtypeid))));
				}
				attrMap->attnums[i] = inatt->attnum;
				break;
			}
		}
		if (attrMap->attnums[i] == 0)
			ereport(ERROR,
					(errcode(ERRCODE_DATATYPE_MISMATCH),
					 errmsg("could not convert row type"),
					 errdetail("Attribute \"%s\" of type %s does not exist in type %s.",
							   attname,
							   format_type_be(outdesc->tdtypeid),
							   format_type_be(indesc->tdtypeid))));
	}
	return attrMap;
}

/*
 * check_attrmap_match() copied from PG core and adjusted so it takes coercion
 * into account.
 */
static bool
check_attrmap_match_ext(TupleDesc indesc,
						TupleDesc outdesc,
						AttrMapExt *attrMap)
{
	int			i;

	/* no match if attribute numbers are not the same */
	if (indesc->natts != outdesc->natts)
		return false;

	/* no match if there is at least one coercion expression */
	for (i = 0; i < attrMap->maplen; i++)
	{
		if (attrMap->coerceExprs[i])
			return false;
	}

	for (i = 0; i < attrMap->maplen; i++)
	{
		Form_pg_attribute inatt = TupleDescAttr(indesc, i);
		Form_pg_attribute outatt = TupleDescAttr(outdesc, i);

		/*
		 * If the input column has a missing attribute, we need a conversion.
		 */
		if (inatt->atthasmissing)
			return false;

		if (attrMap->attnums[i] == (i + 1))
			continue;

		/*
		 * If it's a dropped column and the corresponding input column is also
		 * dropped, we don't need a conversion.  However, attlen and attalign
		 * must agree.
		 */
		if (attrMap->attnums[i] == 0 &&
			inatt->attisdropped &&
			inatt->attlen == outatt->attlen &&
			inatt->attalign == outatt->attalign)
			continue;

		return false;
	}

	return true;
}

/*
 * Like convert_tuples_by_name_attrmap() but handle coerce expressions.
 */
static TupleConversionMapExt *
convert_tuples_by_name_attrmap_ext(TupleDesc indesc,
								   TupleDesc outdesc,
								   AttrMapExt *attrMap)
{
	int			n = outdesc->natts;
	TupleConversionMapExt *map;
	EState *estate;

	Assert(attrMap != NULL);

	/* Prepare the map structure */
	map = (TupleConversionMapExt *) palloc(sizeof(TupleConversionMapExt));
	map->indesc = indesc;
	map->outdesc = outdesc;
	map->attrMap = attrMap;
	/* preallocate workspace for Datum arrays */
	map->outvalues = (Datum *) palloc(n * sizeof(Datum));
	map->outisnull = (bool *) palloc(n * sizeof(bool));
	n = indesc->natts + 1;		/* +1 for NULL */
	map->invalues = (Datum *) palloc(n * sizeof(Datum));
	map->inisnull = (bool *) palloc(n * sizeof(bool));
	map->invalues[0] = (Datum) 0;	/* set up the NULL entry */
	map->inisnull[0] = true;

	map->coerceExprs = (ExprState **) palloc0_array(ExprState *, n);
	estate = CreateExecutorState();
	for (int i = 0; i < outdesc->natts; i++)
	{
		Expr	*expr = (Expr *) attrMap->coerceExprs[i];

		if (expr)
			map->coerceExprs[i] = ExecPrepareExpr(expr, estate);
	}
	map->estate = estate;
	map->in_slot = MakeSingleTupleTableSlot(indesc, &TTSOpsHeapTuple);

	return map;
}

/*
 * execute_attr_map_tuple() copied from PG core and adjusted to handle coerce
 * expressions.
 */
HeapTuple
pg_rewrite_execute_attr_map_tuple(HeapTuple tuple, TupleConversionMapExt *map)
{
	AttrMapExt    *attrMap = map->attrMap;
	Datum	   *invalues = map->invalues;
	bool	   *inisnull = map->inisnull;
	Datum	   *outvalues = map->outvalues;
	bool	   *outisnull = map->outisnull;
	int			i;
	ExprContext *ecxt;

	/*
	 * Extract all the values of the old tuple, offsetting the arrays so that
	 * invalues[0] is left NULL and invalues[1] is the first source attribute;
	 * this exactly matches the numbering convention in attrMap.
	 */
	heap_deform_tuple(tuple, map->indesc, invalues + 1, inisnull + 1);

	/* Prepare for evaluation of coercion expressions. */
	ResetPerTupleExprContext(map->estate);
	ecxt = GetPerTupleExprContext(map->estate);
	ExecStoreHeapTuple(tuple, map->in_slot, false);
	ecxt->ecxt_scantuple = map->in_slot;

	/*
	 * Transpose into proper fields of the new tuple.
	 */
	Assert(attrMap->maplen == map->outdesc->natts);
	for (i = 0; i < attrMap->maplen; i++)
	{
		int			j = attrMap->attnums[i];
		ExprState	*coerceExpr = map->coerceExprs[i];

		if (coerceExpr == NULL)
		{
			/* Simply copy the value. */
			outvalues[i] = invalues[j];
			outisnull[i] = inisnull[j];
		}
		else
		{
			/* Perform the coercion. */
			outvalues[i] = ExecEvalExprSwitchContext(coerceExpr, ecxt,
													 &outisnull[i]);
		}
	}

	/* Cleanup. */
	ExecClearTuple(map->in_slot);

	/*
	 * Now form the new tuple.
	 */
	return heap_form_tuple(map->outdesc, outvalues, outisnull);
}

/*
 * free_conversion_map() copied from PG core and adjusted to handle coerce
 * expressions.
 */
static void
free_conversion_map_ext(TupleConversionMapExt *map)
{
	/* indesc and outdesc are not ours to free */
	free_attrmap_ext(map->attrMap);
	pfree(map->invalues);
	pfree(map->inisnull);
	pfree(map->outvalues);
	pfree(map->outisnull);
	FreeExecutorState(map->estate);
	ExecDropSingleTupleTableSlot(map->in_slot);
	pfree(map);
}


#define	TASK_LIST_RES_ATTRS	9

/* Get information on squeeze workers on the current database. */
PG_FUNCTION_INFO_V1(pg_rewrite_get_task_list);
Datum
pg_rewrite_get_task_list(PG_FUNCTION_ARGS)
{
	WorkerTask *tasks,
			   *dst;
	int			i,
				ntasks = 0;
#if PG_VERSION_NUM >= 150000
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;

	InitMaterializedSRF(fcinfo, 0);
#else
	FuncCallContext *funcctx;
	int			call_cntr,
				max_calls;
	HeapTuple  *tuples;
#endif

	/*
	 * Copy the task information at once.
	 */
	tasks = (WorkerTask *) palloc(MAX_TASKS * sizeof(WorkerTask));
	dst = tasks;
	for (i = 0; i < MAX_TASKS; i++)
	{
		WorkerTask *task = &workerTasks[i];
		Oid		dbid;
		pid_t	pid;

		SpinLockAcquire(&task->mutex);
		dbid = task->dbid;
		pid = task->pid;
		/*
		 * A system call (see memcpy() below) while holding a spinlock is
		 * probably not a good practice.
		 */
		SpinLockRelease(&task->mutex);

		if (dbid == MyDatabaseId && pid != InvalidPid)
		{
			memcpy(dst, task, sizeof(WorkerTask));

			/*
			 * Since we copied the data w/o locking, verify if the task is
			 * still owned by the same backend and the same worker. (In
			 * theory, PID could be reused by another worker by now, but it's
			 * very unlikely and even if that happened, it cannot cause
			 * anything like data corruption.)
			 */
			SpinLockAcquire(&task->mutex);
			if (task->dbid == dbid && task->pid == pid)
			{
				dst++;
				ntasks++;
			}
			SpinLockRelease(&task->mutex);
		}
	}

#if PG_VERSION_NUM >= 150000
	for (i = 0; i < ntasks; i++)
	{
		WorkerTask	*task = &tasks[i];
		TaskProgress *progress = &task->progress;
		Datum		values[TASK_LIST_RES_ATTRS];
		bool		isnull[TASK_LIST_RES_ATTRS];

		memset(isnull, false, TASK_LIST_RES_ATTRS * sizeof(bool));

		if (strlen(NameStr(task->relschema)) > 0)
			values[0] = NameGetDatum(&task->relschema);
		else
			isnull[0] = true;
		values[1] = NameGetDatum(&task->relname);
		if (strlen(NameStr(task->relschema_dst)) > 0)
			values[2] = NameGetDatum(&task->relschema_dst);
		else
			isnull[2] = true;
		values[3] = NameGetDatum(&task->relname_dst);
		values[4] = NameGetDatum(&task->relname_new);

		values[5] = Int64GetDatum(progress->ins_initial);
		values[6] = Int64GetDatum(progress->ins);
		values[7] = Int64GetDatum(progress->upd);
		values[8] = Int64GetDatum(progress->del);

		tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, isnull);
	}

	return (Datum) 0;
#else
	/* Less trivial implementation, to be removed when PG 14 is EOL. */
	if (SRF_IS_FIRSTCALL())
	{
		MemoryContext oldcontext;
		TupleDesc	tupdesc;
		int			ntuples = 0;

		funcctx = SRF_FIRSTCALL_INIT();
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("function returning record called in context "
							"that cannot accept type record")));
		/* XXX Is this necessary? */
		funcctx->attinmeta = TupleDescGetAttInMetadata(tupdesc);

		/* Process only the slots that we really can display. */
		tuples = (HeapTuple *) palloc0(ntasks * sizeof(HeapTuple));
		for (i = 0; i < ntasks; i++)
		{
			WorkerTask *task = &tasks[i];
			TaskProgress *progress = &task->progress;
			Datum	   *values;
			bool	   *isnull;

			values = (Datum *) palloc(TASK_LIST_RES_ATTRS * sizeof(Datum));
			isnull = (bool *) palloc0(TASK_LIST_RES_ATTRS * sizeof(bool));


			if (strlen(NameStr(task->relschema_src)) > 0)
				values[0] = NameGetDatum(&task->relschema_src);
			else
				isnull[0] = true;
			values[1] = NameGetDatum(&task->relname_src);
			if (strlen(NameStr(task->relschema_dst)) > 0)
				values[2] = NameGetDatum(&task->relschema_dst);
			else
				isnull[2] = true;
			values[3] = NameGetDatum(&task->relname_dst);
			values[4] = NameGetDatum(&task->relname_new);

			values[5] = Int64GetDatum(progress->ins_initial);
			values[6] = Int64GetDatum(progress->ins);
			values[7] = Int64GetDatum(progress->upd);
			values[8] = Int64GetDatum(progress->del);

			tuples[ntuples++] = heap_form_tuple(tupdesc, values, isnull);
		}
		funcctx->user_fctx = tuples;
		funcctx->max_calls = ntuples;;

		MemoryContextSwitchTo(oldcontext);
	}

	funcctx = SRF_PERCALL_SETUP();

	call_cntr = funcctx->call_cntr;
	max_calls = funcctx->max_calls;
	tuples = (HeapTuple *) funcctx->user_fctx;

	if (call_cntr < max_calls)
	{
		HeapTuple	tuple = tuples[call_cntr];
		Datum		result;

		result = HeapTupleGetDatum(tuple);
		SRF_RETURN_NEXT(funcctx, result);
	}
	else
		SRF_RETURN_DONE(funcctx);
#endif
}
