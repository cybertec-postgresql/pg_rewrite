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
#include "catalog/pg_depend.h"
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
#include "executor/spi.h"
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
#if PG_VERSION_NUM >= 170000
#include "utils/injection_point.h"
#endif
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/ruleutils.h"
#include "utils/syscache.h"
#include "utils/varlena.h"

PG_MODULE_MAGIC;

#define	REPL_SLOT_BASE_NAME	"pg_rewrite_slot_"
#define	REPL_PLUGIN_NAME	"pg_rewrite"

/*
 * Information needed to set sequences belonging the destination table
 * according to the corresponding sequences of the source table.
 */
typedef struct SequenceValue
{
	NameData	attname;
	int64		last_value;
} SequenceValue;

static void rewrite_table_impl(char *relschema_src, char *relname_src,
							   char *relname_new, char *relschema_dst,
							   char *relname_dst);
static Relation get_identity_index(Relation rel_dst, Relation rel_src);
static partitions_hash *get_partitions(Relation rel_src, Relation rel_dst,
									   int *nparts,
									   Relation **parts_dst_p,
									   ScanKey *ident_key_p,
									   int *ident_key_nentries);
static List *get_sequences(Relation rel);
static List *getOwnedSequences_internal(Oid relid, AttrNumber attnum,
										char deptype);
static void set_sequences(Relation rel, List *seqs_src);

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
static void send_message(WorkerTask *task, int elevel, const char *message,
						 const char *detail);

static void check_prerequisites(Relation rel);
static LogicalDecodingContext *setup_decoding(Relation rel);
static void decoding_cleanup(LogicalDecodingContext *ctx);
static ModifyTableState *get_modify_table_state(EState *estate, Relation rel,
												CmdType operation);
static void free_modify_table_state(ModifyTableState *mtstate);
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
								Relation rel_src,
								ScanKey ident_key,
								int ident_key_nentries,
								Relation ident_index,
								TupleTableSlot *slot_dst_ind,
								LogicalDecodingContext *ctx,
								partitions_hash *ident_indexes,
								TupleConversionMapExt *conv_map);
static void close_partitions(partitions_hash *partitions);

static AttrMapExt *make_attrmap_ext(int maplen);
static void free_attrmap_ext(AttrMapExt *map);
static TupleConversionMapExt *convert_tuples_by_name_ext(TupleDesc indesc,
														 TupleDesc outdesc);
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
static void copy_constraints(Oid relid_dst, const char *relname_dst,
							 Oid relid_src);
static void dump_fk_constraint(HeapTuple tup, Oid relid_dst,
							   const char *relname_dst, Oid relid_src,
							   StringInfo buf);
static void dump_check_constraint(Oid relid_dst, const char *relname_dst,
								  HeapTuple tup, StringInfo buf);
#if PG_VERSION_NUM >= 180000
static void dump_null_constraint(Oid relid_dst, const char *relname_dst,
								 HeapTuple tup, StringInfo buf);
#endif
static void dump_constraint_common(const char *nsp, const char *relname,
								   Form_pg_constraint con, StringInfo buf);
static int decompile_column_index_array(Datum column_index_array, Oid relId,
										StringInfo buf);

/*
 * The maximum time to hold AccessExclusiveLock on the source table during the
 * final processing. Note that it only pg_rewrite_process_concurrent_changes()
 * execution time is included here.
 */
static int			rewrite_max_xlock_time = 0;

#if PG_VERSION_NUM >= 150000
static shmem_request_hook_type prev_shmem_request_hook = NULL;
#endif
static shmem_startup_hook_type prev_shmem_startup_hook = NULL;

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
	task->elevel = -1;

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
	int		elevel = -1;

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
	{
		msg = pstrdup(task->msg);
		elevel = task->elevel;
	}
	if (strlen(task->msg_detail) > 0)
		msg_detail = pstrdup(task->msg_detail);

	release_task(task, false);

	/* Report the worker's ERROR in the backend. */
	if (msg)
	{
		if (msg_detail)
			ereport(elevel, (errmsg("%s", msg),
							 errdetail("%s", msg_detail)));
		else
			ereport(elevel, (errmsg("%s", msg)));
	}

}

/*
 * Send log message from the worker to the backend that launched it.
 *
 * Currently we only copy 'message' and 'detail. More fields can be added to
 * WorkerTask if needed. Another limitation is that if the worker sends
 * multiple messages, the backend only receives the last one.
 *
 * (Ideally we should use the message queue like parallel workers do, but the
 * related PG core functions have some parallel worker specific arguments.)
 */
static void
send_message(WorkerTask *task, int elevel, const char *message,
			 const char *detail)
{
	strlcpy(task->msg, message, MAX_ERR_MSG_LEN);
	task->elevel = elevel;
	if (detail && strlen(detail) > 0)
		strlcpy(task->msg_detail, detail, MAX_ERR_MSG_LEN);
	else
		/*
		 * Message with elevel < ERROR could already have been written here.
		 */
		task->msg_detail[0] = '\0';
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

#if PG_VERSION_NUM >= 170000
		/*
		 * In regression tests, use this injection point to check that
		 * the changes are visible by other transactions.
		 */
		INJECTION_POINT("pg_rewrite-after-commit");
#endif
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

		send_message(task, ERROR, edata->message, edata->detail);

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
	Oid			relid_dst;
	Oid			ident_idx_src;
	Oid			relid_src;
	Relation	ident_index = NULL;
	ScanKey		ident_key;
	TupleTableSlot	*slot_dst_ind = NULL;
	int			i,
				ident_key_nentries = 0;
	LogicalDecodingContext *ctx;
	ReplicationSlot *slot;
	Snapshot	snap_hist;
	XLogRecPtr	end_of_wal;
	XLogRecPtr	xlog_insert_ptr;
	bool		source_finalized;
	Relation   *parts_dst = NULL;
	int		nparts;
	partitions_hash *partitions = NULL;
	TupleConversionMapExt *conv_map;
	EState	   *estate;
	ModifyTableState *mtstate;
	struct PartitionTupleRouting *proute = NULL;
	List	*seqs_src;

	/*
	 * Use ShareUpdateExclusiveLock as it allows DML commands but does block
	 * most of DDLs (including CREATE INDEX).
	 */
	relrv = makeRangeVar(relschema_src, relname_src, -1);
	rel_src = table_openrv(relrv, ShareUpdateExclusiveLock);
	relid_src = RelationGetRelid(rel_src);

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

	/* Prepare for decoding of "concurrent data changes". */
	ctx = setup_decoding(rel_src);

	/*
	 * No one should need to access the destination table during our
	 * processing. We will eventually need AccessExclusiveLock for renaming,
	 * so acquire it right away.
	 *
	 * This should not be done before the call of setup_decoding() as the
	 * exclusive lock does assign XID. (setup_decoding() would then wait for
	 * our transaction to complete.)
	 */
	relrv = makeRangeVar(relschema_dst, relname_dst, -1);
	rel_dst = table_openrv(relrv, AccessExclusiveLock);
	relid_dst = RelationGetRelid(rel_dst);

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
	 * Build a "historic snapshot", i.e. one that reflect the table state at
	 * the moment the snapshot builder reached SNAPBUILD_CONSISTENT state.
	 */
	snap_hist = build_historic_snapshot(ctx->snapshot_builder);

	/*
	 * Create a conversion map so that we can handle difference(s) in the
	 * tuple descriptor. Note that a copy is passed to 'indesc' since the map
	 * contains a tuple slot based on this descriptor and since 'rel_src' will
	 * be closed and re-opened (in order to acquire AccessExclusiveLock)
	 * before the last use of 'conv_map'.
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
		slot_dst_ind = table_slot_create(rel_dst, NULL);
	}

	Assert(ident_key_nentries > 0);

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

#if PG_VERSION_NUM >= 170000
    /*
     * During testing, wait for another backend to perform concurrent data
     * changes which we will process below.
     */
    INJECTION_POINT("pg_rewrite-before-lock");
#endif

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
										  ident_key,
										  ident_key_nentries,
										  ident_index,
										  slot_dst_ind,
										  NoLock,
										  partitions,
										  conv_map,
										  NULL);

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
								rel_src, ident_key, ident_key_nentries,
								ident_index, slot_dst_ind,
								ctx, partitions, conv_map))
		{
			source_finalized = true;
			break;
		}
		else
			elog(DEBUG1,
				 "pg_rewrite: exclusive lock on table %u had to be released.",
				 relid_src);
	}
	if (!source_finalized)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_IN_USE),
				 errmsg("pg_rewrite: \"max_xlock_time\" prevented partitioning from completion")));

	/*
	 * Retrieve information on sequences so that we can eventually set them on
	 * rel_dst.
	 */
	seqs_src = get_sequences(rel_src);

	/* rel_src cache entry is not needed anymore, but the lock is. */
	table_close(rel_src, NoLock);

	/*
	 * Done with decoding.
	 *
	 * XXX decoding_cleanup() frees tup_desc_src, although we've used it not
	 * only for the decoding.
	 */
	decoding_cleanup(ctx);
	ReplicationSlotRelease();

	pfree(ident_key);

	/*
	 * Besides explicitly closing rel_dst, make sure it (and possibly its
	 * partitions) is not referenced indirectly copy_constraints() below runs
	 * ALTER TABLE which in turn does not like leftover relcache references.
	 */
	if (proute)
	{
		ExecCleanupTupleRouting(mtstate, proute);
		pfree(proute);
	}
	if (estate->es_partition_directory)
	{
		DestroyPartitionDirectory(estate->es_partition_directory);
		estate->es_partition_directory = NULL;
	}
	if (partitions)
	{
		close_partitions(partitions);

		for (i = 0; i < nparts; i++)
			table_close(parts_dst[i], AccessExclusiveLock);
		pfree(parts_dst);
	}

	/*
	 * If the source table had sequences, apply their values to the
	 * corresponding sequences of the destination table.
	 */
	if (seqs_src)
	{
		set_sequences(rel_dst, seqs_src);

		list_free_deep(seqs_src);
	}

	/*
	 * The relcache reference is no longer needed, so close it. Unlocking will
	 * will take place at the end of transaction.
	 *
	 * Note that RenameRelationInternal(relid_dst, ...) below will lock the
	 * relation using AccessExclusiveLock mode once more. This lock will also
	 * be released at the end of our transaction.
	 */
	table_close(rel_dst, NoLock);

	/*
	 * Rename the source table so that we can reuse its name (relname_src)
	 * below.
	 *
	 * The lock acquired by perform_final_merge() will be released at the end
	 * of transaction - no need to deal with it here. (The same applies to the
	 * lock acquired by this call.)
	 */
	RenameRelationInternal(relid_src, relname_new, false, false);

	/*
	 * The relation we have just populated will be renamed so it replaces the
	 * original one. Before that, make sure that the previous renaming is
	 * visible so that we can reuse relname_src.
	 */
	CommandCounterIncrement();

	/*
	 * Finally, rename the newly populated relation so it replaces the
	 * original one.
	 *
	 * We have AccessExclusiveLock lock on relid_dst since we opened it first
	 * time and it will be released at the end of transaction (The same
	 * applies to the lock acquired by this call.)
	 */
	RenameRelationInternal(relid_dst, relname_src, false, false);

	/*
	 * Create FK and CHECK constraints on rel_dst (renamed now to relname_src)
	 * according to rel_src (renamed now to relname_new), and mark them NOT
	 * VALID. See the comments of copy_constraints() for details.
	 */
	copy_constraints(relid_dst, relname_src, relid_src);

	/* Cleanup */
	if (partitions == NULL)
	{
		index_close(ident_index, AccessShareLock);
		ExecDropSingleTupleTableSlot(slot_dst_ind);
	}

	free_modify_table_state(mtstate);
	/*
	 * ExecFindPartition() might have pinned tuple descriptors of the
	 * partitions.
	 */
	ExecResetTupleTable(estate->es_tupleTable, true);
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
		entry->slot_ind = table_slot_create(partition, NULL);
		entry->slot =  MakeSingleTupleTableSlot(RelationGetDescr(rel_dst),
												&TTSOpsHeapTuple);
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

/*
 * Return a list of SequenceValue for given relation.
 */
static List *
get_sequences(Relation rel)
{
	Oid		foid;
	FmgrInfo	flinfo;
	TupleDesc	tupdesc;
	List	*result = NIL;
	bool	skipped = false;

	foid = fmgr_internal_function("pg_sequence_last_value");
	Assert(OidIsValid(foid));
	fmgr_info(foid, &flinfo);

	tupdesc = RelationGetDescr(rel);
	for (int i = 0; i < tupdesc->natts; i++)
	{
		Form_pg_attribute attr = TupleDescAttr(tupdesc, i);
		List	*seqlist;
		Oid		seqid;
		LOCAL_FCINFO(fcinfo, 1);
		Datum	last_value;

		Assert(attr->attnum == (i + 1));
		if (attr->attisdropped)
			continue;
		seqlist = getOwnedSequences_internal(RelationGetRelid(rel),
											 attr->attnum, 0);
		/*
		 * Obviously do nothing if there is no sequence for the attribute. If
		 * there are more then one, ignore that attribute too. The latter
		 * probably should not happen, but if it does, we issue a log message
		 * rather than aborting the whole rewrite.
		 */
		if (list_length(seqlist) != 1)
		{
			if (list_length(seqlist) > 1)
				skipped = true;
			continue;
		}

		seqid = linitial_oid(seqlist);

		/*
		 * FunctionCall1() cannot be used here because the function we call
		 * can return NULL.
		 */
		InitFunctionCallInfoData(*fcinfo, &flinfo, 1, InvalidOid, NULL, NULL);
		fcinfo->args[0].value = ObjectIdGetDatum(seqid);
		fcinfo->args[0].isnull = false;
		last_value = FunctionCallInvoke(fcinfo);
		if (!fcinfo->args[0].isnull)
		{
			SequenceValue	*sv = palloc_object(SequenceValue);

			sv->attname = attr->attname;
			sv->last_value = DatumGetInt64(last_value);
			result = lappend(result, sv);
		}
	}

	if (skipped)
		send_message(MyWorkerTask,
					 NOTICE,
					 "could not get sequence value(s) from the source table",
					 NULL);

	return result;
}

/*
 * Copy & pasted from PG core. The problem is that we need to call the
 * function with attnum > 0 and deptype = 0. PG core does not expose function
 * that would do exactly that.
 */
static List *
getOwnedSequences_internal(Oid relid, AttrNumber attnum, char deptype)
{
	List	   *result = NIL;
	Relation	depRel;
	ScanKeyData key[3];
	SysScanDesc scan;
	HeapTuple	tup;

	depRel = table_open(DependRelationId, AccessShareLock);

	ScanKeyInit(&key[0],
				Anum_pg_depend_refclassid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(RelationRelationId));
	ScanKeyInit(&key[1],
				Anum_pg_depend_refobjid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(relid));
	if (attnum)
		ScanKeyInit(&key[2],
					Anum_pg_depend_refobjsubid,
					BTEqualStrategyNumber, F_INT4EQ,
					Int32GetDatum(attnum));

	scan = systable_beginscan(depRel, DependReferenceIndexId, true,
							  NULL, attnum ? 3 : 2, key);

	while (HeapTupleIsValid(tup = systable_getnext(scan)))
	{
		Form_pg_depend deprec = (Form_pg_depend) GETSTRUCT(tup);

		/*
		 * We assume any auto or internal dependency of a sequence on a column
		 * must be what we are looking for.  (We need the relkind test because
		 * indexes can also have auto dependencies on columns.)
		 */
		if (deprec->classid == RelationRelationId &&
			deprec->objsubid == 0 &&
			deprec->refobjsubid != 0 &&
			(deprec->deptype == DEPENDENCY_AUTO || deprec->deptype == DEPENDENCY_INTERNAL) &&
			get_rel_relkind(deprec->objid) == RELKIND_SEQUENCE)
		{
			if (!deptype || deprec->deptype == deptype)
				result = lappend_oid(result, deprec->objid);
		}
	}

	systable_endscan(scan);

	table_close(depRel, AccessShareLock);

	return result;
}

/*
 * Set sequences according to the information retrieved by get_sequences().
 */
static void
set_sequences(Relation rel, List *seqs_src)
{
	Oid		relid = RelationGetRelid(rel);
	Oid		foid;
	FmgrInfo	flinfo;
	ListCell	*lc;
	bool	skipped = false;

	foid = fmgr_internal_function("setval_oid");
	Assert(OidIsValid(foid));
	fmgr_info(foid, &flinfo);

	foreach(lc, seqs_src)
	{
		SequenceValue	*sv = (SequenceValue *) lfirst(lc);
		AttrNumber	attnum;
		List	*seqlist;
		Oid		seqid;

		attnum = get_attnum(relid, NameStr(sv->attname));
		seqlist = getOwnedSequences_internal(relid, attnum, 0);

		/*
		 * Unlike get_sequences(), here we have a problem even if there is no
		 * sequence on the attribute. (Because we know that the sequence
		 * exists on the source table.)
		 */
		if (list_length(seqlist) != 1)
		{
			skipped = true;
			continue;
		}

		seqid = linitial_oid(seqlist);

		FunctionCall2(&flinfo, ObjectIdGetDatum(seqid),
					  Int64GetDatum(sv->last_value));
	}

	if (skipped)
		send_message(MyWorkerTask, NOTICE,
					 "could not identify sequence(s) on the target table",
					 NULL);
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
setup_decoding(Relation rel)
{
	Oid	relid = RelationGetRelid(rel);
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

	/*
	 * Tuple descriptor of the source relation might be needed for decoding.
	 */
	dstate->tupdesc_src = RelationGetDescr(rel);

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
					Relation rel_src,
					ScanKey ident_key,
					int ident_key_nentries,
					Relation ident_index,
					TupleTableSlot *slot_dst_ind,
					LogicalDecodingContext *ctx,
					partitions_hash *partitions,
					TupleConversionMapExt *conv_map)
{
	bool		success;
	XLogRecPtr	xlog_insert_ptr,
				end_of_wal;
	struct timeval t_end;
	struct timeval *t_end_ptr = NULL;
	char		dummy_rec_data = '\0';
	List	*indexes;
	ListCell	*lc;

	/*
	 * Lock the source table exclusively, to finalize the work.
	 */
	LockRelationOid(RelationGetRelid(rel_src), AccessExclusiveLock);

	/*
	 * Lock the indexes too, as ALTER INDEX does not need table lock.
	 */
	indexes = RelationGetIndexList(rel_src);
	foreach(lc, indexes)
		LockRelationOid(lfirst_oid(lc), AccessExclusiveLock);

	if (rewrite_max_xlock_time > 0)
	{
		int64		usec;
		struct timeval t_start;

		gettimeofday(&t_start, NULL);
		/* Add the whole seconds. */
		t_end.tv_sec = t_start.tv_sec + rewrite_max_xlock_time / 1000;
		/* Add the rest, expressed in microseconds. */
		usec = t_start.tv_usec + 1000 * (rewrite_max_xlock_time % 1000);
		/* The number of microseconds could have overflown. */
		t_end.tv_sec += usec / USECS_PER_SEC;
		t_end.tv_usec = usec % USECS_PER_SEC;
		t_end_ptr = &t_end;

		elog(DEBUG1,
			 "pg_rewrite: completion required by %lu.%lu, current time is %lu.%lu.",
			 t_end_ptr->tv_sec, t_end_ptr->tv_usec, t_start.tv_sec,
			 t_start.tv_usec);
	}

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
													ident_key,
													ident_key_nentries,
													ident_index,
													slot_dst_ind,
													AccessExclusiveLock,
													partitions,
													conv_map,
													t_end_ptr);

	if (t_end_ptr)
	{
		struct timeval t_now;

		gettimeofday(&t_now, NULL);
		elog(DEBUG1,
			 "pg_rewrite: concurrent changes processed at %lu.%lu, result: %u",
			 t_now.tv_sec, t_now.tv_usec, success);
	}

	if (!success)
	{
		/* Unlock the relation and indexes. */
		UnlockRelationOid(RelationGetRelid(rel_src), AccessExclusiveLock);

		foreach(lc, indexes)
			UnlockRelationOid(lfirst_oid(lc), AccessExclusiveLock);

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
											  ident_key,
											  ident_key_nentries,
											  ident_index,
											  slot_dst_ind,
											  AccessExclusiveLock,
											  partitions,
											  conv_map,
											  NULL);
	}

	list_free(indexes);
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
		ExecDropSingleTupleTableSlot(entry->slot);
		ExecDropSingleTupleTableSlot(entry->slot_ind);
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
	res->dropped_attr = false;
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
		{
			attrMap->dropped_attr = true;
			continue;			/* attrMap->attnums[i] is already 0 */
		}
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
			{
				attrMap->dropped_attr = true;
				continue;
			}
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

	/*
	 * Dropped attribute in either descriptor makes the function return false,
	 * even if it appears in both descriptors and at the same position. Thus
	 * we (mis)use the map to get rid of the values of the dropped columns.
	 */
	if (attrMap->dropped_attr)
		return false;

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
	ExecClearTuple(map->in_slot);
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

/*
 * Create constraints on "destination relation" according to "source relation"
 * and mark them NOT VALID.
 *
 * Type conversion(s) that we've done during rewriting must not break any
 * constraints on the table. Even though all the tuples we insert (possibly
 * converted) into the destination tuple had to be validated in the source
 * table, we should be careful to say that the validity in the source table
 * implies validity in the destination table. An obvious example is that
 * float-to-int conversion on the FK side of an RI constraint can leave some
 * rows in FK table with no matching rows in the PK table.
 *
 * We don't have to address PK, UNIQUE and EXCLUDE constraints here because
 * these are enforced immediately as we run DMLs on the destination
 * table. Thus we only need to tell the user that he should create these
 * constraints. However, rewrite_table() works at low level and thus it
 * by-passes checking of the other kinds of constraints.
 *
 * One way to address this problem could be to create the FK, CHECK and NOT
 * NULL constraints on the rewritten table *after* the completion of
 * rewrite_table(), but that would leave the table w/o constraints for some
 * time. Moreover, AccessExclusiveLock would be needed for the constraint
 * creation.
 *
 * Another possible approach would be to create the constraints while we're
 * still holding AccessExclusiveLock (around the time we do table
 * renaming). That would never leave the table w/o constraints, but it would
 * still block access to the table for significant time. (Although it'd still
 * be better than regular ALTER TABLE ... ALTER COLUMN ... SET DATA TYPE
 * ... command, because this command holds the AccessExclusiveLock lock during
 * the actual rewriting.)
 *
 * The least disruptive approach is apparently that we create the FK and CHECK
 * constraints on the destination table and mark them NOT VALID, and let the
 * user validate them "manually". The validation only needs
 * ShareUpdateExclusiveLock, which does not block read / write access to the
 * table. (Note that all data changes performed after rewrite_table() has
 * finished are checked even with NOT VALID constraints.)
 *
 * Note on NOT NULL: this constraint cannot be created as NOT VALID in PG <=
 * 17, so the only perfect way to handle this one is to create it after the
 * completion of rewrite_table(). However, as type conversions usually do not
 * change non-NULL value to NULL, it's probably o.k. to create the constraint
 * before running rewrite_table().
 *
 * We actually create the NOT VALID constraints even if there is no type
 * conversion - this is to avoid excessive blocking as explained
 * above. However, in this case we can then safely change the 'convalidated'
 * field of pg_constraint w/o actual validation. The user can also ask
 * rewrite_table() to fake the validation this way if he is confident that
 * particular data type conversion cannot affect validity of any
 * constraints. This is probably true in the (supposedly) most common case of
 * changing data type from integer to bigint. TODO The "fake validation" is
 * yet to be implemented.
 *
 * OID and name of the destination table is passed instead of an open relcache
 * entry because SPI requires the relation to be closed. We expect that the
 * both relations are locked using AccessExclusiveLock mode.
 */
static void
copy_constraints(Oid relid_dst, const char *relname_dst, Oid relid_src)
{
	Relation	rel;
	TableScanDesc scan;
	HeapTuple	tuple;
	StringInfo	buf = makeStringInfo();
	List	*cmds = NIL;
	ListCell	*lc;

	/*
	 * Iterate through all the constraints as we need to check both conrelid
	 * and confrelid (there's no index on the latter).
	 */
	rel = table_open(ConstraintRelationId, AccessShareLock);
	scan = table_beginscan_catalog(rel, 0, NULL);
	while ((tuple = heap_getnext(scan, ForwardScanDirection)) != NULL)
	{
		Form_pg_constraint	con = (Form_pg_constraint) GETSTRUCT(tuple);

		resetStringInfo(buf);

		switch (con->contype)
		{
			case CONSTRAINT_CHECK:
				if (con->conrelid == relid_src)
					dump_check_constraint(relid_dst, relname_dst, tuple, buf);
				break;

			case CONSTRAINT_FOREIGN:
				{
					if (con->conrelid == relid_src ||
						con->confrelid == relid_src)
						dump_fk_constraint(tuple, relid_dst, relname_dst,
										   relid_src, buf);
					break;
				}

#if PG_VERSION_NUM >= 180000
			case CONSTRAINT_NOTNULL:
				{
					if (con->conrelid == relid_src)
						dump_null_constraint(relid_dst, relname_dst, tuple,
											 buf);
					break;
				}
#endif
			default:
				break;
		}

		/* Add the DDL to a list. */
		if (strlen(buf->data) > 0)
		{
			appendStringInfoString(buf, " NOT VALID");

			cmds = lappend(cmds, pstrdup(buf->data));
		}
	}
	table_endscan(scan);
	table_close(rel, AccessShareLock);

	if (cmds == NIL)
		return;

	/* Run the commands. */
	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());
	foreach(lc, cmds)
	{
		char	*cmd = (char *) lfirst(lc);
		int	ret;

		ret = SPI_execute(cmd, false, 0);
		if (ret != SPI_OK_UTILITY)
			ereport(ERROR, (errmsg("command failed: \"%s\"", cmd)));
	}
	PopActiveSnapshot();
	SPI_finish();
	list_free_deep(cmds);
}

static void
dump_fk_constraint(HeapTuple tup, Oid relid_dst, const char *relname_dst,
				   Oid relid_src, StringInfo buf)
{
	Oid	relid_other;
	const char *pkrelname, *pknsp, *fkrelname, *fknsp;
	Form_pg_constraint	con = (Form_pg_constraint) GETSTRUCT(tup);
	Datum		val;
#if PG_VERSION_NUM >= 150000
	bool		isnull;
#endif
	const char *string;

	Assert(con->contype == CONSTRAINT_FOREIGN);

	if (con->conrelid == relid_src)
	{
		/*
		 * ALTER TABLE ... ADD CONSTRAINT ... FOREIGN KEY ... NOT VALID is
		 * currently not supported for partitioned FK table.
		 */
		if (get_rel_relkind(relid_dst) == RELKIND_PARTITIONED_TABLE)
		{
			send_message(MyWorkerTask,
						 NOTICE,
						 "FOREIGN KEY with NOT VALID option cannot be added to partitioned table",
						 NULL);
			return;
		}

		fknsp = get_namespace_name(relid_dst);
		fkrelname = relname_dst;

		relid_other = con->confrelid;
		pknsp = get_namespace_name(relid_other);
		pkrelname = get_rel_name(relid_other);
	}
	else
	{
		Assert(con->confrelid == relid_src);

		/*
		 * Like above, but check the existing FK table (because what we
		 * rewrite now is the PK table.)
		 */
		if (get_rel_relkind(con->conrelid) == RELKIND_PARTITIONED_TABLE)
			return;

		pknsp = get_namespace_name(relid_dst);
		pkrelname = relname_dst;

		relid_other = con->conrelid;
		fknsp = get_namespace_name(relid_other);
		fkrelname = get_rel_name(relid_other);
	}

	dump_constraint_common(fknsp, fkrelname, con, buf);

	/*
	 * The rest is mostly copied from pg_get_constraintdef_worker() in PG
	 * core.
	 */

	/* Start off the constraint definition */
	appendStringInfoString(buf, "FOREIGN KEY (");

	/* Fetch and build referencing-column list */
#if PG_VERSION_NUM >= 160000
	val = SysCacheGetAttrNotNull(CONSTROID, tup,
								 Anum_pg_constraint_conkey);
#else
	{
		bool	isnull;

		val = SysCacheGetAttr(CONSTROID, tup, Anum_pg_constraint_conkey,
							  &isnull);
		Assert(!isnull);
	}
#endif

	decompile_column_index_array(val, con->conrelid, buf);

	/* add foreign relation name */
	appendStringInfo(buf, ") REFERENCES %s(",
					 quote_qualified_identifier(pknsp, pkrelname));

	/* Fetch and build referenced-column list */
#if PG_VERSION_NUM >= 160000
	val = SysCacheGetAttrNotNull(CONSTROID, tup,
								 Anum_pg_constraint_confkey);
#else
	{
		bool	isnull;

		val = SysCacheGetAttr(CONSTROID, tup, Anum_pg_constraint_confkey,
							  &isnull);
		Assert(!isnull);
	}
#endif

	decompile_column_index_array(val, con->confrelid, buf);

	appendStringInfoChar(buf, ')');

	/* Add match type */
	switch (con->confmatchtype)
	{
		case FKCONSTR_MATCH_FULL:
			string = " MATCH FULL";
			break;
		case FKCONSTR_MATCH_PARTIAL:
			string = " MATCH PARTIAL";
			break;
		case FKCONSTR_MATCH_SIMPLE:
			string = "";
			break;
		default:
			elog(ERROR, "unrecognized confmatchtype: %d",
				 con->confmatchtype);
			string = "";	/* keep compiler quiet */
			break;
	}
	appendStringInfoString(buf, string);

	/* Add ON UPDATE and ON DELETE clauses, if needed */
	switch (con->confupdtype)
	{
		case FKCONSTR_ACTION_NOACTION:
			string = NULL;	/* suppress default */
			break;
		case FKCONSTR_ACTION_RESTRICT:
			string = "RESTRICT";
			break;
		case FKCONSTR_ACTION_CASCADE:
			string = "CASCADE";
			break;
		case FKCONSTR_ACTION_SETNULL:
			string = "SET NULL";
			break;
		case FKCONSTR_ACTION_SETDEFAULT:
			string = "SET DEFAULT";
			break;
		default:
			elog(ERROR, "unrecognized confupdtype: %d",
				 con->confupdtype);
			string = NULL;	/* keep compiler quiet */
			break;
	}
	if (string)
		appendStringInfo(buf, " ON UPDATE %s", string);

	switch (con->confdeltype)
	{
		case FKCONSTR_ACTION_NOACTION:
			string = NULL;	/* suppress default */
			break;
		case FKCONSTR_ACTION_RESTRICT:
			string = "RESTRICT";
			break;
		case FKCONSTR_ACTION_CASCADE:
			string = "CASCADE";
			break;
		case FKCONSTR_ACTION_SETNULL:
			string = "SET NULL";
			break;
		case FKCONSTR_ACTION_SETDEFAULT:
			string = "SET DEFAULT";
			break;
		default:
			elog(ERROR, "unrecognized confdeltype: %d",
				 con->confdeltype);
			string = NULL;	/* keep compiler quiet */
			break;
	}
	if (string)
		appendStringInfo(buf, " ON DELETE %s", string);

#if PG_VERSION_NUM >= 150000
	/*
	 * Add columns specified to SET NULL or SET DEFAULT if
	 * provided.
	 */
	val = SysCacheGetAttr(CONSTROID, tup,
						  Anum_pg_constraint_confdelsetcols, &isnull);
	if (!isnull)
	{
		appendStringInfoString(buf, " (");
		decompile_column_index_array(val, con->conrelid, buf);
		appendStringInfoChar(buf, ')');
	}
#endif
}

/*
 * Mostly copied from pg_get_constraintdef_worker() in PG core.
 */
static void
dump_check_constraint(Oid relid_dst, const char *relname_dst, HeapTuple tup,
					  StringInfo buf)
{
	Datum		val;
	char	   *conbin;
	char	   *consrc;
	Node	   *expr;
	List	   *context;
	Form_pg_constraint	con;
	const char	*nsp;

	con = (Form_pg_constraint) GETSTRUCT(tup);

	nsp = get_namespace_name(relid_dst);
	dump_constraint_common(nsp, relname_dst, con, buf);

	/* Fetch constraint expression in parsetree form */
#if PG_VERSION_NUM >= 160000
	val = SysCacheGetAttrNotNull(CONSTROID, tup,
								 Anum_pg_constraint_conbin);
#else
	{
		bool	isnull;

		val = SysCacheGetAttr(CONSTROID, tup, Anum_pg_constraint_conbin,
							  &isnull);
		Assert(!isnull);
	}
#endif

	conbin = TextDatumGetCString(val);
	expr = stringToNode(conbin);

	/* Set up deparsing context for Var nodes in constraint */
	if (con->conrelid != InvalidOid)
	{
		/* relation constraint */
		context = deparse_context_for(get_rel_name(con->conrelid),
									  con->conrelid);
	}
	else
	{
		/* domain constraint --- can't have Vars */
		context = NIL;
	}

	consrc = deparse_expression(expr, context, false, false);

	/*
	 * Now emit the constraint definition, adding NO INHERIT if
	 * necessary.
	 *
	 * There are cases where the constraint expression will be
	 * fully parenthesized and we don't need the outer parens ...
	 * but there are other cases where we do need 'em.  Be
	 * conservative for now.
	 *
	 * Note that simply checking for leading '(' and trailing ')'
	 * would NOT be good enough, consider "(x > 0) AND (y > 0)".
	 */
	appendStringInfo(buf, "CHECK (%s)%s",
					 consrc,
					 con->connoinherit ? " NO INHERIT" : "");
}

#if PG_VERSION_NUM >= 180000
static void
dump_null_constraint(Oid relid_dst, const char *relname_dst,
					 HeapTuple tup, StringInfo buf)
{
	Form_pg_constraint	con = (Form_pg_constraint) GETSTRUCT(tup);
	const char	*nsp;
	AttrNumber	attnum;

	Assert(con->contype == CONSTRAINT_NOTNULL);

	nsp = get_namespace_name(relid_dst);
	dump_constraint_common(nsp, relname_dst, con, buf);

	attnum = extractNotNullColumn(tup);

	appendStringInfo(buf, "NOT NULL %s",
					 quote_identifier(get_attname(con->conrelid,
												  attnum, false)));
	if (((Form_pg_constraint) GETSTRUCT(tup))->connoinherit)
		appendStringInfoString(buf, " NO INHERIT");

}
#endif

static void
dump_constraint_common(const char *nsp, const char *relname,
					   Form_pg_constraint con, StringInfo buf)
{
	NameData	conname_new;
	int		conname_len = strlen(NameStr(con->conname));

	if ((conname_len + 1) == NAMEDATALEN)
		/*
		 * XXX Is it worth generating an unique name in another way? Not sure,
		 * smart user can rename the original constraint.
		 */
		ereport(ERROR,
				(errmsg("constraint name \"%s\" is too long, cannot add suffix",
						NameStr(con->conname))));
	else
	{
		namestrcpy(&conname_new, NameStr(con->conname));
		/* Add '2' as a suffix. */
		NameStr(conname_new)[conname_len] = '2';
	}

	appendStringInfo(buf, "ALTER TABLE %s ADD CONSTRAINT %s ",
					 quote_qualified_identifier(nsp, relname),
					 quote_identifier(NameStr(conname_new)));
}

/*
 * Copied from PG core.
 */
static int
decompile_column_index_array(Datum column_index_array, Oid relId,
							 StringInfo buf)
{
	Datum	   *keys;
	int			nKeys;
	int			j;

	/* Extract data from array of int16 */
#if PG_VERSION_NUM >= 160000
	deconstruct_array_builtin(DatumGetArrayTypeP(column_index_array), INT2OID,
							  &keys, NULL, &nKeys);
#else
	deconstruct_array(DatumGetArrayTypeP(column_index_array), INT2OID,
					  sizeof(int16), true, TYPALIGN_SHORT,
					  &keys, NULL, &nKeys);
#endif

	for (j = 0; j < nKeys; j++)
	{
		char	   *colName;

		colName = get_attname(relId, DatumGetInt16(keys[j]), false);

		if (j == 0)
			appendStringInfoString(buf, quote_identifier(colName));
		else
			appendStringInfo(buf, ", %s", quote_identifier(colName));
	}

	return nKeys;
}
