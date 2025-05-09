/*-----------------------------------------------------------------------------------
 *
 * concurrent.c
 *     Tools for maintenance that requires table rewriting.
 *
 *	   This file handles changes that took place while the data is being
 *	   copied from one table to another one.
 *
 * Copyright (c) 2021-2023, Cybertec PostgreSQL International GmbH
 *
 *-----------------------------------------------------------------------------------
 */


#include "pg_rewrite.h"

#include "access/heaptoast.h"
#include "executor/execPartition.h"
#include "executor/executor.h"
#include "replication/decode.h"
#include "utils/rel.h"

static void apply_concurrent_changes(EState *estate, ModifyTableState *mtstate,
									 struct PartitionTupleRouting *proute,
									 Relation rel_dst,
									 DecodingOutputState *dstate,
									 ScanKey key, int nkeys,
									 Relation ident_index,
									 TupleTableSlot	*ind_slot,
									 partitions_hash *partitions,
									 TupleConversionMapExt *conv_map);
static void apply_insert(Relation rel, HeapTuple tup, TupleTableSlot *slot,
						 EState *estate, ModifyTableState *mtstate,
						 struct PartitionTupleRouting *proute,
						 partitions_hash *partitions,
						 TupleConversionMapExt *conv_map,
						 BulkInsertState bistate);
static void apply_update_or_delete(Relation rel, HeapTuple tup,
								   HeapTuple tup_old,
								   ConcurrentChangeKind change_kind,
								   TupleTableSlot *slot, EState *estate,
								   ScanKey key, int nkeys, Relation ident_index,
								   TupleTableSlot	*ind_slot,
								   ModifyTableState *mtstate,
								   struct PartitionTupleRouting *proute,
								   partitions_hash *partitions,
								   TupleConversionMapExt *conv_map);
static void find_tuple_in_partition(HeapTuple tup, Relation partition,
									partitions_hash *partitions,
									ScanKey key, int nkeys, ItemPointer ctid);
static void find_tuple(HeapTuple tup, Relation rel, Relation ident_index,
					   ScanKey key, int nkeys, ItemPointer ctid,
					   TupleTableSlot *ind_slot);
static bool processing_time_elapsed(struct timeval *utmost);

static void plugin_startup(LogicalDecodingContext *ctx,
						   OutputPluginOptions *opt, bool is_init);
static void plugin_shutdown(LogicalDecodingContext *ctx);
static void plugin_begin_txn(LogicalDecodingContext *ctx,
							 ReorderBufferTXN *txn);
static void plugin_commit_txn(LogicalDecodingContext *ctx,
							  ReorderBufferTXN *txn, XLogRecPtr commit_lsn);
static void plugin_change(LogicalDecodingContext *ctx, ReorderBufferTXN *txn,
						  Relation rel, ReorderBufferChange *change);
static void store_change(LogicalDecodingContext *ctx,
						 ConcurrentChangeKind kind, HeapTuple tuple);
static HeapTuple get_changed_tuple(ConcurrentChange *change);
static bool plugin_filter(LogicalDecodingContext *ctx, RepOriginId origin_id);

/*
 * Decode and apply concurrent changes. If there are too many of them, split
 * the processing into multiple iterations so that the intermediate storage
 * (tuplestore) is not likely to be written to disk.
 *
 * See check_catalog_changes() for explanation of lock_held argument.
 *
 * Returns true if must_complete is NULL or if managed to complete by the time
 * *must_complete indicates.
 */
bool
pg_rewrite_process_concurrent_changes(EState *estate,
									  ModifyTableState *mtstate,
									  struct PartitionTupleRouting *proute,
									  LogicalDecodingContext *ctx,
									  XLogRecPtr end_of_wal,
									  CatalogState *cat_state,
									  Relation rel_dst, ScanKey ident_key,
									  int ident_key_nentries,
									  Relation ident_index,
									  TupleTableSlot *ind_slot,
									  LOCKMODE lock_held,
									  partitions_hash *partitions,
									  TupleConversionMapExt *conv_map,
									  struct timeval *must_complete)
{
	DecodingOutputState *dstate;
	bool		done;

	/*
	 * Some arguments are specific to partitioned table, some to
	 * non-partitioned one. XXX Is some refactoring needed here, such as using
	 * an union?
	 */
	Assert((ident_index && ind_slot && partitions == NULL && proute == NULL) ||
		   (ident_index == NULL && ind_slot == NULL && partitions && proute));

	dstate = (DecodingOutputState *) ctx->output_writer_private;
	done = false;
	while (!done)
	{
		pg_rewrite_exit_if_requested();

		done = pg_rewrite_decode_concurrent_changes(ctx, end_of_wal,
													must_complete);

		if (processing_time_elapsed(must_complete))
			/* Caller is responsible for applying the changes. */
			return false;

		if (dstate->nchanges == 0)
			continue;

		/* Make sure the changes are still applicable. */
		pg_rewrite_check_catalog_changes(cat_state, lock_held);

		/*
		 * XXX Consider if it's possible to check *must_complete and stop
		 * processing partway through. Partial cleanup of the tuplestore seems
		 * non-trivial.
		 */
		apply_concurrent_changes(estate, mtstate, proute, rel_dst,
								 dstate, ident_key, ident_key_nentries,
								 ident_index, ind_slot, partitions, conv_map);
	}

	return true;
}

/*
 * Decode logical changes from the XLOG sequence up to end_of_wal.
 *
 * Returns true iff done (for now), i.e. no more changes below the end_of_wal
 * can be decoded.
 */
bool
pg_rewrite_decode_concurrent_changes(LogicalDecodingContext *ctx,
									 XLogRecPtr end_of_wal,
									 struct timeval *must_complete)
{
	DecodingOutputState *dstate;
	ResourceOwner resowner_old;

	/*
	 * Invalidate the "present" cache before moving to "(recent) history".
	 *
	 * Note: The cache entry of the transient relation is not affected
	 * (because it was created by the current transaction), but the tuple
	 * descriptor shouldn't change anyway (as opposed to index info, which we
	 * change at some point). Moreover, tuples of the transient relation
	 * should not actually be deconstructed: reorderbuffer.c records the
	 * tuples, but - as it never receives the corresponding commit record -
	 * does not examine them in detail.
	 */
	InvalidateSystemCaches();

	dstate = (DecodingOutputState *) ctx->output_writer_private;
	resowner_old = CurrentResourceOwner;
	CurrentResourceOwner = dstate->resowner;

	PG_TRY();
	{
		while (ctx->reader->EndRecPtr < end_of_wal)
		{
			XLogRecord *record;
			XLogSegNo	segno_new;
			char	   *errm = NULL;
			XLogRecPtr	end_lsn;

			record = XLogReadRecord(ctx->reader, &errm);
			if (errm)
				elog(ERROR, "%s", errm);

			if (record != NULL)
				LogicalDecodingProcessRecord(ctx, ctx->reader);

			if (processing_time_elapsed(must_complete))
				break;

			/*
			 * If WAL segment boundary has been crossed, inform PG core that
			 * we no longer need the previous segment.
			 */
			end_lsn = ctx->reader->EndRecPtr;
			XLByteToSeg(end_lsn, segno_new, wal_segment_size);
			if (segno_new != rewrite_current_segment)
			{
				LogicalConfirmReceivedLocation(end_lsn);
				elog(DEBUG1, "pg_rewrite: confirmed receive location %X/%X",
					 (uint32) (end_lsn >> 32), (uint32) end_lsn);
				rewrite_current_segment = segno_new;
			}

			pg_rewrite_exit_if_requested();
		}
		InvalidateSystemCaches();
		CurrentResourceOwner = resowner_old;
	}
	PG_CATCH();
	{
		InvalidateSystemCaches();
		CurrentResourceOwner = resowner_old;
		PG_RE_THROW();
	}
	PG_END_TRY();

	elog(DEBUG1, "pg_rewrite: %.0f changes decoded but not applied yet",
		 dstate->nchanges);

	return ctx->reader->EndRecPtr >= end_of_wal;
}

/*
 * Apply changes that happened during the initial load.
 *
 * Scan key is passed by caller, so it does not have to be constructed
 * multiple times. Key entries have all fields initialized, except for
 * sk_argument.
 */
static void
apply_concurrent_changes(EState *estate, ModifyTableState *mtstate,
						 struct PartitionTupleRouting *proute,
						 Relation rel_dst,
						 DecodingOutputState *dstate,
						 ScanKey key, int nkeys,
						 Relation ident_index,
						 TupleTableSlot	*ind_slot,
						 partitions_hash *partitions,
						 TupleConversionMapExt *conv_map)
{
	TupleTableSlot *slot;
	BulkInsertState	bistate = NULL;
	HeapTuple	tup_old = NULL;

	if (dstate->nchanges == 0)
		return;

	/* See perform_initial_load() */
	if (proute == NULL)
		bistate = GetBulkInsertState();

	/*
	 * TupleTableSlot is needed to pass the tuple to ExecInsertIndexTuples().
	 */
	slot = MakeSingleTupleTableSlot(dstate->tupdesc, &TTSOpsHeapTuple);

	/*
	 * In case functions in the index need the active snapshot and caller
	 * hasn't set one.
	 */
	PushActiveSnapshot(GetTransactionSnapshot());

	while (tuplestore_gettupleslot(dstate->tstore, true, false,
								   dstate->tsslot))
	{
		bool		shouldFree;
		HeapTuple	tup_change,
					tup;
		char	   *change_raw;
		ConcurrentChange *change;
		bool		isnull[1];
		Datum		values[1];

		/* Get the change from the single-column tuple. */
		tup_change = ExecFetchSlotHeapTuple(dstate->tsslot, false, &shouldFree);
		heap_deform_tuple(tup_change, dstate->tupdesc_change, values, isnull);
		Assert(!isnull[0]);

		/* This is bytea, but char* is easier to work with. */
		change_raw = (char *) DatumGetByteaP(values[0]);

		change = (ConcurrentChange *) VARDATA(change_raw);

		tup = get_changed_tuple(change);

		if (change->kind == CHANGE_UPDATE_OLD)
		{
			Assert(tup_old == NULL);
			tup_old = tup;
		}
		else if (change->kind == CHANGE_INSERT)
		{
			Assert(tup_old == NULL);
			apply_insert(rel_dst, tup, slot, estate, mtstate, proute,
						 partitions, conv_map, bistate);
		}
		else if (change->kind == CHANGE_UPDATE_NEW ||
				 change->kind == CHANGE_DELETE)
		{
			apply_update_or_delete(rel_dst, tup, tup_old, change->kind,
								   slot, estate, key, nkeys, ident_index,
								   ind_slot, mtstate, proute, partitions,
								   conv_map);

			/* The function is responsible for freeing. */
			if (tup_old != NULL)
				tup_old = NULL;
		}
		else
			elog(ERROR, "Unrecognized kind of change: %d", change->kind);

		/* If there's any change, make it visible to the next iteration. */
		if (change->kind != CHANGE_UPDATE_OLD)
		{
			CommandCounterIncrement();
			UpdateActiveSnapshotCommandId();
		}

		/* TTSOpsMinimalTuple has .get_heap_tuple==NULL. */
		Assert(shouldFree);
		pfree(tup_change);
	}

	tuplestore_clear(dstate->tstore);
	dstate->nchanges = 0;

	PopActiveSnapshot();

	/* Cleanup. */
	ExecDropSingleTupleTableSlot(slot);
	if (bistate)
		FreeBulkInsertState(bistate);
}

static void
apply_insert(Relation rel, HeapTuple tup, TupleTableSlot *slot,
			 EState *estate, ModifyTableState *mtstate,
			 struct PartitionTupleRouting *proute,
			 partitions_hash *partitions, TupleConversionMapExt *conv_map,
			 BulkInsertState bistate)
{
	List	   *recheck;
	Relation	rel_ins;
	ResultRelInfo *rri = NULL;

	if (conv_map)
		tup = convert_tuple_for_dest_table(tup, conv_map);
	ExecStoreHeapTuple(tup, slot, false);
	if (proute)
	{
		PartitionEntry	*entry;

		/* Which partition does the tuple belong to? */
		rri = ExecFindPartition(mtstate, mtstate->rootResultRelInfo,
								proute, slot, estate);
		rel_ins = rri->ri_RelationDesc;

		entry = get_partition_entry(partitions,
									RelationGetRelid(rel_ins));
		bistate = entry->bistate;

		/*
		 * Make sure the tuple matches the partition. The typical problem we
		 * address here is that a partition was attached that has a different
		 * order of columns.
		 */
		if (entry->conv_map)
		{
			tup = convert_tuple_for_dest_table(tup, entry->conv_map);
			ExecClearTuple(slot);
			ExecStoreHeapTuple(tup, slot, false);
		}
	}
	else
	{
		/* Non-partitioned table. */
		rri = mtstate->resultRelInfo;
		rel_ins = rel;
		/* Use bistate passed by the caller. */
	}
	Assert(bistate != NULL);
	table_tuple_insert(rel_ins, slot, GetCurrentCommandId(true), 0,
					   bistate);

#if PG_VERSION_NUM < 140000
	estate->es_result_relation_info = rri;
#endif
	/* Update indexes. */
	recheck = ExecInsertIndexTuples(
#if PG_VERSION_NUM >= 140000
		rri,
#endif
		slot,
		estate,
#if PG_VERSION_NUM >= 140000
		false,	/* update */
#endif
		false,	/* noDupErr */
		NULL,	/* specConflict */
		NIL		/* arbiterIndexes */
#if PG_VERSION_NUM >= 160000
		, false /* onlySummarizing */
#endif
		);
	ExecClearTuple(slot);

	pfree(tup);

	/*
	 * If recheck is required, it must have been preformed on the source
	 * relation by now. (All the logical changes we process here are already
	 * committed.)
	 */
	list_free(recheck);

	/* Update the progress information. */
	SpinLockAcquire(&MyWorkerTask->mutex);
	MyWorkerTask->progress.ins++;
	SpinLockRelease(&MyWorkerTask->mutex);
}

static void
apply_update_or_delete(Relation rel, HeapTuple tup, HeapTuple tup_old,
					   ConcurrentChangeKind change_kind,
					   TupleTableSlot *slot, EState *estate,
					   ScanKey key, int nkeys, Relation ident_index,
					   TupleTableSlot	*ind_slot,
					   ModifyTableState *mtstate,
					   struct PartitionTupleRouting *proute,
					   partitions_hash *partitions,
					   TupleConversionMapExt *conv_map)
{
	ResultRelInfo *rri, *rri_old = NULL;

	/*
	 * Convert the tuple(s) to match the destination table.
	 */
	if (conv_map)
	{
		tup = convert_tuple_for_dest_table(tup, conv_map);

		if (tup_old)
		{
			Assert(change_kind == CHANGE_UPDATE_NEW);

			tup_old = convert_tuple_for_dest_table(tup_old, conv_map);
		}
	}

	/* Which partition does the tuple belong to? */
	ExecStoreHeapTuple(tup, slot, false);
	rri = ExecFindPartition(mtstate, mtstate->rootResultRelInfo,
							proute, slot, estate);
	ExecClearTuple(slot);

	/* Is this a cross-partition update? */
	if (partitions && change_kind == CHANGE_UPDATE_NEW && tup_old)
	{
		ExecStoreHeapTuple(tup_old, slot, false);
		rri_old = ExecFindPartition(mtstate, mtstate->rootResultRelInfo,
									proute, slot, estate);
		ExecClearTuple(slot);
	}

	if (rri_old &&
		RelationGetRelid(rri_old->ri_RelationDesc) !=
		RelationGetRelid(rri->ri_RelationDesc))
	{
		ItemPointerData ctid;
		List	   *recheck;
		PartitionEntry *entry;

		/*
		 * Cross-partition update. Delete the old tuple from its partition.
		 */
		find_tuple_in_partition(tup_old, rri_old->ri_RelationDesc,
								partitions, key, nkeys, &ctid);
		simple_heap_delete(rri_old->ri_RelationDesc, &ctid);

		/* Update the progress information. */
		SpinLockAcquire(&MyWorkerTask->mutex);
		MyWorkerTask->progress.del++;
		SpinLockRelease(&MyWorkerTask->mutex);

		/*
		 * Insert the new tuple into its partition. This might include
		 * conversion to match the partition, see above.
		 */
		entry = get_partition_entry(partitions,
									RelationGetRelid(rri->ri_RelationDesc));
		if (entry->conv_map)
			tup = convert_tuple_for_dest_table(tup, entry->conv_map);
		ExecStoreHeapTuple(tup, slot, false);
		table_tuple_insert(rri->ri_RelationDesc, slot,
						   GetCurrentCommandId(true), 0, NULL);

#if PG_VERSION_NUM < 140000
		estate->es_result_relation_info = rri;
#endif
		/* Update indexes. */
		recheck = ExecInsertIndexTuples(
#if PG_VERSION_NUM >= 140000
			rri,
#endif
			slot,
			estate,
#if PG_VERSION_NUM >= 140000
			false,	/* update */
#endif
			false,	/* noDupErr */
			NULL,	/* specConflict */
			NIL		/* arbiterIndexes */
#if PG_VERSION_NUM >= 160000
			, false /* onlySummarizing */
#endif
			);
		ExecClearTuple(slot);

		/* Update the progress information. */
		SpinLockAcquire(&MyWorkerTask->mutex);
		MyWorkerTask->progress.ins++;
		SpinLockRelease(&MyWorkerTask->mutex);

		list_free(recheck);
	}
	else
	{
		HeapTuple	tup_key;
		ItemPointerData ctid;

		/*
		 * Both old and new tuple are in the same partition (or the target
		 * table is not partitioned). Find the tuple to be updated or deleted.
		 */
		if (change_kind == CHANGE_UPDATE_NEW)
			tup_key = tup_old != NULL ? tup_old : tup;
		else
		{
			Assert(change_kind == CHANGE_DELETE);
			Assert(tup_old == NULL);
			tup_key = tup;
		}

		if (partitions)
			find_tuple_in_partition(tup_key, rri->ri_RelationDesc,
									partitions, key, nkeys, &ctid);
		else
			find_tuple(tup_key, rel, ident_index, key, nkeys, &ctid,
					   ind_slot);

		if (change_kind == CHANGE_UPDATE_NEW)
		{
#if PG_VERSION_NUM >= 160000
			TU_UpdateIndexes	update_indexes;
#endif

			if (partitions)
			{
				PartitionEntry *entry;

				/*
				 * Make sure the tuple matches the partition.
				 */
				entry = get_partition_entry(partitions,
											RelationGetRelid(rri->ri_RelationDesc));
				if (entry->conv_map)
					tup = convert_tuple_for_dest_table(tup,
													   entry->conv_map);
			}

			simple_heap_update(rri->ri_RelationDesc, &ctid, tup
#if PG_VERSION_NUM >= 160000
							   , &update_indexes
#endif
				);
			if (!HeapTupleIsHeapOnly(tup))
			{
				List	   *recheck;

				ExecStoreHeapTuple(tup, slot, false);

				/*
				 * XXX Consider passing update=true, however it requires
				 * es_range_table to be initialized. Is it worth the
				 * complexity?
				 */
				recheck = ExecInsertIndexTuples(
#if PG_VERSION_NUM >= 140000
					rri,
#endif
					slot,
					estate,
#if PG_VERSION_NUM >= 140000
					false,	/* update */
#endif
					false,	/* noDupErr */
					NULL,	/* specConflict */
					NIL		/* arbiterIndexes */
#if PG_VERSION_NUM >= 160000
					/* onlySummarizing */
					, update_indexes == TU_Summarizing
#endif
					);
				ExecClearTuple(slot);
				list_free(recheck);
			}

			/* Update the progress information. */
			SpinLockAcquire(&MyWorkerTask->mutex);
			MyWorkerTask->progress.upd++;
			SpinLockRelease(&MyWorkerTask->mutex);
		}
		else
		{
			Assert(change_kind == CHANGE_DELETE);

			simple_heap_delete(rri->ri_RelationDesc, &ctid);

			/* Update the progress information. */
			SpinLockAcquire(&MyWorkerTask->mutex);
			MyWorkerTask->progress.del++;
			SpinLockRelease(&MyWorkerTask->mutex);
		}
	}

	pfree(tup);
	if (tup_old)
		pfree(tup_old);
}

/*
 * Find tuple whose identity key is passed as 'tup' in relation 'rel' and put
 * its location into 'ctid'.
 */
static void
find_tuple_in_partition(HeapTuple tup, Relation partition,
						partitions_hash *partitions,
						ScanKey key, int nkeys, ItemPointer ctid)
{
	Oid			part_oid = RelationGetRelid(partition);
	HeapTuple	tup_mapped = NULL;
	PartitionEntry *entry;

	entry = partitions_lookup(partitions, part_oid);
	if (entry == NULL)
		elog(ERROR, "identity index not found for partition %u", part_oid);
	Assert(entry->part_oid == part_oid);

	/*
	 * Make sure the tuple matches the partition.
	 */
	if (entry->conv_map)
	{
		/*
		 * convert_tuple_for_dest_table() is not suitable here because we need
		 * to keep the original tuple. XXX Should we add a boolean argument to
		 * the function that indicates whether it should free the original
		 * tuple?
		 */
		tup_mapped = pg_rewrite_execute_attr_map_tuple(tup,
													   entry->conv_map);
		tup = tup_mapped;
	}
	find_tuple(tup, partition, entry->ident_index, key, nkeys, ctid,
			   entry->ind_slot);
	if (tup_mapped)
		pfree(tup_mapped);
}

/*
 * Find tuple whose identity key is passed as 'tup' in relation 'rel' and put
 * its location into 'ctid'.
 */
static void
find_tuple(HeapTuple tup, Relation rel, Relation ident_index, ScanKey key,
		   int nkeys, ItemPointer ctid, TupleTableSlot *ind_slot)
{
	Form_pg_index ident_form;
	int2vector *ident_indkey;
	IndexScanDesc scan;
	int			i;
	HeapTuple	tup_exist;

	ident_form = ident_index->rd_index;
	ident_indkey = &ident_form->indkey;
	scan = index_beginscan(rel, ident_index, GetActiveSnapshot(), nkeys, 0);
	index_rescan(scan, key, nkeys, NULL, 0);

	/* Use the incoming tuple to finalize the scan key. */
	for (i = 0; i < scan->numberOfKeys; i++)
	{
		ScanKey		entry;
		bool		isnull;
		int16		attno_heap;

		entry = &scan->keyData[i];
		attno_heap = ident_indkey->values[i];
		entry->sk_argument = heap_getattr(tup,
										  attno_heap,
										  rel->rd_att,
										  &isnull);
		Assert(!isnull);
	}
	if (index_getnext_slot(scan, ForwardScanDirection, ind_slot))
	{
		bool		shouldFreeInd;

		tup_exist = ExecFetchSlotHeapTuple(ind_slot, false,
										   &shouldFreeInd);
		/* TTSOpsBufferHeapTuple has .get_heap_tuple != NULL. */
		Assert(!shouldFreeInd);
	}
	else
		tup_exist = NULL;
	if (tup_exist == NULL)
		elog(ERROR, "Failed to find target tuple");
	ItemPointerCopy(&tup_exist->t_self, ctid);
	index_endscan(scan);
}

static bool
processing_time_elapsed(struct timeval *utmost)
{
	struct timeval now;

	if (utmost == NULL)
		return false;

	gettimeofday(&now, NULL);

	if (now.tv_sec < utmost->tv_sec)
		return false;

	if (now.tv_sec > utmost->tv_sec)
		return true;

	return now.tv_usec >= utmost->tv_usec;
}

/*
 * Convert tuple according to the map and free the original one.
 */
HeapTuple
convert_tuple_for_dest_table(HeapTuple tuple,
							 TupleConversionMapExt *conv_map)
{
	HeapTuple	orig = tuple;

	tuple = pg_rewrite_execute_attr_map_tuple(tuple, conv_map);
	pfree(orig);

	return tuple;
}

void
_PG_output_plugin_init(OutputPluginCallbacks *cb)
{
	AssertVariableIsOfType(&_PG_output_plugin_init, LogicalOutputPluginInit);

	cb->startup_cb = plugin_startup;
	cb->begin_cb = plugin_begin_txn;
	cb->change_cb = plugin_change;
	cb->commit_cb = plugin_commit_txn;
	cb->filter_by_origin_cb = plugin_filter;
	cb->shutdown_cb = plugin_shutdown;
}


/* initialize this plugin */
static void
plugin_startup(LogicalDecodingContext *ctx, OutputPluginOptions *opt,
			   bool is_init)
{
	ctx->output_plugin_private = NULL;

	/* Probably unnecessary, as we don't use the SQL interface ... */
	opt->output_type = OUTPUT_PLUGIN_BINARY_OUTPUT;

	if (ctx->output_plugin_options != NIL)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("This plugin does not expect any options")));
	}
}

static void
plugin_shutdown(LogicalDecodingContext *ctx)
{
}

/*
 * As we don't release the slot during processing of particular table, there's
 * no room for SQL interface, even for debugging purposes. Therefore we need
 * neither OutputPluginPrepareWrite() nor OutputPluginWrite() in the plugin
 * callbacks. (Although we might want to write custom callbacks, this API
 * seems to be unnecessarily generic for our purposes.)
 */

/* BEGIN callback */
static void
plugin_begin_txn(LogicalDecodingContext *ctx, ReorderBufferTXN *txn)
{
}

/* COMMIT callback */
static void
plugin_commit_txn(LogicalDecodingContext *ctx, ReorderBufferTXN *txn,
				  XLogRecPtr commit_lsn)
{
}

/*
 * Callback for individual changed tuples
 */
static void
plugin_change(LogicalDecodingContext *ctx, ReorderBufferTXN *txn,
			  Relation relation, ReorderBufferChange *change)
{
	DecodingOutputState *dstate;

	dstate = (DecodingOutputState *) ctx->output_writer_private;

	/* Only interested in one particular relation. */
	if (relation->rd_id != dstate->relid)
		return;

	/* Decode entry depending on its type */
	switch (change->action)
	{
		case REORDER_BUFFER_CHANGE_INSERT:
			{
				HeapTuple	newtuple;

				newtuple = change->data.tp.newtuple != NULL ?
#if PG_VERSION_NUM >= 170000
					change->data.tp.newtuple : NULL;
#else
					&change->data.tp.newtuple->tuple : NULL;
#endif

				/*
				 * Identity checks in the main function should have made this
				 * impossible.
				 */
				if (newtuple == NULL)
					elog(ERROR, "Incomplete insert info.");

				store_change(ctx, CHANGE_INSERT, newtuple);
			}
			break;
		case REORDER_BUFFER_CHANGE_UPDATE:
			{
				HeapTuple	oldtuple,
							newtuple;

				oldtuple = change->data.tp.oldtuple != NULL ?
#if PG_VERSION_NUM >= 170000
					change->data.tp.oldtuple : NULL;
#else
					&change->data.tp.oldtuple->tuple : NULL;
#endif
				newtuple = change->data.tp.newtuple != NULL ?
#if PG_VERSION_NUM >= 170000
					change->data.tp.newtuple : NULL;
#else
					&change->data.tp.newtuple->tuple : NULL;
#endif

				if (newtuple == NULL)
					elog(ERROR, "Incomplete update info.");

				if (oldtuple != NULL)
					store_change(ctx, CHANGE_UPDATE_OLD, oldtuple);

				store_change(ctx, CHANGE_UPDATE_NEW, newtuple);
			}
			break;
		case REORDER_BUFFER_CHANGE_DELETE:
			{
				HeapTuple	oldtuple;

				oldtuple = change->data.tp.oldtuple ?
#if PG_VERSION_NUM >= 170000
					change->data.tp.oldtuple : NULL;
#else
					&change->data.tp.oldtuple->tuple : NULL;
#endif

				if (oldtuple == NULL)
					elog(ERROR, "Incomplete delete info.");

				store_change(ctx, CHANGE_DELETE, oldtuple);
			}
			break;
		default:
			/* Should not come here */
			Assert(0);
			break;
	}
}

/* Store concurrent data change. */
static void
store_change(LogicalDecodingContext *ctx, ConcurrentChangeKind kind,
			 HeapTuple tuple)
{
	DecodingOutputState *dstate;
	char	   *change_raw;
	ConcurrentChange *change;
	MemoryContext oldcontext;
	bool		flattened = false;
	Size		size;
	Datum		values[1];
	bool		isnull[1];
	char	   *dst;

	dstate = (DecodingOutputState *) ctx->output_writer_private;

	/*
	 * ReorderBufferCommit() stores the TOAST chunks in its private memory
	 * context and frees them after having called apply_change(). Therefore we
	 * need flat copy (including TOAST) that we eventually copy into the
	 * memory context which is available to
	 * pg_rewrite_decode_concurrent_changes().
	 */
	if (HeapTupleHasExternal(tuple))
	{
		/*
		 * toast_flatten_tuple_to_datum() might be more convenient but we
		 * don't want the decompression it does.
		 */
		tuple = toast_flatten_tuple(tuple, dstate->tupdesc);
		flattened = true;
	}

	size = MAXALIGN(VARHDRSZ) + sizeof(ConcurrentChange) + tuple->t_len;
	/* XXX Isn't there any function / macro to do this? */
	if (size >= 0x3FFFFFFF)
		elog(ERROR, "Change is too big.");

	oldcontext = MemoryContextSwitchTo(ctx->context);
	change_raw = (char *) palloc(size);
	MemoryContextSwitchTo(oldcontext);

	SET_VARSIZE(change_raw, size);
	change = (ConcurrentChange *) VARDATA(change_raw);

	/*
	 * Copy the tuple.
	 *
	 * CAUTION: change->tup_data.t_data must be fixed on retrieval!
	 */
	memcpy(&change->tup_data, tuple, sizeof(HeapTupleData));
	dst = (char *) change + sizeof(ConcurrentChange);
	memcpy(dst, tuple->t_data, tuple->t_len);

	/* The other field. */
	change->kind = kind;

	/* The data has been copied. */
	if (flattened)
		pfree(tuple);

	/* Store as tuple of 1 bytea column. */
	values[0] = PointerGetDatum(change_raw);
	isnull[0] = false;
	tuplestore_putvalues(dstate->tstore, dstate->tupdesc_change,
						 values, isnull);

	/* Accounting. */
	dstate->nchanges++;

	/* Cleanup. */
	pfree(change_raw);
}

/*
 * Retrieve tuple from a change structure. As for the change, no alignment is
 * assumed.
 */
static HeapTuple
get_changed_tuple(ConcurrentChange *change)
{
	HeapTupleData tup_data;
	HeapTuple	result;
	char	   *src;

	/*
	 * Ensure alignment before accessing the fields. (This is why we can't use
	 * heap_copytuple() instead of this function.)
	 */
	memcpy(&tup_data, &change->tup_data, sizeof(HeapTupleData));

	result = (HeapTuple) palloc(HEAPTUPLESIZE + tup_data.t_len);
	memcpy(result, &tup_data, sizeof(HeapTupleData));
	result->t_data = (HeapTupleHeader) ((char *) result + HEAPTUPLESIZE);
	src = (char *) change + sizeof(ConcurrentChange);
	memcpy(result->t_data, src, result->t_len);

	return result;
}

/*
 * A filter that recognizes changes produced by the initial load.
 */
static bool
plugin_filter(LogicalDecodingContext *ctx, RepOriginId origin_id)
{
	DecodingOutputState *dstate;

	dstate = (DecodingOutputState *) ctx->output_writer_private;

	/* dstate is not initialized during decoding setup - should it be? */
	if (dstate && dstate->rorigin != InvalidRepOriginId &&
		origin_id == dstate->rorigin)
		return true;

	return false;
}
