setup
{
    CREATE EXTENSION injection_points;
    CREATE EXTENSION pg_rewrite;

    CREATE TABLE tbl_src(i int primary key, j int);
    INSERT INTO tbl_src(i, j) VALUES (1, 1), (4, 4);

    CREATE TABLE tbl_dst(i int primary key, j int) PARTITION BY RANGE(i);
    CREATE TABLE tbl_dst_part_1 PARTITION OF tbl_dst FOR VALUES FROM (1) TO (4);
    CREATE TABLE tbl_dst_part_2 PARTITION OF tbl_dst FOR VALUES FROM (4) TO (8);
}

teardown
{
    DROP EXTENSION injection_points;
    DROP EXTENSION pg_rewrite;
    DROP TABLE tbl_src;
}

session s1
setup
{
    SELECT injection_points_attach('pg_rewrite-before-lock', 'wait');
}
# Perform the initial load and wait for s2 to do some data changes.
#
# Since pg_rewrite uses background worker, the isolation tester does not
# recognize that the session waits on an injection point (because the worker
# is who waits). Therefore use rewrite_table_nowait(), which only launches the
# worker and goes on. The 'wait_for_s1_sleep' step below then checks until the
# waiting started.
step wait_before_lock
{
    SELECT rewrite_table_nowait('tbl_src', 'tbl_dst', 'tbl_src_old');
}
# Wait until the changes have been surely applied.
step wait_for_backend_exit
{
DO $$
BEGIN
        LOOP
		PERFORM FROM pg_rewrite_progress;

		IF NOT FOUND THEN
			EXIT;
		END IF;

		PERFORM pg_sleep(.1);
	END LOOP;
END;
$$;

}
# Check the data.
step check
{
    SELECT i, j FROM tbl_src ORDER BY i, j;
}

session s2
# Since s1 uses background worker, the backend executing 'wait_before_lock'
# does not appear to be waiting on the injection point. Instead we need to
# check explicitly if the waiting on the injection point is in progress, and
# wait if it's not.
step wait_for_s1_sleep
{
DO $$
BEGIN
        LOOP
		PERFORM pg_stat_clear_snapshot();

	        PERFORM
		FROM pg_stat_activity
		WHERE (wait_event_type, wait_event)=('InjectionPoint', 'pg_rewrite-before-lock');

		IF FOUND THEN
			EXIT;
		END IF;

		PERFORM pg_sleep(.1);
	END LOOP;
END;
$$
}
step do_changes
{
	-- Insert one row into each partition.
	INSERT INTO tbl_src VALUES (2, 2), (3, 3), (5, 5);

	-- Update with no identity change.
	UPDATE tbl_src SET j=0 WHERE i=1;

	-- Update with identity change but within the same partition.
	UPDATE tbl_src SET i=6 WHERE i=5;

	-- Cross-partition update.
	UPDATE tbl_src SET i=7 WHERE i=3;

	-- Update a row we inserted and updated, to check that it's visible.
	UPDATE tbl_src SET j=4 WHERE i=7;

	-- Delete.
	DELETE FROM tbl_src WHERE i=4;
}
step wakeup_before_lock
{
    SELECT injection_points_wakeup('pg_rewrite-before-lock');
    SELECT injection_points_detach('pg_rewrite-before-lock');
}

permutation
    wait_before_lock
    wait_for_s1_sleep
    do_changes
    wakeup_before_lock
    wait_for_backend_exit
    check
