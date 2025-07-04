setup
{
    CREATE EXTENSION injection_points;
    CREATE EXTENSION pg_rewrite;

    CREATE TABLE tbl_src(i int primary key, t text);

    INSERT INTO tbl_src(i, t)
    SELECT x, string_agg(random()::text, '')
    FROM generate_series(1, 2) g(x), generate_series(1, 200) h(y)
    GROUP BY x;

    CREATE TABLE tbl_dst(i int primary key, t text);
}

teardown
{
    DROP EXTENSION injection_points;
    DROP EXTENSION pg_rewrite;
    DROP TABLE tbl_src;
    DROP TABLE tbl_src_old;
}

session s1
setup
{
    SELECT injection_points_attach('pg_rewrite-before-lock', 'wait');
    SELECT injection_points_attach('pg_rewrite-after-commit', 'wait');
}
# Perform the initial load and wait for s2 to do some data changes.
#
# Since pg_rewrite uses background worker, the isolation tester does not
# recognize that the session waits on an injection point (because the worker
# is who waits). Therefore use rewrite_table_nowait(), which only launches the
# worker and goes on. The 'wait_for_s1_sleep' step below then checks until the
# waiting started.
step do_rewrite
{
    SELECT rewrite_table_nowait('tbl_src', 'tbl_dst', 'tbl_src_old');
}
# Check the data.
step do_check
{
    TABLE pg_rewrite_progress;

    -- Each row should contain TOASTed value.
    SELECT count(*) FROM tbl_src WHERE pg_column_toast_chunk_id(t) ISNULL;

    -- The contents of the new table should be identical to that of the old
    -- one.
    SELECT count(*)
    FROM tbl_src t1 JOIN tbl_src_old t2 ON t1.i = t2.i
    WHERE t1.t <> t2.t;
}

session s2
# Since s1 uses background worker, the backend executing 'wait_before_lock'
# does not appear to be waiting on the injection point. Instead we need to
# check explicitly if the waiting on the injection point is in progress, and
# wait if it's not.
step wait_for_before_lock_ip
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
$$;
}
step do_changes
{
    INSERT INTO tbl_src(i, t)
    SELECT 5, string_agg(random()::text, '')
    FROM generate_series(1, 200) h(y);

    UPDATE tbl_src SET t = t || 'x' WHERE i = 1;
}
step wakeup_before_lock_ip
{
    SELECT injection_points_wakeup('pg_rewrite-before-lock');
}
# Wait until the concurrent changes have been committed by the pg_rewrite
# worker.
step wait_for_after_commit_ip
{
DO $$
BEGIN
        LOOP
		PERFORM pg_stat_clear_snapshot();

	        PERFORM
		FROM pg_stat_activity
		WHERE (wait_event_type, wait_event)=('InjectionPoint', 'pg_rewrite-after-commit');

		IF FOUND THEN
			EXIT;
		END IF;

		PERFORM pg_sleep(.1);
	END LOOP;
END;
$$;
}
# Like wakeup_before_lock_ip above.
step wakeup_after_commit_ip
{
    SELECT injection_points_wakeup('pg_rewrite-after-commit');
}
teardown
{
    SELECT injection_points_detach('pg_rewrite-before-lock');
    SELECT injection_points_detach('pg_rewrite-after-commit');
}

permutation
    do_rewrite
    wait_for_before_lock_ip
    do_changes
    wakeup_before_lock_ip
    wait_for_after_commit_ip
    do_check
    wakeup_after_commit_ip
