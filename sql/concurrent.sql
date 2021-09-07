-- Give the "main" test enough time to create an populate the tables and to
-- perform the initial load.
SELECT pg_sleep(4);

-- Check that the initial load is done and that the "main" session is waiting
-- for us.
SELECT l.mode, l.granted
FROM   pg_catalog.pg_locks l
WHERE  l.classid=(
           SELECT oid
           FROM    pg_catalog.pg_class AS c
           WHERE   c.relname = 'pg_extension')
       AND
       l.objid=(
           SELECT oid
           FROM pg_catalog.pg_extension e
           WHERE e.extname = 'pg_rewrite');

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

-- Again, check that the "main" session still waits.
SELECT l.mode, l.granted
FROM   pg_catalog.pg_locks l
WHERE  l.classid=(
           SELECT oid
           FROM    pg_catalog.pg_class AS c
           WHERE   c.relname = 'pg_extension')
       AND
       l.objid=(
           SELECT oid
           FROM pg_catalog.pg_extension e
           WHERE e.extname = 'pg_rewrite');
