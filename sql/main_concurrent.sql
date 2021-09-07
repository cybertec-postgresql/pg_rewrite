DROP EXTENSION IF EXISTS pg_rewrite;

CREATE TABLE tbl_src(i int primary key, j int);
-- Test that the source and destination tuple descriptors do not have to be
-- exactly the same.
ALTER TABLE tbl_src DROP COLUMN j;
ALTER TABLE tbl_src ADD COLUMN j int;
INSERT INTO tbl_src(i, j) VALUES (1, 1), (4, 4);

CREATE TABLE tbl_dst(i int primary key, j int) PARTITION BY RANGE(i);
CREATE TABLE tbl_dst_part_1 PARTITION OF tbl_dst FOR VALUES FROM (1) TO (4);
CREATE TABLE tbl_dst_part_2 PARTITION OF tbl_dst FOR VALUES FROM (4) TO (8);

CREATE EXTENSION pg_rewrite;

-- Check that the input data is already there.
SELECT * FROM tbl_src ORDER BY i;

SET rewrite.wait_after_load=8;
SELECT partition_table('tbl_src', 'tbl_dst', 'tbl_src_old');
SELECT * FROM tbl_src ORDER BY i;
