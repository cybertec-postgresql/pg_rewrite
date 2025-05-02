DROP EXTENSION IF EXISTS pg_rewrite;
CREATE EXTENSION pg_rewrite;

CREATE TABLE tab1(i int PRIMARY KEY, j int, k int);
-- If a dropped column is encountered, the source tuple should be converted
-- so it matches the destination table.
ALTER TABLE tab1 DROP COLUMN k;
ALTER TABLE tab1 ADD COLUMN k int;
INSERT INTO tab1(i, j, k)
SELECT i, i / 2, i
FROM generate_series(0, 1023) g(i);

-- Backup the source table.
CREATE TABLE tab1_bkp (LIKE tab1 INCLUDING ALL);
INSERT INTO tab1_bkp(i, j, k)
SELECT i, j, k FROM tab1;

CREATE TABLE tab1_new(i int PRIMARY KEY, j int, k int) PARTITION BY RANGE(i);
CREATE TABLE tab1_new_part_1 PARTITION OF tab1_new FOR VALUES FROM (0) TO (256);
CREATE TABLE tab1_new_part_2 PARTITION OF tab1_new FOR VALUES FROM (256) TO (512);
CREATE TABLE tab1_new_part_3 PARTITION OF tab1_new FOR VALUES FROM (512) TO (768);
CREATE TABLE tab1_new_part_4 PARTITION OF tab1_new FOR VALUES FROM (768) TO (1024);

-- Process the table.
SELECT rewrite_table('tab1', 'tab1_new', 'tab1_orig');

-- tab1 should now be partitioned.
EXPLAIN SELECT * FROM tab1;

-- Check that the contents has not changed.
SELECT count(*) FROM tab1;

SELECT *
FROM tab1 t FULL JOIN tab1_bkp o ON t.i = o.i
WHERE t.i ISNULL OR o.i ISNULL;

-- List partitioning
CREATE TABLE tab2(i int, j int, PRIMARY KEY (i, j));
INSERT INTO tab2(i, j)
SELECT i, j
FROM generate_series(1, 4) g(i), generate_series(1, 4) h(j);

CREATE TABLE tab2_new(i int, j int, PRIMARY KEY (i, j)) PARTITION BY LIST(i);
CREATE TABLE tab2_new_part_1 PARTITION OF tab2_new FOR VALUES IN (1);
CREATE TABLE tab2_new_part_2 PARTITION OF tab2_new FOR VALUES IN (2);
CREATE TABLE tab2_new_part_3 PARTITION OF tab2_new FOR VALUES IN (3);
CREATE TABLE tab2_new_part_4 PARTITION OF tab2_new FOR VALUES IN (4);

SELECT rewrite_table('tab2', 'tab2_new', 'tab2_orig');

TABLE tab2_new_part_1;
TABLE tab2_new_part_2;
TABLE tab2_new_part_3;
TABLE tab2_new_part_4;

-- Hash partitioning
CREATE TABLE tab3(i int, j int, PRIMARY KEY (i, j));
INSERT INTO tab3(i, j)
SELECT i, j
FROM generate_series(1, 4) g(i), generate_series(1, 4) h(j);

CREATE TABLE tab3_new(i int, j int, PRIMARY KEY (i, j)) PARTITION BY HASH(i);
CREATE TABLE tab3_new_part_1 PARTITION OF tab3_new FOR VALUES WITH (MODULUS 4, REMAINDER 0);
CREATE TABLE tab3_new_part_2 PARTITION OF tab3_new FOR VALUES WITH (MODULUS 4, REMAINDER 1);
CREATE TABLE tab3_new_part_3 PARTITION OF tab3_new FOR VALUES WITH (MODULUS 4, REMAINDER 2);
CREATE TABLE tab3_new_part_4 PARTITION OF tab3_new FOR VALUES WITH (MODULUS 4, REMAINDER 3);

SELECT rewrite_table('tab3', 'tab3_new', 'tab3_orig');

TABLE tab3_new_part_1;
TABLE tab3_new_part_2;
TABLE tab3_new_part_3;
TABLE tab3_new_part_4;
