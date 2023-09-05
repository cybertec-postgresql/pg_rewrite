DROP EXTENSION IF EXISTS pg_rewrite;
CREATE EXTENSION pg_rewrite;

CREATE TABLE pk(i int PRIMARY KEY);
INSERT INTO pk(i)
SELECT i
FROM generate_series(0, 1023) g(i);

CREATE TABLE tab1(i int PRIMARY KEY REFERENCES pk, j int, k int);
-- If a dropped column is encountered, the source tuple should be converted
-- so it matches the destination table.
ALTER TABLE tab1 DROP COLUMN k;
ALTER TABLE tab1 ADD COLUMN k int;
INSERT INTO tab1(i, j, k)
SELECT i, i / 2, i
FROM generate_series(0, 1023) g(i);

-- Backup the source table.
CREATE TABLE tab1_orig (LIKE tab1 INCLUDING ALL);
INSERT INTO tab1_orig(i, j, k)
SELECT i, j, k FROM tab1;

CREATE TABLE tab2(i int PRIMARY KEY, j int, k int) PARTITION BY RANGE(i);
CREATE TABLE tab2_part_1 PARTITION OF tab2 FOR VALUES FROM (0) TO (256);
CREATE TABLE tab2_part_2 PARTITION OF tab2 FOR VALUES FROM (256) TO (512);
CREATE TABLE tab2_part_3 PARTITION OF tab2 FOR VALUES FROM (512) TO (768);
CREATE TABLE tab2_part_4 PARTITION OF tab2 FOR VALUES FROM (768) TO (1024);

-- Try to process the table. This should fail because tab1 has a FK but tab2
-- does not.
SELECT partition_table('tab1', 'tab2', 'tab1_orig');

-- Do not check constraints.
SET rewrite.check_constraints TO false;

-- Now the table should be processed.
SELECT partition_table('tab1', 'tab2', 'tab2_orig');

-- tab1 should now be partitioned.
EXPLAIN SELECT * FROM tab1;

-- Check that the contents has not changed.
SELECT count(*) FROM tab1;

SELECT *
FROM tab1 t FULL JOIN tab1_orig o ON t.i = o.i
WHERE t.i ISNULL OR o.i ISNULL;

-- List partitioning
CREATE TABLE tab3(i int, j int, PRIMARY KEY (i, j));
INSERT INTO tab3(i, j)
SELECT i, j
FROM generate_series(1, 4) g(i), generate_series(1, 4) h(j);

CREATE TABLE tab4(i int, j int, PRIMARY KEY (i, j)) PARTITION BY LIST(i);
CREATE TABLE tab4_part_1 PARTITION OF tab4 FOR VALUES IN (1);
CREATE TABLE tab4_part_2 PARTITION OF tab4 FOR VALUES IN (2);
CREATE TABLE tab4_part_3 PARTITION OF tab4 FOR VALUES IN (3);
CREATE TABLE tab4_part_4 PARTITION OF tab4 FOR VALUES IN (4);

SELECT partition_table('tab3', 'tab4', 'tab3_orig');

TABLE tab4_part_1;
TABLE tab4_part_2;
TABLE tab4_part_3;
TABLE tab4_part_4;

-- Hash partitioning
CREATE TABLE tab5(i int, j int, PRIMARY KEY (i, j));
INSERT INTO tab5(i, j)
SELECT i, j
FROM generate_series(1, 4) g(i), generate_series(1, 4) h(j);

CREATE TABLE tab6(i int, j int, PRIMARY KEY (i, j)) PARTITION BY HASH(i);
CREATE TABLE tab6_part_1 PARTITION OF tab6 FOR VALUES WITH (MODULUS 4, REMAINDER 0);
CREATE TABLE tab6_part_2 PARTITION OF tab6 FOR VALUES WITH (MODULUS 4, REMAINDER 1);
CREATE TABLE tab6_part_3 PARTITION OF tab6 FOR VALUES WITH (MODULUS 4, REMAINDER 2);
CREATE TABLE tab6_part_4 PARTITION OF tab6 FOR VALUES WITH (MODULUS 4, REMAINDER 3);

SELECT partition_table('tab5', 'tab6', 'tab5_orig');

TABLE tab6_part_1;
TABLE tab6_part_2;
TABLE tab6_part_3;
TABLE tab6_part_4;
