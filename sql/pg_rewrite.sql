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

CREATE TABLE tab1_new(i int PRIMARY KEY, j int, k int) PARTITION BY RANGE(i);
CREATE TABLE tab1_new_part_1 PARTITION OF tab1_new FOR VALUES FROM (0) TO (256);
CREATE TABLE tab1_new_part_2 PARTITION OF tab1_new FOR VALUES FROM (256) TO (512);
CREATE TABLE tab1_new_part_3 PARTITION OF tab1_new FOR VALUES FROM (512) TO (768);
CREATE TABLE tab1_new_part_4 PARTITION OF tab1_new FOR VALUES FROM (768) TO (1024);

-- Also test handling of constraints that require "manual" validation.
ALTER TABLE tab1 ADD CHECK (k >= 0);

CREATE TABLE tab1_fk(i int REFERENCES tab1);
INSERT INTO tab1_fk(i) VALUES (1);
\d tab1

-- Process the table.
SELECT rewrite_table('tab1', 'tab1_new', 'tab1_orig');

-- tab1 should now be partitioned.
\d tab1

-- Validate the constraints.
ALTER TABLE tab1 VALIDATE CONSTRAINT tab1_k_check2;
ALTER TABLE tab1_fk VALIDATE CONSTRAINT tab1_fk_i_fkey2;

\d tab1

EXPLAIN SELECT * FROM tab1;

-- Check that the contents has not changed.
SELECT count(*) FROM tab1;

SELECT *
FROM tab1 t FULL JOIN tab1_orig o ON t.i = o.i
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

-- Change of precision and scale of a numeric data type.
CREATE TABLE tab4(i int PRIMARY KEY, j numeric(3, 1));
INSERT INTO tab4(i, j) VALUES (1, 0.1);
CREATE TABLE tab4_new(i int PRIMARY KEY, j numeric(4, 2));
TABLE tab4;
SELECT rewrite_table('tab4', 'tab4_new', 'tab4_orig');
TABLE tab4;

-- One more test for "manual" validation of FKs, this time we rewrite the PK
-- table. The NOT VALID constraint cannot be used if the FK table is
-- partitioned, so we need a separate test.
CREATE TABLE tab1_pk(i int primary key);
INSERT INTO tab1_pk(i) VALUES (1);
CREATE TABLE tab1_pk_new(i bigint primary key);

DROP TABLE tab1_fk;
CREATE TABLE tab1_fk(i int REFERENCES tab1_pk);
INSERT INTO tab1_fk(i) VALUES (1);

\d tab1_pk
SELECT rewrite_table('tab1_pk', 'tab1_pk_new', 'tab1_pk_orig');
\d tab1_pk
ALTER TABLE tab1_fk VALIDATE CONSTRAINT tab1_fk_i_fkey2;
\d tab1_pk

-- For the partitioned FK table, test at least that the FK creation is skipped
-- (i.e. ERROR saying that NOT VALID is not supported is no raised)
DROP TABLE tab1_fk;
CREATE TABLE tab1_fk(i int REFERENCES tab1_pk) PARTITION BY RANGE (i);
CREATE TABLE tab1_fk_1 PARTITION OF tab1_fk DEFAULT;
INSERT INTO tab1_fk(i) VALUES (1);

ALTER TABLE tab1_pk_orig RENAME TO tab1_pk_new;
TRUNCATE TABLE tab1_pk_new;

\d tab1_fk
SELECT rewrite_table('tab1_pk', 'tab1_pk_new', 'tab1_pk_orig');
-- Note that tab1_fk still references tab1_pk_orig - that's expected.
\d tab1_fk

-- The same once again, but now rewrite the FK table.
DROP TABLE tab1_fk;
DROP TABLE tab1_pk;
ALTER TABLE tab1_pk_orig RENAME TO tab1_pk;
CREATE TABLE tab1_fk(i int PRIMARY KEY REFERENCES tab1_pk);
INSERT INTO tab1_fk(i) VALUES (1);
CREATE TABLE tab1_fk_new(i int PRIMARY KEY) PARTITION BY RANGE (i);
CREATE TABLE tab1_fk_new_1 PARTITION OF tab1_fk_new DEFAULT;
\d tab1_fk
SELECT rewrite_table('tab1_fk', 'tab1_fk_new', 'tab1_fk_orig');
\d tab1_fk
