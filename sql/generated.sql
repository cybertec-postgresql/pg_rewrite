-- Generated columns - some meaningful combinations of source and destination
-- columns.
CREATE TABLE tab7(
        i int primary key,
	j int,
	k int generated always as (i + 1) virtual,
	l int generated always AS (i + 1) stored,
	m int generated always AS (i + 1) virtual);
CREATE TABLE tab7_new(
        i int primary key,
        -- Override the value copied from the source table.
        j int generated always AS (i - 1) stored,
	-- Check that the expression is evaluated correctly on the source
	-- table.
	k int,
	-- The same for stored expression.
	l int,
	-- Override the value computed on the source table.
	m int generated always as (i - 1) virtual);
INSERT INTO tab7(i, j) VALUES (1, 1);
SELECT rewrite_table('tab7', 'tab7_new', 'tab7_orig');
SELECT * FROM tab7;

CREATE EXTENSION pageinspect;
-- HEAP_HASNULL indicates that the value of 'm' hasn't been copied from the
-- source table.
SELECT raw_flags
FROM heap_page_items(get_raw_page('tab7', 0)),
LATERAL heap_tuple_infomask_flags(t_infomask, t_infomask2);

-- For PG < 18, test without VIRTUAL columns.
CREATE TABLE tab8(
        i int primary key,
        j int,
        k int generated always AS (i + 1) stored);
CREATE TABLE tab8_new(
        i int primary key,
        -- Override the value copied from the source table.
        j int generated always AS (i - 1) stored,
        -- Check that the expression is evaluated correctly on the source
        -- table.
        k int);
INSERT INTO tab8(i, j) VALUES (1, 1);
SELECT rewrite_table('tab8', 'tab8_new', 'tab8_orig');
SELECT * FROM tab8;
