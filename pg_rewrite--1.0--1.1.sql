/* pg_rewrite--1.0--1.1.sql */

-- complain if script is sourced in psql, rather than via ALTER EXTENSION
\echo Use "ALTER EXTENSION pg_rewrite UPDATE TO '1.1'" to load this file. \quit

DROP FUNCTION partition_table(text, text, text);
CREATE FUNCTION partition_table(
       src_table	text,
       dst_table	text,
       src_table_new	text)
RETURNS void
AS 'MODULE_PATHNAME', 'partition_table_new'
LANGUAGE C
STRICT;

CREATE FUNCTION pg_rewrite_get_task_list()
RETURNS TABLE (
	tabschema_src	name,
	tabname_src	name,
	tabschema_dst	name,
	tabname_dst	name,
	tabname_src_new	name,
	ins_initial	bigint,
	ins		bigint,
	upd		bigint,
	del		bigint)
AS 'MODULE_PATHNAME', 'pg_rewrite_get_task_list'
LANGUAGE C;

-- The column names should match the arguments of the partition_table()
-- function.
CREATE VIEW pg_rewrite_progress AS
SELECT	COALESCE(tabschema_src || '.', '') || tabname_src AS src_table,
	COALESCE(tabschema_dst || '.', '') || tabname_dst AS dst_table,
	tabname_src_new AS src_table_new,
	ins_initial, ins, upd, del
FROM pg_rewrite_get_task_list();
