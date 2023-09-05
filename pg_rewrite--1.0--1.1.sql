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
