/* pg_rewrite--1.1--1.2.sql */

-- complain if script is sourced in psql, rather than via ALTER EXTENSION
\echo Use "ALTER EXTENSION pg_rewrite UPDATE TO '1.2'" to load this file. \quit

DROP FUNCTION partition_table(text, text, text);

CREATE FUNCTION rewrite_table(
       src_table	text,
       dst_table	text,
       src_table_new	text)
RETURNS void
AS 'MODULE_PATHNAME', 'rewrite_table'
LANGUAGE C
STRICT;

CREATE FUNCTION rewrite_table_nowait(
       src_table	text,
       dst_table	text,
       src_table_new	text)
RETURNS void
AS 'MODULE_PATHNAME', 'rewrite_table_nowait'
LANGUAGE C
STRICT;
