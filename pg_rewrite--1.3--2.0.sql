/* pg_rewrite--1.3--2.0.sql */

-- complain if script is sourced in psql, rather than via ALTER EXTENSION
\echo Use "ALTER EXTENSION pg_rewrite UPDATE TO '2.0'" to load this file. \quit

CREATE FUNCTION rewrite_table_nowait(
       src_table	text,
       dst_table	text,
       src_table_new	text)
RETURNS void
AS 'MODULE_PATHNAME', 'rewrite_table_nowait'
LANGUAGE C
STRICT;