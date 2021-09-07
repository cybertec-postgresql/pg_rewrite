/* pg_repartition--1.1.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION pg_repartition" to load this file. \quit

CREATE FUNCTION partition_table(
       src_table	text,
       dst_table	text,
       src_table_new	text)
RETURNS void
AS 'MODULE_PATHNAME', 'partition_table'
LANGUAGE C;
