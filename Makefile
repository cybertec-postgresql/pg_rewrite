PG_CONFIG ?= pg_config
MODULE_big = pg_rewrite
OBJS = pg_rewrite.o concurrent.o $(WIN32RES)
PGFILEDESC = "pg_rewrite - tools for maintenance that requires table rewriting."

EXTENSION = pg_rewrite
DATA = pg_rewrite--1.0.sql pg_rewrite--1.0--1.1.sql pg_rewrite--1.1--1.2.sql
DOCS = pg_rewrite.md

REGRESS = pg_rewrite generated
#ISOLATION = pg_rewrite_concurrent pg_rewrite_concurrent_partition

PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

