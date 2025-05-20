# pg_rewrite

`pg_rewrite` is a tool to rewrite table (i.e. to copy its data to a new
file). It allows both read and write access to the table during the rewriting.

Following are the most common reasons to rewrite a table:

1.  Change data type of column(s)

    Typically this is needed if the existing data type is running out of
    values. For example, you may need to change `interger` type to
    `bigint`. `ALTER TABLE` command can do that too, but it allows neither
    write nor read access to the table during the rewriting.

2.  Partition the table

    If you realize that your table is getting much bigger than expected and
    that partitioning would make your life easier, the next question may be
    how to copy the existing data to the new, partitioned table without
    stopping all the applications that run DML commands on the table. (When
    you decide to use partitioning, the amount of data to copy might already
    be significant, so the copying might need a while.)

3.  Change order of columns

    If you conclude that a different order of columns would save significant
    disk space (due to reduced paddding), the problem boils down to copying
    data to a new table like in 2). Again, you may need `pg_rewrite` to make
    the change smooth.

Note that the following use cases can be combined in a single rewrited.


# INSTALLATION

Install PostgreSQL before proceeding. Make sure to have `pg_config` binary,
these are typically included in `-dev` and `-devel` packages. PostgreSQL server
version 13 or later is required.

```bash
git clone https://github.com/cybertec-postgresql/pg_rewrite.git
cd pg_rewrite
git checkout <the latest stable version>
make
make install
```

Add these to `postgresql.conf`:

```
wal_level = logical
max_replication_slots = 1 # ... or add 1 to the current value.
shared_preload_libraries = 'pg_rewrite' # ... or add the library to the existing ones.
```

Restart the cluster, and invoke:

```
CREATE EXTENSION pg_rewrite;
```

# USAGE

Assume you have a table defined like this

```
CREATE TABLE measurement (
    id              int,
    city_id         int not null,
    logdate         date not null,
    peaktemp        int,
    PRIMARY KEY(id, logdate)
);
```

and you need to replace it with a partitioned table. At the same time, you
want to change the data type of the `id` column to `bigint`.


```
CREATE TABLE measurement_aux (
    id              bigint,
    city_id         int not null,
    logdate         date not null,
    peaktemp        int,
    PRIMARY KEY(id, logdate)
) PARTITION BY RANGE (logdate);
```

Then create partitions for all the rows currently present in the `measurement`
table, and also for the data that might be inserted during processing:

```
CREATE TABLE measurement_y2006m02 PARTITION OF measurement_aux
    FOR VALUES FROM ('2006-02-01') TO ('2006-03-01');

CREATE TABLE measurement_y2006m03 PARTITION OF measurement_aux
    FOR VALUES FROM ('2006-03-01') TO ('2006-04-01');

-- ...
```

*It's essential that both the source (`measurement`) and target
(`measurement_aux`) table have an identity index. The easiest way to ensure
this is to create `PRIMARY KEY` or `UNIQUE` constraint. Also note that the key
(i.e. column list) of the identity index of the source and target table must
be identical. The identity is needed to process data changes that applications
make while data is being copied from the source to the target table.*

Then, in order to copy the data into the target table, run the
`rewrite_table()` function and pass it both the source and target table, as
well as a new table name for the source table. For example:

```
SELECT rewrite_table('measurement', 'measurement_aux', 'measurement_old');
```

The call will first copy all rows from `measurement` to `measurement_aux`. The
it will apply to `measurement_aux` all the data changes (INSERT, UPDATE,
DELETE) that took place in `measurement` during the copying. Next, it will
lock `measurement` so that neither read nor write access is possible. Finally
it will rename `measurement` to `measurement_old` and `measurement_aux` to
`measurement`. Thus `measurement` ends up to be the partitioned table, while
`measurement_old` is the original, non-partitioned table.

If a column of the target table has a different data type from the
corresponding column of the source table, an implicit or assignment cast must
exist between the two types.

# Constraints

The target table should obviously end up with same constraints as the source
table. It's recommended to handle constraints creation this way:

1.  Add PRIMARY KEY, UNIQUE and EXCLUDE constraints of the source table to the
    target table before you call `rewrite_table()`. These are enforced during
    the rewriting, so any violation would make `rewrite_table()` fail
    (ROLLBACK). (The constraints must have been enforced in the source table,
    but it does not hurt to check them in the target table, especially if the
    column data type is being changed.)

2.  If the version of PostgreSQL server is 17 or lower, add NOT NULL
    constraints. `rewrite_table()` by-passes validation of these, but all the
    rows it inserts into the target table must have been validated in the
    source table. Even if the column data tape is different in the target
    table, the data type conversion should not turn non-NULL value to NULL or
    vice versa.

3.  CHECK constraints are created automatically by `rewrite_table()` when all
    the data changes have been applied to the target table. However, these
    constraints are created as NOT VALID, so you need to use the `ALTER TABLE
    ... VALIDATE CONSTRAINT ...` command to validate them.

    (The function does not create these constraints immediately as valid,
    because that could imply blocking access to the table for significant
    time.)

4.  If the version of PostgreSQL server is 18 or higher, NOT NULL constraints
    are also created automatically and need to be validated using the `ALTER
    TABLE ... VALIDATE CONSTRAINT ...` command.

5.  FOREIGN KEY constraints are also created automatically and need to be
    validated using the `ALTER TABLE ... VALIDATE CONSTRAINT ...` command,
    unless the referencing table is partitioned: the NOT VALID option is
    currently not supported for partitioned tables.

    Therefore, if the referencing table is partitioned, you need to use the
    `ALTER TABLE ... ADD CONSTRAINT ... FOREIGN KEY ...` command after
    `rewrite_table()` has finished. Please run the command as soon as possible
    to minimize the risk that applications modify the database in a way that
    violates the constraints.

6.  Drop all foreign keys involving the source table.

    You probably want to drop the source table anyway, but if you don't, you
    should at least drop its FOREIGN KEY constraints. As the table was
    renamed, applications will no longer update it. Therefore, attempts to
    update the other tables involved in its foreign keys may cause errors.


# Progress monitoring

If `rewrite_table()` takes long time to finish, you might be interested in the
progress. The `pg_rewrite_progress` view shows all the pending calls of the
function in the current database. The `src_table`, `dst_table` and
`src_table_new` columns contain the arguments of the `rewrite_table()`
function. `ins_initial` is the number of tuples inserted into the new table
storage during the "initial load stage", i.e. the number of tuples present in
the table before the processing started. On the other hand, `ins`, `upd` and
`del` are the numbers of tuples inserted, updated and deleted by applications
during the table processing. (These "concurrent data changes" must also be
incorporated into the partitioned table, otherwise they'd get lost.)

# Limitations

If the target table is partitioned, it's not allowed to have foreign
tables as partitions.

# Configuration

Following is the description of the configuration variables that affect
behavior of the functions of this extension.

* `rewrite.max_xlock_time`

Although the table being processed is available for both read and write
operations by other transactions most of the time, an exclusive lock is needed
to finalize the processing (i.e. to do the table renaming), which blocks both
read and write access. This should take very short time that users should
harly notice.

However, if a significant amount of changes took place in the source table
while the extension was waiting for the (exclusive) lock, the outage might
take proportionally longer time. The point is that those changes need to be
propagated to the target table before the exclusive lock can be released.

If the extension function seems to block access to tables too much, consider
setting `rewrite.max_xlock_time` GUC parameter. For example:

```
SET rewrite.max_xlock_time TO 100;
```

Tells that the exclusive lock shouldn't be held for more than 0.1 second (100
milliseconds). If more time is needed for the final stage, the particular
function releases the exclusive lock, processes the changes committed by the
other transactions in between and tries the final stage again. Error is
reported if the lock duration is exceeded a few more times. If that happens,
you should either increase the setting or try to process the problematic table
later, when the write activity is lower.

The default value is `0`, meaning that the final stage can take as much time as
it needs.

# Concurrency

1. While the rewrite_table() function is executing, `ALTER TABLE` command on
   the same table should be blocked until the rewriting is done. However, in
   some cases the `ALTER TABLE` command and the rewrite_table() function might
   end up in a deadlock. Therefore it's recommended not to run ALTER TABLE on
   a table which is being rewritten.

2. The `rewrite_table()` function allows for MVCC-unsafe behavior described in
   the first paragraph of [mvcc-caveats][1].


[1]: https://www.postgresql.org/docs/current/mvcc-caveats.html
