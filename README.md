# pg_rewrite

`pg_rewrite` aims to be a set of tools to perform maintenance tasks which
require a table to be rewritten (i.e. the table data to be copied to a new
storage) and which are expected to limit the access to the table as little as
possible.

# INSTALL

Install PostgreSQL before proceeding. Make sure to have `pg_config` binary,
these are typically included in `-dev` and `-devel` packages. PostgreSQL server
version 13 or later is required.

```bash
git clone https://github.com/cybertec-postgresql/pg_rewrite.git
cd pg_squeeze
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

Assuming you have a table defined like this:

```
CREATE TABLE measurement (
    id              serial,
    city_id         int not null,
    logdate         date not null,
    peaktemp        int,
    unitsales       int,
    PRIMARY KEY(id, logdate)
);
```

You need to create a partitioned table having the same columns and data types:

```
CREATE TABLE measurement_aux (
    id              serial,
    city_id         int not null,
    logdate         date not null,
    peaktemp        int,
    unitsales       int,
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

*It's essential that both the source (`measurement`) and destination
(`measurement_aux`) table have an identity index, the easiest way to ensure
this is to create `PRIMARY KEY` or `UNIQUE` constraint. Also note that the key
(i.e. column list) of the identity index of the source and destination table
must be identical. The identity is needed to process data changes that
applications make while data is being copied from the source to the destination
table.*

Also, unless you've set `rewrite.check_constraints` to `false`, make sure that
the destination table has all the constraints that the source table has.

Then, in order to copy the data into the destination table, run the
`partition_table()` function and pass it both the source and destination table,
as well as a new table name for the source table. For example:

```
SELECT partition_table('measurement', 'measurement_aux', 'measurement_old');
```

The call will copy data from `measurement` to `measurement_aux`, then it will
lock `measurement` exclusively and rename (1) `measurement` to
`measurement_old`, (2) `measurement_aux` to `measurement`. Thus `measurement`
ends up to be the partitioned table, while `measurement_old` is the original,
non-partitioned table.

# Limitations

Please consider the following before you try to use the function:

* Foreign table partitions are not supported.

* It's not expected that the table that you try to partition is referenced by
  any foreign key. The problem is that the destination table is initially
  empty, so you won't be able to create the foreign keys that reference it.

# Configuration

Following is the description of the configuration variables that affect
behavior of the functions of this extension.

* `rewrite.check_constraints`

Before copying of the data starts, it's checked whether the destination table
has the same constraints as the source table, and throws an ERROR if a
difference is found. The point is that due to (accidentally) missing
constraint on the destination table, data that violate constraints on the
source table would be allowed to appear in the destination table as soon as
the processing is finished. Even an extra constraint on the destination table
is a problem because the extension only assumes that all the data it copies do
satisfy constraints on the source table, however it does not validate them
against the additional constraints on the destination table.

By setting `rewrite.check_constraints` to `false`, the user can turn off the
constraint checks. Please be very cautions before you do so.

The default value is `true`.

* `rewrite.max_xlock_time`

Although the table being processed is available for both read and write
operations by other transactions most of the time, an exclusive lock is needed
to finalize the processing (i.e. to process the remaining concurrent changes
and to rename the tables). If the extension function seems to block access to
tables too much, consider setting `rewrite.max_xlock_time` GUC parameter. For
example:

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

1. The extension does not prevent other transactions from altering table at
   certain stages of the processing. If a `disruptive command` (i.e. `ALTER
   TABLE`) manages to commit before the processing could finish, the table
   processing aborts and all changes done are rolled back.

2. The functions of this extension allow for MVCC-unsafe behavior described in
   the first paragraph of [mvcc-caveats][1].

# Locking

Since the table renaming requires an exclusive lock, applications won't be able
to access the table that you try to process for very short time. However, if a
significant amount of changes took place in the source table while the
extension was waiting for the lock, the outage will take proportionally longer
time. The point is that those changes need to be propagated to the destination
table before the exclusive lock can be released.

[1]: https://www.postgresql.org/docs/13/static/mvcc-caveats.html
