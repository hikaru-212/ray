# Reproducing the recorded comparison

This document distinguishes the commands recorded in the original experiment from portable setup instructions. Packaging did not rerun the experiments. [commands.json](observed/commands.json) preserves the relevant historical command sequences with private locations, database settings, and container identifiers replaced by named variables. Each sequence identifies its original command file and SHA-256 hash.

## Required environment

Use a virtual environment with Python 3.12.3, psycopg2 2.9.13, PyArrow 23.0.1, and a compatible installed Ray runtime. The recorded runtime was Ray 2.58.0 at commit cec0a091b12675bbf101cf8a350c8706107baee1. Full Base and HEAD repository builds were not used.

The original experiment created a small virtual environment, reused an already installed Ray environment through a .pth file, and installed psycopg2-binary (resolved version 2.9.13). The complete pre-existing runtime dependency lock and build artifacts are not included. There is no independently validated from-scratch installation recipe here. A different runtime may reject these pinned modules or change execution.

The database was PostgreSQL 15.19 on aarch64-unknown-linux-musl. The original experiment used an already cached image, with --pull=never, whose local image ID was sha256:fe0737ba566a2c5b2a28f34433c0a423261900ec17b9bf7ad115e1aae7e57f1b. This identifier records the tested image; it is not a portable download instruction.

Provide a fresh disposable database with permission to create types and tables. The runner's fixture is not idempotent: do not use a database containing an events table or pair_key type. It also creates a local SQLite fixture because the original runner combined two investigations. That branch requires SQLite with native MOD available (recorded SQLite 3.45.1). It remains in the script to preserve original behavior; its results are outside this package's regression claim.

Ray requires a working local process environment, two CPUs, and space for its runtime files. The original run used two Ray CPUs and a 128 MiB object store. All test scratch paths must be accessible to the local Ray workers. The 180-second timeout is the recorded bound, not a guarantee of completion on other hosts.

## Exact source selection used

The following is the original source-selection operation with only private directory paths parameterized. Both objects must already exist in REVIEW_SOURCE; no Git branch changes or network retrieval are required.

~~~bash
BASE=8d53e59e3da91a39beb6b1b614265cf05d3d26a4
HEAD=c4ace0b69f82f66772d05c286ed29a52b6f73dc3
for revision in "$BASE" "$HEAD"; do
    git -C "$REVIEW_SOURCE" show "$revision:python/ray/data/_internal/datasource/sql_datasource.py" > "$RUN_DIR/sources/$revision-sql_datasource.py"
done
sha256sum "$RUN_DIR/sources/"*-sql_datasource.py
~~~

Expected SHA-256 values:

~~~text
6ac6859a53b8097ac80ffb4e621983faf95b0456ec16df5bb42da0ca34ed012b  8d53e59e3da91a39beb6b1b614265cf05d3d26a4-sql_datasource.py
f94f014ba391914742786885faead52fb742baf91b98517bef13e5888ba0b325  c4ace0b69f82f66772d05c286ed29a52b6f73dc3-sql_datasource.py
~~~

The byte-identical snapshots are already included under scripts/sources. The runner loads these files with importlib, registers each module with ray.cloudpickle.register_pickle_by_value, and uses that module's SQLDatasource. For end-to-end execution, it temporarily assigns read_api.SQLDatasource and calls ray.data.read_sql with shard_keys=["shard_key"], shard_hash_fn="hash_record", and override_num_blocks=4. The installed read_sql implementation and scheduler/workers remain in use. The original runtime read_sql source hash is c5c291b385f9148ac60aff54ef9c80ac7a832b9f42153ac065a75df5413fa649.

## Portable setup for the copied runner

These setup commands are instructions for a future rerun, not historical output. Set PACKAGE to this package directory, PYTHON to the virtual environment's Python executable, and RUN_DIR to a new private scratch directory outside this repository. Set and export the six REPRO variables below to local values using your normal private configuration. No connection credentials are supplied in this package.

~~~bash
set -euo pipefail
: "${PACKAGE:?set the package directory}"
: "${PYTHON:?set the virtual environment Python executable}"
: "${RUN_DIR:?set a new private scratch directory}"
: "${REPRO_PG_HOST:?set the disposable database host}"
: "${REPRO_PG_PORT:?set the disposable database port}"
: "${REPRO_PG_DATABASE:?set the disposable database name}"
: "${REPRO_PG_USER:?set the disposable database user}"
: "${REPRO_PG_PASSWORD:?set the disposable database password}"
: "${REPRO_RAY_TEMP_DIR:?set a new private Ray scratch path}"
export REPRO_PG_HOST REPRO_PG_PORT REPRO_PG_DATABASE REPRO_PG_USER REPRO_PG_PASSWORD REPRO_RAY_TEMP_DIR
test ! -e "$RUN_DIR"
mkdir -p "$RUN_DIR/sources" "$RUN_DIR/results" "$RUN_DIR/logs"
cp "$PACKAGE/scripts/experiment_bc.py" "$RUN_DIR/experiment_bc.py"
cp "$PACKAGE/scripts/sources/"*-sql_datasource.py "$RUN_DIR/sources/"
cp "$PACKAGE/scripts/postgres-witness.sql" "$RUN_DIR/sources/"
printf '%s:%s\n' "$REPRO_PG_HOST" "$REPRO_PG_PORT" > "$RUN_DIR/results/postgres-address.txt"
~~~

The script still obtains the port from results/postgres-address.txt exactly as the original did. The public copy substitutes environment-variable lookups for the original host, database name, user, password, and Ray temporary path. No test logic, SQL generation, expected-result assertion, or source revision was changed. These substitutions were checked for exact reversibility against the original script, but the resulting copy has not been rerun.

## Database fixture and execution

The fixture below is implemented by the original runner and preserved in [postgres-fixture.sql](scripts/postgres-fixture.sql):

~~~sql
CREATE TYPE pair_key AS (a integer, b integer);
CREATE TABLE events (id integer PRIMARY KEY, shard_key pair_key);
INSERT INTO events
SELECT i, CASE i % 4
    WHEN 0 THEN ROW(NULL::integer, i)::pair_key
    WHEN 1 THEN ROW(i, NULL::integer)::pair_key
    WHEN 2 THEN ROW(i, i + 1)::pair_key
    ELSE ROW(NULL::integer, NULL::integer)::pair_key END
FROM generate_series(1, 200) AS i;
~~~

Run fixture mode once; it executes this SQL and creates the bundled SQLite fixture. Do not also apply the SQL file beforehand. The following interpreter options, modes, environment flags, and timeout match the recorded experiment; directory and interpreter locations are parameterized:

~~~bash
cd "$RUN_DIR"
"$PYTHON" -B experiment_bc.py fixture
"$PYTHON" -B experiment_bc.py direct
export PYTHONDONTWRITEBYTECODE=1
export RAY_USAGE_STATS_ENABLED=0
export RAY_DEDUP_LOGS=0
timeout --signal=TERM 180 "$PYTHON" -B -u experiment_bc.py e2e
~~~

Capture stdout/stderr if retaining a new run, and keep raw runtime logs and connection settings private. Query JSONL files append, so use fresh scratch space for every run.

The independent witness was executed through psql inside the original disposable database container:

~~~bash
docker exec -i "$PG_CONTAINER" psql -X -v ON_ERROR_STOP=1 -U "$REPRO_PG_USER" -d "$REPRO_PG_DATABASE" -f - < "$RUN_DIR/sources/postgres-witness.sql" > "$RUN_DIR/results/postgres-witness-psql.txt"
~~~

PG_CONTAINER represents the original container identity after redaction. This exact command form requires a containerized PostgreSQL instance with an appropriately configured local psql connection. For an externally provided database, run the same SQL with its configured client; that transport was not tested in the recorded experiment.

## What to verify

In results/postgres-{base,head}-direct-tasks.json, expect four task entries with row counts Base [88,44,37,31] and HEAD [88,19,13,11]. The result JSON files should show Base 200 rows, no missing or duplicate IDs; HEAD 131 rows, 69 missing IDs, no duplicates. Direct and end-to-end ID sets must agree.

In the end-to-end query JSONL files, verify four distinct SELECT * FROM (...) shard reads, each executed by a process ID different from driver_pid in its result JSON. Their rowcounts must match the direct task counts. The recorded events show one distinct worker process and four distinct shard SQL statements per revision. No unsharded SELECT id, shard_key FROM events was executed. Four output blocks alone would be insufficient evidence.

For row 1, the witness should show (1,), type pair_key, hash 1072681338, bucket 2, both NULL tests false, Base task 2 true, and all four HEAD task predicates false. [fixture-inspection.json](observed/fixture-inspection.json) and [results.json](observed/results.json) retain the complete observed values and ID sets.

The runner intentionally asserts the characterized HEAD omissions. A successful runner means the recorded regression was reproduced; it does not mean HEAD is correct. It also asserts the bundled SQLite result, so a change in that unrelated behavior can fail the combined run.

## Integrity and boundaries

From this package directory, verify every published file:

~~~bash
sha256sum --check SHA256SUMS
~~~

[PUBLICATION-ALLOWLIST.json](PUBLICATION-ALLOWLIST.json) enumerates the complete public package and its transformations. Checksums establish byte integrity, not independent execution. The original raw evidence stays in its private archive; complete logs and environments are deliberately excluded.

No full revision rebuild, alternative PostgreSQL version, combined PR patch, performance test, or rerun of the sanitized script is claimed. The tested boundary is two exact SQLDatasource source revisions under the same recorded installed runtime.
