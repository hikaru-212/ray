# PR #67406: composite NULL fields cause omitted SQL rows

[Ray PR #67406](https://github.com/ray-project/ray/pull/67406) addresses silent loss of rows with NULL shard keys reported in [Issue #67318](https://github.com/ray-project/ray/issues/67318). This package records a different case: the added predicates omit PostgreSQL composite values with a mixture of NULL and non-NULL fields.

**VERIFIED within the tested boundary:** Base returned all 200 fixture rows; PR HEAD returned 131, with 69 missing and no duplicates. This is a PR-introduced correctness regression in the tested SQLDatasource path. The database and installed Ray runtime were shared between comparisons. Neither full repository revision was built.

## Provenance and environment

| Component | Recorded value |
| --- | --- |
| Base | 8d53e59e3da91a39beb6b1b614265cf05d3d26a4 |
| PR HEAD | c4ace0b69f82f66772d05c286ed29a52b6f73dc3 |
| Python | 3.12.3 (GCC 13.3.0) |
| PostgreSQL | 15.19, aarch64-unknown-linux-musl, Alpine GCC 15.2.0 |
| psycopg2 | 2.9.13 (dt dec pq3 ext lo64) |
| PyArrow | 23.0.1 |
| Common installed Ray runtime | 2.58.0 |
| Runtime commit | cec0a091b12675bbf101cf8a350c8706107baee1 |

The two unmodified SQLDatasource source snapshots were extracted from the exact revisions above. Packaging verified their bytes against the corresponding Git objects. [provenance.json](observed/provenance.json) includes SHA-256 hashes and Git blob IDs.

The experiment dynamically loaded each source module and registered it for serialization by value. Direct tests executed its four read tasks against PostgreSQL. The end-to-end tests replaced only the installed read_api.SQLDatasource binding in memory, then called the installed ray.data.read_sql through its ordinary planner and worker execution. [The runtime entry-point snapshot](scripts/sources/runtime-read_sql.py) is included for inspection, not as a standalone executable.

## Failure timeline and invariant

1. Before this PR, a scalar SQL NULL propagates through hashing, ABS, and MOD. Comparing that NULL with any shard index yields UNKNOWN. WHERE keeps only TRUE, so the original partition predicates can silently lose those rows.
2. The PR routes keys satisfying IS NULL to task 0 and adds IS NOT NULL guards to other tasks.
3. For the tested PostgreSQL composite value (1, NULL), both IS NULL and IS NOT NULL return FALSE. They are not complementary predicates for this value.
4. Its valid integer hash selects bucket 2. Base includes it in task 2. HEAD task 0 rejects it because both alternatives are false; tasks 1–3 reject it because their IS NOT NULL guard is false.
5. The same omission appears in direct tasks and in the installed read_sql path using the exact HEAD datasource source.

For each stable source row, exactly one shard predicate must evaluate to TRUE: no omissions and no duplicates. A successful capability probe or four output blocks alone does not establish this invariant.

The explanation of the original scalar-NULL mechanism is **SOURCE-INFERRED** here; this package's observed regression is the composite case. Remediation and behavior on untested versions remain unestablished.

## Actual composite-key counterexample

~~~sql
CREATE TYPE pair_key AS (a integer, b integer);
CREATE TABLE events (id integer PRIMARY KEY, shard_key pair_key);
~~~

The fixture uses a real typed column, shard_keys=["shard_key"], the built-in hash_record function, and a psycopg2 DB-API connection. It uses neither a SQL-expression shard key nor a custom hash function. The source inspection and successful Base execution establish that this is an executable Ray configuration in the recorded environment.

[The SQL witness](scripts/postgres-witness.sql) and its [unaltered psql output](observed/witness.txt) show:

| Row 1 property | Observed value |
| --- | --- |
| Composite value | (1, NULL), displayed by PostgreSQL as (1,) |
| Composite type | pair_key |
| hash_record(shard_key) | 1072681338 |
| MOD(ABS(hash_record(shard_key)), 4) | 2 |
| shard_key IS NULL | FALSE |
| shard_key IS NOT NULL | FALSE |
| Base task 2 predicate | TRUE |
| HEAD predicates for tasks 0, 1, 2, 3 | FALSE, FALSE, FALSE, FALSE |

## Base versus HEAD

The same immutable 200-row fixture contains 100 mixed-NULL composites, 50 composites with two non-NULL fields, and 50 composites with two NULL fields. It contains no top-level SQL NULL keys.

| Observation | Base | PR HEAD |
| --- | ---: | ---: |
| Source rows | 200 | 200 |
| Direct task output rows | 200 | 131 |
| read_sql output rows | 200 | 131 |
| Missing IDs | 0 | 69 |
| Duplicate IDs | 0 | 0 |
| Task 0 rows | 88 | 88 |
| Task 1 rows | 44 | 19 |
| Task 2 rows | 37 | 13 |
| Task 3 rows | 31 | 11 |
| Distinct worker shard queries | 4 | 4 |
| Output blocks | 4 | 4 |

[results.json](observed/results.json) preserves every returned ID and direct-task membership. [fixture-inspection.json](observed/fixture-inspection.json) preserves all 200 composite values, hashes, buckets, and NULL tests, allowing the missing set to be checked independently. [worker-queries.json](observed/worker-queries.json) contains every captured PostgreSQL end-to-end query, including driver probes and all eight Base/HEAD shard reads, with the original SQL and returned row counts.

The four shard queries in each case executed in a Ray worker process distinct from the driver; this was actual four-way SQL sharding, not a single unsharded read followed by block splitting. The records show **one worker process**, not four simultaneous workers. Numeric process IDs have been consistently replaced with roles. No performance or concurrency benchmark is claimed. Task-0 imbalance is a separate performance concern; the demonstrated failure is omission.

## Interpretation and limitations

The missing 69 IDs are exactly the mixed-NULL composite rows whose hash bucket is nonzero. Mixed-NULL rows in bucket 0 survive through the modulo alternative. All returned rows retain their original IDs and key values. The regression conclusion is based on both the SQL witness and the Base/HEAD comparison, not on the count alone.

This is evidence from one PostgreSQL version and one shared Ray runtime. It does not establish full-revision build compatibility, multi-version coverage, a proposed fix, or a test of a combined patch with another PR. The package was prepared by checking preserved outputs and source objects; the experiments were not rerun during packaging.

The copied runner originally bundled the PostgreSQL and SQLite investigations. Its test logic is retained, including the SQLite case, to preserve provenance. Only connection settings and the Ray temporary directory were replaced with environment-variable references. SQLite findings and the unrelated MOD fallback investigation are excluded from the public observed results. The sanitized runner has been syntax-checked and checked against the original, but has not been executed after sanitization. A compatible external runtime and disposable database remain prerequisites.

See [REPRODUCE.md](REPRODUCE.md) for commands, [output.txt](observed/output.txt) for focused historical output, and [PUBLICATION-ALLOWLIST.json](PUBLICATION-ALLOWLIST.json) for every file's origin, transformations, and exclusions. SHA256SUMS covers every public package file except itself.
