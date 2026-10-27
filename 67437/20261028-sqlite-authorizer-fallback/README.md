# PR #67437: SQLite authorizer test does not deny `MOD()`

[Ray PR #67437](https://github.com/ray-project/ray/pull/67437) adds an end-to-end test intended to force `SQLDatasource` from `MOD()` to the `%` operator, but the test checks the wrong SQLite authorizer argument and therefore does not actually deny `MOD()`.

## Finding

SQLite documents `SQLITE_FUNCTION` as action code `31`, with the third authorizer callback parameter `NULL` and the fourth parameter containing the function name. In Python's five-argument callback these values arrive as `action=31`, `arg1=None`, and `arg2="mod"`. See SQLite's official [Authorizer Action Codes](https://www.sqlite.org/c3ref/c_alter_table.html).

The preserved PR test uses:

```python
if action == sqlite3.SQLITE_FUNCTION and str(arg1).lower() == "mod":
```

The one-condition experimental correction uses `arg2` instead. The exact test-function source extracted from the preserved HEAD file is available at [observed/head-test-function.py.txt](observed/head-test-function.py.txt).

## Source revisions

| Role | Revision | Source status during packaging |
| --- | --- | --- |
| Base | `88f1ffefc6b5a88507fbfdb448edecc44664cefa` | Commit object available locally; preserved datasource bytes match the local Git object. |
| PR HEAD | `134e84169b605d67f36f99c7c841ba113f704356` | Commit object unavailable locally; preserved files match the Git blob IDs recorded by the original acquisition record. |

The HEAD test blob is `3d4d68408ffe38a0c4c4372a079d19f31ee1d65a`; the HEAD datasource blob is `61d24da3295534df392750a56dd56ad4b88a13f9`. The source-to-commit mapping comes from the preserved acquisition record. It has neither an independent signature nor a trusted timestamp. Full details and SHA-256 values are in [observed/provenance.json](observed/provenance.json).

## Native callback observation

The original execution recorded the following callback for `SELECT MOD(5,2)`:

```text
[31, None, "mod", None, None]
```

With the original `arg1` condition, `MOD()` returned `1.0`, `supports_sharding(4)` succeeded, and the selected syntax remained `MOD`. With only `arg1` changed to `arg2`, `MOD()` was rejected as unauthorized, `%` remained usable, and the HEAD datasource selected `%`. The structured observation is in [callback-observations.json](observed/callback-observations.json); selected original log lines are in [output.txt](observed/output.txt).

## Base/HEAD test matrix

| Callback used in the author test | Base | PR HEAD |
| --- | --- | --- |
| Original `arg1` | PASS: 4 blocks, 500 rows | PASS: 4 blocks, 500 rows |
| Experimental `arg2` | Expected `ValueError`: sharding unsupported | PASS: 4 blocks, 500 rows |

All four cases invoked the author's extracted test function. The three passing cases completed its assertions for four materialized blocks and all 500 rows. The experimental Base case entered the function and raised the expected `ValueError` from `ray.data.read_sql` before dataset execution. See [test-matrix.json](observed/test-matrix.json).

This matrix shows that the test as written also passes against Base and therefore does not distinguish the new `%` fallback. It does **not** show a correctness defect in the production fallback.

## Execution boundary

The historical experiment directly invoked the extracted function; it did not run pytest. It used a common installed Ray 2.58.0 runtime at commit `cec0a091b12675bbf101cf8a350c8706107baee1`, temporarily replacing only `ray.data.read_api.SQLDatasource` with an exact Base or HEAD source module loaded in memory. Neither complete repository revision was built.

The public runners in [scripts](scripts/) remove private paths and the unrelated Experiment B/C dependency. They preserve the source-loading and matrix logic but are transformed files and were **not rerun** during packaging. [REPRODUCE.md](REPRODUCE.md) distinguishes historical commands from future reproduction instructions.

## Integrity and publication safety

The original private evidence manifest was rechecked during packaging: 74 of 74 entries passed. `SHA256SUMS` verifies every public package file except itself. Checksums establish byte integrity, not independent execution, source authenticity, or a trusted execution time.

The public package excludes complete raw logs, private paths, host inventory, credentials, database endpoints, binary databases, process/container/session identifiers, the full HEAD test file, the unrelated Experiment B/C body, and all 117 runtime-log files. [PUBLICATION-ALLOWLIST.json](PUBLICATION-ALLOWLIST.json) records every published file, its source hashes, transformations, and exclusions.

## What this package does not establish

- A full Base or HEAD repository build.
- A full pytest run or pytest fixture/collection behavior.
- Correctness of the complete production fallback across supported databases.
- Results under another Ray, Python, or SQLite version.
- A rerun of the sanitized public runners.
- Cryptographically signed or independently timestamped provenance.
