# Reproducing the recorded Experiment A matrix

This document separates the commands that were actually executed in the private experiment from instructions for a possible future rerun. Packaging did not rerun either experiment.

## Recorded environment

| Component | Recorded value |
| --- | --- |
| Python | 3.12.3, GCC 13.3.0 |
| SQLite | 3.45.1 |
| Common installed Ray | 2.58.0 |
| Ray runtime commit | `cec0a091b12675bbf101cf8a350c8706107baee1` |
| Ray CPUs | 2 |
| Object store | 128 MiB |

The experiment used exact Base and HEAD `SQLDatasource` source files inside the common runtime. It directly called the extracted author test function; it did not invoke pytest and did not build either complete repository revision.

## Commands actually executed

The private command wrappers ran these logical commands. Private interpreter, evidence, and Ray scratch paths are represented by variables:

```bash
cd "$ORIGINAL_RUN_DIR"
"$ORIGINAL_PYTHON" -B experiment_a.py

export PYTHONDONTWRITEBYTECODE=1
export RAY_USAGE_STATS_ENABLED=0
export RAY_DEDUP_LOGS=0
timeout --signal=TERM 180 "$ORIGINAL_PYTHON" -B -u experiment_a_test_matrix.py
```

The original command files have SHA-256 values:

```text
8a034a16ae0ff8a32e9e768b9eee50c60f9b569008310a120177e99c0d569a09  commands/04_experiment_a.sh
95cb217ff2efe0c723d186cced66128e69481ed20d737fe4d73654542304fad1  commands/07_experiment_a_test_matrix.sh
```

## Required source files

Place these files in a fresh source directory using the exact names below:

```text
88f1ffefc6b5a88507fbfdb448edecc44664cefa-sql_datasource.py
134e84169b605d67f36f99c7c841ba113f704356-sql_datasource.py
```

Expected identities:

| Revision | SHA-256 | Git blob |
| --- | --- | --- |
| Base | `6ac6859a53b8097ac80ffb4e621983faf95b0456ec16df5bb42da0ca34ed012b` | `2168bd058932e3d25b6300ebb307cd9289ed58fd` |
| HEAD | `7f5d3d43bc3eec591cbf971434e83806834a00a48ff51930608bbd81e445f20d` | `61d24da3295534df392750a56dd56ad4b88a13f9` |

If the corresponding Git objects are already present in a trusted Ray clone, the Base extraction form is:

```bash
git -C "$RAY_SOURCE" show \
  88f1ffefc6b5a88507fbfdb448edecc44664cefa:python/ray/data/_internal/datasource/sql_datasource.py \
  > "$SOURCE_DIR/88f1ffefc6b5a88507fbfdb448edecc44664cefa-sql_datasource.py"
```

Packaging did not fetch the unavailable HEAD object. A future operator must acquire that source through a separately trusted read-only mechanism and verify both identifiers above. No branch checkout is required.

## Public runner transformations

The historical matrix imported `load_source()` from a combined Experiment B/C script. The public package instead provides [source_loader.py](scripts/source_loader.py), which preserves the original importlib loading, `sys.modules` registration, and `ray.cloudpickle.register_pickle_by_value()` operations while accepting an explicit source directory. The rest of Experiment B/C, including database configuration, is omitted.

[authorizer_probe.py](scripts/authorizer_probe.py) is derived from the historical `experiment_a.py`. It replaces private root paths with command-line directories and reads the exact public test-function snippet. Its callback extraction, `arg1` to `arg2` replacement, SQLite probes, assertions, and HEAD datasource checks are retained.

[test_matrix.py](scripts/test_matrix.py) is derived from the historical matrix runner. It replaces private paths with arguments, imports the minimal loader, and writes its result to the supplied scratch directory. The extracted author function, two callback variants, Base/HEAD loop, in-memory `read_api.SQLDatasource` substitution, expected Base `ValueError`, and assertions are retained.

These transformed files are not byte-identical to the historical scripts and were not executed during packaging. They were limited to static syntax parsing and source/provenance comparison.

## Future reproduction instructions

Use a compatible virtual environment with Ray and its Ray Data dependencies. Choose fresh, private scratch paths outside the evidence repository:

```bash
set -euo pipefail
: "${PACKAGE:?set this package directory}"
: "${SOURCE_DIR:?set the verified datasource source directory}"
: "${PYTHON:?set a compatible virtual-environment Python}"
: "${RUN_ROOT:?set a new private scratch directory}"

test ! -e "$RUN_ROOT"
mkdir -p "$RUN_ROOT"

"$PYTHON" -B "$PACKAGE/scripts/authorizer_probe.py" \
  --source-dir "$SOURCE_DIR" \
  --work-dir "$RUN_ROOT/authorizer"

export PYTHONDONTWRITEBYTECODE=1
export RAY_USAGE_STATS_ENABLED=0
export RAY_DEDUP_LOGS=0
timeout --signal=TERM 180 "$PYTHON" -B -u "$PACKAGE/scripts/test_matrix.py" \
  --source-dir "$SOURCE_DIR" \
  --work-dir "$RUN_ROOT/matrix" \
  --ray-temp-dir "$RUN_ROOT/ray"
```

Do not reuse a scratch directory: the scripts create SQLite databases and intentionally fail if their work directory already exists.

## Expected results

The authorizer probe should observe `SQLITE_FUNCTION` with `arg1=None` and `arg2="mod"`. The original callback should allow `MOD()` and retain `MOD`; the experimental callback should deny it and make the HEAD datasource select `%`.

The expected matrix is:

| Callback | Base | HEAD |
| --- | --- | --- |
| Original `arg1` | PASS, 4 blocks, 500 rows | PASS, 4 blocks, 500 rows |
| Experimental `arg2` | Expected `ValueError` | PASS, 4 blocks, 500 rows |

A future rerun would be new evidence and must record its own environment, source identities, complete output, privacy review, and checksums. It must not be represented as the historical execution preserved here.

## Package verification

From the package directory:

```bash
sha256sum --check SHA256SUMS
```

This verifies public bytes only. It does not rerun Ray, SQLite, or pytest.
