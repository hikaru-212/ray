# Reproduction and portability

## What was actually executed

The historical command record is [metadata/executed-test-commands.redacted.json](metadata/executed-test-commands.redacted.json). It contains all six commands, working directories, redirections, and recorded exit codes. The only changes to this copy are literal replacement of machine paths by documented markers. It is evidence of the original execution, not a shell script.

| Run | Parent exit | HEAD exit |
|---|---:|---:|
| run_experiments.py --cases A,B,C | 0 | 0 |
| run_followup.py | 0 | 0 |
| pytest test_candidate_a.py | 0 | 1 |

The command metadata was assembled after the original executions; it is not an independent shell transcript. Full stdout/stderr and the pytest summaries corroborate the outcomes. The two main/follow-up commands test recorded behavioral predictions. The separate pytest asserts complete one-L placement in A and intentionally exposes HEAD's changed result.

For each revision label `parent` and `head`, the historical command shapes were:

```text
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=@PHASE2_ROOT@/<revision>/python @PHASE2_ROOT@/venv/bin/python -B @PHASE2_ROOT@/run_experiments.py --revision <revision> --cases A,B,C > @PHASE2_ROOT@/<revision>-ABC.log 2>&1
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=@PHASE2_ROOT@/<revision>/python @PHASE2_ROOT@/venv/bin/python -B @PHASE2_ROOT@/run_followup.py --revision <revision> > @PHASE2_ROOT@/<revision>-followup.log 2>&1
PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=@PHASE2_ROOT@/<revision>/python @PHASE2_ROOT@/venv/bin/python -B -m pytest -q -s -p no:cacheprovider --rootdir=@PHASE2_ROOT@ --confcutdir=@PHASE2_ROOT@ @PHASE2_ROOT@/test_candidate_a.py > @PHASE2_ROOT@/<revision>-A-quality-pytest.log 2>&1
```

Do not run these against the original evidence directory: the original scripts write result files next to themselves and would overwrite prior results.

## Source and environment prerequisites

Use a new disposable run directory, with exact extracted sources at `parent/python/ray` and `head/python/ray`, plus each revision's `src/ray/protobuf` tree. Obtain them from an existing Ray Git object database using `git archive <revision> python/ray src/ray/protobuf`; the required revisions are:

- parent: `938ab39460b257374624479c6739fa80315939ea`
- head: `3b45229e91b82d367b4706fbc26c2cbf31541c51`

A Git checkout by itself is not sufficient. The original environment used:

1. A new Python 3.12.3 virtualenv, referencing a pre-existing Ray test environment's site-packages through a `.pth` file. No complete dependency lockfile was recorded; the selected version metadata is not a comprehensive lock.
2. An identical Ray 2.58.0 wheel `_raylet.so` copied into each extracted `python/ray` directory. Its SHA-256 is `af46848e8b413a027aa17b958d1dd1bf93f41c4a55b9009002ce7e1112ef6ec0`.
3. Protobuf Python/gRPC modules generated from each exact source tree using grpcio-tools 1.75.0. The 52 input files and command arguments are in [environment metadata](metadata/environment.redacted.json).
4. Generated modules placed in `python/ray/core/generated`; generated import/module namespaces were rewritten from `src.ray.protobuf[.public/.events]` to `ray.core.generated`. Original source/proto files were not edited.

The preserved [successful setup script](reproducer/setup/prepare_runtime_v3.redacted.py) documents these generation steps. Its dependency-path marker must be replaced in a disposable copy if used; its location must also be adjusted so its parent is the run directory. It expects sources and native libraries to exist already. It is not a turnkey installer and writes files.

The first all-proto attempt failed because `reporter.proto` imports unavailable OpenCensus metrics definitions; [the original failure](logs/parent-protoc.log) is included. The successful setup excluded that unrelated file. Import provenance contains no reporter binding, and all 247 loaded Ray modules per revision came from the respective isolated tree. [Successful protoc logs](logs/parent-protoc-v3.log) contain only unused-import warnings. Both generated descriptor sets are byte-identical. This is evidence against a differential protobuf-schema explanation, not proof of complete native compatibility.

## Original scripts

`reproducer/run_experiments.py`, `run_followup.py`, and `test_candidate_a.py` are byte-identical to their historical originals. They execute production `ResourceDemandScheduler.schedule()`, not a rewritten scheduler.

They require `environment.json` beside the scripts, with the recorded `revisions` entries. The main harness verifies the loaded scheduler's SHA-256; the follow-up also checks every loaded Ray module's resolved path. The trace observer depends on exact source-line text, and the sorting preflight extracts a nested code object from these specific revisions. These assumptions make the scripts revision-specific.

To use them unchanged, copy all three into a fresh prepared run directory, place the environment metadata there under `environment.json`, and use the recorded command shapes with that directory and its interpreter. The scripts read only the environment metadata's `revisions` section; path-redacted provenance fields are not runtime paths. Preserve any new run separately from historical evidence.

## Separate portability adaptation — not executed

`reproducer/adapted/` contains separately labeled copies. The adapted main script obtains its output/source root from `RAY_REVIEW_RUN_ROOT` and additionally checks the selected scheduler path. The follow-up and quality-test copies are unchanged. This moves outputs away from the published scripts without changing fixtures, scheduling calls, trace logic, or assertions.

These adaptations were syntax-checked during publication preparation, **not executed against Ray**. They are not the source of any recorded results. They still require prepared exact-revision sources, generated bindings, native support, dependencies, and `environment.json` in the run directory.

The following is a derived reproduction recipe, not a command claimed to have been run. Set `PACKAGE` to this publication directory, `RUN` to a fresh prepared run directory, and `PYTHON` to that run's virtualenv interpreter:

```bash
export RAY_REVIEW_RUN_ROOT="$RUN"
for revision in parent head; do
  PYTHONDONTWRITEBYTECODE=1 PYTHONPATH="$RUN/$revision/python" "$PYTHON" -B "$PACKAGE/reproducer/adapted/run_experiments.py" --revision "$revision" --cases A,B,C > "$RUN/$revision-ABC.log" 2>&1
  PYTHONDONTWRITEBYTECODE=1 PYTHONPATH="$RUN/$revision/python" "$PYTHON" -B "$PACKAGE/reproducer/adapted/run_followup.py" --revision "$revision" > "$RUN/$revision-followup.log" 2>&1
  PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 PYTHONDONTWRITEBYTECODE=1 PYTHONPATH="$PACKAGE/reproducer/adapted:$RUN/$revision/python" "$PYTHON" -B -m pytest -q -s -p no:cacheprovider --rootdir="$RUN" --confcutdir="$RUN" "$PACKAGE/reproducer/adapted/test_candidate_a.py" > "$RUN/$revision-A-quality-pytest.log" 2>&1
done
```

Check each command's exit status individually; HEAD's quality test is expected to exit 1 for the recorded revision. Do not interpret the shell loop's final status as a complete test summary.

## Package-only verification

`checks/verify_package.py` uses the Python standard library and does not import Ray or rerun scheduling. It checks the manifest, artifact hashes, result/log agreement, fixture equality, capacities, traces, and pytest outcomes. With `--original-root PATH`, it additionally validates original hashes and exact redaction transformations against the locally retained Phase 2 files.
