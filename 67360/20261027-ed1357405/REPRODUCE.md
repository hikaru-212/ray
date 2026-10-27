# Reproduction instructions

The main tests, follow-up tests, both conftest files, and both runners in this directory are byte-for-byte copies of the files executed in the original experiment. This document is editorial guidance, translated and clarified for public readers; it is not original experiment output.

The evidence distinguishes three kinds of material:

- **Actually executed in the original experiment:** the unchanged tests and runners, with subprocess arguments, environments, return codes, and durations recorded in [observed/commands.json](observed/commands.json). That record replaces private path prefixes with documented placeholders.
- **Examples prepared for public readers:** the shell blocks below. They have not been executed as a fresh public-package reproduction, including the new virtual-environment installation and three-archive download example.
- **Unexecuted public helpers:** the portable `prepare_runtime.py` derivative and the new `check_sources.py` helper. Their presence does not add runtime validation.

Neither packaging nor English documentation finalization reran tests, generated protobuf bindings, performed a native build, or executed these helpers.

## 1. Use a fresh private execution directory

The Bash examples below are for a future reproduction. Do not run the runners inside the evidence Git repository, an existing source worktree, or an earlier experiment directory.

```bash
export EVIDENCE_PACKAGE=/absolute/path/to/review-evidence/67360/20261027-ed1357405
export RUN_ROOT="$(mktemp -d)"
cp -R "$EVIDENCE_PACKAGE/tests" "$EVIDENCE_PACKAGE/tests_followup" "$EVIDENCE_PACKAGE/scripts" "$RUN_ROOT/"
cp "$EVIDENCE_PACKAGE/provenance.json" "$RUN_ROOT/"
mkdir -p "$RUN_ROOT/sources" "$RUN_ROOT/runtime" "$RUN_ROOT/tmp" "$RUN_ROOT/cache" "$RUN_ROOT/artifacts" "$RUN_ROOT/logs"
python3.12 -m venv "$RUN_ROOT/venv"
export PYTHONDONTWRITEBYTECODE=1
```

The original dedicated venv used a .pth file to read dependencies from an existing environment. The example below installs dependencies into a new venv instead of recreating that private path. Dependency resolution may differ; this is neither a complete lockfile nor a validated installation procedure.

```bash
"$RUN_ROOT/venv/bin/python" -m pip install \
  'ray[default]==2.58.0' 'pytest==7.4.4' \
  'protobuf==6.33.6' 'grpcio==1.75.0' 'grpcio-tools==1.75.0'
export AUDIT_NATIVE_RAY="$("$RUN_ROOT/venv/bin/python" -B -c 'import importlib.util; print(next(iter(importlib.util.find_spec("ray").submodule_search_locations)))')"
```

`AUDIT_NATIVE_RAY` must identify a compatible installed Ray package containing `_raylet.so`, `_version.py`, and `core/generated/`. The original native-extension hash, platform, and selected dependency versions are in provenance.json. A different architecture or wheel may have a different hash; record that as a difference in the reproduction environment rather than claiming to have used the original binary. An existing compatible venv is also an option. Do not install into system Python.

## 2. Obtain exact sources without modifying production code

The required paths can be exported from read-only Git objects. The example below instead downloads all three pinned GitHub archives. Keep the large archives in the new private RUN_ROOT, outside the evidence Git branch.

```bash
while read -r label revision; do
  curl --fail --location \
    "https://codeload.github.com/ray-project/ray/tar.gz/$revision" \
    --output "$RUN_ROOT/artifacts/$label.tar.gz"
  mkdir "$RUN_ROOT/sources/$label"
  tar -xzf "$RUN_ROOT/artifacts/$label.tar.gz" \
    -C "$RUN_ROOT/sources/$label" --strip-components=1 \
    "ray-$revision/python/ray" "ray-$revision/src/ray/protobuf" "ray-$revision/pytest.ini"
done <<'REVISIONS'
parent 3608367390ff80151fe0ab02a54d8e8223905146
old ad540e643022b8c449299653e476f20da35afd2d
new ed1357405af868d81389047835e3562b29ca2751
REVISIONS

"$RUN_ROOT/venv/bin/python" -B "$RUN_ROOT/scripts/check_sources.py"
"$RUN_ROOT/venv/bin/python" -B "$RUN_ROOT/scripts/prepare_runtime.py" \
  > "$RUN_ROOT/logs/prepare-runtime.log" 2>&1
```

`check_sources.py` checks only the selected source files recorded in the public provenance. The original audit also verified all 5,454 exported source files and 229 loaded source modules per revision. The helper's narrower check cannot replace that full audit.

The public `prepare_runtime.py` differs from the original executed all-revision setup in two respects: it adds a provenance docstring, and it replaces the fixed wheel path with the resolved path supplied by `os.environ["AUDIT_NATIVE_RAY"]`. The overlay, protoc invocation, and generated import-path rewriting logic are otherwise unchanged. The original experiment also used a new-only setup after an acquisition timing issue. This example prepares all three revisions after all sources are present; **that public setup sequence has not been executed**.

The overlay links to the source tree without modifying it and writes generated code into the new runtime directory. It compiles 52 protobuf files per revision, excluding reporter.proto, and retains the wheel's reporter bindings. This import scaffold does not establish arbitrary native/protobuf ABI compatibility or RPC wire interoperability.

## 3. Run the same behavioral oracles

```bash
"$RUN_ROOT/venv/bin/python" -B "$RUN_ROOT/scripts/run_matrix.py" \
  > "$RUN_ROOT/logs/run-matrix.log" 2>&1
"$RUN_ROOT/venv/bin/python" -B "$RUN_ROOT/scripts/run_followup.py" \
  > "$RUN_ROOT/logs/run-followup.log" 2>&1
```

Each runner starts a separate Python subprocess for each revision, with a 180-second timeout. The unchanged runners record subprocess return codes but do not themselves exit nonzero merely because Parent has expected failures. **Inspect the per-revision result JSON and pytest logs; do not rely solely on the runner's shell exit code.**

The arguments actually used in the original experiment are preserved in [observed/commands.json](observed/commands.json). `@AUDIT_ROOT@` denotes the original private experiment root; `@DEPENDENCY_SITE_PACKAGES@` denotes the existing dependency environment; `@READ_ONLY_GIT_REPO@` denotes the Git object repository used for source acquisition. These are literal path substitutions in a historical record, not directly executable JSON.

For reference, the main-suite invocation below uses the same pytest arguments for New, with paths relocated to RUN_ROOT. Execute it with RUN_ROOT as the working directory to match the runner. Use the appropriate label for Parent or Old. This is a documentation example, not a separate recorded execution.

```bash
AUDIT_ROOT="$RUN_ROOT" AUDIT_VARIANT=new \
PYTHONPATH="$RUN_ROOT/runtime/new" \
PYTHONDONTWRITEBYTECODE=1 PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 \
TMPDIR="$RUN_ROOT/tmp" XDG_CACHE_HOME="$RUN_ROOT/cache" RAY_TMPDIR="$RUN_ROOT/tmp" \
OMP_NUM_THREADS=1 OPENBLAS_NUM_THREADS=1 \
"$RUN_ROOT/venv/bin/python" -m pytest -c /dev/null \
  --confcutdir="$RUN_ROOT/tests" -vv -ra -p no:cacheprovider \
  --basetemp="$RUN_ROOT/tmp/pytest-new" \
  --junitxml="$RUN_ROOT/artifacts/new.xml" "$RUN_ROOT/tests/test_behavior.py"
```

The follow-up uses `AUDIT_VARIANT=new-long` and `PYTHONPATH="$RUN_ROOT/runtime/new:$RUN_ROOT/tests"`, changes confcutdir to tests_followup, and uses new-long output names. The full original argument vectors are in commands.json. Prefer the unchanged runners to preserve all environment isolation settings.

## 4. Expected results and interpretation

| Suite | Parent | Old | New |
| --- | --- | --- | --- |
| Main suite, 29 cases | 20 passed / 9 failed; pytest exit 1 | 29 passed; exit 0 | 29 passed; exit 0 |
| Follow-up, 8 cases | 8 failed; pytest exit 1 | 8 passed; exit 0 | 8 passed; exit 0 |
| Common behavioral invariants only | 14 passed / 17 failed | 31 passed | 31 passed |
| Total pytest results | 20 passed / 17 failed | 37 passed | 37 passed |

The main suite's four `test_ordering_characterization` cases and two `test_reconstruction_component_boundary` cases must remain separate from the 31 common behavioral invariant cases. Passing a reconstruction case does not mean lifetime counts survived. The 17 Parent failures are test cases, not 17 independent defects.

Original failing Parent node IDs are listed in observed/summary.json. Each case's call outcome, category, and observations are in observed/cases.json. Keep those classifications when comparing a new run.

New outputs are written to RUN_ROOT/artifacts and RUN_ROOT/logs, with traces under artifacts/traces. The unchanged conftest records environment information, sys.path, and loaded module paths. Treat these as private reproduction outputs; inspect and minimize them separately before any publication.

This documentation update checked existing evidence and public-file integrity without executing the setup or tests above. Preserve any future reproduction in a separate new directory so that neither this package nor earlier evidence is overwritten.
