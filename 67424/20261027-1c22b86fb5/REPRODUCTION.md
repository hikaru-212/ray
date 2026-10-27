# Reproducing A1 from pinned sources

These are manual instructions, not commands executed during package preparation. Use disposable source exports. Do not run historical setup scripts against retained artifacts or the review worktree.

## Exact historical runs

[history/run.sh](history/run.sh) is the original recording wrapper. Each `logs/<label>.command` records cwd and shell-escaped argv; `.output` is combined stdout/stderr; `.exit` is the command exit status; `.start/.end` are UTC. Raw files are untrimmed and unredacted.

| Run | Command | Build / test exit | Selection |
| --- | --- | --- | --- |
| HEAD final | [command](logs/head-experiments-3.command) | success / 0 | Phase2.A* (5 tests, including A1) |
| Parent final | [command](logs/parent-experiments-1.command) | success / 0 | Phase2.A* (same 5 tests) |
| Parent A1 confirmation | [command](logs/parent-a1-only.command) | success / 0 | Phase2.A1DelayedStatusAndFinalLineageRelease |

HEAD used `PHASE2_EXPECT_DELETE=1`; parent used `0`. Both used jobs 2, local RAM 3072 MiB, JVM heap 1536 MiB and per-test timeout 60 seconds. Outer limits were 900 seconds for HEAD, 1800 seconds for the initial parent run and 180 seconds for cached parent A1, with a 20-second kill grace. Successful runs did not reach those bounds.

The HEAD baseline build and two earlier harness failures are preserved. The latter are not parent production failures. `history/fix_test_visibility.py` and `history/fix_message_move.py` record symmetric fixture fixes; **do not execute them against the retained original experiment**. Pre-fix files demonstrate the exact minimal changes. Earlier source hashes predate the final callback fix.

Original XML files provide existing test summaries. Full logs contain A2/A3; the README/JSON extract A1 only. All five test bodies remain in the identical source to preserve its hash; use the A1-only filter below.

## Environment

Source pins Bazel 7.5.0; retained venv metadata records Python 3.12.3. Packaging-time observations were Ubuntu 24.04.4 LTS, aarch64 and GCC 13.3.0. These are post-experiment observations, not a full contemporaneous environment capture. See [environment metadata](metadata/environment-observed.json), copied `.bazelrc` files and `pyvenv.cfg`. The full inherited environment, system package inventory and resolved dependency cache were not archived.

Use a supported Ray C++ build environment with Bazel already installed at the pinned version; see the [pinned development guide](https://github.com/ray-project/ray/blob/1c22b86fb5769f09a436bfdd4666aedef4a73847/doc/source/ray-contribute/development.md). This fixture needs no running Ray cluster or modified Python wheel. Bazel may fetch dependencies. The historical local repository cache is not included.

The runs reused a Bazel output base after shutting down the HEAD build server. Do not use the old saved HEAD executable: shared-library paths point into subsequently reused build output, so it is not a portable HEAD binary. Rebuild from pinned sources with independent output bases.

## Fresh isolated setup

Set the first three paths explicitly. A local Ray repository must already contain both commits. The scratch path must not exist. This creates exports, not branches or worktrees, and changes no refs.

```bash
set -euo pipefail
A1_PACKAGE=/absolute/path/to/67424/20261027-1c22b86fb5
A1_RAY_REPO=/absolute/path/to/ray
A1_ROOT=/absolute/path/to/new-disposable-a1-directory
A1_BAZEL=$(command -v bazel)

test ! -e "$A1_ROOT"
mkdir "$A1_ROOT"
python3 -m venv "$A1_ROOT/venv"
mkdir "$A1_ROOT/logs"

for A1_VARIANT in parent head; do
  case "$A1_VARIANT" in
    parent) A1_REV=ed1d83ac1f353aec7c0473bc46a57e04d8767ee2 ;;
    head) A1_REV=1c22b86fb5769f09a436bfdd4666aedef4a73847 ;;
  esac
  test "$(git -C "$A1_RAY_REPO" rev-parse "$A1_REV^{commit}")" = "$A1_REV"
  mkdir "$A1_ROOT/$A1_VARIANT"
  git -C "$A1_RAY_REPO" archive "$A1_REV" |
    tar -x -C "$A1_ROOT/$A1_VARIANT"
  cp "$A1_PACKAGE/reproducer/phase2_borrower_rpc_test.cc" \
    "$A1_ROOT/$A1_VARIANT/src/ray/core_worker/tests/"
  cat "$A1_PACKAGE/reproducer/BUILD.append.bazel" >> \
    "$A1_ROOT/$A1_VARIANT/src/ray/core_worker/tests/BUILD.bazel"
  cmp "$A1_PACKAGE/reproducer/$A1_VARIANT.BUILD.bazel" \
    "$A1_ROOT/$A1_VARIANT/src/ray/core_worker/tests/BUILD.bazel"
  sha256sum "$A1_ROOT/$A1_VARIANT/src/ray/core_worker/tests/phase2_borrower_rpc_test.cc"
done
```

Both test hashes must equal `0783cc096ed4d0421c11ff472b54cfdb334ba00eb85382f14ed51ba5249b9b21`. The BUILD suffix is the exact original append; never append twice. No production rule/source is replaced.

## Run only A1

Continue in the same Bash session. This uses distinct output bases and a 30-minute build limit. It preserves A1's logical experiment but is not a claim of bit-for-bit build reproducibility or a newly executed command.

```bash
for A1_VARIANT in parent head; do
  case "$A1_VARIANT" in
    parent) A1_EXPECT=0 ;;
    head) A1_EXPECT=1 ;;
  esac
  A1_COMMAND=(
    timeout --signal=TERM --kill-after=20s 1800s
    env "PATH=$A1_ROOT/venv/bin:$PATH"
    "VIRTUAL_ENV=$A1_ROOT/venv"
    "PYTHON3_BIN_PATH=$A1_ROOT/venv/bin/python"
    "$A1_BAZEL"
    "--output_user_root=$A1_ROOT/bazel-$A1_VARIANT"
    "--output_base=$A1_ROOT/output-$A1_VARIANT"
    --host_jvm_args=-Xmx1536m
    test //src/ray/core_worker/tests:phase2_borrower_rpc_test
    --test_filter=Phase2.A1DelayedStatusAndFinalLineageRelease
    "--test_env=PHASE2_EXPECT_DELETE=$A1_EXPECT"
    --test_output=all --test_timeout=60
    --jobs=2 --local_ram_resources=3072 --noshow_progress
  )
  (
    cd "$A1_ROOT/$A1_VARIANT"
    { printf 'cwd=%q\n' "$PWD"; printf '%q ' "${A1_COMMAND[@]}"; printf '\n'; } \
      > "$A1_ROOT/logs/$A1_VARIANT.command"
    date -u +%FT%TZ > "$A1_ROOT/logs/$A1_VARIANT.start"
    set +e
    "${A1_COMMAND[@]}" > "$A1_ROOT/logs/$A1_VARIANT.output" 2>&1
    A1_EXIT=$?
    set -e
    printf '%s\n' "$A1_EXIT" > "$A1_ROOT/logs/$A1_VARIANT.exit"
    date -u +%FT%TZ > "$A1_ROOT/logs/$A1_VARIANT.end"
    printf '%s: exit %s\n' "$A1_VARIANT" "$A1_EXIT"
  )
done
```

The fixture's default Await is 5 seconds; the idle observation is 100 ms. Inspect actual observation fields and failures. A timeout or build failure alone does not verify a production defect.

After the decisive observations, AbortForHarness explicitly removes pending callbacks and injects an error reply. This is test-only cleanup, not a production cancellation path. Preserve the exact fixture and assertions when comparing revisions.

## Verify the evidence without running Ray

Using an existing Python 3 environment:

```bash
python3 -B "$A1_PACKAGE/checks/verify_package.py"
```

This reads package files and prints an integrity/result summary. It does not run C++ tests, builds, historical scripts or network requests.
