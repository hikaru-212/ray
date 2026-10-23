[Ray PR #67013](https://github.com/ray-project/ray/pull/67013) fixes a Repeater bug that could discard a valid aggregate solely because a failed repeat completed last. This package preserves focused, independently executed evidence for that correction. It supports a positive technical assessment within the tested scope; it does not claim that the patch resolves every NaN/error policy.

| Revision | Exact commit |
| --- | --- |
| Pre-fix parent | `e00c7f4bed3d92da863230c2dfdc3ea81eec1778` |
| PR HEAD | `8ffddc460b8b65da3930b2a789399aafe0282568` |

The parent is the PR commit's direct parent. The runner verifies that relationship using local Git objects. It never checks out or edits Ray production files.

Repeater asks the wrapped Searcher for one configuration, creates three repeated trials, and forwards one aggregate under the primary trial ID once all three report. Missing results/metrics become NaN; `np.nanmean` ignores those NaNs. Before this patch, the last completion's `error` flag accompanied the mean. BayesOptSearch returns without registering a result when `error=True`; HyperOptSearch records `JOB_STATE_ERROR`. The patch removes the `error` keyword when the mean is not NaN, allowing the Searcher's default `False` to apply. It retains the last flag when the mean is NaN. The condition is `not np.isnan(mean)`, not `np.isfinite(mean)` or an explicit count of successful trials. Our valid-score cases use finite scalar values.

The default suite has 12 cases, selected from the independently executed Phase 2 matrix. It reuses that matrix's production replay and assertions, rather than replacing them with mock optimizer behavior. The observer records arguments, then delegates unchanged to the real downstream callback. The author's tests already catch the original bug, but use a fixed completion order and different failure subsets. These tests fix each trial's outcome and change only completion order, while inspecting actual BayesOpt and HyperOpt state.

| Retained cases | Count | Purpose |
| --- | ---: | --- |
| `one_failure`: scores 1, 5, failure; orders 012 and 210; both real searchers | 4 | I1/I2/I4: mean 3 must be accepted whether the failure finishes last or first. |
| `two_failures`: score 1, failure, failure; orders 012 and 210; both searchers | 4 | I1/I2/I4: a single finite score must survive; the final callback alternates between failure and success. |
| `all_success`: scores 1, 5, 9; one order; both searchers | 2 | Positive control: mean 5 and successful downstream state in both revisions. |
| `failure_missing_nan`: failure, missing metric, explicit NaN; orders 012 and 210; HyperOpt | 2 | I5/I6: characterize inherited flags, states and RuntimeWarning. Expected GREEN in both revisions because these assertions describe existing behavior. |
| `probe_all_nan.py`, explicitly invoked separately | 1 | I5: assert the stronger order-invariant state requirement. Expected RED in both revisions; not discovered by default pytest naming. |

Every replay also checks I3: callback counts after successive completions are exactly `[0, 0, 1]`, the callback uses the primary trial ID, and no unrelated result metric is forwarded. This covers one completion per trial; it does not establish idempotence under duplicate notifications. Fixed score lists and explicit expected means give the valid-score cases identical expectations across the two completion orders. There is no revision-dependent assertion logic.

For the smallest original-bug example, bind A=1, B=5, C=(no result, error=True):

| Completion order | Parent | PR |
| --- | --- | --- |
| C, B, A | Mean 3; BayesOpt target 3; HyperOpt DONE/ok with loss -3 | Same accepted result |
| A, B, C | Mean 3 but error=True; BayesOpt targets `[]`; HyperOpt ERROR with no accepted completed trial | Error keyword omitted; BayesOpt targets `[3.0]`; HyperOpt DONE/ok with loss -3 |

The single-score/two-failure cases similarly preserve mean 1. In max mode HyperOpt converts the aggregate to negative loss. State evidence comes from the actual BayesOpt target space and HyperOpt trial record/live-trial mapping, not a mock acceptance rule. In failed baseline cases the full downstream state is recorded before the valid-mean assertion fails.

For the all-NaN boundary, bind A=(no result, error=True), B=(missing metric, error=False), C=(NaN, error=False). In **both** revisions:

| Completion order | Aggregate/error | HyperOpt state |
| --- | --- | --- |
| A, B, C | NaN / False | DONE, status ok, loss NaN |
| C, B, A | NaN / True | ERROR, error metadata set |

The separate strict probe requires those states to be equal and fails on both revisions: **Parent RED / PR RED**, an inherited policy ambiguity, not a regression introduced by this PR. Each completion of such a group also emits `RuntimeWarning: Mean of empty slice` from Repeater: parent line 176, PR line 178. The default characterization cases check that warning explicitly and pass on both revisions. HyperOpt's `ok` status for NaN does not imply a useful optimization observation. Dependency deprecation warnings are recorded separately from NumPy RuntimeWarning.

Reproduce with Python 3.12 and a local Ray clone containing both exact objects. In the following commands, `package` is this directory; `ray_repo` may be any suitable clone/worktree. Run these commands from this directory, replacing only the repository location. The venv and all generated output are created outside both repositories. No prior Phase 2 temporary directory is needed.

```bash
package="$PWD"
ray_repo=/path/to/ray
verification_env=$(mktemp -d)
python3.12 -m venv "$verification_env/venv"
"$verification_env/venv/bin/python" -m pip install -r "$package/environment-lock.txt"

# Default expected-green PR suite; also checks the known parent failures.
"$verification_env/venv/bin/python" -B "$package/run_verification.py" \
  --ray-repo "$ray_repo"

# Same suite plus the explicitly expected RED/RED all-NaN probe.
"$verification_env/venv/bin/python" -B "$package/run_verification.py" \
  --ray-repo "$ray_repo" --probe-all-nan
```

The second command is sufficient to reproduce all preserved Phase 3 evidence; the first shows how to omit the diagnostic. Each invocation creates a fresh run directory and prints its location. Raw pytest logs, JUnit XML, callback/trace records and import audits are retained there. `summary.json` is portable and contains source/test hashes, relative import paths, exact dependencies, test counts, failure names, observed downstream states and warning locations.

The runner returns zero only if the expected complete pattern matches: 12 collected core cases; the four specific parent failures; all PR core cases passing; and, when requested, precisely the known single failure in each diagnostic run. Collection/import errors, skipped tests, different failed cases, or changed Ray repository status are not treated as successful reproduction. Individual pytest exits remain visible as 0 or 1. No `xfail` or variant-dependent weakening hides a failure. `probe_all_nan.py` is explicitly selected only with `--probe-all-nan` and is not part of ordinary `test_*.py` discovery.

The loader exports all Tune sources from each exact commit with `git archive`, verifies every exported file against its Git blob (242 files per revision in the recorded run), and routes `ray.tune` imports exclusively to that snapshot. Missing Tune modules fail instead of falling back to wheel Tune code. The session fixture verifies eight relevant imported modules against the corresponding Git object and records SHA256 hashes. Execution tracing confirms the real SearchGenerator, Repeater, trial group and downstream completion functions ran, including the changed PR lines. Ray native/support modules are supplied by the pinned wheel.

The permanent package was actually executed on 2026-10-23 with the checked Phase 2 venv; all 47 installed distributions matched the preserved lock. This was a new run of the packaged code, not reuse of the historical test verdicts. A fresh installation of the lock was not repeated in Phase 3.

| Newly rerun Phase 3 package | Parent | PR |
| --- | --- | --- |
| Default `test_repeater.py` | **8 passed, 4 failed**, pytest exit 1 | **12 passed**, exit 0 |
| Explicit `probe_all_nan.py` | **1 failed**, exit 1 | **1 failed**, exit 1 |

All four expected execution patterns matched; runner exit was 0. The four RED/GREEN cases are `one_failure` and `two_failures`, order 012, for BayesOpt and HyperOpt. Every replay had one downstream callback under the primary ID. The two default all-NaN cases emitted two RuntimeWarnings per revision, at the expected production source locations. See [the newly executed results](observed/phase3-results.json).

Historical Phase 2 observations are listed separately; the full matrices were **not** rerun or copied into this package:

| Historical Phase 2 suite | Parent | PR |
| --- | --- | --- |
| Independent matrix/buffer/kwargs tests | 214 passed, 32 failed | 246 passed |
| Strict order-invariant probes | 3 passed, 9 failed | 9 passed, 3 failed |
| Author's complete test file, identical on both sources | 16 passed, 2 failed | 18 passed |

The historical strict failures shared by both revisions were the all-NaN mixed-error cases across three backends. Both historical matrices captured 72 all-NaN RuntimeWarnings. Original code, logs and results were inspected; fingerprints and the specific adaptations are retained in [the Phase 2 history](observed/phase2-history.json). This historical record must not be mistaken for execution of newly packaged assertions.

The retained files have these roles:

```text
67013/
├── README.md
├── bootstrap.py                 exact-revision Tune import finder
├── conftest.py                  import provenance and trainable registration
├── environment-lock.txt        pinned, verified runtime dependencies
├── run_verification.py          immutable source export, execution and result audit
├── test_repeater.py             12 focused expected-green PR cases
├── probe_all_nan.py             separate, expected RED/RED diagnostic
└── observed/
    ├── phase2-history.json      historical results, source hashes and adaptations
    └── phase3-results.json      actual packaged rerun results and provenance
```

Omitted deliberately: the 246-case matrix, four redundant permutations per outcome, Recorder-only copies of real-searcher cases, separate none/empty/missing variations, synthetic error-with-metric cases, marker kwargs and BayesOpt warmup-buffer probes, the author's copied test file, full source trees, venv, bulk traces/logs, initial harness-debugging failures, full Git ref inventories, and the temporary report/reproduction text. These remain historical Phase 2 work; they were not deleted. The package needs no auxiliary BayesOpt or HyperOpt test implementation beyond the retained replay helper and locked dependencies. It does not modify other review packages or the evidence repository's root index.

Limits: tested on Python 3.12.3, NumPy 2.2.6, Ray 2.58.0 wheel, bayesian-optimization 1.4.3, HyperOpt 0.2.7 and pytest 7.4.4. The complete Tune Python source is revision-exact, but this is not a native build of either full Ray commit. A registered mock trainable is used only to construct real Trial objects; no training, distributed cluster, `Tuner.fit`, full TuneController scheduling, or full Ray test suite is run. Missing metrics are injected at the completion API; upstream strict metric validation can constrain actual trainable paths. HyperOpt uses its released 0.2.7 package rather than the repository's Git-pinned dependency. The compact suite chooses two diagnostic orders, not every permutation, and omits BayesOpt warmup buffering (covered historically). This is finite-scalar correctness evidence, not proof for every numeric type, infinity, duplicate callback or NaN policy.

Manual publication, after reviewing every added line and rerunning the tests, should stage only `67013/`. The commands below reject an unexpected branch, remote, or nonempty staging area; they do not stage other review evidence. The normal push has no force option. These commands are documentation only and were not executed during packaging.

```bash
set -e
cd /path/to/ray-review-evidence
test "$(git branch --show-current)" = review-evidence
test "$(git remote get-url --push origin)" = https://github.com/hikaru-212/ray.git
test -z "$(git diff --cached --name-only)"
git status --short --branch
git add -- 67013/
git diff --cached --check
git diff --cached --stat
git diff --cached -- 67013/
# Review the displayed diff before running the following two commands.
git commit -s -m "Preserve independent review evidence for Ray PR 67013"
git push origin HEAD:review-evidence
```

No commit, push, GitHub review, comment, issue or PR was created by this preservation task. A future positive technical review can cite the independent real-searcher RED/GREEN evidence while explicitly separating the inherited RED/RED NaN boundary.
