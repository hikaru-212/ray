# Publication audit

## Scope and classification

This package preserves historical scheduler evidence and adds publication metadata, documentation, and a read-only package verifier. No scheduler experiment was rerun during publication preparation. No commit, push, PR, issue, review, or comment was created by this preparation.

The supported conclusion is **VERIFIED SCHEDULING-QUALITY REGRESSION — CONTRACT UNRESOLVED**. A complete one-L placement exists in A; the parent discovers it and the reviewed HEAD does not under the same cap. No verified completeness contract requires this greedy scheduler to discover every feasible placement.

## Artifact discovery and allowlist

All 17 explicitly required historical artifacts were found. Additional result files for all unused-fallback controls, ABC stdout/stderr, source-import provenance, setup scripts/logs, Git snapshots, source archives, generated descriptors, extracted sources, and a virtualenv were discovered. See [the inventory](checks/original-artifact-inventory.json).

The exact publication allowlist is [PUBLICATION-ALLOWLIST.txt](PUBLICATION-ALLOWLIST.txt). Only selected text evidence is included:

- 14 result JSON files; six A/B/C files are path-redacted, eight finite-cap/fallback-control files are byte-identical.
- 10 logs, including pytest pass/fail, main/follow-up runs, and successful/failed protobuf setup.
- Environment, exact command-record, import-provenance, and original preservation metadata.
- Three original test scripts and a path-redacted successful setup script.
- Separate adapted scripts, documentation, provenance, package-verification code, and preparation audit records.

Excluded: both approximately 89 MB source archives, native libraries, protobuf descriptor binaries, extracted source/generated trees, the virtualenv, caches, redundant failed setup source, the empty setup-v2 log, and the mismatched-format intermediate Git identity snapshot. Their discovered top-level names/hashes or directory classifications remain in the inventory. Originals were neither removed nor overwritten.

## Public-content inspection and transformations

Selected historical text was inspected for credential/token patterns, private-key headers, authentication-bearing content, URLs, private network addresses, email addresses, local paths, and irrelevant runtime artifacts. No credentials, private keys, private URLs, or private network addresses were found. Fixture authentication is an empty object. Node/cloud identifiers and label values are synthetic fixture data; launch UUIDs and timestamps are diagnostic output.

Only unnecessary local filesystem prefixes required masking. Fourteen files are explicitly named `.redacted.*`; they use literal `@PHASE2_ROOT@` and/or `@DEPENDENCY_SITE@` markers. No JSON parse/reserialize step was used to create those copies. No numeric values, list ordering, launch IDs, timestamps, scores, assertions, or scheduler reply fields were changed. The other 23 selected historical files are byte-identical.

[PROVENANCE.json](PROVENANCE.json) records each source filename, original/publication size and SHA-256, and marker substitution counts. The local verifier can validate every replacement against the retained original. Public metadata explains markers without disclosing the withheld absolute prefixes. Original evidence remains unchanged.

The historical command file was assembled after execution and is cross-checked against logs; it is not an independently captured shell transcript. No missing output has been reconstructed from the narrative.

## Reproducer inspection

The original scripts call production `ResourceDemandScheduler.schedule()`, importing the revision's `sched_request` and `make_autoscaler_instance` fixtures. They do not implement a replacement scheduler.

Configuration validation runs before `skip_content_hash=True` returns. Public task-option validators are called. The production sort key is extracted for the preflight only; scheduling uses its own unchanged sort. A/C assert R1 then R2, one selector each, type quotas, the head count, and an independent L-placement witness. Parent/HEAD serialized fixtures are compared during package verification. C differs from A only in the global maximum; C_finite uses a validated finite global maximum.

The observer reads locals from the actual scheduler's `_try_schedule` and `_sched_best_node` frames. It records candidate before/after capacity, scores, group membership, cap checks, choices, and remaining demand. A separate initial execution has no observer. Semantic equality assertions compare these runs.

The semantic projection aggregates launches by type and omits random launch UUIDs/timestamps. The package audit separately checks that raw launch lists contain unique types, counts match, and raw replies agree with logs; it does not rely solely on that projection. Full raw replies, protobuf text, and log ordering are preserved.

Prediction assertions test the observed behavior matrix. The separate quality pytest tests complete one-L placement and fails on HEAD. That desired-quality assertion is not treated as a documented contract.

Original scripts require sibling environment metadata and exact source layout; setup metadata contains redacted local paths. Trace hooks are revision-specific. Separate adaptations change output-root selection and add source-path validation only; they were syntax-checked, not rerun. See [REPRODUCTION.md](REPRODUCTION.md).

## Attempts to falsify the conclusion

| Alternative explanation | Check and outcome |
|---|---|
| Invalid fixture | Production config/task validation passed; no IPPR or placement constraints; independent production L witness fits both requests. No provider or actual cluster deployment was tested. |
| Wrong node cap | A's validated max_workers=1 yields total cap 2 including one head. Trace changes from count 1/2 to 2/2; L still has ten type slots. |
| Changed production sorting | Recorded production ordering is R1 then R2 in both versions. No sorting override is used. |
| Different source revision | Scheduler hashes match exact Git blobs. Each extracted tree retains 5,436 byte-identical archived files; all 247 loaded Ray modules per revision resolve within that copy. |
| Instrumentation interference | Initial executions are uninstrumented; traced semantic outputs match. A also has a separate uninstrumented pytest pass/fail pair. |
| Incompatible protobuf bindings | Original proto definitions do not differ; generated descriptor sets are identical. No scheduler import failure is recorded. A shared native wheel remains an explicit limitation; complete ABI/end-to-end equivalence was not proved. |
| JSON normalization artifact | Copied redacted results use byte substitutions only. Raw replies agree with stdout/stderr JSON and independent pytest reply prints; launch uniqueness/counts and capacities are checked independently. |
| Merely expected primary-preference behavior | A has no fallbacks; both unique selectors can be satisfied on L. The unused-fallback control changes grouping while preserving primary satisfaction. B separately confirms the intended preference improvement. |
| Globally impossible remaining demand | A's production L witness fits both requests with exactly zero CPU left. “Infeasible” in the planning result does not establish global impossibility. |
| A node-cap accounting defect | The recorded cap is enforced correctly. The poorer first-group choice consumes the sole additional worker slot. |
| Proven violation of a completeness guarantee | Not established. Verified source describes a greedy algorithm and no backtracking. |

No listed alternative displaced the narrow scheduling-quality conclusion. The experiment does not establish long-term starvation, universal workload impact, actual cloud cost, broad performance impact, or maintainer acceptance of this tradeoff.

The logs' “outdated head” warning is emitted by the common head-node branch before config-hash comparison. The fixture's expected/actual hash agrees and both replies retain the head. It does not explain the differential outcome.

## Git and history isolation

Initial inspection found a clean source checkout on `review/pr-67455` at the reviewed HEAD and a clean evidence checkout on `review-evidence` at `e5eb5df62926c52e48d1b282099f11ac8b3ea01a`.

**An external change occurred before publication writes.** A pre-write guard stopped when evidence HEAD had advanced to `cbad7347b0e20c19fdd4b590334e10320835ca74`. Read-only inspection identified an unrelated merge of existing #67229 evidence, adding eight files under `67229/` and changing the root README. This preparation issued no merge, commit, or ref-update command. No #67455 publication file existed at that point.

The new clean pre-write baseline preserves that external work. Its complete, non-shallow, nine-commit history excludes the reviewed PR HEAD. The initial direct ancestry command could not resolve the PR object in the evidence repository; absence of an object was not accepted as an ancestry result. A successful complete `git rev-list HEAD` enumeration established exclusion without fetching or changing refs.

Both initial and pre-write baselines are retained in [checks/preparation-baselines.json](checks/preparation-baselines.json). [The final isolation audit](checks/git-isolation-final.json) compares against the pre-write baseline, including branch/ref hashes, index bytes, remotes, pre-existing file inventory, and history. Therefore “no refs/files changed during the entire wall-clock interval” is not claimed; **no such changes were made by this preparation**, and the external change is explicitly disclosed.

Only the new `67455/20261027-3b45229e91/` directory is prepared for manual publication. Nothing was staged. Its local `.gitignore` only makes these publication logs visible despite the repository-wide log exclusion. No existing ignore file or Git configuration was modified. Any eventual manual evidence commit belongs on the existing `review-evidence` branch, never on the author's PR branch.

## Integrity verification

`MANIFEST.sha256` hashes every publication file except itself. Its own SHA-256 is supplied separately with the preparation report to avoid circular self-hashing. The allowlist includes the manifest. The standard-library verifier cross-checks file membership, hashes, copied-artifact provenance, raw result/log consistency, fixture equality, capacities, trace evidence, command exits, and pytest assertions.

The final audit does not turn original observational assertions into contract guarantees. A failed quality assertion on HEAD and passing observation-matrix assertions are intentionally reported separately.
