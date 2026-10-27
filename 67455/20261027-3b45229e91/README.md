# Ray PR #67455: selector-grouping scheduling-quality regression

## Summary

Independent Python scheduler A/B testing found a scheduling-quality regression associated with grouping pending resource requests by their label-selector lists. Under a global node cap, the parent plans one worker that satisfies both requests; the reviewed PR plans a smaller worker and leaves one request unscheduled. The same PR also improves the opposing-fallback-preference positive control.

This establishes a regression for the tested workload, **not a proven violation of a documented scheduling-completeness guarantee**. Here, “unscheduled” means left in the autoscaler's planning reply, not proof of globally impossible demand or permanent task starvation.

## Revision provenance

- Reviewed PR: [ray-project/ray#67455](https://github.com/ray-project/ray/pull/67455)
- Parent: `938ab39460b257374624479c6739fa80315939ea`
- PR HEAD: `3b45229e91b82d367b4706fbc26c2cbf31541c51`
- The parent is both HEAD's direct parent and their merge-base. Each revision was extracted with `git archive`; the active review checkout was not switched.
- Execution: CPython 3.12.3, pytest 7.4.4, protobuf 6.33.6, grpcio-tools 1.75.0, jsonschema 4.24.1, in an isolated virtualenv reusing existing dependency packages.
- Both revisions used the same Ray 2.58.0 wheel's native `_raylet.so` (wheel commit `cec0a091b12675bbf101cf8a350c8706107baee1`). Python production source and protobuf definitions came from the specified revisions.

These are actual `ResourceDemandScheduler.schedule()` executions, using the revisions' existing test fixtures. They are **not full source-built, end-to-end cluster tests**. [Source audit](checks/source-verification.json) verifies scheduler Git-blob hashes, 5,436 unchanged archived files per revision, 247 isolated Ray imports per revision, identical generated descriptor sets, and identical native-library hashes. Identical protobuf definitions/descriptors reduce the likelihood of a differential schema explanation; they do not establish complete native ABI compatibility.

## Experiment A — Global node cap

| Component | Configuration |
|---|---|
| Existing head | CPU=0; no worker instances |
| Worker S | CPU=2; labels `{x: "1"}` |
| Worker L | CPU=3; labels `{x: "1", y: "1"}` |
| R1 | CPU=2; only selector `x=1` |
| R2 | CPU=1; only selector `y=1` |
| Global cap | `max_workers=1` becomes `max_num_nodes=2`, including the head |
| Worker-type quotas | S and L each allow 10 workers, minimum 0, priority 0 |
| Other conditions | No IPPR, gang requests, placement constraints, or cloud failures; equal availability defaults |

The autoscaling configuration and task options passed production validation. Production sorting puts R1 before R2. A production `SchedulingNode.try_schedule()` witness fits both on L, consuming all 3 CPU; S rejects R2 without consuming capacity.

| Revision | Planned launches | Remaining demand |
|---|---|---|
| Parent | L × 1 | None |
| PR HEAD | S × 1 | R2: CPU=1, selector y=1 |

Evidence: [parent](results/parent-A.redacted.json), [HEAD](results/head-A.redacted.json). The independent uninstrumented quality test passes on [parent](logs/parent-A-quality-pytest.log) and fails on [HEAD](logs/head-A-quality-pytest.log) at `assert not reply.infeasible_resource_requests`.

The first causal divergence is candidate evaluation scope, after identical request sorting:

| Candidate | Parent evaluates R1 and R2 together | HEAD evaluates only the first group, R1 |
|---|---|---|
| S | score `(1, True, 1, 2, 2)`; CPU remaining 0 | same score; CPU remaining 0 |
| L | score `(1, True, 1, 3, 3)`; CPU remaining 0 | score `(1, True, 1, 0.8888888888888886, 0.8888888888888886)`; CPU remaining 1 |

Parent selects L. HEAD selects S, after which the global count is 2/2 and fresh nodes are excluded. L still has 10 type-specific slots, so this is not an exhausted type quota. Both requests have only one selector: normalization leaves their selector scores unchanged.

Relevant reviewed source: [sorting/grouping](https://github.com/ray-project/ray/blob/3b45229e91b82d367b4706fbc26c2cbf31541c51/python/ray/autoscaler/v2/scheduler.py#L1721-L1772), [utilization score](https://github.com/ray-project/ray/blob/3b45229e91b82d367b4706fbc26c2cbf31541c51/python/ray/autoscaler/v2/scheduler.py#L715-L749), and [global cap/launch selection](https://github.com/ray-project/ray/blob/3b45229e91b82d367b4706fbc26c2cbf31541c51/python/ray/autoscaler/v2/scheduler.py#L1929-L1985).

## Experiment B — Positive control

Types A and B each have 2 CPU and labels `accel=A` and `accel=B`, respectively. R1 requests 1 CPU with selectors `[A, B]`; R2 requests 1 CPU with selectors `[B, A]`. The global limit permits both workers (`max_workers=10`).

| Revision | Planned launches | Remaining demand |
|---|---|---|
| Parent | A × 1 | None |
| PR HEAD | A × 1 + B × 1 | None |

HEAD honors the two separate primary preferences. This confirms the intended improvement independently of A. Evidence: [parent](results/parent-B.redacted.json), [HEAD](results/head-B.redacted.json). Tables describe counts, not the order of entries in the raw launch list.

## Experiment C — Capacity control

The no-cap control changes only A's `SchedulingRequest.max_num_nodes` from 2 to `None`; the harness asserts all other request fields are identical. A separate finite-cap control uses validated configuration `max_workers=2`, producing `max_num_nodes=3`. Worker definitions and resource requests stay the same.

| Control | Parent launches / worker CPU | HEAD launches / worker CPU | Remaining demand |
|---|---|---|---|
| No cap | L × 1 / 3 CPU | S × 1 + L × 1 / 5 CPU | None in either revision |
| Finite cap of 3 including head | L × 1 / 3 CPU | S × 1 + L × 1 / 5 CPU | None in either revision |

Evidence: no-cap [parent](results/parent-C.redacted.json)/[HEAD](results/head-C.redacted.json); finite-cap [parent](results/parent-C_finite.json)/[HEAD](results/head-C_finite.json).

HEAD plans 2 additional CPU (approximately 66.7% more worker CPU) and one additional worker. This measures **planned capacity**, not actual incurred cloud cost. The finite-cap control avoids relying on whether an absent global maximum is accepted by a public deployment configuration.

## Unused fallback control

This variant keeps A's node types, CPU requests, and cap, but changes both primary selectors to `x=1`. Fallbacks `unused=u0` and `unused=u1` match neither worker type.

| Selector lists | Parent | PR HEAD |
|---|---|---|
| R1: [x=1, unused=u0]; R2: [x=1, unused=u1] | L; complete | S; CPU=1 request remains |
| Both: [x=1, unused=u0] | L; complete | L; complete |
| Both: [x=1] | L; complete | L; complete |

All primary preferences can be satisfied by one L. Changing an unused fallback changes grouping and node selection without improving primary-selector satisfaction. Evidence is in the six `results/*-D_*.json` files and the [parent](logs/parent-followup.log)/[HEAD](logs/head-followup.log) follow-up logs.

## Interpretation and limitations

The tested scheduler is explicitly [greedy](https://github.com/ray-project/ray/blob/3b45229e91b82d367b4706fbc26c2cbf31541c51/python/ray/autoscaler/v2/scheduler.py#L1996-L2009), and node packing performs [no backtracking](https://github.com/ray-project/ray/blob/3b45229e91b82d367b4706fbc26c2cbf31541c51/python/ray/autoscaler/v2/scheduler.py#L595-L611). A globally complete solution is not guaranteed by the verified source contract. A scheduling-quality regression is established; a documented correctness-contract violation is not established. Whether the tradeoff is acceptable remains a maintainer policy question.

A does not involve fallback selection, and choosing L would satisfy both unique/primary selectors. The intended preference improvement in B therefore does not, by itself, justify A's changed placement.

No full-cluster validation, real-cloud cost measurement, broad performance benchmark, or long-term starvation experiment was performed. No scheduler tests were rerun during publication preparation.

The original harness first runs uninstrumented production scheduling, then observes a separate run with `sys.settrace`. Recorded semantic replies agree. The observer reads frame locals; it does not override sorting, eligibility, scoring, or node selection. UUIDs/timestamps and launch-list ordering are not the behavioral assertions; original replies and protobuf text remain available.

The pytest quality expectation is deliberately separate from the observation matrix's prediction assertions. Its HEAD failure is evidence of changed behavior, not proof of a completeness contract.

## Reproduction and evidence integrity

See [REPRODUCTION.md](REPRODUCTION.md) for historical commands, source/environment prerequisites, and explicitly unexecuted portability adaptations. The original scripts are revision-specific and are not a standalone dependency bundle.

Of 37 selected historical artifacts, 23 are byte-identical and 14 have **only literal local-path substitutions**, clearly named `.redacted.*`. Scheduler replies, scores, assertions, UUIDs, timestamps, and list order are not rewritten. [PROVENANCE.json](PROVENANCE.json) records original/publication hashes and transformation counts; [PUBLICATION-AUDIT.md](PUBLICATION-AUDIT.md) records falsification checks and exclusions.

Verify package integrity and cross-artifact consistency without running Ray:

```bash
sha256sum -c MANIFEST.sha256
python3 -B checks/verify_package.py
```

The manifest hashes every other publication file. Its own digest must be checked against the separately supplied digest; a file cannot contain a non-circular hash of itself.
