# Publication and preservation audit

Only paths in `PUBLICATION-ALLOWLIST.txt` are intended for manual publication. No GitHub write or Git staging/commit/push occurred during preparation.

## Raw evidence and transformations

All 170 artifacts in `PROVENANCE.json` are byte-for-byte copies, with original paths, sizes and SHA-256 values. No raw log, command, XML, source or historical script was redacted or normalized. The public OS-identification file was copied from the resolved target of /etc/os-release; no symlinks are published.

`reproducer/BUILD.append.bazel` is an exact byte slice after the tracked BUILD prefix, identical across snapshots. README tables and `results/A1-observations.json` are derived from named A1 blocks, not raw output. New prose summarizes existing experiments/source analysis. Historical scripts and pre-fix files are records, not steps to execute.

Original UTC start/end timestamps are unchanged. XML timestamps have no explicit offset and are not relabelled. Audit timestamps explicitly state UTC.

## Security and metadata

The package was scanned for private-key blocks, common GitHub/AWS/OpenAI/Slack token patterns, credential-bearing URLs, authorization values, JWT-shaped strings and private IPv4 addresses. Results/limitations are in `checks/public-safety-scan.json`. Pattern scanning supports review; it cannot prove arbitrary secrets are impossible.

The retained /home/researcher paths, /tmp/ray-pr67424-phase2-lgVIc0KN paths, Bazel cache paths and loopback addresses are generic experiment provenance. No credentials were discovered in them. Random worker/object IDs and loopback ports are fixture identifiers. Public Ray links and the user-specified public fork remote are intentional provenance.

No full environment, shell history, authentication files or unrelated source content was collected. Existing raw text was inspected through its source, commands, outcomes and safety scan. No redaction transformations were necessary.

Excluded: binaries, shared libraries, wheels, Bazel caches, virtualenv contents except pyvenv.cfg, full source exports and unrelated PR #67455 files. The 55 existing PR #67455 files remain outside this package; only preservation counts/digests and their existing Git status paths appear in baseline metadata.

## Source and Git isolation

The review worktree starts and ends on `review/pr-67424` at `1c22b86fb5769f09a436bfdd4666aedef4a73847`, clean. Both source exports were verified against their exact Git trees: only the appended isolated test BUILD rule differs among tracked files. Existing rules and production files are unchanged; the test source and BUILD suffix match across revisions.

The evidence repository starts and ends on `review-evidence` at `cbad7347b0e20c19fdd4b590334e10320835ca74`, remote https://github.com/hikaru-212/ray.git. Its nine reachable commits exclude the PR HEAD. No merge, cherry-pick, branch switch or ref update occurred. This is a new untracked package, not a production branch import.

The target initially did not exist. The 155 pre-existing evidence files (55 for PR #67455) are preserved by a per-path hash/mode inventory digest. Source-worktree and original experiment source/log/script inventories are checked too. Git indexes, refs and relevant reflog metadata are compared. `checks/git-preservation.json` records the final verification.

The original experiment byte inventory excludes the large bazel-root, venv and bin trees; they were not rebuilt, altered or used to rerun tests. The existing venv Python interpreter was used with bytecode disabled for artifact audits and new-package creation. No regression tests or builds ran. Filesystem access times are excluded from preservation claims.

## Allowlist and manifest

The exact allowlist includes itself and the manifest; no wildcard authorizes publication. The manifest hashes every other file, including the allowlist, but excludes itself to avoid circular hashing. The verifier checks this exact-set relationship, copied hashes, successful exits/XML, derived observations and README table.

The package-local .gitignore permits the preserved logs/*.log file hidden by the repository-wide ignore rule. The final audit checks every allowlisted file is visible to Git without staging.

Preparation does not supply full historical environment capture, signed build attestation or measured public API impact. Manual review remains appropriate before publication.
