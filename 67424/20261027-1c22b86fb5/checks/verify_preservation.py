#!/usr/bin/env python3
"""Read-only local preservation audit; requires the original repository/artifacts."""
from pathlib import Path
import datetime
import hashlib
import json
import os
import stat
import subprocess

PACKAGE = Path(__file__).resolve().parent.parent
SOURCE = Path("/home/researcher/oss/worktrees/ray-review-4")
EVIDENCE = Path("/home/researcher/oss/ray-review-evidence")
RAW = Path("/tmp/ray-pr67424-phase2-lgVIc0KN")

def sha(b):
    return hashlib.sha256(b).hexdigest()

def git(p, *args):
    return subprocess.check_output(["git","--no-optional-locks",*args],cwd=p)

def item(p):
    if p.is_symlink():
        return {"kind":"symlink","target":os.readlink(p)}
    data = p.read_bytes()
    return {"kind":"file","bytes":len(data),"sha256":sha(data),
            "mode":stat.S_IMODE(p.stat().st_mode)}

def inventory(p, exclusions=()):
    rows = {}
    for base, dirs, files in os.walk(p, followlinks=False):
        base = Path(base)
        dirs[:] = sorted(d for d in dirs
                         if (base/d).relative_to(p).as_posix() not in exclusions)
        for d in list(dirs):
            q = base/d
            if q.is_symlink():
                rows[q.relative_to(p).as_posix()] = item(q)
                dirs.remove(d)
        for f in sorted(files):
            q = base/f
            rel = q.relative_to(p).as_posix()
            if rel not in exclusions:
                rows[rel] = item(q)
    encoded = json.dumps(rows,sort_keys=True,separators=(",",":")).encode()
    return {"entries":len(rows),
            "regular_file_bytes":sum(r.get("bytes",0) for r in rows.values()),
            "inventory_sha256":sha(encoded)}

def gitstate(p):
    refs = git(p,"for-each-ref","--format=%(refname) %(objectname)")
    index = Path(git(p,"rev-parse","--git-path","index").decode().strip())
    if not index.is_absolute():
        index = p/index
    gd = Path(git(p,"rev-parse","--absolute-git-dir").decode().strip())
    common = Path(git(p,"rev-parse","--git-common-dir").decode().strip())
    if not common.is_absolute():
        common = p/common
    meta = {}
    for root in sorted(set([gd,common])):
        for part in ["HEAD","index","packed-refs","FETCH_HEAD","ORIG_HEAD","refs","logs"]:
            q = root/part
            if q.is_file():
                meta[str(q)] = item(q)
            elif q.is_dir():
                meta[str(q)] = inventory(q)
    return {
        "branch":git(p,"branch","--show-current").decode().strip(),
        "head":git(p,"rev-parse","HEAD").decode().strip(),
        "status":git(p,"status","--porcelain=v1","--untracked-files=all").decode(),
        "remotes":git(p,"remote","-v").decode(),
        "refs_sha256":sha(refs),"refs_count":len(refs.splitlines()),
        "index_sha256":sha(index.read_bytes()),"git_metadata":meta}

def main():
    baseline = json.loads((PACKAGE/"checks/preparation-baselines.json").read_text())
    source = gitstate(SOURCE)
    evidence = gitstate(EVIDENCE)
    rel = PACKAGE.relative_to(EVIDENCE).as_posix()
    allowed = (PACKAGE/"PUBLICATION-ALLOWLIST.txt").read_text().splitlines()
    expected_new = {"?? "+rel+"/"+name for name in allowed}
    before_status = set(baseline["evidence_git"]["status"].splitlines())
    current_status = set(evidence["status"].splitlines())
    checks = {"source_git_state_unchanged":source==baseline["source_git"],
              "evidence_status_only_adds_allowlist":
                  current_status==before_status|expected_new}
    for k in baseline["evidence_git"]:
        if k != "status":
            checks["evidence_"+k+"_unchanged"] = (
                evidence[k]==baseline["evidence_git"][k])
    measured = {
        "source_files":inventory(SOURCE,(".git",)),
        "preexisting_evidence_files":inventory(EVIDENCE,(".git",rel)),
        "pr67455_files":inventory(EVIDENCE/"67455"),
        "original_experiment_files":inventory(RAW,("bazel-root","venv","bin"))}
    for k,v in measured.items():
        checks[k+"_unchanged"] = v==baseline[k]
    commits = git(EVIDENCE,"rev-list","HEAD").decode().splitlines()
    checks["evidence_history_unchanged"] = commits==baseline["evidence_reachable_commits"]
    checks["reviewed_pr_head_not_in_evidence_ancestry"] = (
        baseline["source_git"]["head"] not in commits)
    # Verify public copies directly against the original files at final audit.
    prov = json.loads((PACKAGE/"PROVENANCE.json").read_text())
    checks["all_raw_copies_equal_originals"] = all(
        (PACKAGE/name).read_bytes()==Path(rec["original_path"]).read_bytes()
        for name,rec in prov["copied_artifacts"].items())
    checks["no_staged_changes_in_either_repository"] = all(
        subprocess.run(["git","--no-optional-locks","diff","--cached","--quiet"],
                       cwd=p,check=False).returncode==0 for p in [SOURCE,EVIDENCE])
    result = {
        "captured_utc":datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "all_checks_passed":all(checks.values()),"checks":checks,
        "final_source_git":source,"final_evidence_git":evidence,
        "measured_file_inventories":measured,
        "evidence_reachable_commits":commits,
        "new_directory":rel,"allowlisted_files":len(allowed),
        "method":"Read-only Git and per-path byte/mode/symlink-target inventories; no staging, ref update, commit or push.",
        "limitations":"Large original bazel-root, venv and bin trees are excluded from byte inventory; no experiment rerun or writes to those trees occurred. Filesystem access times are excluded."}
    print(json.dumps(result,indent=2,sort_keys=True))
    if not result["all_checks_passed"]:
        raise SystemExit(1)

if __name__=="__main__":
    main()
