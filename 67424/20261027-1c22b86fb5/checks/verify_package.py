#!/usr/bin/env python3
"""Read-only verification of the public evidence package. Does not run Ray."""
from pathlib import Path
import hashlib
import json
import re
import xml.etree.ElementTree as ET

ROOT = Path(__file__).resolve().parent.parent
A1 = "Phase2.A1DelayedStatusAndFinalLineageRelease"
STAGES = ["before_owner_release", "before_borrower_release", "after_borrower_release",
          "after_delayed_handler", "after_idle_poll", "after_final_lineage_release"]
FIELDS = ["store", "owner", "lineage", "out_of_scope", "should_delete",
          "handler", "pending", "client_active", "reply"]

def sha(data):
    return hashlib.sha256(data).hexdigest()

def get_json(name):
    return json.loads((ROOT / name).read_text())

def observe(label):
    text = (ROOT / "logs" / (label + ".output")).read_text()
    begin = text.index("[ RUN      ] " + A1)
    end = text.index("[       OK ] " + A1, begin)
    result = {}
    for line in text[begin:end].splitlines():
        if line.startswith("OBS "):
            parts = line.split()
            result[parts[1]] = {k: int(v) for k, v in
                                (part.split("=") for part in parts[2:])}
    assert list(result) == STAGES, label
    assert "Build completed successfully" in text, label
    assert (ROOT / "logs" / (label + ".exit")).read_text().strip() == "0", label
    xml = ET.parse(ROOT / "logs" / (label + ".xml")).getroot()
    assert xml.attrib["failures"] == "0" and xml.attrib["errors"] == "0", label
    cases = [c for c in xml.iter("testcase")
             if c.attrib["name"] == A1.split(".", 1)[1]]
    assert len(cases) == 1 and cases[0].attrib["result"] == "completed", label
    return result

def main():
    files = {p.relative_to(ROOT).as_posix() for p in ROOT.rglob("*") if p.is_file()}
    assert not any(p.is_symlink() for p in ROOT.rglob("*")), "Symlink in package"
    allow_lines = (ROOT / "PUBLICATION-ALLOWLIST.txt").read_text().splitlines()
    allowed = set(allow_lines)
    assert len(allowed) == len(allow_lines) and allowed == files, "Allowlist mismatch"
    manifest = {}
    for line in (ROOT / "MANIFEST.sha256").read_text().splitlines():
        digest, name = line.split("  ", 1)
        assert re.fullmatch(r"[0-9a-f]{64}", digest), name
        assert name in allowed and name not in manifest, name
        assert not Path(name).is_absolute() and ".." not in Path(name).parts, name
        assert sha((ROOT / name).read_bytes()) == digest, name
        manifest[name] = digest
    assert set(manifest) == allowed - {"MANIFEST.sha256"}, "Manifest coverage"
    prov = get_json("PROVENANCE.json")
    for name, record in prov["copied_artifacts"].items():
        data = (ROOT / name).read_bytes()
        assert len(data) == record["bytes"] and sha(data) == record["sha256"], name
    test = (ROOT / "reproducer/phase2_borrower_rpc_test.cc").read_bytes()
    assert sha(test) == prov["source_test_sha256"]
    old = (ROOT / "history/phase2_borrower_rpc_test.cc.v5").read_bytes()
    assert old.replace(b"s.callback(msg);", b"s.callback(rpc::PubMessage(msg));") == test
    for variant in ["parent", "head"]:
        old_build = (ROOT / ("history/" + variant + "-test-BUILD.v1")).read_bytes()
        build = (ROOT / ("reproducer/" + variant + ".BUILD.bazel")).read_bytes()
        marker = old_build.index(b"# Isolated Phase 2 experiment only.")
        redundant = b'        "//src/ray/core_worker_rpc_client:core_worker_client",\n'
        assert old_build[:marker] + old_build[marker:].replace(redundant, b"") == build
        assert build.endswith((ROOT / "reproducer/BUILD.append.bazel").read_bytes())
        assert get_json("checks/source-verification.json")[variant]["production_source_changes"] == 0
    results = get_json("results/A1-observations.json")
    obs = {label: observe(label) for label in results["a1_observations"]}
    assert obs == results["a1_observations"], "Derived JSON diverges from raw A1"
    parent = obs["parent-experiments-1"]
    head = obs["head-experiments-3"]
    assert parent == obs["parent-a1-only"]
    assert all(parent[s] == head[s] for s in STAGES[:2])
    assert {k for k in parent[STAGES[2]] if parent[STAGES[2]][k] != head[STAGES[2]][k]} == {"store"}
    assert parent[STAGES[2]]["store"] == 1 and head[STAGES[2]]["store"] == 0
    for s in STAGES[3:]:
        for field, pval, hval in [("pending",0,1),("client_active",0,1),("reply",1,0)]:
            assert parent[s][field] == pval and head[s][field] == hval
    assert parent[STAGES[-1]]["owner"] == head[STAGES[-1]]["owner"] == 0
    assert "lineage" not in parent[STAGES[-1]] and "lineage" not in head[STAGES[-1]]
    table = [
        "| Stage | Revision | Store | Owner metadata | Lineage | OutOfScope | ShouldDelete | Handler | Pending GetAsync | Active client RPC | Reply |",
        "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |"]
    for stage in STAGES:
        for variant, rows in [("Parent",parent), ("HEAD",head)]:
            table.append("| " + " | ".join([stage,variant] +
                [str(rows[stage].get(k, "N/A")) for k in FIELDS]) + " |")
    readme = (ROOT / "README.md").read_text()
    embedded = readme.split("<!-- BEGIN A1 OBSERVATION TABLE -->\n",1)[1].split(
        "\n<!-- END A1 OBSERVATION TABLE -->",1)[0]
    assert embedded == "\n".join(table), "README table diverges from original logs"
    assert "caller" in readme and "precondition" in readme and "test-only" in readme
    # Package-relative Markdown links must point to existing package files.
    for doc in ROOT.glob("*.md"):
        for target in re.findall(r"\]\(([^)]+)\)", doc.read_text()):
            if "://" not in target and not target.startswith("#"):
                assert (ROOT / target.split("#",1)[0]).is_file(), (doc.name,target)
    safety = get_json("checks/public-safety-scan.json")
    assert safety["high_confidence_secret_findings"] == []
    preservation = get_json("checks/git-preservation.json")
    assert preservation["all_checks_passed"] is True
    print(json.dumps({
        "result":"PASS", "files":len(files),
        "bytes":sum((ROOT/p).stat().st_size for p in files),
        "manifest_entries":len(manifest),
        "byte_identical_copies":len(prov["copied_artifacts"]),
        "test_source_sha256":sha(test),
        "successful_a1_runs":len(obs),
        "first_divergence":results["first_observable_difference"],
        "readme_table_matches_raw_logs":True,
        "ray_builds_or_tests_executed":False
    }, indent=2))

if __name__ == "__main__":
    main()
