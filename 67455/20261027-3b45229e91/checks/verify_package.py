"""Read-only consistency verifier; no Ray imports and no scheduler execution."""
import argparse
import ast
import hashlib
import json
import pathlib
import re
from collections import Counter

ROOT = pathlib.Path(__file__).resolve().parents[1]
PARENT = "938ab39460b257374624479c6739fa80315939ea"
HEAD = "3b45229e91b82d367b4706fbc26c2cbf31541c51"


def digest(data):
    return hashlib.sha256(data).hexdigest()


def read_json(path):
    return json.loads(path.read_text())


def objects_in_log(path):
    text = path.read_text()
    decoder = json.JSONDecoder()
    return [decoder.raw_decode(text[m.start():])[0]
            for m in re.finditer(r"(?m)^\{\n", text)]


def behavioral_reply(reply):
    launches = reply["to_launch"]
    types = [item["instance_type"] for item in launches]
    assert len(types) == len(set(types)), "Projection must not hide duplicate launch types"
    return {
        "launches": {item["instance_type"]: item["count"] for item in launches},
        "remaining": reply["infeasible_resource_requests"],
        "cluster_resources": reply["cluster_resources"],
        "to_ippr": reply["to_ippr"],
        "to_terminate": reply["to_terminate"],
        "gang": reply["infeasible_gang_resource_requests"],
        "constraints": reply["infeasible_cluster_resource_constraints"],
    }


def fixture(result):
    return result["preflight"]["scheduling_request"] if "preflight" in result else result["fixture"]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--original-root", type=pathlib.Path)
    args = parser.parse_args()
    assert __debug__, "Do not disable assertions with python -O"
    files = sorted(str(p.relative_to(ROOT)) for p in ROOT.rglob("*") if p.is_file())
    assert not any(p.is_symlink() for p in ROOT.rglob("*")), "Unexpected symlink"
    allowlist = (ROOT / "PUBLICATION-ALLOWLIST.txt").read_text().splitlines()
    assert files == allowlist, "Allowlist/file membership mismatch"
    entries = {}
    for line in (ROOT / "MANIFEST.sha256").read_text().splitlines():
        sha, name = line.split("  ", 1)
        assert name not in entries
        assert (ROOT / name).resolve().is_relative_to(ROOT)
        assert digest((ROOT / name).read_bytes()) == sha, name
        entries[name] = sha
    assert set(entries) == set(files) - {"MANIFEST.sha256"}

    provenance = read_json(ROOT / "PROVENANCE.json")
    assert provenance["parent"] == PARENT and provenance["head"] == HEAD
    by_original = {}
    treatments = Counter()
    for item in provenance["copied_artifacts"]:
        p = ROOT / item["publication_path"]
        b = p.read_bytes()
        assert len(b) == item["publication_bytes"]
        assert digest(b) == item["publication_sha256"]
        treatments[item["treatment"]] += 1
        if item["treatment"] == "byte-identical":
            assert digest(b) == item["original_sha256"]
            assert item["original_bytes"] == len(b)
        else:
            assert ".redacted." in p.name
        by_original[item["original_basename"]] = p
        if args.original_root:
            original = (args.original_root / item["original_basename"]).read_bytes()
            assert digest(original) == item["original_sha256"]
            assert len(original) == item["original_bytes"]
            transformed = original
            # Derive the local dependency prefix from retained original metadata.
            dep = read_json(args.original_root / "environment.json")["dependency_site"]
            counts = {}
            for old, new in [(dep.encode(), b"@DEPENDENCY_SITE@"),
                             (str(args.original_root.resolve()).encode(), b"@PHASE2_ROOT@")]:
                n = transformed.count(old)
                if n:
                    counts[new.decode()] = n
                    transformed = transformed.replace(old, new)
            assert counts == item["replacement_counts"]
            assert transformed == b, item["original_basename"]
    assert treatments == {"byte-identical": 23, "literal-path-redaction": 14}
    inventory = read_json(ROOT / "checks/original-artifact-inventory.json")
    assert inventory["missing_required"] == []
    assert set(inventory["required_artifacts"]) <= set(by_original)
    if args.original_root:
        for item in inventory["top_level_artifacts"]:
            if item["type"] == "file":
                b = (args.original_root / item["name"]).read_bytes()
                assert len(b) == item["bytes"] and digest(b) == item["sha256"]

    results = {}
    summary = []
    cases = ["A", "B", "C", "C_finite", "D_distinct_unused_fallbacks",
             "D_same_unused_fallbacks", "D_primary_only"]
    for revision, sha in [("parent", PARENT), ("head", HEAD)]:
        for case in cases:
            result = read_json(by_original[f"{revision}-{case}.json"])
            results[revision, case] = result
            assert result["revision"] == sha
            assert result.get("comparison_merge_base", result.get("merge_base")) == PARENT
            assert all(v is True for v in result["assertions"].values())
            reply = behavioral_reply(result["raw_reply"])
            expected_launches = (
                {"A": 1} if revision == "parent" and case == "B" else
                {"A": 1, "B": 1} if case == "B" else
                {"S": 1} if revision == "head" and case in ("A", "D_distinct_unused_fallbacks") else
                {"S": 1, "L": 1} if revision == "head" and case in ("C", "C_finite") else
                {"L": 1}
            )
            assert reply["launches"] == expected_launches
            unmet = revision == "head" and case in ("A", "D_distinct_unused_fallbacks")
            assert len(reply["remaining"]) == (1 if unmet else 0)
            f = fixture(result)
            if unmet:
                cpu1 = [x["request"] for x in f["resource_requests"]
                        if x["request"]["resources_bundle"]["CPU"] == 1]
                assert reply["remaining"] == cpu1
            assert not reply["to_ippr"] and not reply["to_terminate"]
            assert not reply["gang"] and not reply["constraints"]
            cpu = sum(f["node_type_configs"][name]["resources"]["CPU"] * count
                      for name, count in reply["launches"].items())
            assert reply["cluster_resources"] == {"CPU": cpu}
            assert result["semantic_reply"]["to_launch"] == reply["launches"]
            assert result["semantic_reply"]["remaining"] == reply["remaining"]
            summary.append({"revision": revision, "case": case,
                            "launches": reply["launches"], "remaining_count": len(reply["remaining"]),
                            "planned_worker_cpu": cpu})
        for suffix, expected_cases in [("ABC.log", cases[:3]), ("followup.log", cases[3:])]:
            objects = objects_in_log(by_original[f"{revision}-{suffix}"])
            assert len(objects) == len(expected_cases)
            for obj, case in zip(objects, expected_cases):
                assert obj == results[revision, case], f"Result/log discrepancy: {revision} {case}"
        pytest = by_original[f"{revision}-A-quality-pytest.log"].read_text()
        assert ("1 passed in 0.41s" if revision == "parent" else "1 failed in 0.44s") in pytest
        raw = ast.literal_eval(next(line[len("ACTUAL_REPLY "):] for line in pytest.splitlines()
                                    if line.startswith("ACTUAL_REPLY ")))
        assert behavioral_reply(raw) == behavioral_reply(results[revision, "A"]["raw_reply"])
        if revision == "head":
            assert ">       assert not reply.infeasible_resource_requests" in pytest
        a = fixture(results[revision, "A"])
        assert a["max_num_nodes"] == 2 and len(a["current_instances"]) == 1
        assert a["ippr_specs"] is None and a["ippr_statuses"] == {}
        c = dict(fixture(results[revision, "C"]))
        assert c["max_num_nodes"] is None
        c["max_num_nodes"] = 2
        assert c == a
        finite = dict(fixture(results[revision, "C_finite"]))
        assert finite["max_num_nodes"] == 3
        finite["max_num_nodes"] = 2
        assert finite == a
        preflight = results[revision, "A"]["preflight"]
        assert preflight["node_type_quotas"] == {"head": 0, "S": 10, "L": 10}
        assert preflight["complete_placement_witness"]["available"] == {"CPU": 0.0}
        assert preflight["S_after_R2_attempt"]["available"] == {"CPU": 2}
        assert preflight["config_validation"] == preflight["task_option_validation"] == "PASS"
        order = preflight["production_sorted_requests"]
        assert [r["resources_bundle"]["CPU"] for r in order] == [2, 1]
        assert all(len(r["label_selectors"]) == 1 for r in order)
        trace = results[revision, "A"]["trace"]
        assert next(e["requests"] for e in trace if e["event"] == "sorted_requests") == order
        groups = next(e["groups"] for e in trace if e["event"] == "groups_and_initial_state")
        assert groups == ([order] if revision == "parent" else [[order[0]], [order[1]]])
        candidates = [e for e in trace if e["event"] == "candidate"]
        expected_scores = (
            [[1, True, 1, 2.0, 2.0], [1, True, 1, 3.0, 3.0]] if revision == "parent"
            else [[1.0, True, 1, 2.0, 2.0],
                  [1.0, True, 1, 0.8888888888888886, 0.8888888888888886]]
        )
        assert [e["before"]["node_type"] for e in candidates[:2]] == ["S", "L"]
        assert [e["score"] for e in candidates[:2]] == expected_scores
        if revision == "head":
            assert any(e["event"] == "global_cap" and e["count"] == e["max"] == 2
                       and not e["can_launch"] and e["quotas"]["L"] == 10 for e in trace)
    for case in cases:
        assert fixture(results["parent", case]) == fixture(results["head", case]), case

    commands = read_json(by_original["executed-test-commands.json"])
    assert [c["exit_code"] for c in commands] == [0, 0, 0, 0, 0, 1]
    assert all(c["cwd"] == "@PHASE2_ROOT@" for c in commands)
    for revision, offset in [("parent", 0), ("head", 3)]:
        for c in commands[offset:offset+3]:
            assert f"PYTHONPATH=@PHASE2_ROOT@/{revision}/python" in c["command"]
            assert "PYTHONDONTWRITEBYTECODE=1" in c["command"]
        assert f"--revision {revision} --cases A,B,C" in commands[offset]["command"]
        assert "test_candidate_a.py" in commands[offset+2]["command"]

    env = read_json(by_original["environment.json"])
    source = read_json(ROOT / "checks/source-verification.json")
    assert source["generated_descriptor_sets_byte_identical"]
    for revision in ("parent", "head"):
        assert env["revisions"][revision]["scheduler_sha256"] == source[revision]["scheduler_sha256"]
        imports = read_json(by_original[f"{revision}-import-provenance.json"])
        assert len(imports) == 247
        assert all(p.startswith(f"@PHASE2_ROOT@/{revision}/python/ray/") for p in imports.values())
    for adapted in read_json(ROOT / "checks/adaptation-provenance.json"):
        assert digest((ROOT / adapted["publication_path"]).read_bytes()) == adapted["sha256"]
        assert adapted["scheduler_execution_status"] == "not run"

    for p in ROOT.rglob("*.py"):
        ast.parse(p.read_text(), filename=str(p))
    stored = read_json(ROOT / "checks/result-summary.json")
    assert stored == summary
    print(json.dumps({"status": "PASS", "publication_files": len(files),
                      "manifest_entries": len(entries), "historical_artifacts": len(by_original),
                      "treatments": dict(treatments), "result_pairs_checked": len(cases),
                      "result_log_objects_checked": len(results), "original_files_checked": bool(args.original_root),
                      "scheduler_executed": False}, indent=2))


if __name__ == "__main__":
    main()
