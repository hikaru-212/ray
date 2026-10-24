"""Run portable evidence against an already prepared, exact Ray source export.

Use a virtual environment. A parent phase2 run intentionally exits 1 for three
regression checks; the head run must exit 0. Never reuse an output directory.
"""
import argparse
import hashlib
import importlib
import json
import os
from pathlib import Path
import sys


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--variant", choices=("parent", "head"), required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--suite", choices=("phase2", "recent", "gc", "all", "legacy"), default="all")
    args = parser.parse_args()
    if sys.prefix == sys.base_prefix:
        parser.error("Run with a virtual-environment Python.")
    package = Path(__file__).resolve().parent
    source = args.source.resolve()
    output = args.output_dir.resolve()
    if output == source or source in output.parents or output == package or package in output.parents:
        parser.error("Choose a fresh output directory outside the source and package.")
    if args.suite == "legacy" and args.variant != "parent":
        parser.error("The legacy fixture must be generated with parent source.")
    provenance = json.loads((package / "provenance.json").read_text())
    for name, expected in provenance["source_files"][args.variant].items():
        if digest(source / name) != expected:
            parser.error("Source hash mismatch: " + name)
    output.mkdir(parents=True, exist_ok=False)
    (output / "artifacts").mkdir()
    for suffix in ("hex", "json"):
        fixture = package / "fixtures" / ("legacy-parent-truncated." + suffix)
        target = output / "artifacts" / ("legacy-parent-truncated." + ("pb" if suffix == "hex" else suffix))
        target.write_bytes(bytes.fromhex(fixture.read_text()) if suffix == "hex" else fixture.read_bytes())
    expected_fixture = provenance["historical_inputs"]["phase2/artifacts/legacy-parent-truncated.pb"]["sha256"]
    assert digest(output / "artifacts/legacy-parent-truncated.pb") == expected_fixture
    sys.dont_write_bytecode = True
    os.environ.update(RAY_EVIDENCE_OUTPUT=str(output), EVIDENCE_VARIANT=args.variant,
                      AUDIT_VARIANT=args.variant, PYTEST_DISABLE_PLUGIN_AUTOLOAD="1")
    sys.path.insert(0, str(source / "python"))
    import ray
    from ray.core.generated.instance_manager_pb2 import Instance
    import pytest
    assert Path(ray.__file__).resolve().is_relative_to(source), ray.__file__
    assert ("status_transition_counts" in Instance.DESCRIPTOR.fields_by_name) == (args.variant == "head")
    if args.suite == "legacy":
        harness = importlib.import_module("test_history")
        # Replay the archived synthetic launch ID so protobuf bytes are stable.
        from ray.autoscaler.v2.instance_manager import reconciler
        legacy = Instance.FromString((output / "artifacts/legacy-parent-truncated.pb").read_bytes())
        with pytest.MonkeyPatch.context() as patch:
            patch.setattr(reconciler.uuid, "uuid4", lambda: legacy.launch_request_id)
            harness.make_parent_legacy(patch)
        assert digest(output / "artifacts/legacy-parent-truncated.pb") == expected_fixture
        code = 0
    else:
        tests = [str(package / "test_history.py")] if args.suite == "phase2" else [str(package / "test_recent_consumers.py")]
        if args.suite == "all":
            tests.insert(0, str(package / "test_history.py"))
        options = ["-c", str(package / "pytest.ini"), "--confcutdir=" + str(package),
                   "-vv", "-s", "-ra", "-p", "no:cacheprovider",
                   "--basetemp=" + str(output / "tmp"),
                   "--junitxml=" + str(output / "results.xml")]
        if args.suite in ("recent", "gc"):
            options += ["-k", "recent_consumers" if args.suite == "recent" else "gc_bounds"]
        code = int(pytest.main(options + tests))
    modules = {}
    for name, module in sorted(sys.modules.items()):
        if name == "ray" or name.startswith("ray."):
            file = getattr(module, "__file__", None)
            if file and Path(file).is_file():
                path = Path(file).resolve()
                assert path.is_relative_to(source), (name, str(path))
                modules[name] = {"path": str(path.relative_to(source)), "sha256": digest(path)}
    (output / "run-provenance.json").write_text(json.dumps({
        "revision": provenance["revisions"][args.variant], "variant": args.variant,
        "suite": args.suite, "python": sys.version, "pytest": pytest.__version__,
        "exit_code": code, "modules": modules,
    }, indent=2) + "\n")
    return code


if __name__ == "__main__":
    sys.exit(main())
