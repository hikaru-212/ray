"""Portable adaptation of the recorded author-test Base/HEAD matrix.

This public adaptation was not executed during evidence packaging.
"""

import argparse
import ast
import json
import pathlib
import sqlite3

import ray
import ray.data
import ray.data.read_api as read_api

from source_loader import load_source


BASE = "88f1ffefc6b5a88507fbfdb448edecc44664cefa"
HEAD = "134e84169b605d67f36f99c7c841ba113f704356"
PACKAGE_ROOT = pathlib.Path(__file__).resolve().parents[1]
TEST_SNIPPET = PACKAGE_ROOT / "observed" / "head-test-function.py.txt"


def test_sources():
    text = TEST_SNIPPET.read_text()
    tree = ast.parse(text)
    node = next(
        item
        for item in tree.body
        if isinstance(item, ast.FunctionDef)
        and item.name == "test_read_sql_falls_back_to_percent_when_mod_denied"
    )
    original = ast.get_source_segment(text, node)
    assert original.count("str(arg1).lower()") == 1
    fixed = original.replace("str(arg1).lower()", "str(arg2).lower()")
    return node.name, original, fixed


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-dir", type=pathlib.Path, required=True)
    parser.add_argument("--work-dir", type=pathlib.Path, required=True)
    parser.add_argument("--ray-temp-dir", type=pathlib.Path, required=True)
    args = parser.parse_args()

    args.work_dir.mkdir(parents=True, exist_ok=False)
    function_name, original, fixed = test_sources()
    results = {}

    ray.init(
        address="local",
        num_cpus=2,
        include_dashboard=False,
        object_store_memory=128 * 1024 * 1024,
        _temp_dir=str(args.ray_temp_dir),
    )
    original_datasource = read_api.SQLDatasource
    try:
        for variant, source in [
            ("original_arg1", original),
            ("experimental_arg2", fixed),
        ]:
            for name, revision in [("base", BASE), ("head", HEAD)]:
                label = variant + "-" + name
                module = load_source(revision, args.source_dir)
                read_api.SQLDatasource = module.SQLDatasource
                namespace = {"sqlite3": sqlite3, "ray": ray}
                exec(compile(source, str(TEST_SNIPPET), "exec"), namespace)
                test = namespace[function_name]
                expected_failure = variant == "experimental_arg2" and name == "base"
                try:
                    test(str(args.work_dir / (label + ".sqlite")))
                except ValueError as error:
                    assert expected_failure
                    assert "Database does not support sharding" in str(error)
                    results[label] = {
                        "outcome": "expected ValueError",
                        "error": str(error),
                    }
                else:
                    assert not expected_failure
                    results[label] = {"outcome": "PASS", "rows": 500, "blocks": 4}
    finally:
        read_api.SQLDatasource = original_datasource
        ray.shutdown()

    assert len(results) == 4
    output = args.work_dir / "matrix-result.json"
    output.write_text(json.dumps(results, indent=2))
    print(json.dumps(results, indent=2))


if __name__ == "__main__":
    main()
