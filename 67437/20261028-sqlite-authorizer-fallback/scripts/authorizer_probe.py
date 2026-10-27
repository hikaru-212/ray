"""Portable adaptation of the recorded SQLite authorizer probe.

This public adaptation was not executed during evidence packaging.
"""

import argparse
import ast
import json
import pathlib
import sqlite3

from source_loader import load_source


HEAD = "134e84169b605d67f36f99c7c841ba113f704356"
PACKAGE_ROOT = pathlib.Path(__file__).resolve().parents[1]
TEST_SNIPPET = PACKAGE_ROOT / "observed" / "head-test-function.py.txt"


def callback_sources():
    text = TEST_SNIPPET.read_text()
    tree = ast.parse(text)
    test = next(
        node
        for node in tree.body
        if isinstance(node, ast.FunctionDef)
        and node.name == "test_read_sql_falls_back_to_percent_when_mod_denied"
    )
    callback = next(
        node
        for node in test.body
        if isinstance(node, ast.FunctionDef) and node.name == "deny_mod"
    )
    original = ast.get_source_segment(text, callback)
    original = "\n".join(
        [original.splitlines()[0]]
        + [line[4:] for line in original.splitlines()[1:]]
    )
    assert original.count("str(arg1).lower()") == 1
    fixed = original.replace("str(arg1).lower()", "str(arg2).lower()")
    assert original != fixed
    return original, fixed


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-dir", type=pathlib.Path, required=True)
    parser.add_argument("--work-dir", type=pathlib.Path, required=True)
    args = parser.parse_args()

    args.work_dir.mkdir(parents=True, exist_ok=False)
    module = load_source(HEAD, args.source_dir)
    original, fixed = callback_sources()
    database = args.work_dir / "authorizer.sqlite"

    with sqlite3.connect(database) as connection:
        unrestricted = connection.execute("SELECT MOD(5,2)").fetchone()
        connection.execute("CREATE TABLE events (id INTEGER, key TEXT)")
        connection.executemany(
            "INSERT INTO events VALUES (?, ?)",
            [(index, str(index)) for index in range(200)],
        )

    results = {"unrestricted_mod": unrestricted}
    for name, source in [("original_arg1", original), ("experimental_arg2", fixed)]:
        namespace = {"sqlite3": sqlite3}
        exec(compile(source, str(TEST_SNIPPET), "exec"), namespace)
        callback = namespace["deny_mod"]
        calls = []
        queries = []

        def recording_callback(action, arg1, arg2, database_name, trigger):
            if action == sqlite3.SQLITE_FUNCTION:
                calls.append([action, arg1, arg2, database_name, trigger])
            return callback(action, arg1, arg2, database_name, trigger)

        def connect():
            connection = sqlite3.connect(database)
            connection.set_authorizer(recording_callback)
            connection.set_trace_callback(queries.append)
            return connection

        outcomes = {}
        with connect() as connection:
            for sql in ["SELECT MOD(5,2)", "SELECT 5 % 2"]:
                try:
                    outcomes[sql] = {"value": connection.execute(sql).fetchone()}
                except sqlite3.DatabaseError as error:
                    outcomes[sql] = {
                        "error_type": type(error).__name__,
                        "error": str(error),
                    }

        datasource = module.SQLDatasource(
            "SELECT * FROM events", connect, "unicode", shard_keys=["key"]
        )
        supported = datasource.supports_sharding(4)
        results[name] = {
            "queries": queries,
            "callbacks": calls,
            "outcomes": outcomes,
            "supports_sharding": supported,
            "selected_syntax": datasource._modulo_syntax,
        }

    assert results["original_arg1"]["outcomes"]["SELECT MOD(5,2)"]["value"] == (1.0,)
    assert results["original_arg1"]["selected_syntax"] == "MOD"
    assert (
        "not authorized"
        in results["experimental_arg2"]["outcomes"]["SELECT MOD(5,2)"]["error"]
    )
    assert results["experimental_arg2"]["outcomes"]["SELECT 5 % 2"]["value"] == (1,)
    assert results["experimental_arg2"]["selected_syntax"] == "%"
    assert any(
        call[1] is None and call[2].lower() == "mod"
        for call in results["original_arg1"]["callbacks"]
    )

    output = args.work_dir / "authorizer-result.json"
    output.write_text(json.dumps(results, indent=2))
    print(json.dumps(results, indent=2))


if __name__ == "__main__":
    main()
