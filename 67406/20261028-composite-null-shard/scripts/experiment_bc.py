import argparse
import collections
import hashlib
import importlib.util
import inspect
import json
import os
import pathlib
import sqlite3
import sys
import time
import traceback

import psycopg2
import ray
import ray.data
import ray.data.read_api as read_api
import ray.cloudpickle

ROOT = pathlib.Path(__file__).parent
BASE = "8d53e59e3da91a39beb6b1b614265cf05d3d26a4"
HEAD = "c4ace0b69f82f66772d05c286ed29a52b6f73dc3"
SQL = "SELECT id, shard_key FROM events"
PORT = int((ROOT / "results/postgres-address.txt").read_text().strip().rsplit(":", 1)[1])

def save(name, value):
    (ROOT / "results" / name).write_text(json.dumps(value, indent=2, default=str))

def load_source(revision):
    path = ROOT / "sources" / (revision + "-sql_datasource.py")
    spec = importlib.util.spec_from_file_location("pinned_sql_" + revision, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    ray.cloudpickle.register_pickle_by_value(module)
    return module

def make_factory(engine, label):
    # Record successful and failed cursor.execute calls from driver and workers.
    log_path = str(ROOT / "results" / (label + "-queries.jsonl"))
    db_path = str(ROOT / "results" / "empty-string.sqlite")
    port = PORT
    def connect():
        import json
        import os
        import sqlite3
        import psycopg2
        def record(query, rowcount=None, error=None):
            entry = {"pid": os.getpid(), "query": query, "rowcount": rowcount, "error": error}
            with open(log_path, "a") as out:
                out.write(json.dumps(entry) + "\n")
        if engine == "postgres":
            class RecordingCursor(psycopg2.extensions.cursor):
                def execute(self, sql, params=None):
                    try:
                        result = super().execute(sql, params)
                        record(self.query.decode(), self.rowcount)
                        return result
                    except Exception as exc:
                        record(str(sql), error=repr(exc))
                        raise
            return psycopg2.connect(host=os.environ["REPRO_PG_HOST"], port=port, dbname=os.environ["REPRO_PG_DATABASE"],
                                    user=os.environ["REPRO_PG_USER"], password=os.environ["REPRO_PG_PASSWORD"],
                                    cursor_factory=RecordingCursor)
        class RecordingCursor(sqlite3.Cursor):
            def execute(self, sql, parameters=()):
                try:
                    result = super().execute(sql, parameters)
                    record(sql, self.rowcount)
                    return result
                except Exception as exc:
                    record(sql, error=repr(exc))
                    raise
        class RecordingConnection(sqlite3.Connection):
            def cursor(self, *args, **kwargs):
                kwargs.setdefault("factory", RecordingCursor)
                return super().cursor(*args, **kwargs)
        return sqlite3.connect(db_path, factory=RecordingConnection)
    return connect

def fixture():
    con = psycopg2.connect(host=os.environ["REPRO_PG_HOST"], port=PORT, dbname=os.environ["REPRO_PG_DATABASE"],
                           user=os.environ["REPRO_PG_USER"], password=os.environ["REPRO_PG_PASSWORD"])
    with con:
        with con.cursor() as cur:
            setup_sql = """CREATE TYPE pair_key AS (a integer, b integer);
CREATE TABLE events (id integer PRIMARY KEY, shard_key pair_key);
INSERT INTO events
SELECT i, CASE i % 4
    WHEN 0 THEN ROW(NULL::integer, i)::pair_key
    WHEN 1 THEN ROW(i, NULL::integer)::pair_key
    WHEN 2 THEN ROW(i, i + 1)::pair_key
    ELSE ROW(NULL::integer, NULL::integer)::pair_key END
FROM generate_series(1, 200) AS i;"""
            (ROOT / "sources" / "postgres-fixture.sql").write_text(setup_sql)
            cur.execute(setup_sql)
            cur.execute("SELECT version()")
            print("PostgreSQL:", cur.fetchone()[0])
            inspect_sql = """SELECT id, shard_key::text, pg_typeof(shard_key)::text,
hash_record(shard_key), MOD(ABS(hash_record(shard_key)), 4),
shard_key IS NULL, shard_key IS NOT NULL FROM events ORDER BY id"""
            cur.execute(inspect_sql)
            rows = cur.fetchall()
            save("postgres-fixture-inspection.json", {"sql": inspect_sql, "rows": rows})
            assert all(row[2] == "pair_key" and isinstance(row[3], int) for row in rows)
            witness = next(row for row in rows if not row[5] and not row[6] and row[4] != 0)
            save("postgres-witness.json", witness)
            print("POSTGRES WITNESS [id, key, type, hash, bucket, is_null, is_not_null]:", witness)
            cur.execute(SQL + " ORDER BY id")
            save("postgres-source-rows.json", cur.fetchall())
            print("Postgres source count:", len(rows))
    con.close()
    with sqlite3.connect(ROOT / "results" / "empty-string.sqlite") as con:
        con.execute("CREATE TABLE events (id INTEGER PRIMARY KEY, shard_key TEXT)")
        rows = [(0, "")] + [(i, "ABCD"[(i - 1) % 4] + str(i)) for i in range(1, 200)]
        con.executemany("INSERT INTO events VALUES (?, ?)", rows)
        save("sqlite-source-rows.json", rows)
        scalar = con.execute("SELECT MOD(5,2), unicode(''), '' IS NULL, '' IS NOT NULL").fetchone()
        print("SQLite [MOD(5,2), unicode(empty), is_null, is_not_null]:", scalar)
        assert scalar == (1.0, None, 0, 1)

def queries(label):
    path = ROOT / "results" / (label + "-queries.jsonl")
    return [json.loads(line) for line in path.read_text().splitlines()]

def check_output(engine, label, rows, expected_ids):
    ids = [row["id"] for row in rows]
    counts = collections.Counter(ids)
    duplicate_ids = sorted(key for key, count in counts.items() if count > 1)
    missing_ids = sorted(set(expected_ids) - set(ids))
    read_queries = [q for q in queries(label) if q["query"].startswith("SELECT * FROM (")]
    predicates = sorted({q["query"] for q in read_queries})
    assert len(predicates) == 4, predicates
    assert all("MOD(ABS(" in q and "WHERE" in q for q in predicates)
    assert not any(q["query"] == SQL for q in queries(label)), "Unexpected unsharded read"
    result = {"count": len(rows), "ids": sorted(ids), "rows": sorted(rows, key=lambda r: r["id"]),
              "missing_ids": missing_ids, "duplicate_ids": duplicate_ids,
              "distinct_shard_queries": predicates, "driver_pid": os.getpid()}
    save(label + "-result.json", result)
    print(label, "count", len(rows), "missing IDs", missing_ids, "duplicate IDs", duplicate_ids)
    for query in predicates:
        print(label, "SHARD SQL:", query)
    assert not duplicate_ids
    if engine == "sqlite":
        assert missing_ids == [0] and len(rows) == 199
    elif "base" in label:
        assert len(rows) == 200 and not missing_ids
    else:
        inspections = json.loads((ROOT / "results/postgres-fixture-inspection.json").read_text())["rows"]
        expected_missing = [row[0] for row in inspections if not row[5] and not row[6] and row[4] != 0]
        assert missing_ids == expected_missing and missing_ids
    return result

def direct():
    results = {}
    for engine, hash_fn, expected_ids in [("postgres", "hash_record", list(range(1, 201))),
                                         ("sqlite", "unicode", list(range(200)))]:
        for name, revision in [("base", BASE), ("head", HEAD)]:
            label = engine + "-" + name + "-direct"
            module = load_source(revision)
            ds = module.SQLDatasource(SQL, make_factory(engine, label), hash_fn, shard_keys=["shard_key"])
            assert ds.supports_sharding(4)
            tasks = ds.get_read_tasks(4)
            assert len(tasks) == 4
            rows = []
            task_rows = []
            for i, task in enumerate(tasks):
                part = [record for block in task() for record in block.to_pylist()]
                task_rows.append({"task_id": i, "ids": [r["id"] for r in part], "count": len(part)})
                rows.extend(part)
            save(label + "-tasks.json", task_rows)
            print(label, "task counts:", [part["count"] for part in task_rows])
            results[label] = check_output(engine, label, rows, expected_ids)
    save("direct-summary.json", {key: {field: val[field] for field in ["count", "missing_ids", "duplicate_ids"]}
                                for key, val in results.items()})
    print("DIRECT EXPERIMENTS B AND C VERIFIED")

def e2e():
    print("Runtime: Ray", ray.__version__, "commit", ray.__commit__, "file", ray.__file__)
    api_source = inspect.getsource(read_api.read_sql)
    (ROOT / "sources" / "runtime-read_sql.py").write_text(api_source)
    print("Runtime read_sql SHA256:", hashlib.sha256(api_source.encode()).hexdigest())
    print("Only read_api.SQLDatasource is substituted in memory with each exact pinned source module.")
    print("Unmodified Ray read_sql/read_datasource/planner/worker path is otherwise used.")
    ray.init(address="local", num_cpus=2, include_dashboard=False,
             object_store_memory=128 * 1024 * 1024, _temp_dir=os.environ["REPRO_RAY_TEMP_DIR"])
    outcomes = {}
    original = read_api.SQLDatasource
    try:
        for engine, hash_fn, expected_ids in [("postgres", "hash_record", list(range(1, 201))),
                                             ("sqlite", "unicode", list(range(200)))]:
            for name, revision in [("base", BASE), ("head", HEAD)]:
                label = engine + "-" + name + "-e2e"
                module = load_source(revision)
                read_api.SQLDatasource = module.SQLDatasource
                print("E2E START:", label, "datasource revision", revision, flush=True)
                try:
                    ds = ray.data.read_sql(SQL, make_factory(engine, label),
                                           shard_keys=["shard_key"], shard_hash_fn=hash_fn,
                                           override_num_blocks=4).materialize()
                    rows = ds.take_all()
                    print(label, "output blocks:", ds.num_blocks())
                    result = check_output(engine, label, rows, expected_ids)
                    result["output_blocks"] = ds.num_blocks()
                    worker_queries = [q for q in queries(label)
                                      if q["query"].startswith("SELECT * FROM (") and q["pid"] != os.getpid()]
                    assert len({q["query"] for q in worker_queries}) == 4
                    result["worker_pids"] = sorted({q["pid"] for q in worker_queries})
                    save(label + "-result.json", result)
                    print(label, "worker pids:", result["worker_pids"], flush=True)
                    outcomes[label] = {"status": "VERIFIED", "count": len(rows), "missing_ids": result["missing_ids"]}
                except Exception:
                    error = traceback.format_exc()
                    print(error, flush=True)
                    outcomes[label] = {"status": "INCONCLUSIVE", "error": error}
    finally:
        read_api.SQLDatasource = original
        ray.shutdown()
        save("e2e-summary.json", outcomes)
    assert all(v["status"] == "VERIFIED" for v in outcomes.values()), outcomes
    print("ALL FOUR END-TO-END CASES VERIFIED")

if __name__ == "__main__":
    print("Python:", sys.version)
    print("SQLite:", sqlite3.sqlite_version)
    print("psycopg2:", psycopg2.__version__)
    mode = argparse.ArgumentParser()
    mode.add_argument("mode", choices=["fixture", "direct", "e2e"])
    args = mode.parse_args()
    globals()[args.mode]()
