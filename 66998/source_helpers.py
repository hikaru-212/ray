"""Load pinned production source with git show; never modify the Ray checkout."""

import ast
import atexit
from functools import lru_cache
import hashlib
import importlib
import inspect
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
from types import ModuleType, SimpleNamespace

REVISIONS = {
    "baseline": "de28d2005add958b576b0bf7d9da8d905c042bb7",
    "initial": "337a326cabc5ae2d48a379fb4f59fd8fcc9a12b1",
    "followup": "451b8fa2354452236c96f0c0820c174c37df141c",
    "head": "772d9c718608dccb82473d524c876be21bda9bfd",
}
REVISION = os.environ.get("RAY_EVIDENCE_REVISION", "head")
SHA = REVISIONS[REVISION]
REPO = Path(os.environ["RAY_SOURCE_REPO"]).expanduser().resolve()
CACHE_PATH = "python/ray/_private/runtime_env/uri_cache.py"


def git(*args):
    return subprocess.check_output(["git", "-C", str(REPO), *args])


assert git("rev-parse", SHA + "^{commit}").decode().strip() == SHA


@lru_cache(None)
def source(path):
    return git("show", f"{SHA}:{path}")


def emit(kind, **data):
    print(json.dumps({"kind": kind, "revision": REVISION, "sha": SHA, **data}, sort_keys=True))


@lru_cache(None)
def load_uri_cache():
    filename = f"git:{SHA}:{CACHE_PATH}"
    module = ModuleType("evidence_uri_cache")
    exec(compile(source(CACHE_PATH), filename, "exec"), module.__dict__)
    assert module.URICache.add.__code__.co_filename == filename
    emit("cache_provenance", source_sha256=hashlib.sha256(source(CACHE_PATH)).hexdigest())
    return module.URICache


def consistent(cache, expected):
    """An assertion oracle, not a replacement implementation of the cache."""
    assert not cache._used_uris & cache._unused_uris
    assert cache._used_uris | cache._unused_uris == set(expected)
    assert cache.get_total_size_bytes() == sum(expected.values())
    if hasattr(cache, "_uri_sizes"):
        assert cache._uri_sizes == expected
    cache._check_valid()


@lru_cache(None)
def load_components():
    # The wheel supplies native/dependency imports; tested Python modules come
    # from the selected Git revision. Use a fresh process for each revision.
    import ray
    import ray._raylet
    import ray._common
    import ray._private.runtime_env

    temp = tempfile.TemporaryDirectory(prefix="ray-66998-source-")
    atexit.register(temp.cleanup)
    root = Path(temp.name)
    names = [
        "ray._private.runtime_env." + name
        for name in (
            "uri_cache", "context", "conda_utils", "protocol",
            "packaging", "constants", "plugin", "working_dir",
        )
    ] + ["ray._common.runtime_env_package", "ray._common.runtime_env_uri"]
    for name in names:
        relative = name.replace(".", "/") + ".py"
        destination = root / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(source("python/" + relative))
    ray._private.runtime_env.__path__.insert(0, str(root / "ray/_private/runtime_env"))
    ray._common.__path__.insert(0, str(root / "ray/_common"))
    for name in list(sys.modules):
        if name.startswith("ray._private.runtime_env.") or name in names:
            del sys.modules[name]
    modules = {name: importlib.import_module(name) for name in names}
    hashes = {}
    for name, module in modules.items():
        path = Path(module.__file__).resolve()
        assert path.is_relative_to(root), f"Wrong import: {name}: {path}"
        raw = source("python/" + name.replace(".", "/") + ".py")
        assert path.read_bytes() == raw, f"Source mismatch: {name}"
        hashes[name] = hashlib.sha256(raw).hexdigest()

    from ray._private.utils import get_directory_size_bytes
    from ray.runtime_env import RuntimeEnv

    expected = next(
        node for node in ast.parse(source("python/ray/_private/utils.py")).body
        if isinstance(node, ast.FunctionDef) and node.name == "get_directory_size_bytes"
    )
    actual = ast.parse(inspect.getsource(get_directory_size_bytes)).body[0]
    assert ast.dump(expected) == ast.dump(actual), "Incompatible wheel size helper"
    cache = modules["ray._private.runtime_env.uri_cache"]
    plugin = modules["ray._private.runtime_env.plugin"]
    working_dir = modules["ray._private.runtime_env.working_dir"]
    packaging = modules["ray._private.runtime_env.packaging"]
    for function in (
        cache.URICache.add, working_dir.WorkingDirPlugin.create,
        working_dir.WorkingDirPlugin.delete_uri, plugin.create_for_plugin_if_needed,
        packaging.delete_package, packaging.download_and_unpack_package,
    ):
        assert Path(function.__code__.co_filename).resolve().is_relative_to(root)
    emit(
        "component_provenance", python=sys.version.split()[0],
        ray_version=ray.__version__, ray_commit=ray.__commit__,
        native_imported=True, size_helper_ast_matches=True, sources_sha256=hashes,
    )
    return SimpleNamespace(
        URICache=cache.URICache, WorkingDirPlugin=working_dir.WorkingDirPlugin,
        create_for_plugin_if_needed=plugin.create_for_plugin_if_needed,
        RuntimeEnvContext=modules["ray._private.runtime_env.context"].RuntimeEnvContext,
        RuntimeEnv=RuntimeEnv, packaging=packaging,
    )
