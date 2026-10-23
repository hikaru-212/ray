"""Phase 2 component reproduction and filesystem failure/recovery tests.

MemoryGCS replaces only zip-byte transport. Caller, unpacking, locks, directory
measurement, URICache, WorkingDirPlugin and rmtree are real production code.
"""

import asyncio
import io
import os
import shutil
from unittest import mock
import zipfile

import pytest

from source_helpers import consistent, emit, load_components


class MemoryGCS:
    def __init__(self):
        self.packages = {}
        self.calls = []

    def put(self, uri, files):
        out = io.BytesIO()
        with zipfile.ZipFile(out, "w") as archive:
            for name, data in files.items():
                archive.writestr(name, data)
        self.packages[uri] = out.getvalue()

    async def async_internal_kv_get(self, key, namespace, timeout):
        self.calls.append(key.decode())
        return self.packages[key.decode()]


def make_component(tmp_path, cap):
    production = load_components()
    gcs = MemoryGCS()
    plugin = production.WorkingDirPlugin(str(tmp_path), gcs)
    cache = production.URICache(plugin.delete_uri, cap, debug_mode=False)
    return production, gcs, plugin, cache


def create(production, gcs, plugin, cache, uri, files):
    gcs.put(uri, files)
    context = production.RuntimeEnvContext()
    asyncio.run(production.create_for_plugin_if_needed(
        production.RuntimeEnv(working_dir=uri), plugin, cache, context,
    ))
    return production.packaging.get_local_dir_from_uri(uri, plugin._resources_dir)


def test_three_growing_directories(tmp_path):
    production, gcs, plugin, cache = make_component(tmp_path, cap=1)
    totals, directories = [], []
    for number in range(3):
        uri = f"gcs://_ray_pkg_job{number}.zip"
        directory = create(production, gcs, plugin, cache, uri, {"code": b"xx"})
        (directory / "output").write_bytes(b"x" * 18)
        cache.mark_unused(uri)
        totals.append(cache.get_total_size_bytes())
        directories.append(directory)
    retained = sum(directory.exists() for directory in directories)
    emit("three_directories", totals=totals, retained=retained)
    assert len(gcs.calls) == 3
    # Assert after all jobs so baseline still prints its complete -18/-16/-14
    # timeline and 2/3 retained directories. Expected failures are not xfailed.
    assert totals == [0, 0, 0]
    assert retained == 0
    consistent(cache, {})


@pytest.mark.parametrize("failure", ["before", "partial"])
def test_real_delete_failure_and_redownload(tmp_path, failure):
    if failure == "before" and (os.name != "posix" or os.geteuid() == 0):
        pytest.skip("Real chmod failure requires an unprivileged POSIX user")
    if failure == "partial" and not shutil.rmtree.avoids_symlink_attacks:
        pytest.skip("Partial unlink injection requires fd-based rmtree")
    production, gcs, plugin, cache = make_component(tmp_path, cap=1)
    cache.add("retained", 1)
    uri = "gcs://_ray_pkg_failure.zip"
    files = {"code": b"xx", "aux": b"yyy"}
    directory = create(production, gcs, plugin, cache, uri, files)
    consistent(cache, {"retained": 1, uri: 5})
    if failure == "before":
        directory.chmod(0o500)
        try:
            if os.access(directory, os.W_OK):
                pytest.skip("Filesystem does not enforce the requested chmod")
            with pytest.raises(PermissionError):
                cache.mark_unused(uri)
        finally:
            directory.chmod(0o700)
        assert len(list(directory.iterdir())) == 2
    else:
        real_unlink = os.unlink
        calls = []

        def fail_second_unlink(path, *args, **kwargs):
            # Explicit filesystem fault injection. First package-file unlink
            # is real; the second raises. Lockfile cleanup is not intercepted.
            if kwargs.get("dir_fd") is not None and os.fspath(path) in files:
                calls.append(path)
                if len(calls) == 2:
                    raise PermissionError("injected failure on second unlink")
            return real_unlink(path, *args, **kwargs)

        with mock.patch.object(production.packaging.os, "unlink", side_effect=fail_second_unlink):
            with pytest.raises(PermissionError, match="second unlink"):
                cache.mark_unused(uri)
        assert len(calls) == 2
        assert len(list(directory.iterdir())) == 1
    assert directory.exists()
    emit("physical_boundary", failure=failure, files_left=len(list(directory.iterdir())),
         total=cache.get_total_size_bytes(), target_in_cache=uri in cache)
    consistent(cache, {"retained": 1})
    remaining = {path.name: path.read_bytes() for path in directory.iterdir()}
    cache.mark_unused(uri)
    assert {path.name: path.read_bytes() for path in directory.iterdir()} == remaining
    assert uri not in cache
    # Underlying failure is now removed. Real create(overwrite=True) must
    # replace leftovers, download again and restore both original files.
    directory = create(production, gcs, plugin, cache, uri, files)
    assert len(gcs.calls) == 2
    assert {path.name: path.read_bytes() for path in directory.iterdir()} == files
    consistent(cache, {"retained": 1, uri: 5})
    cache.mark_unused(uri)
    assert not directory.exists()
    consistent(cache, {"retained": 1})
    emit("recovery", failure=failure, downloads=len(gcs.calls),
         directory_exists=directory.exists(), total=cache.get_total_size_bytes())
