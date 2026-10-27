"""Minimal source loader extracted from the recorded Experiment B/C helper.

This public adaptation was not executed during evidence packaging.
"""

import importlib.util
import pathlib
import sys

import ray.cloudpickle


def load_source(revision: str, source_dir: pathlib.Path):
    """Load and register a pinned SQLDatasource module by value."""
    path = source_dir / (revision + "-sql_datasource.py")
    spec = importlib.util.spec_from_file_location("pinned_sql_" + revision, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Unable to load source module for {revision}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    ray.cloudpickle.register_pickle_by_value(module)
    return module
