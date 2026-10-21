"""Independent production-path evidence for Ray PR #67192.

The tests run the real ``batch_blocks`` pipeline.  The required environment
variables prevent a convenient installed Ray from being tested accidentally.
"""

import os
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pytest

import ray
from ray.data.block import BlockAccessor
from ray.data.context import DataContext
from ray.data._internal import batcher as batcher_module
from ray.data._internal.arrow_block import ArrowBlockAccessor
from ray.data._internal.arrow_ops import transform_pyarrow as transform
from ray.data._internal.block_batching import block_batching as block_batching_module
from ray.data._internal.block_batching import util as batching_util
from ray.data._internal.block_batching.block_batching import batch_blocks
from ray.data._internal.pandas_block import PandasBlockAccessor
from ray.data._internal.tensor_extensions.arrow import (
    ArrowTensorArray,
    ArrowVariableShapedTensorArray,
    FixedShapeTensorFormat,
)


def _variant():
    value = os.environ.get("RAY_REVIEW_VARIANT")
    if value not in {"parent", "pr"}:
        raise pytest.UsageError("set RAY_REVIEW_VARIANT to 'parent' or 'pr'")
    return value


def _is_pr():
    return _variant() == "pr"


@pytest.fixture(scope="session", autouse=True)
def verify_selected_ray_implementation():
    package_root_value = os.environ.get("RAY_REVIEW_PACKAGE_ROOT")
    if not package_root_value:
        raise pytest.UsageError("set RAY_REVIEW_PACKAGE_ROOT to the imported ray package")
    package_root = Path(package_root_value).absolute()
    modules = [ray, batcher_module, block_batching_module, batching_util, transform]
    for module in modules:
        module_path = Path(module.__file__).absolute()
        try:
            module_path.relative_to(package_root)
        except ValueError:
            pytest.fail(f"{module.__name__} came from {module_path}, outside {package_root}")

    has_pr_api = hasattr(transform, "converts_to_non_object_numpy")
    assert has_pr_api is _is_pr(), (
        f"RAY_REVIEW_VARIANT={_variant()} does not match imported code at {package_root}"
    )
    expected_native = os.environ.get("RAY_REVIEW_NATIVE_COMMIT")
    if expected_native:
        assert ray.__commit__ == expected_native
    print(
        f"variant={_variant()} ray={ray.__file__} batcher={batcher_module.__file__} "
        f"native_commit={ray.__commit__} pyarrow={pa.__version__} numpy={np.__version__}"
    )


def _fixed_tensor():
    context = DataContext.get_current()
    previous = context.arrow_fixed_shape_tensor_format
    try:
        context.arrow_fixed_shape_tensor_format = FixedShapeTensorFormat.V2
        values = np.arange(8, dtype=np.int64).reshape(2, 2, 2)
        return ArrowTensorArray.from_numpy(values)
    finally:
        context.arrow_fixed_shape_tensor_format = previous


def _ragged_tensor():
    return ArrowVariableShapedTensorArray.from_numpy(
        [np.array([1, 2], dtype=np.int64), np.array([3, 4, 5], dtype=np.int64)]
    )


def _ipc_roundtrip(table):
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, table.schema) as writer:
        writer.write_table(table)
    return pa.ipc.open_stream(sink.getvalue().to_pybytes()).read_all()


def _buffers(table):
    result = []
    for column in table.columns:
        for chunk in column.chunks:
            result.extend(buffer for buffer in chunk.buffers() if buffer and buffer.size)
    return result


def _snapshot(table):
    return [buffer.to_pybytes() for buffer in _buffers(table)]


def _arrays(value):
    if isinstance(value, np.ndarray):
        yield value
        if value.dtype.hasobject:
            for item in value.flat:
                yield from _arrays(item)
    elif isinstance(value, dict):
        for item in value.values():
            yield from _arrays(item)
    elif isinstance(value, (list, tuple)):
        for item in value:
            yield from _arrays(item)


def _aliases_source(value, table):
    return any(
        np.shares_memory(array, np.frombuffer(buffer, dtype=np.uint8))
        for array in _arrays(value)
        for buffer in _buffers(table)
    )


def _instrument(monkeypatch):
    counts = {"block_copy": 0, "numpy_copy": 0}
    original_slice = ArrowBlockAccessor.slice
    original_copy = batching_util._copy_batch

    def traced_slice(self, start, end, copy=False):
        counts["block_copy"] += int(copy)
        return original_slice(self, start, end, copy=copy)

    def traced_copy(batch):
        counts["numpy_copy"] += 1
        return original_copy(batch)

    monkeypatch.setattr(ArrowBlockAccessor, "slice", traced_slice)
    monkeypatch.setattr(batching_util, "_copy_batch", traced_copy)
    return counts


@pytest.mark.parametrize("kind", ["int64", "fixed_tensor"])
@pytest.mark.parametrize("batch_size", [None, 2])
def test_safe_numpy_batch_is_writable_and_source_isolated(kind, batch_size, monkeypatch):
    column = pa.array([1, 2], type=pa.int64()) if kind == "int64" else _fixed_tensor()
    source = pa.table({"x": column})
    before = _snapshot(source)
    counts = _instrument(monkeypatch)

    batch = next(
        batch_blocks(
            iter([source]),
            batch_size=batch_size,
            batch_format="numpy",
            ensure_copy=True,
        )
    )

    assert counts == {"block_copy": 0 if _is_pr() else 1, "numpy_copy": 1}
    assert all(array.flags.writeable for array in _arrays(batch))
    assert not _aliases_source(batch, source)
    batch["x"] += 10
    assert _snapshot(source) == before


def test_read_only_ipc_ragged_tensor_uses_fallback(monkeypatch):
    source = _ipc_roundtrip(pa.table({"x": _ragged_tensor()}))
    assert all(not buffer.is_mutable for buffer in _buffers(source))
    before = _snapshot(source)
    counts = _instrument(monkeypatch)

    batch = next(
        batch_blocks(
            iter([source]), batch_size=2, batch_format="numpy", ensure_copy=True
        )
    )

    assert counts == {"block_copy": 1, "numpy_copy": 1}
    assert [item.shape for item in batch["x"]] == [(2,), (3,)]
    assert all(item.flags.writeable for item in batch["x"])
    for item in batch["x"]:
        item += 10
    assert _snapshot(source) == before


def test_mixed_numeric_and_ragged_schema_uses_fallback(monkeypatch):
    source = pa.table({"id": pa.array([0, 1]), "tensor": _ragged_tensor()})
    before = _snapshot(source)
    counts = _instrument(monkeypatch)

    batch = next(
        batch_blocks(
            iter([source]), batch_size=2, batch_format="numpy", ensure_copy=True
        )
    )

    assert counts == {"block_copy": 1, "numpy_copy": 1}
    batch["id"] += 10
    for item in batch["tensor"]:
        item += 10
    assert _snapshot(source) == before


def test_nullable_boolean_object_dtype_has_only_immutable_referents(monkeypatch):
    source = pa.table({"x": pa.array([True, None], type=pa.bool_())})
    converted = BlockAccessor.for_block(source).to_batch_format("numpy")["x"]
    assert converted.dtype == np.dtype(object)
    assert all(value is None or type(value) is bool for value in converted)
    if _is_pr():
        assert transform.converts_to_non_object_numpy(source.schema) is True

    before = _snapshot(source)
    counts = _instrument(monkeypatch)
    batch = next(
        batch_blocks(
            iter([source]), batch_size=2, batch_format="numpy", ensure_copy=True
        )
    )

    assert counts == {"block_copy": 0 if _is_pr() else 1, "numpy_copy": 1}
    assert batch["x"].dtype == np.dtype(object)
    assert not _aliases_source(batch, source)
    batch["x"][0] = False
    assert source["x"].to_pylist() == [True, None]
    assert _snapshot(source) == before


def test_multi_chunk_partial_batches_keep_order_and_isolation(monkeypatch):
    source = pa.table(
        {
            "id": pa.chunked_array([pa.array([0, 1]), pa.array([2, 3, 4])]),
            "value": pa.chunked_array([pa.array([10, 11]), pa.array([12, 13, 14])]),
        }
    )
    before = _snapshot(source)
    counts = _instrument(monkeypatch)

    batches = list(
        batch_blocks(
            iter([source]), batch_size=3, batch_format="numpy", ensure_copy=True
        )
    )

    assert [batch["id"].tolist() for batch in batches] == [[0, 1, 2], [3, 4]]
    assert [batch["value"].tolist() for batch in batches] == [
        [10, 11, 12],
        [13, 14],
    ]
    assert counts == {
        "block_copy": 0 if _is_pr() else len(batches),
        "numpy_copy": len(batches),
    }
    for batch in batches:
        assert not _aliases_source(batch, source)
        batch["id"] += 100
        batch["value"] += 100
    assert _snapshot(source) == before


@pytest.mark.parametrize("batch_format", ["pyarrow", "pandas", "default", None])
@pytest.mark.parametrize("batch_size", [None, 2])
def test_non_numpy_formats_keep_arrow_block_copy(batch_format, batch_size, monkeypatch):
    source = pa.table({"x": [1, 2]})
    before = _snapshot(source)
    counts = _instrument(monkeypatch)
    batch = next(
        batch_blocks(
            iter([source]),
            batch_size=batch_size,
            batch_format=batch_format,
            ensure_copy=True,
        )
    )

    assert counts == {"block_copy": 1, "numpy_copy": 1}
    if batch_format in {"pyarrow", None}:
        assert isinstance(batch, pa.Table)
        assert (
            batch["x"].chunk(0).buffers()[1].address
            != source["x"].chunk(0).buffers()[1].address
        )
    else:
        assert isinstance(batch, pd.DataFrame)
        batch["x"] += 10
    assert _snapshot(source) == before


def test_pandas_source_is_not_covered_by_arrow_optimization(monkeypatch):
    source = pd.DataFrame({"x": np.array([1, 2], dtype=np.int64)})
    copied = []
    original_slice = PandasBlockAccessor.slice

    def traced_slice(self, start, end, copy=False):
        copied.append(copy)
        return original_slice(self, start, end, copy=copy)

    monkeypatch.setattr(PandasBlockAccessor, "slice", traced_slice)
    batch = next(
        batch_blocks(
            iter([source]), batch_size=2, batch_format="numpy", ensure_copy=True
        )
    )
    assert copied == [True]
    batch["x"] += 10
    assert source["x"].tolist() == [1, 2]
