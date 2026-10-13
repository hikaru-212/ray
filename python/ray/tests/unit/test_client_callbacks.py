import sys
import threading
import warnings
from types import MethodType, SimpleNamespace
from unittest.mock import Mock

import pytest

import ray
import ray.cloudpickle as cloudpickle
from ray.core.generated import ray_client_pb2
from ray.util.client import common
from ray.util.client.common import (
    OBJECT_TRANSFER_CHUNK_SIZE,
    OBJECT_TRANSFER_WARNING_SIZE,
    ClientObjectRef,
)
from ray.util.client.dataclient import ACKNOWLEDGE_BATCH_SIZE, DataClient
from ray.util.client.worker import Worker
from ray.util.debug import reset_log_once

pytestmark = pytest.mark.usefixtures("propagate_logs")


@pytest.fixture
def client_worker(monkeypatch):
    worker = Mock(spec=Worker)
    worker._reconnect_enabled = True
    # Only the transport thread and connection are mocked. Use the production
    # Worker -> RegisterGetCallback -> ChunkCollector registration path.
    monkeypatch.setattr(DataClient, "_start_datathread", lambda self: Mock())
    worker.data_client = DataClient(worker, "test-client", [])
    worker.register_callback = MethodType(Worker.register_callback, worker)
    monkeypatch.setattr(
        common,
        "ray",
        SimpleNamespace(get_context=lambda: SimpleNamespace(client_worker=worker)),
    )
    yield worker
    worker.data_client.asyncio_waiting_data.clear()
    worker.data_client.outstanding_requests.clear()


def make_future(worker, object_id=1):
    ref = ClientObjectRef(bytes([object_id]) * ray.ObjectRef.size())
    future = ref.future()
    request = worker.data_client.request_queue.get_nowait()
    assert request.get.ids == [ref.id]
    assert request.get.asynchronous
    return future, request


def get_response(request, value=42, *, error=False, chunk_id=0, total_chunks=1):
    payload = cloudpickle.dumps(value)
    if error:
        response = ray_client_pb2.GetResponse(valid=False, error=payload)
    else:
        start = len(payload) * chunk_id // total_chunks
        end = len(payload) * (chunk_id + 1) // total_chunks
        response = ray_client_pb2.GetResponse(
            valid=True,
            data=payload[start:end],
            chunk_id=chunk_id,
            total_chunks=total_chunks,
            total_size=len(payload),
        )
    return ray_client_pb2.DataResponse(req_id=request.req_id, get=response)


@pytest.mark.parametrize("state", ["pending", "cancelled", "finished"])
@pytest.mark.parametrize("error", [False, True])
def test_future_reply_removes_callback(client_worker, caplog, state, error):
    data = client_worker.data_client
    future, request = make_future(client_worker)
    if state == "cancelled":
        assert future.cancel()
    elif state == "finished":
        future.set_result("existing result")
    value = ValueError("remote failure") if error else 42
    data._acknowledge_counter = ACKNOWLEDGE_BATCH_SIZE - 1
    data._process_response(get_response(request, value, error=error))

    assert not data.asyncio_waiting_data
    assert not data.outstanding_requests
    assert data.request_queue.get_nowait().acknowledge.req_id == request.req_id
    assert "Callback error:" not in caplog.text
    if state == "cancelled":
        assert future.cancelled()
    elif state == "finished":
        assert future.result() == "existing result"
    elif error:
        with pytest.raises(ValueError, match="remote failure"):
            future.result()
    else:
        assert future.result() == value


def test_cancelled_future_late_reply_then_shutdown(client_worker):
    data = client_worker.data_client
    cancelled, request = make_future(client_worker)
    assert cancelled.cancel()
    data._process_response(get_response(request))
    pending, _ = make_future(client_worker, object_id=2)

    data._shutdown()

    assert cancelled.cancelled()
    assert pending.done()
    with pytest.raises(ConnectionError, match="disconnected"):
        pending.result()
    assert not data.asyncio_waiting_data


@pytest.mark.parametrize("error", [False, True])
def test_cancellation_races_with_reply(client_worker, monkeypatch, caplog, error):
    data = client_worker.data_client
    future, request = make_future(client_worker)
    completing = threading.Event()
    cancelled = threading.Event()
    setter_name = "set_exception" if error else "set_result"
    setter = getattr(future, setter_name)

    def complete_after_cancellation(value):
        completing.set()
        assert cancelled.wait(timeout=5)
        setter(value)

    monkeypatch.setattr(future, setter_name, complete_after_cancellation)

    def cancel():
        if completing.wait(timeout=5):
            future.cancel()
            cancelled.set()

    thread = threading.Thread(target=cancel)
    thread.start()
    try:
        value = ValueError("remote failure") if error else 42
        data._process_response(get_response(request, value, error=error))
    finally:
        thread.join(timeout=10)
    assert not thread.is_alive()
    assert future.cancelled()
    assert not data.asyncio_waiting_data
    assert "Callback error:" not in caplog.text


@pytest.mark.parametrize("chunked", [False, True])
def test_failed_dispatch_removes_callback(client_worker, caplog, chunked):
    data = client_worker.data_client
    callback = Mock(side_effect=ValueError("callback failed"))
    if chunked:
        data.RegisterGetCallback(
            ray_client_pb2.GetRequest(ids=[b"object-id"], asynchronous=True), callback
        )
    else:
        data.Schedule(ray_client_pb2.ClientTask(), callback)
    request = data.request_queue.get_nowait()
    data._process_response(get_response(request))

    callback.assert_called_once()
    assert "callback failed" in caplog.text
    assert not data.asyncio_waiting_data
    assert not data.outstanding_requests
    assert data._acknowledge_counter == 1
    data._reconnect_channel()
    assert data.request_queue.empty()
    data._shutdown()
    callback.assert_called_once()


@pytest.mark.parametrize("transport_error", [None, RuntimeError("transport failed")])
def test_shutdown_isolates_callback_errors(client_worker, caplog, transport_error):
    data = client_worker.data_client
    callback = Mock(side_effect=ValueError("callback failed"))
    ref = ClientObjectRef(b"\x01" * ray.ObjectRef.size())
    ref._on_completed(callback)
    data.request_queue.get_nowait()
    pending, _ = make_future(client_worker, object_id=2)
    data._last_exception = transport_error

    data._shutdown()

    callback.assert_called_once()
    assert isinstance(callback.call_args.args[0], ConnectionError)
    assert "callback failed" in caplog.text
    assert pending.done()
    assert isinstance(pending.exception(), ConnectionError)
    if transport_error:
        assert "transport failed" in str(pending.exception())
    assert not data.asyncio_waiting_data


@pytest.mark.parametrize("cancelled", [False, True])
def test_partial_reply_survives_reconnect(client_worker, cancelled):
    data = client_worker.data_client
    future, request = make_future(client_worker)
    first_chunk = get_response(request, chunk_id=0, total_chunks=2)
    data._process_response(first_chunk)
    assert not future.done()
    if cancelled:
        future.cancel()

    data._reconnect_channel()
    resumed = data.request_queue.get_nowait()
    assert resumed.req_id == request.req_id
    assert resumed.get.start_chunk_id == 1
    assert request.req_id in data.asyncio_waiting_data
    assert request.req_id in data.outstanding_requests
    assert data._acknowledge_counter == 0

    # A replayed chunk must not complete the request or duplicate its payload.
    data._process_response(first_chunk)
    assert request.req_id in data.asyncio_waiting_data
    assert data._acknowledge_counter == 0
    data._process_response(get_response(request, chunk_id=1, total_chunks=2))
    assert not data.asyncio_waiting_data
    assert not data.outstanding_requests
    assert data._acknowledge_counter == 1
    if cancelled:
        assert future.cancelled()
    else:
        assert future.result() == 42


def large_object_chunk(request, chunk_id):
    # The first two chunks of a protocol 5 pickle of a 2 GiB bytes object.
    # Materialize only received chunks, while keeping production chunk sizes
    # and warning thresholds. BINBYTES is followed by its uint32 length;
    # MEMOIZE and STOP account for the final two bytes.
    value_size = OBJECT_TRANSFER_WARNING_SIZE
    header = b"\x80\x05B" + value_size.to_bytes(4, "little")
    total_size = len(header) + value_size + 2
    payload = b"x" * OBJECT_TRANSFER_CHUNK_SIZE
    if chunk_id == 0:
        payload = header + payload[len(header) :]
    return ray_client_pb2.DataResponse(
        req_id=request.req_id,
        get=ray_client_pb2.GetResponse(
            valid=True,
            data=payload,
            chunk_id=chunk_id,
            total_chunks=(total_size + OBJECT_TRANSFER_CHUNK_SIZE - 1)
            // OBJECT_TRANSFER_CHUNK_SIZE,
            total_size=total_size,
        ),
    )


@pytest.mark.parametrize("continuation", ["next_chunk", "reconnect", "shutdown"])
def test_chunk_collection_error_preserves_ownership(client_worker, continuation):
    data = client_worker.data_client
    future, request = make_future(client_worker)
    first_chunk = large_object_chunk(request, 0)
    data._acknowledge_counter = ACKNOWLEDGE_BATCH_SIZE - 1
    reset_log_once("client_object_transfer_size_warning")
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("error", UserWarning)
            data._process_response(first_chunk)

        # The warning raised before storing the chunk or delivering a result.
        # Keep both the callback and replay request until terminal dispatch.
        assert request.req_id in data.asyncio_waiting_data
        assert request.req_id in data.outstanding_requests
        assert request.get.start_chunk_id == 0
        assert data._acknowledge_counter == ACKNOWLEDGE_BATCH_SIZE - 1
        assert data.request_queue.empty()
        assert not future.done()

        if continuation == "next_chunk":
            data._process_response(large_object_chunk(request, 1))
            assert future.done()
            with pytest.raises(
                RuntimeError, match="Received chunk 1 when we expected 0"
            ):
                future.result()
            assert not data.asyncio_waiting_data
            assert not data.outstanding_requests
            assert data.request_queue.get_nowait().acknowledge.req_id == request.req_id
        else:
            if continuation == "reconnect":
                data._reconnect_channel()
                resumed = data.request_queue.get_nowait()
                assert resumed.req_id == request.req_id
                assert resumed.get.start_chunk_id == 0
                data._process_response(first_chunk)
                assert resumed.get.start_chunk_id == 1
                assert request.req_id in data.asyncio_waiting_data
                assert request.req_id in data.outstanding_requests
                assert data._acknowledge_counter == ACKNOWLEDGE_BATCH_SIZE - 1
                assert not future.done()
            data._shutdown()
            assert future.done()
            with pytest.raises(ConnectionError, match="disconnected"):
                future.result()
    finally:
        reset_log_once("client_object_transfer_size_warning")


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
