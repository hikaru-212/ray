import asyncio
import pickle
import threading
from unittest.mock import MagicMock, patch

import pytest

import ray
from ray import ObjectRefGenerator
from ray.serve._private.common import ReplicaQueueLengthInfo, RequestMetadata
from ray.serve._private.replica_result import (
    ActorReplicaResult,
    _consume_generator_ref_when_ready,
)


def _metadata() -> RequestMetadata:
    return RequestMetadata(request_id="phase2", internal_request_id="phase2-internal")


class ControlledUserRef:
    def __init__(self, *, block_registration: bool = False):
        self.block_registration = block_registration
        self.callback = None
        self.register_entered = threading.Event()
        self.release_registration = threading.Event()
        self.events = []
        self.callback_thread_id = None
        self.cancel_calls = 0

    def _on_ready(self, callback):
        self.callback = callback
        self.events.append("register_enter")
        self.register_entered.set()
        if self.block_registration:
            assert self.release_registration.wait(timeout=5)
        self.events.append("register_return")
        return self.cancel_wait

    def fire(self, exc=None):
        assert self.callback is not None

        def invoke():
            self.callback_thread_id = threading.get_ident()
            self.events.append("callback_enter")
            self.callback(exc)
            self.events.append("callback_return")

        callback_thread = threading.Thread(target=invoke, name="phase2-io-callback")
        callback_thread.start()
        callback_thread.join(timeout=5)
        assert not callback_thread.is_alive()

    def cancel_wait(self):
        self.cancel_calls += 1
        self.events.append("cancel_wait")
        self.callback(RuntimeError("WaitAsync cancelled"))


async def _accept(user_ref):
    obj_ref_gen = MagicMock(spec=ObjectRefGenerator)
    rejection_frame = asyncio.get_running_loop().create_future()
    rejection_frame.set_result(
        pickle.dumps(ReplicaQueueLengthInfo(accepted=True, num_ongoing_requests=1))
    )
    obj_ref_gen.__anext__.return_value = rejection_frame
    obj_ref_gen._get_next_ref_n.return_value = [user_ref]
    replica_result = ActorReplicaResult(obj_ref_gen, _metadata(), with_rejection=True)
    assert (await replica_result.get_rejection_response()).accepted
    return replica_result, obj_ref_gen


@pytest.mark.asyncio
async def test_callback_after_handle_storage():
    ref = ControlledUserRef()
    result, generator = await _accept(ref)
    assert result._cancel_consume_wait == ref.cancel_wait
    assert ref.events == ["register_enter", "register_return"]

    caller_thread_id = threading.get_ident()
    ref.fire(None)

    assert ref.callback_thread_id != caller_thread_id
    assert result._cancel_consume_wait is None
    generator._consume_next_ref_n.assert_called_once_with(1)
    assert ref.events == [
        "register_enter",
        "register_return",
        "callback_enter",
        "callback_return",
    ]
    print("after_storage_events", ref.events)


@pytest.mark.asyncio
async def test_error_callback_clears_without_consuming():
    ref = ControlledUserRef()
    result, generator = await _accept(ref)

    ref.fire(RuntimeError("terminal error"))

    assert result._cancel_consume_wait is None
    generator._consume_next_ref_n.assert_not_called()
    print("error_events", ref.events)


@pytest.mark.asyncio
async def test_cancel_after_registration_uses_handle_and_error_callback():
    ref = ControlledUserRef()
    result, generator = await _accept(ref)

    with patch("ray.cancel") as cancel_generator:
        result.cancel()

    assert ref.cancel_calls == 1
    assert result._cancel_consume_wait is None
    generator._consume_next_ref_n.assert_not_called()
    cancel_generator.assert_called_once_with(generator)
    print("cancel_after_registration_events", ref.events)


def test_cancel_overlaps_registration_with_stream_terminal_callback():
    ref = ControlledUserRef(block_registration=True)
    generator = MagicMock(spec=ObjectRefGenerator)
    result = ActorReplicaResult(generator, _metadata(), with_rejection=True)
    result._obj_ref = ref

    registration_thread = threading.Thread(
        target=_consume_generator_ref_when_ready,
        args=(result,),
        name="phase2-registration",
    )
    registration_thread.start()
    assert ref.register_entered.wait(timeout=5)

    def terminal_cancel(cancelled_generator):
        assert cancelled_generator is generator
        ref.events.append("generator_cancel")
        ref.fire(RuntimeError("stream terminal cancellation"))

    try:
        with patch("ray.cancel", side_effect=terminal_cancel) as cancel_generator:
            result.cancel()
            cancel_generator.assert_called_once_with(generator)
    finally:
        ref.release_registration.set()
        registration_thread.join(timeout=5)

    assert not registration_thread.is_alive()
    assert ref.cancel_calls == 0
    assert result._cancel_consume_wait is None
    generator._consume_next_ref_n.assert_not_called()
    assert ref.events == [
        "register_enter",
        "generator_cancel",
        "callback_enter",
        "callback_return",
        "register_return",
    ]
    print("cancel_overlap_events", ref.events)
