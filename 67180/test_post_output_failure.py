"""One scenario: a valid pair queued, then a producer body error at budget 0."""
import hashlib
import inspect
import json
import os
from pathlib import Path

import pytest
import ray
from ray._common.test_utils import SignalActor, wait_for_condition
from ray.data._internal.execution.block_ref_counter import BlockRefCounter
from ray.data._internal.execution.interfaces import ExecutionOptions
from ray.data._internal.execution.interfaces.physical_operator import DataOpTask
from ray.data._internal.execution.metadata_fetcher import InlineMetadataFetcher, ThreadedMetadataFetcher
from ray.data._internal.execution.operators.input_data_buffer import InputDataBuffer
from ray.data._internal.execution.operators.map_operator import MapOperator
from ray.data._internal.execution.streaming_executor_state import build_streaming_topology, process_completed_tasks
from ray.data._internal.execution.util import make_ref_bundles
from ray.data._internal.stats import Timer
from ray.data.block import BlockAccessor
from ray.data.context import DataContext
from ray.data.tests.util import create_map_transformer_from_block_fn
from ray.exceptions import ObjectLostError, RayTaskError
from ray.tests.conftest import _ray_start

RUN = Path(os.environ["PHASE2_RUN_DIR"])
SOURCE = Path(os.environ["PHASE2_SOURCE"]).resolve()
LABEL = os.environ["PHASE2_LABEL"]
RAY_TMP = Path(os.environ["PHASE2_RAY_TMP"])
ERROR_MARKER = "phase2_after_valid_pair"
EXPECTED = {
    "head": {
        "sha": "817d5f128c7e3905f87ca39a2d1e918d8c23ebb1",
        "data_op_task_sha256": "0964b67c2e78c3f64e6e78b4ccbfbfb9616fcb298b89a081ece18a4e095dd501",
    },
    "baseline": {
        "sha": "6a3ba37e9e09803703b06787cdab6b085581788c",
        "data_op_task_sha256": "4a3eb16831dafa54f5df5d0645611c2eec44672cc81a84323b77b9fa3e45e6f3",
    },
}


@pytest.fixture
def ray_session():
    # Use the real fixture context, without importing destructive global port
    # cleanup fixtures into this standalone pytest suite.
    with _ray_start(num_cpus=2, include_dashboard=False, log_to_driver=True,
                    _temp_dir=str(RAY_TMP)):
        yield


@pytest.mark.parametrize("threaded", [False, True], ids=["inline", "threaded"])
def test_failure_after_queued_pair_at_zero_budget(ray_session, threaded):
    mode = "threaded" if threaded else "inline"
    source = EXPECTED[LABEL]
    loaded = Path(inspect.getsourcefile(DataOpTask)).resolve()
    loaded.relative_to(SOURCE)
    assert hashlib.sha256(loaded.read_bytes()).hexdigest() == source["data_op_task_sha256"]
    assert ray.__commit__ == "a70516160723a5dca49b3d70fb0ee9c6823382e2"
    events = []

    def record(event, **details):
        item = {"sequence": len(events), "event": event, **details}
        events.append(item)
        with (RUN / f"{mode}-events.jsonl").open("a") as f:
            f.write(json.dumps(item, sort_keys=True) + "\n")
        print(json.dumps(item, sort_keys=True), flush=True)

    release_failure = SignalActor.remote()

    def transform(blocks, ctx):
        yield from blocks
        # Released only after output is queued and executor sees budget == 0.
        ray.get(release_failure.wait.remote())
        raise ValueError(ERROR_MARKER)

    context = DataContext.get_current().copy()
    context.retried_map_errors = False
    context.max_map_retries = 0
    inputs = make_ref_bundles([[42]])
    source_op = InputDataBuffer(context, [])
    op = MapOperator.create(
        create_map_transformer_from_block_fn(transform, disable_block_shaping=True),
        source_op, context, ray_remote_args={"num_cpus": 1, "max_retries": 0},
    )
    counter = BlockRefCounter()
    topology = build_streaming_topology(op, ExecutionOptions(), counter)

    class Budget:
        name = "FocusedVerificationBudget"
        value = 1

        def max_task_output_bytes_to_read(self, candidate):
            return self.value if candidate is op else None

    class NoEscape:
        def should_unblock(self, candidate):
            return False

        def notify_release_emitted(self, candidate):
            raise AssertionError("No escape-hatch release was authorized")

    budget = Budget()
    fetcher = ThreadedMetadataFetcher() if threaded else InlineMetadataFetcher()
    fetcher.start()
    outputs, completions, block_refs, meta_refs, tick_errors = [], [], [], [], []
    task = None

    def tick():
        errors = process_completed_tasks(topology, [budget], 1, NoEscape(),
                                         metadata_fetcher=fetcher)
        tick_errors.append(errors)
        return errors

    def state():
        return {
            "state": task._state.name, "finished": task.has_finished,
            "drained": task.is_drained(), "pending_emits": task.has_pending_emits(),
            "active_tasks": len(op.get_active_tasks()), "logical_cpu": op.current_logical_usage().cpu,
            "running": op.metrics.num_tasks_running, "finished_tasks": op.metrics.num_tasks_finished,
            "failed_tasks": op.metrics.num_tasks_failed, "processed_inputs": op.metrics.num_task_inputs_processed,
            "submitted_tasks": op.metrics.num_tasks_submitted,
            "queue_count": len(topology[op].output_queue), "queue_bytes": topology[op].output_queue_bytes(),
            "live_bytes": counter.get_object_store_memory_usage(op.id),
            "outputs": len(outputs), "done_callbacks": len(completions),
            "block_refs_seen": len(block_refs), "metadata_refs_seen": len(meta_refs),
            "errors_recorded": sum(tick_errors),
        }

    try:
        op.add_input(inputs[0], 0)
        task = op.get_active_tasks()[0]
        assert type(task) is DataOpTask and not task.is_lineage_tracked
        gen = task.get_waitable()
        original_output, original_done = task._output_ready_callback, task._task_done_callback
        original_block, original_meta = task._block_ready_callback, task._metadata_ready_callback

        def observe_output(bundle):
            original_output(bundle)
            outputs.append(bundle)
            record("output_emitted", refs=[r.hex() for r in bundle.block_refs])

        def observe_done(error, worker_stats, driver_stats):
            completions.append(error)
            record("done_callback_enter", error_type=type(error).__name__, error=str(error),
                   state=task._state.name, worker_stats_is_none=worker_stats is None,
                   driver_stats_is_none=driver_stats is None)
            original_done(error, worker_stats, driver_stats)
            record("done_callback_return", logical_cpu=op.current_logical_usage().cpu,
                   active_tasks=len(op.get_active_tasks()))

        def observe_block(ref):
            original_block(ref)
            block_refs.append(ref)
            record("block_or_error_ref_read", ref=ref.hex())

        def observe_meta(ref):
            original_meta(ref)
            meta_refs.append(ref)
            record("metadata_ref_read", ref=ref.hex())

        task._output_ready_callback, task._task_done_callback = observe_output, observe_done
        task._block_ready_callback, task._metadata_ready_callback = observe_block, observe_meta
        record("submitted", revision=source["sha"], mode=mode,
               fetcher=type(fetcher).__name__, lineage_tracked=task.is_lineage_tracked)

        def queued():
            assert tick() == 0
            return len(topology[op].output_queue) == 1

        wait_for_condition(queued, timeout=30, retry_interval_ms=10)
        assert len(outputs) == len(block_refs) == len(meta_refs) == 1
        assert not task.has_pending_emits() and not task.has_finished
        bundle = topology[op].output_queue._get_queue_for(None).peek_next()
        assert bundle.block_refs == outputs[0].block_refs
        expected = {"id": [42]}
        assert BlockAccessor.for_block(ray.get(bundle.block_refs[0])).to_arrow().to_pydict() == expected
        initial = state()
        assert initial["logical_cpu"] == initial["running"] == initial["submitted_tasks"] == 1
        assert initial["queue_bytes"] > 0 and initial["live_bytes"] > 0
        wait_for_condition(lambda: ray.get(release_failure.cur_num_waiters.remote()) == 1,
                           timeout=30, retry_interval_ms=10)
        record("valid_output_queued_producer_parked", **initial)

        budget.value = 0
        assert tick() == 0
        assert not ray.wait([gen.completed()], timeout=0, fetch_local=False)[0]
        record("zero_budget_before_failure", **state())
        ray.get(release_failure.send.remote())
        # Body failure is a stream item; static completion success must not be
        # mistaken for DataOpTask success.
        terminal_value = ray.get(gen.completed(), timeout=30)
        assert terminal_value is None
        assert ray.wait([gen], timeout=30, fetch_local=False)[0] == [gen]
        assert not gen._stream_exhausted()
        record("core_completed_with_unread_error", terminal_value=terminal_value,
               stream_exhausted=gen._stream_exhausted(), **state())

        for index in range(5):
            assert tick() == 0
            assert state() == initial
            assert not gen._stream_exhausted() and task.task_error is None
            record("zero_budget_tick", tick=index, **state())

        budget.value = 1

        def finished():
            tick()
            return task.has_finished

        wait_for_condition(finished, timeout=30, retry_interval_ms=10)
        assert task.has_finished and not task.is_drained()
        assert len(completions) == 1
        error = completions[0]
        assert isinstance(error, RayTaskError) and isinstance(error, ValueError)
        assert ERROR_MARKER in str(error) and not isinstance(error, ObjectLostError)
        assert error is task.task_error
        assert len(outputs) == 1 and len(block_refs) == 2 and len(meta_refs) == 1
        assert task.pending_block_ref == block_refs[-1]
        assert block_refs[-1] != gen.completed()
        with pytest.raises(ValueError, match=ERROR_MARKER):
            ray.get(task.pending_block_ref, timeout=10)
        assert sum(tick_errors) == 1
        final = state()
        assert final["logical_cpu"] == final["active_tasks"] == final["running"] == 0
        assert final["finished_tasks"] == final["failed_tasks"] == final["processed_inputs"] == 1
        assert final["submitted_tasks"] == 1 and final["queue_count"] == 1
        assert final["queue_bytes"] == initial["queue_bytes"]
        assert final["live_bytes"] == initial["live_bytes"]
        record("failure_accounted_after_budget_resumed", **final)

        budget.value = 0
        for _ in range(3):
            assert tick() == 0
            assert task.on_data_ready(0, fetcher) == 0
            fetcher.submit("post_finished_observation", [task])
            assert fetcher.emit_ready_and_fire_done_callbacks() == []
            assert state() == final
        record("post_completion_calls_no_duplicates", **state())

        consumed = topology[op].output_queue.pop()
        assert consumed.block_refs == bundle.block_refs
        actual = BlockAccessor.for_block(ray.get(consumed.block_refs[0])).to_arrow().to_pydict()
        assert actual == expected and len(topology[op].output_queue) == 0
        record("downstream_consumed_after_failure", rows=actual, **state())
        summary = {
            "revision": source["sha"], "mode": mode, "valid_output_count": len(outputs),
            "done_callback_count": len(completions), "executor_error_count": sum(tick_errors),
            "final_accounting": final, "rows": actual, "terminal_returned_none": terminal_value is None,
            "error_ref_differs_from_terminal": block_refs[-1] != gen.completed(),
        }
        (RUN / f"{mode}-result.json").write_text(json.dumps(summary, indent=2) + "\n")
    finally:
        ray.get(release_failure.send.remote(), timeout=10)
        fetcher.stop()
        op.shutdown(timer=Timer())
        source_op.shutdown(timer=Timer())
