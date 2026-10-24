# Portable derivative: only the output directory differs from the archived test.
"""Independent observable-behavior tests; no changes to either source snapshot.
Hypotheses: bounded-history retry exhaustion, ordering non-equivalence,
finite post-legacy retry budget, and rejected-update isolation.
"""
import hashlib
import json
import logging
import os
from pathlib import Path
from types import SimpleNamespace

import pytest

from ray.autoscaler.v2.instance_manager import common
from ray.autoscaler.v2.instance_manager.config import InstanceReconcileConfig
from ray.autoscaler.v2.instance_manager.instance_manager import InstanceManager
from ray.autoscaler.v2.instance_manager.instance_storage import InstanceStorage
from ray.autoscaler.v2.instance_manager.storage import InMemoryStorage
from ray.autoscaler.v2.instance_manager.reconciler import Reconciler
from ray.autoscaler.v2.instance_manager.node_provider import CloudInstance
from ray.core.generated.instance_manager_pb2 import (
    Instance, InstanceUpdateEvent, NodeKind, StatusCode,
    UpdateInstanceManagerStateRequest,
)

E = Path(os.environ["RAY_EVIDENCE_OUTPUT"])
VARIANT = os.environ["EVIDENCE_VARIANT"]
S = 1_000_000_000
LOG = logging.getLogger("independent-evidence")
HAS_SUMMARY = "status_transition_counts" in Instance.DESCRIPTOR.fields_by_name
assert HAS_SUMMARY == (VARIANT == "head")


def record(name, data):
    (E/"artifacts"/f"{VARIANT}-{name}.json").write_text(json.dumps(data, indent=2))
    print("OBSERVATION", name, json.dumps(data, sort_keys=True), flush=True)


def image(instance):
    return {
        "status": Instance.InstanceStatus.Name(instance.status),
        "version": instance.version,
        "history": [[t.instance_status, t.timestamp_ns] for t in instance.status_history],
        "counts": dict(instance.status_transition_counts) if HAS_SUMMARY else None,
        "first_times": dict(instance.first_status_transition_times_ns) if HAS_SUMMARY else None,
    }


class Subscriber:
    def __init__(self):
        self.events = []

    def notify(self, events):
        self.events.extend(InstanceUpdateEvent.FromString(e.SerializeToString()) for e in events)


class Driver:
    def __init__(self, monkeypatch, cap=10):
        self.now = S
        monkeypatch.setattr(common, "MAX_STATUS_HISTORY_LENGTH", cap)
        monkeypatch.setattr(common.time, "time_ns", lambda: self.now)
        self.backend = InMemoryStorage()
        self.storage = InstanceStorage("phase2", self.backend)
        self.subscriber = Subscriber()
        self.manager = InstanceManager(self.storage, [self.subscriber])

    def at(self, seconds):
        self.now = int(seconds*S)

    def get(self, iid="A"):
        return self.storage.get_instances()[0][iid]

    def update(self, iid, status, new=False):
        _, version = self.storage.get_instances()
        event = InstanceUpdateEvent(
            instance_id=iid, new_instance_status=status,
            instance_type="type-1", upsert=new,
        )
        if status == Instance.REQUESTED:
            event.launch_request_id = self.get(iid).launch_request_id or "request-"+iid
        reply = self.manager.update_instance_manager_state(
            UpdateInstanceManagerStateRequest(expected_version=version, updates=[event])
        )
        assert reply.status.code == StatusCode.OK
        return reply

    def new(self, iid="A"):
        self.update(iid, Instance.QUEUED, new=True)

    def launch(self, slots=1):
        config = SimpleNamespace(
            get_upscaling_speed=lambda: 1.0,
            get_max_concurrent_launches=lambda: slots,
        )
        Reconciler._handle_instances_launch(self.manager, config)

    def timeout(self, retries, timeout_s=1):
        Reconciler._handle_stuck_instances(
            self.manager,
            InstanceReconcileConfig(
                request_status_timeout_s=timeout_s,
                max_num_retry_request_to_allocate=retries,
                transient_status_warn_interval_s=10**9,
            ),
            LOG,
        )


@pytest.mark.parametrize("cap,retries", [(10, 0), (10, 3), (10, 5), (200, 100)])
def test_retry_exhaustion_through_manager(monkeypatch, cap, retries):
    d = Driver(monkeypatch, cap)
    d.new()
    attempts = []
    terminal_attempt = None
    for attempt in range(1, retries+7):
        assert d.get().status == Instance.QUEUED
        d.now += S
        d.launch()
        requested = d.get()
        assert requested.status == Instance.REQUESTED
        assert len(requested.status_history) <= cap
        requested_at = d.now
        version = d.backend.get_version()
        events = len(d.subscriber.events)
        # Exact timeout boundary must not trigger a transition.
        d.now = requested_at+S
        d.timeout(retries)
        assert d.backend.get_version() == version
        assert len(d.subscriber.events) == events
        assert d.get().status == Instance.REQUESTED
        # One nanosecond beyond the boundary must trigger retry/failure.
        d.now += 1
        d.timeout(retries)
        after = d.get()
        attempts.append({"attempt":attempt, **image(after)})
        if after.status == Instance.ALLOCATION_FAILED:
            terminal_attempt = attempt
            break
        assert after.status == Instance.QUEUED
    actual_requests = sum(e.new_instance_status == Instance.REQUESTED for e in d.subscriber.events)
    record(f"retry-cap{cap}-limit{retries}", {
        "cap":cap, "retry_limit":retries, "expected_failure_attempt":retries+1,
        "terminal_attempt":terminal_attempt, "observed_requests":actual_requests,
        "exact_timeout_boundary_unchanged":True, "trace":attempts,
    })
    # Same correctness assertion on parent and HEAD: expected parent failures
    # must remain real failures in the raw pytest log.
    assert terminal_attempt == retries+1
    assert actual_requests == retries+1


def make_parent_legacy(monkeypatch):
    assert VARIANT == "parent" and not HAS_SUMMARY
    d = Driver(monkeypatch, 10)
    d.new()
    for attempt in range(1, 13):
        d.now += S
        d.launch()
        assert d.get().status == Instance.REQUESTED
        if attempt != 12:
            d.now += 2*S
            # Same retry limit as migration: no configuration change confound.
            d.timeout(7)
            assert d.get().status == Instance.QUEUED
    instance = d.get()
    assert len(instance.status_history) == 10
    assert sum(t.instance_status == Instance.REQUESTED for t in instance.status_history) == 5
    payload = instance.SerializeToString(deterministic=True)
    (E/"artifacts"/"legacy-parent-truncated.pb").write_bytes(payload)
    meta = {"true_requests":12, "retained_requests":5, "cap":10, "retry_limit":7,
            "now_ns":d.now, "sha256":hashlib.sha256(payload).hexdigest(),
            "instance_id":instance.instance_id, **image(instance)}
    (E/"artifacts"/"legacy-parent-truncated.json").write_text(json.dumps(meta, indent=2))
    print("LEGACY_GENERATED", json.dumps(meta), flush=True)


def test_legacy_migration_eventually_exhausts(monkeypatch):
    d = Driver(monkeypatch, 10)
    meta = json.loads((E/"artifacts"/"legacy-parent-truncated.json").read_text())
    assert meta["retry_limit"] == 7
    payload = (E/"artifacts"/"legacy-parent-truncated.pb").read_bytes()
    assert hashlib.sha256(payload).hexdigest() == meta["sha256"]
    instance = Instance.FromString(payload)
    assert instance.instance_id == "A"
    assert d.storage.upsert_instance(instance).success
    if HAS_SUMMARY:
        assert not d.get().status_transition_counts
        assert not d.get().first_status_transition_times_ns
    initial_first = {}
    for t in instance.status_history:
        initial_first[t.instance_status] = min(initial_first.get(t.instance_status, t.timestamp_ns), t.timestamp_ns)
    d.now = meta["now_ns"]+2*S
    trace = []
    terminal_new_requests = None
    for new_requests in range(0, 10):
        d.timeout(7)
        after_timeout = d.get()
        trace.append({"new_requests":new_requests, **image(after_timeout)})
        if HAS_SUMMARY:
            assert after_timeout.status_transition_counts[Instance.REQUESTED] == 5+new_requests
            for status, timestamp in initial_first.items():
                assert after_timeout.first_status_transition_times_ns[status] == timestamp
        if after_timeout.status == Instance.ALLOCATION_FAILED:
            terminal_new_requests = new_requests
            break
        assert after_timeout.status == Instance.QUEUED
        # Persisted manager read reparses the protobuf on every access.
        # Recreate the manager as well, ruling out utility-local state.
        d.manager = InstanceManager(d.storage, [d.subscriber])
        d.now += S
        d.launch()
        assert d.get().status == Instance.REQUESTED
        if HAS_SUMMARY:
            assert d.get().status_transition_counts[Instance.REQUESTED] == 6+new_requests
        d.now += 2*S
    record("legacy-migration", {
        "input_sha256":meta["sha256"], "true_requests_before":12,
        "known_requests_before":5, "retry_limit":7,
        "terminal_after_additional_requests":terminal_new_requests,
        "observed_additional_requests":sum(e.new_instance_status == Instance.REQUESTED for e in d.subscriber.events),
        "final_status":Instance.InstanceStatus.Name(d.get().status),
        "true_total_at_terminal":None if terminal_new_requests is None else 12+terminal_new_requests,
        "trace":trace,
    })
    assert terminal_new_requests == 3


@pytest.mark.parametrize("kind", ["queue_tie", "allocation_tie", "queue_rollback", "allocation_rollback", "queue_distinct_first", "allocation_distinct_first"])
def test_sort_characterization(monkeypatch, kind):
    d = Driver(monkeypatch)
    if kind == "queue_tie":
        d.at(1); d.new("A"); d.new("B")
        d.at(2); d.update("A", Instance.REQUESTED)
        d.at(3); d.update("A", Instance.QUEUED)
    elif kind == "allocation_tie":
        d.at(0); d.new("A"); d.new("B")
        d.at(1); d.update("A", Instance.REQUESTED); d.update("B", Instance.REQUESTED)
        d.at(2); d.update("A", Instance.QUEUED)
        d.at(3); d.update("A", Instance.REQUESTED)
    elif kind == "queue_rollback":
        d.at(100); d.new("A")
        d.at(200); d.update("A", Instance.REQUESTED)
        d.at(50); d.update("A", Instance.QUEUED)
        d.at(75); d.new("B")
    elif kind == "allocation_rollback":
        # Actual timeout and launch paths: A retries at 205; only B times out
        # at 301. Clock rolls back between B's requeue and its next request.
        d.at(90); d.new("A")
        d.at(100); d.launch(slots=2)
        d.at(190); d.new("B")
        d.at(200); d.launch(slots=2)
        d.at(201); d.timeout(10, timeout_s=100)
        assert d.get("A").status == Instance.QUEUED
        assert d.get("B").status == Instance.REQUESTED
        d.at(205); d.launch(slots=2)
        d.at(301); d.timeout(10, timeout_s=100)
        assert d.get("A").status == Instance.REQUESTED
        assert d.get("B").status == Instance.QUEUED
        d.at(50); d.launch(slots=2)
    else:
        d.at(1); d.new("A")
        if kind.startswith("allocation"):
            d.at(2); d.update("A", Instance.REQUESTED)
        d.at(3); d.new("B")
        if kind.startswith("allocation"):
            d.at(4); d.update("B", Instance.REQUESTED)
    before = {iid:image(d.get(iid)) for iid in ("A", "B")}
    d.subscriber.events.clear()
    if kind.startswith("queue"):
        rows, _ = d.storage.get_instances()
        candidates = [rows["A"], rows["B"]]
        chosen = Reconciler._compute_to_launch(candidates, [], [], 1.0, 1)
        selected = [x.instance_id for x in chosen["type-1"]]
        d.launch()
        actual = [e.instance_id for e in d.subscriber.events if e.new_instance_status == Instance.REQUESTED]
        assert actual == selected
    else:
        Reconciler._handle_cloud_instance_allocation(
            d.manager,
            {"cloud-1":CloudInstance("cloud-1", "type-1", True, NodeKind.WORKER)},
            [],
        )
        selected = [iid for iid in ("A", "B") if d.get(iid).status == Instance.ALLOCATED]
    expected = "A"
    if kind in ("queue_tie", "allocation_tie") and VARIANT == "parent":
        expected = "B"
    if kind == "allocation_rollback" and VARIANT == "head":
        expected = "B"
    record(kind, {"input_order":["A","B"], "before":before, "selected_ids":selected,
                  "expected_characterization":[expected]})
    # This is characterization, deliberately not a claim of correctness policy.
    assert selected == [expected]


@pytest.mark.parametrize("conflict", ["stale_at_entry", "race_at_commit"])
def test_rejected_update_isolation(monkeypatch, conflict):
    d = Driver(monkeypatch)
    d.new()
    for _ in range(6):
        d.now += S; d.update("A", Instance.REQUESTED)
        d.now += S; d.update("A", Instance.QUEUED)
    before = d.get()
    before_bytes = before.SerializeToString(deterministic=True)
    _, initial_version = d.storage.get_instances()
    d.subscriber.events.clear()
    request = UpdateInstanceManagerStateRequest(
        expected_version=initial_version-(conflict == "stale_at_entry"),
        updates=[InstanceUpdateEvent(instance_id="A", new_instance_status=Instance.REQUESTED,
                                     instance_type="type-1", launch_request_id="request-A")],
    )
    if conflict == "race_at_commit":
        original = d.backend.batch_update
        fired = False
        def race(table, mutation=None, deletion=None, expected_version=None):
            nonlocal fired
            if not fired:
                fired = True
                # Competing storage write after manager has already calculated
                # its new summaries, before the real atomic compare-and-swap.
                assert original("competing-writer", {"other":"value"}, {}, expected_version).success
            return original(table, mutation, deletion, expected_version)
        monkeypatch.setattr(d.backend, "batch_update", race)
    rejected = d.manager.update_instance_manager_state(request)
    assert rejected.status.code == StatusCode.VERSION_MISMATCH
    assert d.get().SerializeToString(deterministic=True) == before_bytes
    assert not d.subscriber.events
    version_after_rejection = d.backend.get_version()
    assert version_after_rejection == initial_version+(conflict == "race_at_commit")
    request.expected_version = version_after_rejection
    committed = d.manager.update_instance_manager_state(request)
    assert committed.status.code == StatusCode.OK
    after = d.get()
    assert after.status == Instance.REQUESTED
    assert len(d.subscriber.events) == 1
    assert d.backend.get_version() == version_after_rejection+1
    if HAS_SUMMARY:
        assert after.status_transition_counts[Instance.REQUESTED] == before.status_transition_counts[Instance.REQUESTED]+1 == 7
        assert dict(after.first_status_transition_times_ns) == dict(before.first_status_transition_times_ns)
    # Replaying REQUESTED -> REQUESTED is invalid; it must not commit.
    request.expected_version = d.backend.get_version()
    saved = after.SerializeToString(deterministic=True)
    with pytest.raises(AssertionError, match="Invalid status transition"):
        d.manager.update_instance_manager_state(request)
    assert d.get().SerializeToString(deterministic=True) == saved
    assert len(d.subscriber.events) == 1
    assert d.backend.get_version() == committed.version
    record("atomicity-"+conflict, {
        "before":image(before), "after":image(after),
        "rejected_status":int(rejected.status.code),
        "persisted_bytes_unchanged_after_rejection":True,
        "initial_storage_version":initial_version,
        "version_after_rejection":version_after_rejection,
        "final_storage_version":d.backend.get_version(),
        "notifications_after_rejection":0, "notifications_after_retry":1,
        "invalid_replay_unchanged":True,
    })
