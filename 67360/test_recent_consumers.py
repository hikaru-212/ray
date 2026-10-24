# Portable derivative: only the output directory differs from the archived test.
"""Stage A O4/O5/O7/O8/O9: small, predefined, observable supplemental checks."""
import json
import logging
import os
from pathlib import Path
from types import SimpleNamespace

import pytest
from ray.autoscaler.v2.instance_manager import common, reconciler
from ray.autoscaler.v2.instance_manager.config import InstanceReconcileConfig
from ray.autoscaler.v2.instance_manager.instance_manager import InstanceManager
from ray.autoscaler.v2.instance_manager.instance_storage import InstanceStorage
from ray.autoscaler.v2.instance_manager.storage import InMemoryStorage
from ray.autoscaler.v2.instance_manager.reconciler import Reconciler
from ray.autoscaler.v2.instance_manager.node_provider import CloudInstance, LaunchNodeError
from ray.core.generated.autoscaler_pb2 import AutoscalingState
from ray.core.generated.instance_manager_pb2 import (
    Instance, InstanceUpdateEvent, UpdateInstanceManagerStateRequest, StatusCode, NodeKind,
)

ROOT = Path(os.environ["RAY_EVIDENCE_OUTPUT"])
VARIANT = os.environ['AUDIT_VARIANT']
S = 1_000_000_000
HAS_SUMMARY = 'status_transition_counts' in Instance.DESCRIPTOR.fields_by_name
assert HAS_SUMMARY == (VARIANT == 'head')


def record(name, value):
    (ROOT/'artifacts'/f'{VARIANT}-{name}.json').write_text(json.dumps(value, indent=2))
    print('OBSERVATION', name, json.dumps(value, sort_keys=True), flush=True)


class Driver:
    def __init__(self, monkeypatch):
        self.now = S
        self.ledger = {}
        self.notifications = 0
        monkeypatch.setattr(common, 'MAX_STATUS_HISTORY_LENGTH', 10)
        monkeypatch.setattr(common.time, 'time_ns', lambda: self.now)
        monkeypatch.setattr(reconciler.uuid, 'uuid4', lambda: 'fixed-request')
        self.backend = InMemoryStorage()
        self.storage = InstanceStorage('stage-c', self.backend)
        self.manager = InstanceManager(self.storage, [self])
        self.config = SimpleNamespace(
            get_upscaling_speed=lambda: 1.0,
            get_max_concurrent_launches=lambda: 1,
            get_provider_instance_type=lambda t: 'provider-'+t,
        )

    def notify(self, events):
        self.notifications += len(events)
        for e in events:
            self.ledger.setdefault(e.instance_id, []).append(
                (e.new_instance_status, self.now, e.details))

    def get(self, iid='A'):
        return self.storage.get_instances()[0][iid]

    def update(self, iid, status, new=False, details='fixed'):
        event = InstanceUpdateEvent(instance_id=iid, instance_type='type-1',
                                    new_instance_status=status, upsert=new, details=details)
        if status == Instance.REQUESTED:
            event.launch_request_id = 'request-'+iid
        _, version = self.storage.get_instances()
        result = self.manager.update_instance_manager_state(
            UpdateInstanceManagerStateRequest(expected_version=version, updates=[event]))
        assert result.status.code == StatusCode.OK

    def launch(self):
        Reconciler._handle_instances_launch(self.manager, self.config)

    def timeout(self):
        Reconciler._handle_stuck_instances(
            self.manager, InstanceReconcileConfig(request_status_timeout_s=1,
                max_num_retry_request_to_allocate=1000,
                transient_status_warn_interval_s=10**9), logging.getLogger('stage-c'))

    def state(self):
        state = AutoscalingState()
        Reconciler._fill_autoscaling_state(self.manager, state, self.config)
        return state

    def check_tail_and_size(self, iid='A'):
        i = self.get(iid)
        tail = [(t.instance_status, t.timestamp_ns, t.details) for t in i.status_history]
        assert [(s,t) for s,t,_ in tail] == [(s,t) for s,t,_ in self.ledger[iid][-10:]]
        # Inherited manager omission, proven before trimming in round1:
        # creation forwards details, subsequent set_status calls do not.
        expected_details = ([self.ledger[iid][0][2]] + ['']*(len(self.ledger[iid])-1))[-10:]
        assert [detail for _,_,detail in tail] == expected_details
        assert len(tail) <= 10
        # Clear fixed metadata/history to measure only the new protobuf fields.
        only = Instance()
        visited = {status for status, _, _ in self.ledger[iid]}
        if HAS_SUMMARY:
            only.status_transition_counts.update(i.status_transition_counts)
            only.first_status_transition_times_ns.update(i.first_status_transition_times_ns)
            assert set(i.status_transition_counts) == visited
            assert set(i.first_status_transition_times_ns) == visited
            for status in visited:
                events = [t for t in self.ledger[iid] if t[0] == status]
                assert i.status_transition_counts[status] == len(events)
                assert i.first_status_transition_times_ns[status] == min(t[1] for t in events)
        # Positive enum keys, uint64 count, int64 timestamp, map tags/lengths:
        # 48 bytes per status safely exceeds their maximum legal wire sizes.
        assert only.ByteSize() <= 48*len(visited)
        return {'wire_bytes': i.ByteSize(), 'summary_bytes': only.ByteSize(),
                'history_length':len(tail), 'visited_statuses':len(visited)}


@pytest.mark.parametrize('outcome', ['failed', 'allocated'])
def test_recent_consumers_and_bounded_summaries(monkeypatch, outcome):
    d = Driver(monkeypatch)
    d.update('A', Instance.QUEUED, new=True)
    samples = []
    first_requested = None
    for attempt in range(1, 102):
        d.now += S
        d.launch()
        assert d.get().status == Instance.REQUESTED
        latest_requested = d.now
        first_requested = first_requested or latest_requested
        measurement = d.check_tail_and_size()
        state = d.state()
        assert len(state.pending_instance_requests) == 1
        assert state.pending_instance_requests[0].request_ts == latest_requested//S
        if attempt in (6, 51, 101):
            samples.append({'attempt':attempt, **measurement})
        if attempt < 101:
            d.now += 2*S
            d.timeout()
            assert d.get().status == Instance.QUEUED
            d.check_tail_and_size()
            assert d.state().pending_instance_requests[0].request_ts == latest_requested//S
    assert latest_requested != first_requested
    assert all(t.timestamp_ns != first_requested for t in d.get().status_history)
    # A new manager/backend is seeded with the actual serialized state, not a
    # manually edited history or constructed summary. The branch changes state
    # only through real production handlers / legal manager updates.
    payload = d.get().SerializeToString(deterministic=True)
    d.backend = InMemoryStorage()
    d.storage = InstanceStorage('stage-c-restored', d.backend)
    assert d.storage.upsert_instance(Instance.FromString(payload)).success
    d.manager = InstanceManager(d.storage, [d])
    d.check_tail_and_size()
    d.now += S
    if outcome == 'failed':
        error = LaunchNodeError(node_type='type-1', count=1,
            request_id=d.get().launch_request_id, timestamp_ns=d.now, details='capacity')
        Reconciler._handle_cloud_instance_allocation(d.manager, {}, [error])
        assert d.get().status == Instance.ALLOCATION_FAILED
        failed = d.state().failed_instance_requests
        assert len(failed) == 1
        assert failed[0].start_ts == latest_requested//S
        assert failed[0].failed_ts == d.now//S
        assert failed[0].reason == ''  # inherited manager omission; not a correctness claim
        assert failed[0].count == 1
        observation = {'start_ts':failed[0].start_ts, 'failed_ts':failed[0].failed_ts,
                       'reason':failed[0].reason, 'event_reason':d.ledger['A'][-1][2],
                       'intended_reason':'launch failed with '+str(error)}
    else:
        cloud = CloudInstance(cloud_instance_id='cloud-1', node_type='type-1',
                              node_kind=NodeKind.WORKER, is_running=True)
        Reconciler._handle_cloud_instance_allocation(d.manager, {'cloud-1':cloud}, [])
        assert d.get().status == Instance.ALLOCATED
        assert d.state().pending_instances[0].details == ''  # inherited omission
        d.now += S
        d.update('A', Instance.RAY_INSTALLING, details='latest-install-detail')
        pending = d.state().pending_instances
        assert len(pending) == 1 and pending[0].details == ''  # inherited omission
        observation = {'instance_id':pending[0].instance_id, 'details':pending[0].details,
                       'event_details':d.ledger['A'][-1][2], 'intended_details':'latest-install-detail'}
    final = d.check_tail_and_size()
    record('recent-'+outcome, {'samples':samples, 'final':final,
           'first_requested_ts':first_requested//S, 'latest_requested_ts':latest_requested//S,
           'committed_events':len(d.ledger['A']), 'observed':observation})


def test_gc_bounds_churn_and_defers_conflicting_delete(monkeypatch):
    d = Driver(monkeypatch)
    config = InstanceReconcileConfig(terminated_instance_gc_retention_s=3)
    sizes = []
    race = {}
    for tick in range(20):
        d.now = (100+tick)*S
        dead, failed = f'T{tick:04}', f'F{tick:04}'
        d.update(dead, Instance.QUEUED, new=True)
        d.update(dead, Instance.TERMINATED)
        d.update(failed, Instance.QUEUED, new=True)
        d.update(failed, Instance.REQUESTED)
        d.update(failed, Instance.ALLOCATION_FAILED, details='capacity')
        # State reporting precedes collection, as in the production step.
        assert any(r.failed_ts == d.now//S for r in d.state().failed_instance_requests)
        before, _ = d.storage.get_instances()
        before_bytes = {iid:i.SerializeToString(deterministic=True) for iid,i in before.items()}
        notices = d.notifications
        if tick == 3:
            # The oldest pair expires exactly now; 1ns earlier it must survive.
            d.now -= 1
            Reconciler._gc_terminated_instances(d.manager, config, [])
            assert set(d.storage.get_instances()[0]) == set(before)
            d.now += 1
        if tick == 10:
            original = d.backend.batch_update
            armed = [True]
            version_before = d.backend.get_version()
            def interleave(table, mutation=None, deletion=None, expected_version=None):
                if deletion and armed[0]:
                    armed[0] = False
                    assert original('unrelated', {'writer':'v'}, {}, expected_version).success
                return original(table, mutation, deletion, expected_version)
            monkeypatch.setattr(d.backend, 'batch_update', interleave)
            Reconciler._gc_terminated_instances(d.manager, config, [])
            after = d.storage.get_instances()[0]
            assert {iid:i.SerializeToString(deterministic=True) for iid,i in after.items()} == before_bytes
            assert d.notifications == notices
            assert d.backend.get_version() == version_before+1
            race = {'version_before':version_before, 'version_after_rejection':d.backend.get_version(),
                    'preserved_ids':sorted(after), 'notifications_after_rejection':d.notifications-notices}
        # On the race tick this is the next GC call, with fresh state/version.
        Reconciler._gc_terminated_instances(d.manager, config, [])
        after = d.storage.get_instances()[0]
        expected = {f'{kind}{t:04}' for kind in ('T','F') for t in range(max(0,tick-2),tick+1)}
        assert set(after) == expected
        assert d.notifications == notices
        assert len(after) == 2*min(tick+1, 3)
        for i in after.values():
            assert i.status in (Instance.TERMINATED, Instance.ALLOCATION_FAILED)
        sizes.append({'tick':tick, 'retained':len(after),
                      'total_wire_bytes':sum(i.ByteSize() for i in after.values())})
        if tick == 10:
            race['ids_after_retry'] = sorted(after)
            race['version_after_retry'] = d.backend.get_version()
    record('gc-churn', {'samples':sizes, 'race':race, 'created_terminal_records':40,
                       'maximum_retained_after_gc':max(s['retained'] for s in sizes),
                       'exact_boundary_checked':True})
