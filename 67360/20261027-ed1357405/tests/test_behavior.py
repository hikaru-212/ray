"""Independent behavioral oracles; no production source patches."""
import inspect
import json
import logging
import os
from collections import Counter
from copy import deepcopy
from pathlib import Path
from unittest.mock import patch

import pytest
from ray.autoscaler.v2.instance_manager import common
from ray.autoscaler.v2.instance_manager.common import InstanceUtil
from ray.autoscaler.v2.instance_manager.config import InstanceReconcileConfig
from ray.autoscaler.v2.instance_manager.instance_manager import InstanceManager
from ray.autoscaler.v2.instance_manager.instance_storage import InstanceStorage
from ray.autoscaler.v2.instance_manager.storage import InMemoryStorage, StoreStatus
from ray.autoscaler.v2.instance_manager.node_provider import CloudInstance
from ray.autoscaler.v2.instance_manager.reconciler import Reconciler
from ray.core.generated.instance_manager_pb2 import (
    Instance, InstanceUpdateEvent, NodeKind, StatusCode,
    UpdateInstanceManagerStateRequest,
)

SECOND = 1_000_000_000
ROOT = Path(os.environ["AUDIT_ROOT"])
VARIANT = os.environ["AUDIT_VARIANT"]

def status_name(status):
    return Instance.InstanceStatus.Name(status)

class Subscriber:
    def __init__(self): self.events = []
    def notify(self, events): self.events.extend(deepcopy(list(events)))

class Config:
    def get_upscaling_speed(self): return 1.0
    def get_max_concurrent_launches(self): return 1

class Rig:
    def __init__(self, monkeypatch, record):
        self.record = record
        self.clock = 0
        monkeypatch.setattr("time.time_ns", lambda: self.clock)
        monkeypatch.setattr(common, "MAX_STATUS_HISTORY_LENGTH", 200)
        self.backend = InMemoryStorage()
        self.storage = InstanceStorage("independent-audit", self.backend)
        self.subscriber = Subscriber()
        self.im = InstanceManager(self.storage, [self.subscriber])
        self.trace = []
        self.monkeypatch = monkeypatch

    def cap(self, n): self.monkeypatch.setattr(common, "MAX_STATUS_HISTORY_LENGTH", n)
    def at(self, seconds): self.clock = int(seconds * SECOND)
    def advance(self, seconds=2): self.clock += int(seconds * SECOND)
    def all(self): return self.storage.get_instances()[0]
    def get(self, id="subject"): return self.all()[id]
    def event(self, id, status, details=""):
        return InstanceUpdateEvent(
            instance_id=id, new_instance_status=status, instance_type="type-1",
            launch_request_id=id+"-request", upsert=True,
            cloud_instance_id=id+"-cloud", node_kind=NodeKind.WORKER, details=details,
        )
    def emit(self, operation, **extra):
        instances, version = self.storage.get_instances()
        snap = {}
        for id, inst in instances.items():
            item = {
                "status":status_name(inst.status), "instance_version":inst.version,
                "history_length":len(inst.status_history),
                "visible_counts":dict(Counter(status_name(x.instance_status) for x in inst.status_history)),
                "history_first_ns":inst.status_history[0].timestamp_ns if inst.status_history else None,
                "history_last_ns":inst.status_history[-1].timestamp_ns if inst.status_history else None,
            }
            # Diagnostic only: do not call a lazy getter while observing a cold cache.
            if hasattr(self.im, "_status_summaries"):
                summary=self.im._status_summaries.get(id)
                item["summary"] = None if summary is None else {
                    "counts":{status_name(k):v for k,v in summary.counts.items()},
                    "first_ns":{status_name(k):v for k,v in summary.first_times_ns.items()},
                }
            elif hasattr(inst, "status_transition_counts"):
                item["summary"]={
                    "counts":{status_name(k):v for k,v in inst.status_transition_counts.items()},
                    "first_ns":{status_name(k):v for k,v in inst.first_status_transition_times_ns.items()},
                }
            snap[id]=item
        self.trace.append({"operation":operation,"clock_ns":self.clock,"storage_version":version,"instances":snap,**extra})
    def update_many(self, pairs, expected=-1, details=""):
        reply=self.im.update_instance_manager_state(UpdateInstanceManagerStateRequest(
            expected_version=expected, updates=[self.event(id,s,details) for id,s in pairs]))
        self.emit("update", requested=[(id,status_name(s)) for id,s in pairs],reply_code=reply.status.code)
        return reply
    def update(self, status, id="subject", expected=-1, details=""):
        return self.update_many([(id,status)],expected,details)
    def ok(self, status, id="subject", details=""):
        reply=self.update(status,id,details=details)
        assert reply.status.code == StatusCode.OK
        return self.get(id)
    def requested_commits(self, id="subject"):
        return sum(e.instance_id==id and e.new_instance_status==Instance.REQUESTED for e in self.subscriber.events)
    def stuck(self, limit):
        Reconciler._handle_stuck_instances(
            self.im,
            InstanceReconcileConfig(request_status_timeout_s=1,max_num_retry_request_to_allocate=limit),
            logging.getLogger("independent-audit"),
        )
        self.emit("reconciler_stuck", retry_limit=limit)
    def launch(self):
        Reconciler._handle_instances_launch(self.im, Config())
        self.emit("reconciler_launch")
    def allocate(self):
        Reconciler._handle_cloud_instance_allocation(
            self.im, {"cloud-1":CloudInstance("cloud-1","type-1",NodeKind.WORKER,True)}, [])
        self.emit("reconciler_allocate")
    def reconstruct(self):
        self.im=InstanceManager(self.storage,[self.subscriber])
        self.emit("reconstruct_manager")
    def decision(self, limit, id="subject"):
        # Only adapter needed for a changed internal signature; same decision oracle.
        fn=Reconciler._handle_stuck_requested_instance
        args=[self.get(id),1,limit]
        if "instance_manager" in inspect.signature(fn).parameters: args.insert(0,self.im)
        event=fn(*args)
        return None if event is None else status_name(event.new_instance_status)
    def seed_requests(self, count, finish_queued=True, id="subject"):
        self.ok(Instance.QUEUED,id)
        for n in range(count):
            self.advance()
            self.ok(Instance.REQUESTED,id)
            if n < count-1 or finish_queued:
                self.advance()
                self.ok(Instance.QUEUED,id)
    def bytes(self):
        return {k:v.SerializeToString().hex() for k,v in self.all().items()}
    def delete(self, expected=None):
        result=self.im.delete_instances(["subject"],expected_storage_version=expected)
        self.emit("delete", success=result.success, result_version=result.version)
        return result

@pytest.fixture
def rig(monkeypatch, request):
    request.node.audit_record={"nodeid":request.node.nodeid,"variant":VARIANT,"observations":{}}
    r=Rig(monkeypatch,request.node.audit_record)
    yield r
    path=ROOT/"artifacts"/"traces"/VARIANT/(request.node.name+".json")
    path.parent.mkdir(parents=True,exist_ok=True)
    path.write_text(json.dumps(r.trace,indent=2))
    request.node.audit_record["trace"]=str(path)
    request.node.audit_record["observations"]["committed_requested_events"]=r.requested_commits()

@pytest.mark.parametrize("cap",[10,200])
@pytest.mark.parametrize("limit",[3,100])
def test_retry_exact_boundary(rig, cap, limit):
    rig.cap(cap)
    rig.ok(Instance.QUEUED)
    outcomes=[]
    for attempt in range(1,limit+2):
        rig.advance()
        rig.ok(Instance.REQUESTED)
        assert rig.requested_commits()==attempt
        rig.advance()
        rig.stuck(limit)
        actual=rig.get().status
        outcomes.append(status_name(actual))
        rig.record["observations"].update(attempt=attempt,outcomes=outcomes,cap=cap,limit=limit)
        assert len(rig.get().status_history)<=cap
        assert actual == (Instance.QUEUED if attempt<=limit else Instance.ALLOCATION_FAILED)

@pytest.mark.parametrize("cap",[10,200])
def test_bounded_history_roundtrips(rig, cap):
    rig.cap(cap)
    rig.seed_requests(120)
    inst=Instance.FromString(rig.get().SerializeToString())
    rig.record["observations"].update(cap=cap,history_length=len(inst.status_history),requested_commits=120,visible_requests=sum(x.instance_status==Instance.REQUESTED for x in inst.status_history))
    assert len(inst.status_history)==cap
    assert inst.status_history[-1].timestamp_ns==rig.clock
    assert inst.status_history[0].timestamp_ns>0
    assert rig.requested_commits()==120

@pytest.mark.parametrize("cold",[False,True],ids=["warm","cold"])
@pytest.mark.parametrize("failure",["request_version","write_version","write_failure"])
def test_failed_batch_and_exactly_once_retry(rig, cold, failure):
    rig.cap(10)
    rig.seed_requests(2)
    rig.ok(Instance.QUEUED,"second")
    if cold: rig.reconstruct()
    before=rig.bytes()
    events=len(rig.subscriber.events)
    old_version=rig.backend.get_version()
    pairs=[("subject",Instance.REQUESTED),("second",Instance.REQUESTED),("new",Instance.QUEUED)]
    rig.advance()
    if failure=="request_version":
        reply=rig.update_many(pairs,expected=old_version+1)
        expected_code=StatusCode.VERSION_MISMATCH
    elif failure=="write_version":
        original=rig.backend.batch_update
        def competing_write(*args,**kwargs):
            # Actual CAS conflict: an unrelated table commits after the read.
            original("unrelated-audit-table",{"sentinel":b"concurrent-commit"})
            return original(*args,**kwargs)
        with patch.object(rig.backend,"batch_update",side_effect=competing_write):
            reply=rig.update_many(pairs,expected=old_version)
        assert rig.backend.get_version()==old_version+1
        expected_code=StatusCode.VERSION_MISMATCH
    else:
        with patch.object(rig.storage,"batch_upsert_instances",return_value=StoreStatus(False,old_version)):
            reply=rig.update_many(pairs,expected=old_version)
        expected_code=StatusCode.UNKNOWN_ERRORS
    assert reply.status.code==expected_code
    assert rig.bytes()==before
    assert len(rig.subscriber.events)==events
    assert "new" not in rig.all()
    if hasattr(rig.im,"_status_summaries"):
        cache=rig.im._status_summaries
        assert "new" not in cache
        if cold and failure=="request_version": assert not cache
        else: assert cache["subject"].counts.get(Instance.REQUESTED,0)==2
        rig.record["observations"]["cache_ids_after_failure"]=sorted(cache)
    # A failed candidate must not make committed request #3 exceed limit=3.
    reply=rig.update_many(pairs,expected=rig.backend.get_version())
    assert reply.status.code==StatusCode.OK
    assert rig.requested_commits()==3
    assert rig.requested_commits("second")==1
    rig.advance()
    rig.stuck(3)
    assert rig.get().status==Instance.QUEUED
    rig.advance()
    rig.ok(Instance.REQUESTED)
    rig.advance()
    rig.stuck(3)
    assert rig.get().status==Instance.ALLOCATION_FAILED
    assert rig.requested_commits()==4
    rig.record["observations"].update(failure=failure,cold=cold,failed_candidate_persisted=False,request3="QUEUED",request4="ALLOCATION_FAILED")

def test_lazy_seed_before_failed_commit_preserves_available_history(rig):
    rig.cap(1000)
    rig.seed_requests(100)
    assert len(rig.get().status_history)==201
    rig.reconstruct()
    rig.cap(10)
    before=rig.bytes()
    version=rig.backend.get_version()
    with patch.object(rig.storage,"batch_upsert_instances",return_value=StoreStatus(False,version)):
        assert rig.update(Instance.REQUESTED).status.code==StatusCode.UNKNOWN_ERRORS
    assert rig.bytes()==before
    if hasattr(rig.im,"_status_summaries"):
        assert rig.im._status_summaries["subject"].counts[Instance.REQUESTED]==100
    rig.advance()
    rig.ok(Instance.REQUESTED)
    assert len(rig.get().status_history)==10
    rig.advance()
    rig.stuck(100)
    rig.record["observations"].update(requested_commits=rig.requested_commits(),outcome=status_name(rig.get().status))
    assert rig.requested_commits()==101
    assert rig.get().status==Instance.ALLOCATION_FAILED

@pytest.mark.parametrize("failure",["version","unknown"])
def test_failed_deletion_retains_summary(rig,failure):
    rig.cap(10)
    rig.seed_requests(2)
    before=rig.bytes()
    if failure=="version":
        result=rig.delete(expected=0)
    else:
        with patch.object(rig.storage,"batch_delete_instances",return_value=StoreStatus(False,rig.backend.get_version())):
            result=rig.delete()
    assert not result.success
    assert rig.bytes()==before
    rig.advance()
    rig.ok(Instance.REQUESTED)
    rig.advance()
    rig.stuck(2)
    assert rig.get().status==Instance.ALLOCATION_FAILED

def test_delete_recreate_same_id_resets_counts_and_order(rig):
    rig.cap(10)
    rig.seed_requests(100)
    rig.advance()
    rig.ok(Instance.QUEUED,"older-survivor")
    old_requested=rig.requested_commits()
    assert rig.delete(expected=rig.backend.get_version()).success
    assert "subject" not in rig.all()
    if hasattr(rig.im,"_status_summaries"): assert "subject" not in rig.im._status_summaries
    rig.advance()
    recreated_time=rig.clock
    rig.ok(Instance.QUEUED)
    rig.launch()
    assert rig.get("older-survivor").status==Instance.REQUESTED
    assert rig.get().status==Instance.QUEUED
    rig.advance()
    rig.ok(Instance.REQUESTED)
    rig.advance()
    rig.stuck(1)
    assert rig.get().status==Instance.QUEUED
    rig.advance()
    rig.ok(Instance.REQUESTED)
    rig.advance()
    rig.stuck(1)
    assert rig.get().status==Instance.ALLOCATION_FAILED
    assert rig.requested_commits()-old_requested==2
    rig.record["observations"].update(recreated_time_ns=recreated_time,new_lifetime_requests=2,selected="older-survivor")

def ordered_pair(rig, kind, zero=False):
    if kind=="queued":
        rig.at(0 if zero else 1); rig.ok(Instance.QUEUED,"older")
        rig.at(2); rig.ok(Instance.QUEUED,"newer")
        for _ in range(100):
            rig.advance(); rig.ok(Instance.REQUESTED,"older")
            rig.advance(); rig.ok(Instance.QUEUED,"older")
        key_status=Instance.QUEUED
    else:
        rig.at(-1 if zero else 0); rig.ok(Instance.QUEUED,"older")
        rig.at(0 if zero else 1); rig.ok(Instance.REQUESTED,"older")
        rig.at(2); rig.ok(Instance.QUEUED,"newer")
        rig.at(3); rig.ok(Instance.REQUESTED,"newer")
        for _ in range(100):
            rig.advance(); rig.ok(Instance.QUEUED,"older")
            rig.advance(); rig.ok(Instance.REQUESTED,"older")
        key_status=Instance.REQUESTED
    original=0 if zero else SECOND
    assert all(x.timestamp_ns!=original or x.instance_status!=key_status for x in rig.get("older").status_history)
    if kind=="queued":
        rig.launch(); selected=[k for k,v in rig.all().items() if v.status==Instance.REQUESTED]
    else:
        rig.allocate(); selected=[k for k,v in rig.all().items() if v.status==Instance.ALLOCATED]
    rig.record["observations"].update(kind=kind,selected=selected,original_timestamp_ns=original,original_timestamp_trimmed=True)
    assert selected==["older"]

@pytest.mark.parametrize("cap",[10,200])
@pytest.mark.parametrize("kind",["queued","requested"])
def test_oldest_after_history_trimming(rig,cap,kind):
    rig.cap(cap)
    ordered_pair(rig,kind)

@pytest.mark.parametrize("kind",["queued","requested"])
def test_timestamp_zero_survives_trimming(rig,kind):
    rig.cap(10)
    ordered_pair(rig,kind,zero=True)

@pytest.mark.parametrize("kind",["queued","requested"])
@pytest.mark.parametrize("scenario",["ties","rollback"])
def test_ordering_characterization(rig,kind,scenario):
    # No total-order oracle for ties: report selection instead of inventing a contract.
    if kind=="queued":
        rig.at(10); rig.ok(Instance.QUEUED,"b")
        rig.at(10 if scenario=="ties" else 20); rig.ok(Instance.QUEUED,"a")
        if scenario=="ties":
            rig.at(11); rig.ok(Instance.REQUESTED,"b")
            rig.at(30); rig.ok(Instance.QUEUED,"b")
        rig.at(21); rig.ok(Instance.REQUESTED,"a")
        rig.at(20 if scenario=="ties" else 5); rig.ok(Instance.QUEUED,"a")
        rig.launch(); chosen=[k for k,v in rig.all().items() if v.status==Instance.REQUESTED]
    else:
        rig.at(0); rig.ok(Instance.QUEUED,"b")
        rig.at(10); rig.ok(Instance.REQUESTED,"b")
        rig.at(1); rig.ok(Instance.QUEUED,"a")
        rig.at(10 if scenario=="ties" else 20); rig.ok(Instance.REQUESTED,"a")
        if scenario=="ties":
            rig.at(11); rig.ok(Instance.QUEUED,"b")
            rig.at(30); rig.ok(Instance.REQUESTED,"b")
        rig.at(21); rig.ok(Instance.QUEUED,"a")
        rig.at(20 if scenario=="ties" else 5); rig.ok(Instance.REQUESTED,"a")
        rig.allocate(); chosen=[k for k,v in rig.all().items() if v.status==Instance.ALLOCATED]
    rig.record["observations"].update(kind=kind,scenario=scenario,selected=chosen)
    assert len(chosen)==1 and chosen[0] in {"a","b"}

def test_clock_rollback_timeout_boundary(rig):
    rig.at(0); rig.ok(Instance.QUEUED)
    rig.at(100); rig.ok(Instance.REQUESTED)
    rig.at(90); rig.stuck(1)
    assert rig.get().status==Instance.REQUESTED
    rig.at(101); rig.stuck(1)
    assert rig.get().status==Instance.REQUESTED
    rig.at(102); rig.stuck(1)
    assert rig.get().status==Instance.QUEUED

@pytest.mark.parametrize("cap",[10,200])
def test_reconstruction_component_boundary(rig,cap):
    rig.cap(cap)
    rig.seed_requests(101,finish_queued=False)
    rig.advance()
    before=rig.bytes()
    before_decision=rig.decision(100)
    rig.emit("before_reconstruction_decision",decision=before_decision)
    visible=sum(t.instance_status==Instance.REQUESTED for t in rig.get().status_history)
    rig.reconstruct()
    assert rig.bytes()==before
    after_decision=rig.decision(100)
    rig.emit("after_reconstruction_decision",decision=after_decision)
    rig.stuck(100)
    additional=0
    while rig.get().status==Instance.QUEUED and additional<102:
        rig.advance(); rig.ok(Instance.REQUESTED)
        additional+=1
        rig.advance(); rig.stuck(100)
    rig.record["observations"].update(cap=cap,before_decision=before_decision,after_decision=after_decision,retained_requests=visible,additional_requests=additional,final_status=status_name(rig.get().status),scope="component-only; no production reachability established")
    if before_decision=="ALLOCATION_FAILED" and after_decision=="ALLOCATION_FAILED":
        assert additional==0
    if before_decision=="ALLOCATION_FAILED" and after_decision=="QUEUED":
        assert additional==101-visible
        assert rig.get().status==Instance.ALLOCATION_FAILED
