"""Follow-up: failed operations after committed history has already been truncated."""
from unittest.mock import patch
import pytest
from test_behavior import rig, Instance, StatusCode, StoreStatus, status_name

@pytest.mark.parametrize("cap",[10,200])
@pytest.mark.parametrize("failure",["write_version","write_failure","delete_version","delete_failure"])
def test_failure_preserves_full_lifetime_after_truncation(rig,cap,failure):
    rig.cap(cap)
    rig.seed_requests(120)
    assert len(rig.get().status_history)==cap
    before=rig.bytes()
    events=len(rig.subscriber.events)
    version=rig.backend.get_version()
    if failure=="write_version":
        original=rig.backend.batch_update
        def competing_write(*args,**kwargs):
            original("unrelated-audit-table",{"sentinel":b"commit"})
            return original(*args,**kwargs)
        with patch.object(rig.backend,"batch_update",side_effect=competing_write):
            reply=rig.update(Instance.REQUESTED,expected=version)
        assert reply.status.code==StatusCode.VERSION_MISMATCH
    elif failure=="write_failure":
        with patch.object(rig.storage,"batch_upsert_instances",return_value=StoreStatus(False,version)):
            assert rig.update(Instance.REQUESTED).status.code==StatusCode.UNKNOWN_ERRORS
    elif failure=="delete_version":
        assert not rig.delete(expected=0).success
    else:
        with patch.object(rig.storage,"batch_delete_instances",return_value=StoreStatus(False,version)):
            assert not rig.delete().success
    assert rig.bytes()==before
    assert len(rig.subscriber.events)==events
    rig.advance(); rig.ok(Instance.REQUESTED)
    rig.advance(); rig.stuck(121)
    first=status_name(rig.get().status)
    rig.record["observations"].update(cap=cap,failure=failure,request121=first)
    assert rig.requested_commits()==121
    assert rig.get().status==Instance.QUEUED
    rig.advance(); rig.ok(Instance.REQUESTED)
    rig.advance(); rig.stuck(121)
    rig.record["observations"]["request122"]=status_name(rig.get().status)
    assert rig.requested_commits()==122
    assert rig.get().status==Instance.ALLOCATION_FAILED
