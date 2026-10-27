"""A scheduling-quality expectation, not an assertion of a documented completeness contract."""
import run_experiments as experiment


def test_candidate_a_complete_one_worker_plan():
    raw, options, request, requests = experiment.fixture('A')
    experiment.preflight('A', raw, options, request, requests)
    reply = experiment.sched.ResourceDemandScheduler().schedule(request)
    print('ACTUAL_REPLY', experiment.plain(reply))
    assert not reply.infeasible_resource_requests
    assert {r.instance_type: r.count for r in reply.to_launch} == {'L': 1}
