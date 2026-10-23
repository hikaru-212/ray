"""Probe the user's broad invariant without assuming all-NaN policy is resolved.

Explicit-only diagnostic: expected FAIL on BOTH parent and PR (RED/RED).
This file is not collected by default pytest discovery (probe_ prefix).
"""
import json
import pytest
import test_repeater as evidence

@pytest.mark.parametrize('case', ['failure_missing_nan'])
@pytest.mark.parametrize('backend', ['hyperopt'])
def test_same_outcomes_same_aggregate(case, backend, tmp_path):
    rows = [evidence.replay(backend, evidence.CASES[case][0], order, tmp_path / ''.join(map(str, order))) for order in evidence.ORDERS]
    scores = {json.dumps(evidence.encode(row['scores_in_suggestion_order'])) for row in rows}
    assert len(scores) == 1, 'Score content must stay fixed when completion order changes'
    signatures = {json.dumps(evidence.encode({k: row[k] for k in ('mean', 'error', 'callback_counts', 'state')}), sort_keys=True) for row in rows}
    assert len(signatures) == 1, f'{case}: same outcomes produced order-dependent aggregate states: {sorted(signatures)}'
