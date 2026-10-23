"""Same assertions for exact parent and head. No implementation/version branching.

Outcomes are bound to suggestion index; ONLY callback arrival order changes.
Real SearchGenerator, Repeater, _TrialGroup, BayesOptSearch and HyperOptSearch.
The observer records arguments and delegates to the real downstream callback.
"""
import copy
import json
import math
from pathlib import Path
import sys
import warnings

import numpy as np
import pytest

import bootstrap
from ray.tune.search.repeater import Repeater
from ray.tune.search.search_generator import SearchGenerator
from ray.tune.search.bayesopt import BayesOptSearch
from ray.tune.search.hyperopt import HyperOptSearch
from ray.tune.utils.mock_trainable import MOCK_TRAINABLE_NAME
import hyperopt

FAIL = (None, True)
MISSING = ({'unrelated': 71}, False)
NAN = ({'score': float('nan')}, False)
def ok(value):
    return ({'score': value, 'unrelated': 'must not reach optimizer'}, False)

# Explicit expectations, not computed by calling np.nanmean again.
CASES = {
    'all_success': ((ok(1), ok(5), ok(9)), 5.0),
    'one_failure': ((ok(1), ok(5), FAIL), 3.0),
    'two_failures': ((ok(1), FAIL, FAIL), 1.0),
    'failure_missing_nan': ((FAIL, MISSING, NAN), float('nan')),
}
# Two fixed orders are sufficient to expose the original last-completion bug.
ORDERS = [(0, 1, 2), (2, 1, 0)]
BACKENDS = ['bayesopt', 'hyperopt']
PARAMETERS = [
    (case, order, backend)
    for case in ('one_failure', 'two_failures')
    for order in ORDERS
    for backend in BACKENDS
] + [
    ('all_success', ORDERS[0], backend) for backend in BACKENDS
] + [
    ('failure_missing_nan', order, 'hyperopt') for order in ORDERS
]

def encode(value):
    if isinstance(value, (float, np.floating)):
        if math.isnan(value):
            return 'NaN'
        if math.isinf(value):
            return 'Infinity' if value > 0 else '-Infinity'
        return float(value)
    if isinstance(value, dict):
        return {k: encode(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [encode(v) for v in value]
    if isinstance(value, np.generic):
        return value.item()
    return value

def record(row):
    with (bootstrap.ROOT / f'results-{bootstrap.VARIANT}.jsonl').open('a') as stream:
        stream.write(json.dumps(encode(row), sort_keys=True) + '\n')

def replay(backend, outcomes, order, tmp_path):
    assert backend in BACKENDS
    if backend == 'bayesopt':
        downstream = BayesOptSearch(space={'x': (0.0, 1.0)}, metric='score', mode='max', random_state=19, random_search_steps=0)
    else:
        downstream = HyperOptSearch(space={'x': hyperopt.hp.uniform('x', 0, 1)}, metric='score', mode='max', random_state_seed=19)
    calls = []
    original = downstream.on_trial_complete
    def observe(trial_id, result=None, **kwargs):
        calls.append({'id': trial_id, 'result': copy.deepcopy(result), 'kwargs': copy.deepcopy(kwargs), 'error': kwargs.get('error', False)})
        return original(trial_id, result=result, **kwargs)
    downstream.on_trial_complete = observe
    repeater = Repeater(downstream, repeat=3)
    algorithm = SearchGenerator(repeater)
    algorithm.add_configurations({'test': {'run': MOCK_TRAINABLE_NAME, 'num_samples': 3, 'storage_path': str(tmp_path)}})
    trials = [algorithm.next_trial() for _ in range(3)]
    ids = [t.trial_id for t in trials]
    assert len(set(ids)) == 3
    assert [t.config['__trial_index__'] for t in trials] == [0, 1, 2]
    assert len({t.config['x'] for t in trials}) == 1
    assert len(repeater._groups) == 1
    group = repeater._groups[0]
    assert group.primary_trial_id == ids[0]
    assert group.full() and not group.finished_reporting()
    ho_trial = downstream._get_hyperopt_trial(ids[0]) if backend == 'hyperopt' else None
    traced = set()
    def trace(frame, event, arg):
        file = frame.f_code.co_filename
        if event == 'line' and str(bootstrap.SOURCE / 'ray/tune/search') in file:
            traced.add((str(Path(file).relative_to(bootstrap.SOURCE)), frame.f_code.co_name, frame.f_lineno))
        return trace
    prefix_counts = []
    try:
        sys.settrace(trace)
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter('always')
            for index in order:
                result, error = outcomes[index]
                algorithm.on_trial_complete(ids[index], result=copy.deepcopy(result), error=error)
                prefix_counts.append(len(calls))
    finally:
        sys.settrace(None)
    assert prefix_counts == [0, 0, 1]
    assert len(calls) == 1 and calls[0]['id'] == ids[0]
    assert group.finished_reporting()
    assert set(calls[0]['result']) == {'score'}
    names = {name for _, name, _ in traced}
    assert {'on_trial_complete', 'report', 'scores', 'finished_reporting'} <= names
    assert any(file.endswith('repeater.py') and name == 'on_trial_complete' for file, name, _ in traced)
    assert any(file.endswith('search_generator.py') and name == 'on_trial_complete' for file, name, _ in traced)
    if backend == 'bayesopt':
        assert ids[0] not in downstream._live_trial_mapping
        assert any(file.endswith('bayesopt_search.py') and name == 'on_trial_complete' for file, name, _ in traced)
        state = {'targets': list(downstream.optimizer.space.target), 'buffer_count': len(downstream._buffered_trial_results)}
    elif backend == 'hyperopt':
        assert ids[0] not in downstream._live_trial_mapping
        assert any(file.endswith('hyperopt_search.py') and name == 'on_trial_complete' for file, name, _ in traced)
        state = {'job_state': ho_trial['state'], 'result': ho_trial['result'], 'error_metadata': bool(ho_trial['misc'].get('error')), 'visible_trials': len(downstream._hpopt_trials.trials)}
    return {'backend': backend, 'order': order, 'mean': calls[0]['result']['score'], 'error': calls[0]['error'], 'kwargs': calls[0]['kwargs'], 'callback_counts': prefix_counts, 'primary_id_correct': True, 'scores_in_suggestion_order': group.scores(), 'state': state, 'warnings': [{'category': w.category.__name__, 'message': str(w.message), 'file': w.filename, 'line': w.lineno} for w in caught], 'executed': sorted(traced)}

@pytest.mark.parametrize(
    'case,order,backend', PARAMETERS,
    ids=[f'{case}-{"".join(map(str, order))}-{backend}' for case, order, backend in PARAMETERS],
)
def test_completion_matrix(case, order, backend, tmp_path):
    outcomes, expected_mean = CASES[case]
    row = replay(backend, outcomes, order, tmp_path)
    row['case'] = case
    record(row)  # Evidence persists even when the shared invariant fails on parent.
    runtime_warnings = [w for w in row['warnings'] if w['category'] == 'RuntimeWarning']
    if math.isnan(expected_mean):
        assert math.isnan(row['mean'])
        assert row['error'] == outcomes[order[-1]][1]
        assert [(w['category'], w['message']) for w in runtime_warnings] == [('RuntimeWarning', 'Mean of empty slice')]
    else:
        assert row['mean'] == expected_mean
        assert runtime_warnings == []
        assert row['error'] is False, 'finite aggregate must survive regardless of final repeat error'
    if backend == 'bayesopt':
        expected_targets = [] if math.isnan(expected_mean) else [expected_mean]
        assert row['state']['targets'] == expected_targets
    elif backend == 'hyperopt':
        expected_error = math.isnan(expected_mean) and outcomes[order[-1]][1]
        assert row['state']['job_state'] == (hyperopt.base.JOB_STATE_ERROR if expected_error else hyperopt.base.JOB_STATE_DONE)
        assert row['state']['error_metadata'] == expected_error
        if not expected_error:
            assert row['state']['result']['status'] == 'ok'
            loss = row['state']['result']['loss']
            assert math.isnan(loss) if math.isnan(expected_mean) else loss == -expected_mean
