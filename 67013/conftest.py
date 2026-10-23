import bootstrap  # Install exact-source finder before importing any Tune modules.
import hashlib
import importlib
import importlib.metadata
import json
import os
from pathlib import Path
import subprocess
import sys

import pytest

ROOT = bootstrap.ROOT
REPO = str(bootstrap.REPO)
COMMITS = bootstrap.COMMITS

@pytest.fixture(scope='session', autouse=True)
def source_audit():
    audit = {'variant': bootstrap.VARIANT, 'commit': COMMITS[bootstrap.VARIANT], 'python': sys.version, 'modules': {}}
    for name in ('search.searcher', 'search.repeater', 'search.search_generator', 'search.bayesopt.bayesopt_search', 'search.hyperopt.hyperopt_search', 'search.util', 'utils.util', 'experiment.trial'):
        module = importlib.import_module('ray.tune.' + name)
        path = Path(module.__file__).resolve()
        relative = path.relative_to(bootstrap.SOURCE)
        expected = subprocess.check_output(['git', '-C', REPO, 'show', f'{audit["commit"]}:python/{relative}'])
        assert path.read_bytes() == expected
        audit['modules'][module.__name__] = {'path': str(relative), 'sha256': hashlib.sha256(expected).hexdigest()}
    audit['packages'] = {p: importlib.metadata.version(p) for p in ('ray', 'numpy', 'pytest', 'bayesian-optimization', 'hyperopt', 'scipy', 'scikit-learn')}
    with (ROOT / f'audit-{bootstrap.VARIANT}-{os.environ["PR67013_SUITE"]}.json').open('x') as output:
        json.dump(audit, output, indent=2)

@pytest.fixture(autouse=True)
def register_trainable():
    from ray.tune.utils.mock_trainable import register_mock_trainable
    register_mock_trainable()
