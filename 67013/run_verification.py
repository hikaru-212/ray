"""Export exact Tune sources and run unchanged assertions on parent and PR.

Run with Python 3.12 in a venv installed from environment-lock.txt.
All generated sources, logs and test output go to a fresh temporary directory.
The optional all-NaN diagnostic is expected to FAIL on both revisions.
"""
import argparse
from datetime import datetime, timezone
import hashlib
import importlib.metadata
import io
import json
import os
from pathlib import Path
import subprocess
import sys
import tarfile
import tempfile
import xml.etree.ElementTree as ET

COMMITS = {
    'parent': 'e00c7f4bed3d92da863230c2dfdc3ea81eec1778',
    'head': '8ffddc460b8b65da3930b2a789399aafe0282568',
}
PACKAGE = Path(__file__).resolve().parent


def git(repo, *args):
    return subprocess.check_output(['git', '-C', str(repo), *args])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--ray-repo', type=Path, required=True)
    parser.add_argument('--probe-all-nan', action='store_true',
                        help='Also run the explicitly expected RED/RED diagnostic.')
    args = parser.parse_args()
    repo = args.ray_repo.resolve()
    if sys.version_info[:2] != (3, 12) or sys.prefix == sys.base_prefix:
        parser.error('Use Python 3.12 in a virtual environment.')
    packages = {}
    for line in (PACKAGE / 'environment-lock.txt').read_text().splitlines():
        name, version = line.split('==')
        packages[name] = importlib.metadata.version(name)
        if packages[name] != version:
            parser.error(f'{name}: expected {version}, found {packages[name]}')
    for commit in COMMITS.values():
        assert git(repo, 'rev-parse', f'{commit}^{{commit}}').decode().strip() == commit
    assert git(repo, 'rev-parse', COMMITS['head'] + '^').decode().strip() == COMMITS['parent']
    before = {key: git(repo, *cmd) for key, cmd in {
        'head': ['rev-parse', 'HEAD'],
        'status': ['status', '--porcelain=v1', '--untracked-files=all'],
        'index': ['diff', '--cached', '--binary'],
        'diff': ['diff', '--binary'],
    }.items()}
    # Respect TMPDIR, but refuse to generate output inside either repository.
    temp_parent = Path(tempfile.gettempdir()).resolve()
    for protected in (repo, PACKAGE.parent):
        if temp_parent.is_relative_to(protected):
            parser.error('TMPDIR must be outside the Ray and evidence repositories.')
    output = Path(tempfile.mkdtemp(prefix='ray-67013-verify-'))
    print(f'Generated output: {output}', flush=True)
    summary = {
        'phase': 'Phase 3 packaged rerun',
        'started_utc': datetime.now(timezone.utc).isoformat(),
        'python': sys.version,
        'commits': COMMITS,
        'packages': packages,
        'artifact_sha256': {
            p.name: hashlib.sha256(p.read_bytes()).hexdigest()
            for p in sorted(PACKAGE.iterdir()) if p.suffix == '.py' or p.name == 'environment-lock.txt'
        },
        'runs': [],
    }
    matched = True
    for variant, commit in COMMITS.items():
        snapshot = output / variant
        snapshot.mkdir()
        archive = git(repo, 'archive', commit, 'python/ray/tune')
        with tarfile.open(fileobj=io.BytesIO(archive)) as tree:
            tree.extractall(snapshot, filter='data')
        file_count = 0
        for entry in git(repo, 'ls-tree', '-r', '-z', commit, '--', 'python/ray/tune').split(b'\0'):
            if not entry:
                continue
            meta, name = entry.split(b'\t', 1)
            data = (snapshot / name.decode()).read_bytes()
            blob = hashlib.sha1(b'blob ' + str(len(data)).encode() + b'\0' + data).hexdigest()
            assert blob == meta.split()[2].decode(), name
            file_count += 1
        suites = [('core', 'test_repeater.py', 12, 4 if variant == 'parent' else 0)]
        if args.probe_all_nan:
            suites.append(('all_nan_probe', 'probe_all_nan.py', 1, 1))
        for suite, filename, expected_tests, expected_failures in suites:
            label = f'{variant}-{suite}'
            env = dict(os.environ, PR67013_RAY_REPO=str(repo),
                       PR67013_RUN_DIR=str(output), PR67013_VARIANT=variant,
                       PR67013_SUITE=suite, PYTHONDONTWRITEBYTECODE='1',
                       PYTEST_DISABLE_PLUGIN_AUTOLOAD='1')
            command = [sys.executable, '-B', '-m', 'pytest', '-p', 'no:cacheprovider',
                       '-q', '--tb=short', '--basetemp', str(output / f'{label}-tmp'),
                       '--junitxml', str(output / f'{label}.xml'), str(PACKAGE / filename)]
            with (output / f'{label}.log').open('x') as log:
                result = subprocess.run(command, cwd=output, env=env, stdout=log, stderr=subprocess.STDOUT)
            xml = output / f'{label}.xml'
            cases = list(ET.parse(xml).iter('testcase')) if xml.exists() else []
            failures = [c.attrib['name'] for c in cases if c.find('failure') is not None]
            errors = [c.attrib['name'] for c in cases if c.find('error') is not None]
            skipped = [c.attrib['name'] for c in cases if c.find('skipped') is not None]
            expected_names = []
            if suite == 'core' and variant == 'parent':
                expected_names = [f'test_completion_matrix[{case}-012-{backend}]'
                                  for case in ('one_failure', 'two_failures')
                                  for backend in ('bayesopt', 'hyperopt')]
            elif suite == 'all_nan_probe':
                expected_names = ['test_same_outcomes_same_aggregate[hyperopt-failure_missing_nan]']
            expectation_met = (
                len(cases) == expected_tests and len(failures) == expected_failures
                and sorted(failures) == sorted(expected_names) and not errors and not skipped
                and result.returncode == (1 if expected_failures else 0)
            )
            matched = matched and expectation_met
            audit_path = output / f'audit-{label}.json'
            run = {'variant': variant, 'suite': suite, 'exit_code': result.returncode,
                   'passed': len(cases) - len(failures) - len(errors) - len(skipped),
                   'failed': failures, 'errors': errors, 'skipped': skipped,
                   'expected_pattern_met': expectation_met,
                   'snapshot_files_verified': file_count,
                   'import_audit': json.loads(audit_path.read_text()) if audit_path.exists() else None}
            if suite == 'core':
                rows = [json.loads(line) for line in (output / f'results-{variant}.jsonl').read_text().splitlines()]
                assert len(rows) == expected_tests
                run['executed_repeater_lines'] = sorted({line for r in rows for path, func, line in r['executed']
                                                        if path.endswith('/repeater.py') and func == 'on_trial_complete'})
                run['outcomes'] = []
                for row in rows:
                    small = {key: row[key] for key in ('case', 'backend', 'order', 'mean', 'error', 'kwargs',
                             'state', 'callback_counts', 'primary_id_correct', 'scores_in_suggestion_order')}
                    small['runtime_warnings'] = [
                        {'category': w['category'], 'message': w['message'],
                         'source': str(Path(w['file']).relative_to(snapshot)), 'line': w['line']}
                        for w in row['warnings'] if w['category'] == 'RuntimeWarning'
                    ]
                    run['outcomes'].append(small)
            summary['runs'].append(run)
            print(f'{label}: {run["passed"]} passed, {len(failures)} failed, '
                  f'{len(errors)} errors; exit={result.returncode}; expected_pattern_met={expectation_met}', flush=True)
    after = {key: git(repo, *cmd) for key, cmd in {
        'head': ['rev-parse', 'HEAD'],
        'status': ['status', '--porcelain=v1', '--untracked-files=all'],
        'index': ['diff', '--cached', '--binary'],
        'diff': ['diff', '--binary'],
    }.items()}
    summary['ray_repository_unchanged'] = before == after
    matched = matched and before == after
    summary['all_expected_patterns_met'] = matched
    with (output / 'summary.json').open('x') as stream:
        json.dump(summary, stream, indent=2)
        stream.write('\n')
    print(f'Portable result summary: {output / "summary.json"}')
    return 0 if matched else 1


if __name__ == '__main__':
    raise SystemExit(main())
