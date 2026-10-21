# Phase 3 packaged verification

Executed on 2026-10-21 with the Phase 2 Python 3.12.3 aarch64 environment,
NumPy 2.2.6, Pandas 2.3.3, PyArrow 23.0.1, and native Ray commit
`05d07da40d2c62d803f08533db75a7f3e76f079f`.

The parent command used the exact-baseline wheel package at
`/tmp/ray-67192-phase2-sWeS1S/venv/lib/python3.12/site-packages/ray`.  The PR
command used `/tmp/ray-67192-phase2-sWeS1S/pr/ray`, whose four changed Python
production files were verbatim Git blobs from
`ba3c7a61bdb2c8b8dda1dd69e46d1a0f90b16fbe`; unchanged files linked to that
baseline wheel.  These temporary paths record the executed fixture and are not
requirements of the packaged code.

```bash
E=/tmp/ray-67192-phase2-sWeS1S
T=/home/researcher/oss/ray-review-evidence/67192/test_copy_semantics.py
C=/home/researcher/oss/ray-review-evidence/67192

PYTHONDONTWRITEBYTECODE=1 OPENBLAS_NUM_THREADS=1 RAY_USAGE_STATS_ENABLED=0 \
RAY_REVIEW_NATIVE_COMMIT=05d07da40d2c62d803f08533db75a7f3e76f079f \
PYTHONPATH=$E/venv/lib/python3.12/site-packages \
RAY_REVIEW_PACKAGE_ROOT=$E/venv/lib/python3.12/site-packages/ray \
RAY_REVIEW_VARIANT=parent \
  $E/venv/bin/python -B -m pytest -q -s -p no:cacheprovider \
  --confcutdir="$C" "$T"
# 17 passed, 5 warnings in 0.78s

PYTHONDONTWRITEBYTECODE=1 OPENBLAS_NUM_THREADS=1 RAY_USAGE_STATS_ENABLED=0 \
RAY_REVIEW_NATIVE_COMMIT=05d07da40d2c62d803f08533db75a7f3e76f079f \
PYTHONPATH=$E/pr RAY_REVIEW_PACKAGE_ROOT=$E/pr/ray RAY_REVIEW_VARIANT=pr \
  $E/venv/bin/python -B -m pytest -q -s -p no:cacheprovider \
  --confcutdir="$C" "$T"
# 17 passed, 5 warnings in 0.76s
```

The warnings were dependency/deprecation warnings and no test failed.

Before the permanent copy, the byte-identical staged test was also run with the
Phase 2 PyArrow 26.0.0 overlay:

```bash
S=/tmp/ray-67192-phase3-staging/67192

PYTHONDONTWRITEBYTECODE=1 OPENBLAS_NUM_THREADS=1 RAY_USAGE_STATS_ENABLED=0 \
RAY_REVIEW_NATIVE_COMMIT=05d07da40d2c62d803f08533db75a7f3e76f079f \
PYTHONPATH=$E/arrow26:$E/venv/lib/python3.12/site-packages \
RAY_REVIEW_PACKAGE_ROOT=$E/venv/lib/python3.12/site-packages/ray \
RAY_REVIEW_VARIANT=parent \
  $E/venv/bin/python -B -m pytest -q -p no:cacheprovider \
  --confcutdir="$S" "$S/test_copy_semantics.py"
# 17 passed, 5 warnings in 0.78s

PYTHONDONTWRITEBYTECODE=1 OPENBLAS_NUM_THREADS=1 RAY_USAGE_STATS_ENABLED=0 \
RAY_REVIEW_NATIVE_COMMIT=05d07da40d2c62d803f08533db75a7f3e76f079f \
PYTHONPATH=$E/arrow26:$E/pr RAY_REVIEW_PACKAGE_ROOT=$E/pr/ray \
RAY_REVIEW_VARIANT=pr \
  $E/venv/bin/python -B -m pytest -q -p no:cacheprovider \
  --confcutdir="$S" "$S/test_copy_semantics.py"
# 17 passed, 5 warnings in 0.84s
```

The packaged benchmark received a one-repetition functional smoke run over all
12 parent/PR, layout, and batch-size combinations.  This run validates the
driver and copy counters; its 10,000-row timing/RSS values are not performance
evidence.

```bash
cd /home/researcher/oss/ray-review-evidence
$E/venv/bin/python -B 67192/benchmark_batch_copy.py run \
  --python $E/venv/bin/python \
  --parent-package-root $E/venv/lib/python3.12/site-packages/ray \
  --pr-package-root $E/pr/ray \
  --output-dir /tmp/ray-67192-phase3-final-benchmark-smoke \
  --rows 10000 --columns 2 --chunks 4 --repetitions 1
# exit 0; 12 JSONL samples and one summary generated
```

Observed copy decisions in every smoke configuration:

- parent single/multi/mixed: one Arrow slice copy;
- PR single and multi: zero Arrow slice copies, one NumPy copy;
- PR mixed: one Arrow slice copy and one NumPy copy;
- PR multi: chunk combining still occurred for both numeric columns.

The full one-million-row performance results were not rerun in Phase 3.  They
remain historical Phase 2 observations in `phase2-benchmark-summary.json`.
