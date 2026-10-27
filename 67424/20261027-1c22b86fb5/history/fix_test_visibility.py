from pathlib import Path
root=Path('/tmp/ray-pr67424-phase2-lgVIc0KN')
for variant in ['head','parent']:
    p=root/variant/'src/ray/core_worker/tests/BUILD.bazel'
    (root/(variant+'-test-BUILD.v1')).write_bytes(p.read_bytes())
    s=p.read_text()
    start=s.index('# Isolated Phase 2 experiment only.')
    s=s[:start]+s[start:].replace('        "//src/ray/core_worker_rpc_client:core_worker_client",\n','')
    p.write_text(s)
print('Removed redundant direct dependency; CoreWorkerClient remains provided by core_worker_lib transitively.')
