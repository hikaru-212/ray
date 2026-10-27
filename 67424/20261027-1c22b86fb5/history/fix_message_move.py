from pathlib import Path
root=Path('/tmp/ray-pr67424-phase2-lgVIc0KN')
p=root/'phase2_cases.inc'
(root/'phase2_cases.inc.v5').write_bytes(p.read_bytes())
s=p.read_text().replace('s.callback(msg);', 's.callback(rpc::PubMessage(msg));')
p.write_text(s)
combined=(root/'fixture_prefix.cc').read_text()+s
(root/'phase2_borrower_rpc_test.cc.v5').write_bytes((root/'phase2_borrower_rpc_test.cc').read_bytes())
(root/'phase2_borrower_rpc_test.cc').write_text(combined)
for variant in ['head','parent']:
    (root/variant/'src/ray/core_worker/tests/phase2_borrower_rpc_test.cc').write_text(combined)
