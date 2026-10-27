from pathlib import Path
import hashlib
import os
import subprocess
root=Path('/tmp/ray-pr67424-phase2-lgVIc0KN')
repo='/home/researcher/oss/worktrees/ray-review-4'
revs={'parent':'ed1d83ac1f353aec7c0473bc46a57e04d8767ee2','head':'1c22b86fb5769f09a436bfdd4666aedef4a73847'}
allowed='src/ray/core_worker/tests/BUILD.bazel'
for variant,rev in revs.items():
    raw=subprocess.check_output(['git','ls-tree','-r','-z',rev],cwd=repo)
    checked=0
    changes=[]
    for entry in raw.split(b'\0'):
        if not entry: continue
        meta,name=entry.split(b'\t',1)
        mode,kind,oid=meta.split()
        if kind==b'commit':
            print('SUBMODULE',variant,os.fsdecode(name),oid.decode()); continue
        name=os.fsdecode(name)
        path=root/variant/name
        data=os.fsencode(os.readlink(path)) if mode==b'120000' else path.read_bytes()
        actual=hashlib.sha1(b'blob '+str(len(data)).encode()+b'\0'+data).hexdigest()
        checked+=1
        if actual!=oid.decode():
            changes.append(name)
            assert name==allowed, (variant,name,'unexpected tracked source mutation')
            original=subprocess.check_output(['git','show',rev+':'+name],cwd=repo)
            assert data.startswith(original), (variant,name,'existing build rules modified')
    test=(root/variant/'src/ray/core_worker/tests/phase2_borrower_rpc_test.cc').read_bytes()
    print('VERIFIED',variant,rev,'tracked_files',checked,'changed_tracked_files',changes,
          'production_source_changes',0,'temporary_test_sha256',hashlib.sha256(test).hexdigest())
