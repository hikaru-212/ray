import json, os, pathlib, subprocess, sys, time
root=pathlib.Path(__file__).resolve().parents[1]
records=[]
for label in ("parent","old","new"):
    env=dict(os.environ,PYTHONDONTWRITEBYTECODE="1",PYTEST_DISABLE_PLUGIN_AUTOLOAD="1",PYTHONPATH=str(root/"runtime"/label),AUDIT_ROOT=str(root),AUDIT_VARIANT=label,TMPDIR=str(root/"tmp"),XDG_CACHE_HOME=str(root/"cache"),RAY_TMPDIR=str(root/"tmp"),OMP_NUM_THREADS="1",OPENBLAS_NUM_THREADS="1")
    cmd=[sys.executable,"-m","pytest","-c","/dev/null","--confcutdir="+str(root/"tests"),"-vv","-ra","-p","no:cacheprovider","--basetemp="+str(root/"tmp"/("pytest-"+label)),"--junitxml="+str(root/"artifacts"/(label+".xml")),str(root/"tests/test_behavior.py")]
    record={"variant":label,"command":cmd,"cwd":str(root),"environment":{k:env[k] for k in ("PYTHONDONTWRITEBYTECODE","PYTEST_DISABLE_PLUGIN_AUTOLOAD","PYTHONPATH","AUDIT_ROOT","AUDIT_VARIANT","TMPDIR","XDG_CACHE_HOME","RAY_TMPDIR","OMP_NUM_THREADS","OPENBLAS_NUM_THREADS")}}
    print(json.dumps(record),flush=True)
    start=time.monotonic()
    with (root/"logs"/(label+"-pytest.log")).open("w") as log:
        result=subprocess.run(cmd,cwd=root,env=env,stdout=log,stderr=subprocess.STDOUT,timeout=180)
    record.update(returncode=result.returncode,duration_s=time.monotonic()-start)
    records.append(record)
    print(label,result.returncode,flush=True)
(root/"artifacts/test-commands.json").write_text(json.dumps(records,indent=2))
