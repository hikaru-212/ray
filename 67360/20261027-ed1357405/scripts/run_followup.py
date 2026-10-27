import json, os, pathlib, subprocess, sys, time
root=pathlib.Path(__file__).resolve().parents[1]
records=[]
for label in ("parent","old","new"):
    tag=label+"-long"
    env=dict(os.environ,PYTHONDONTWRITEBYTECODE="1",PYTEST_DISABLE_PLUGIN_AUTOLOAD="1",PYTHONPATH=str(root/"runtime"/label)+os.pathsep+str(root/"tests"),AUDIT_ROOT=str(root),AUDIT_VARIANT=tag,TMPDIR=str(root/"tmp"),XDG_CACHE_HOME=str(root/"cache"),RAY_TMPDIR=str(root/"tmp"),OMP_NUM_THREADS="1",OPENBLAS_NUM_THREADS="1")
    cmd=[sys.executable,"-m","pytest","-c","/dev/null","--confcutdir="+str(root/"tests_followup"),"-vv","-ra","-p","no:cacheprovider","--basetemp="+str(root/"tmp"/("pytest-"+tag)),"--junitxml="+str(root/"artifacts"/(tag+".xml")),str(root/"tests_followup/test_long_history_failures.py")]
    record={"variant":label,"tag":tag,"command":cmd,"cwd":str(root),"environment":{k:env[k] for k in ("PYTHONDONTWRITEBYTECODE","PYTEST_DISABLE_PLUGIN_AUTOLOAD","PYTHONPATH","AUDIT_ROOT","AUDIT_VARIANT","TMPDIR","XDG_CACHE_HOME","RAY_TMPDIR","OMP_NUM_THREADS","OPENBLAS_NUM_THREADS")}}
    start=time.monotonic()
    with (root/"logs"/(tag+"-pytest.log")).open("w") as log:
        result=subprocess.run(cmd,cwd=root,env=env,stdout=log,stderr=subprocess.STDOUT,timeout=180)
    record.update(returncode=result.returncode,duration_s=time.monotonic()-start)
    records.append(record); print(tag,result.returncode,flush=True)
(root/"artifacts/followup-commands.json").write_text(json.dumps(records,indent=2))
