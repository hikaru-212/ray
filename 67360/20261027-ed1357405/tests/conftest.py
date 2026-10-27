import hashlib, importlib.metadata, json, os, platform, sys
from pathlib import Path
import pytest

ROOT=Path(os.environ["AUDIT_ROOT"])
VARIANT=os.environ["AUDIT_VARIANT"]
RESULTS={}
@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(item,call):
    outcome=yield
    report=outcome.get_result()
    entry=RESULTS.setdefault(item.nodeid,{"nodeid":item.nodeid})
    entry[report.when]=report.outcome
    if report.failed: entry["failure"]=str(report.longrepr)
    if hasattr(item,"audit_record"): entry.update(item.audit_record)

def pytest_sessionfinish(session,exitstatus):
    (ROOT/"artifacts"/(VARIANT+"-results.json")).write_text(json.dumps({"variant":VARIANT,"exitstatus":int(exitstatus),"tests":list(RESULTS.values())},indent=2))
    modules={}
    for name,module in sorted(sys.modules.items()):
        if name.startswith("ray"):
            path=getattr(module,"__file__",None)
            if path and Path(path).is_file():
                modules[name]={"path":path,"resolved":str(Path(path).resolve()),"sha256":hashlib.sha256(Path(path).read_bytes()).hexdigest()}
    deps=sorted([{"name":d.metadata["Name"],"version":d.version,"location":str(d.locate_file(""))} for d in importlib.metadata.distributions()],key=lambda x:x["name"] or "")
    data={"variant":VARIANT,"python":sys.version,"executable":sys.executable,"prefix":sys.prefix,"base_prefix":sys.base_prefix,"platform":platform.platform(),"argv":sys.argv,"cwd":os.getcwd(),"sys_path":sys.path,"env":{k:v for k,v in os.environ.items() if k.startswith("AUDIT_") or k in ["PYTHONPATH","PYTHONDONTWRITEBYTECODE","PYTEST_DISABLE_PLUGIN_AUTOLOAD","TMPDIR","XDG_CACHE_HOME","OMP_NUM_THREADS","OPENBLAS_NUM_THREADS","RAY_TMPDIR"]},"dependencies":deps,"ray_modules":modules}
    (ROOT/"artifacts"/(VARIANT+"-environment.json")).write_text(json.dumps(data,indent=2))
