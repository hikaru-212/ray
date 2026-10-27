"""Portable derivative of the archived setup; this public derivative has not been executed.
Only the native package path and this provenance docstring differ.
"""
import hashlib, json, os, pathlib, re, shutil, subprocess, sys
import grpc_tools
root=pathlib.Path(__file__).resolve().parents[1]
wheel=pathlib.Path(os.environ["AUDIT_NATIVE_RAY"]).resolve()
commands=[]
for label in ("parent","old","new"):
    source=root/"sources"/label
    target=root/"runtime"/label/"ray"
    target.mkdir(parents=True)
    for path in (source/"python/ray").iterdir():
        if path.name!="core": (target/path.name).symlink_to(path,target_is_directory=path.is_dir())
    core=target/"core"
    core.mkdir()
    for path in (source/"python/ray/core").iterdir():
        if path.name!="generated": (core/path.name).symlink_to(path,target_is_directory=path.is_dir())
    generated=core/"generated"
    generated.mkdir()
    for path in (wheel/"core/generated").glob("*.py"):
        shutil.copy2(path,generated/path.name)
    for name in ("_raylet.so","_version.py"):
        if not (target/name).exists() and (wheel/name).exists(): (target/name).symlink_to(wheel/name)
    raw=root/"runtime"/label/"generated-raw"
    raw.mkdir()
    protos=sorted(str(p.relative_to(source)) for p in (source/"src/ray/protobuf").rglob("*.proto") if p.name!="reporter.proto")
    cmd=[sys.executable,"-m","grpc_tools.protoc","-I.","-I"+str(pathlib.Path(grpc_tools.__file__).parent/"_proto"),"--python_out="+str(raw),"--grpc_python_out="+str(raw),"--descriptor_set_out="+str(root/"artifacts"/(label+".descriptor.pb")),"--include_imports",*protos]
    commands.append({"argv":cmd,"cwd":str(source)})
    print(json.dumps(commands[-1]),flush=True)
    subprocess.run(cmd,cwd=source,check=True)
    for path in raw.rglob("*.py"):
        text=path.read_text()
        text=re.sub(r"from src\.ray\.protobuf(?:\.public)? import ","from ray.core.generated import ",text)
        text=re.sub(r"(_builder.BuildTopDescriptorsAndMessages\(DESCRIPTOR, ')src\.ray\.protobuf(?:\.public)?\.",r"\1ray.core.generated.",text)
        (generated/path.name).write_text(text)
    print(label,"generated",len(protos),"protos",flush=True)
(root/"artifacts/runtime-commands.json").write_text(json.dumps(commands,indent=2))
native=wheel/"_raylet.so"
(root/"artifacts/native-reuse.json").write_text(json.dumps({"path":str(native),"sha256":hashlib.sha256(native.read_bytes()).hexdigest(),"note":"Existing wheel extension reused; no native build."},indent=2))
