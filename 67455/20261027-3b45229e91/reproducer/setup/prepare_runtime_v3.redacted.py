import hashlib, importlib.metadata, json, pathlib, re, subprocess, sys
import grpc_tools
root = pathlib.Path(__file__).parent
manifest = {'python': sys.version, 'executable': sys.executable, 'prefix': sys.prefix,
            'dependency_site': '@DEPENDENCY_SITE@',
            'packages': {p: importlib.metadata.version(p) for p in ['ray', 'pytest', 'protobuf', 'grpcio-tools', 'jsonschema']},
            'native_wheel_commit': 'cec0a091b12675bbf101cf8a350c8706107baee1', 'revisions': {}}
for label, revision in [('parent','938ab39460b257374624479c6739fa80315939ea'), ('head','3b45229e91b82d367b4706fbc26c2cbf31541c51')]:
    source = root / label
    raw = root / (label + '-generated-raw')
    raw.mkdir(exist_ok=True)
    generated = source / 'python/ray/core/generated'
    generated.mkdir(exist_ok=True)
    protos = sorted(str(p.relative_to(source)) for p in (source/'src/ray/protobuf').rglob('*.proto') if p.name != 'reporter.proto')
    command = [sys.executable, '-B', '-m', 'grpc_tools.protoc', '-I.', '-I'+str(pathlib.Path(grpc_tools.__file__).parent/'_proto'), '--python_out='+str(raw), '--grpc_python_out='+str(raw), '--descriptor_set_out='+str(root/(label+'.descriptor.pb')), '--include_imports', *protos]
    result = subprocess.run(command, cwd=source, text=True, capture_output=True)
    (root/(label+'-protoc-v3.log')).write_text(result.stdout+result.stderr)
    if result.returncode:
        raise RuntimeError(result.stderr)
    for path in raw.rglob('*.py'):
        contents = path.read_text()
        contents = re.sub(r'from src\.ray\.protobuf(?:\.(?:public|events))? import ', 'from ray.core.generated import ', contents)
        contents = re.sub(r"(_builder.BuildTopDescriptorsAndMessages\(DESCRIPTOR, ')src\.ray\.protobuf(?:\.(?:public|events))?\.", r'\1ray.core.generated.', contents)
        (generated/path.name).write_text(contents)
    (generated/'__init__.py').touch()
    manifest['revisions'][label] = {'sha':revision, 'scheduler_sha256':hashlib.sha256((source/'python/ray/autoscaler/v2/scheduler.py').read_bytes()).hexdigest(), 'native_sha256':hashlib.sha256((source/'python/ray/_raylet.so').read_bytes()).hexdigest(), 'proto_count': len(protos), 'protoc_command': command}
(root/'environment.json').write_text(json.dumps(manifest, indent=2)+'\n')
print(json.dumps(manifest, indent=2))
