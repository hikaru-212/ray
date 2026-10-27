import argparse, copy, dataclasses, hashlib, json, pathlib, sys
from ray._common.ray_option_utils import validate_task_options
from ray._private.label_utils import validate_node_labels
from ray.autoscaler.v2.instance_manager.config import AutoscalingConfig
from ray.autoscaler.v2.utils import ResourceRequestUtil
from ray.core.generated.common_pb2 import LabelSelectorOperator
import run_experiments as base

parser=argparse.ArgumentParser(); parser.add_argument('--revision',required=True,choices=['parent','head']); args=parser.parse_args()
results=[]
for case in ['C_finite','D_distinct_unused_fallbacks','D_same_unused_fallbacks','D_primary_only']:
    raw,options,request,requests=base.fixture('A')
    for config in request.node_type_configs.values():
        validate_node_labels(config.labels)
    if case=='C_finite':
        raw['max_workers']=2
        cfg=AutoscalingConfig(copy.deepcopy(raw),skip_content_hash=True)
        assert base.plain(cfg.get_node_type_configs())==base.plain(request.node_type_configs)
        request.max_num_nodes=cfg.get_max_num_nodes()
        assert request.max_num_nodes==3
        expected={'L':1} if args.revision=='parent' else {'S':1,'L':1}
        expected_remaining=[]
    else:
        options=[]; requests=[]
        for i,cpu in enumerate([2,1]):
            selectors=[{'x':'1'}]
            if case!='D_primary_only': selectors.append({'unused':'u'+str(i if case=='D_distinct_unused_fallbacks' else 0)})
            option={'num_cpus':cpu,'label_selector':selectors[0]}
            if len(selectors)>1: option['fallback_strategy']=[{'label_selector':s} for s in selectors[1:]]
            validate_task_options(option,in_options=True)
            options.append(option)
            requests.append(ResourceRequestUtil.make({'CPU':cpu},label_selectors=[[(k,LabelSelectorOperator.LABEL_OPERATOR_IN,[v]) for k,v in s.items()] for s in selectors]))
        request.resource_requests=ResourceRequestUtil.group_by_count(requests)
        expected={'S':1} if args.revision=='head' and case=='D_distinct_unused_fallbacks' else {'L':1}
        expected_remaining=[base.plain(requests[1])] if expected=={'S':1} else []
        witness=base.sched.SchedulingNode.from_node_config(request.node_type_configs['L'],base.sched.SchedulingNodeStatus.TO_LAUNCH,base.NodeKind.WORKER)
        unmet,score=witness.try_schedule(copy.deepcopy(requests),base.PENDING)
        assert not unmet
    reply=base.sched.ResourceDemandScheduler().schedule(copy.deepcopy(request))
    observer=base.Observer(); sys.settrace(observer.trace)
    try: traced=base.sched.ResourceDemandScheduler().schedule(copy.deepcopy(request))
    finally: sys.settrace(None)
    result={'case':case,'revision':base.REVISION[args.revision]['sha'],'merge_base':base.REVISION['parent']['sha'],
            'raw_config':raw,'validated_task_options':options,'fixture':base.plain(request),'raw_reply':base.plain(reply),
            'semantic_reply':base.semantic(reply),'trace':observer.events,
            'assertions':{'predicted_launches':base.semantic(reply)['to_launch']==expected,
                          'predicted_remaining':base.semantic(reply)['remaining']==expected_remaining,
                          'observer_did_not_change_result':base.semantic(reply)==base.semantic(traced)}}
    (base.ROOT/f'{args.revision}-{case}.json').write_text(json.dumps(result,indent=2,sort_keys=True)+'\n')
    results.append(result)
    print(json.dumps(result,indent=2,sort_keys=True),flush=True)
modules={name:str(pathlib.Path(mod.__file__).resolve()) for name,mod in list(sys.modules.items()) if name.startswith('ray.') and getattr(mod,'__file__',None)}
prefix=str(base.ROOT/args.revision/'python/ray')+'/'
external={name:path for name,path in modules.items() if not path.startswith(prefix)}
assert not external, external
assert hashlib.sha256(base.SOURCE.read_bytes()).hexdigest()==base.REVISION[args.revision]['scheduler_sha256']
(base.ROOT/f'{args.revision}-import-provenance.json').write_text(json.dumps(modules,indent=2,sort_keys=True)+'\n')
raise SystemExit(0 if all(all(r['assertions'].values()) for r in results) else 1)
