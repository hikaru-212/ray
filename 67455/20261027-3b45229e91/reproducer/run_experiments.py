import argparse
import copy
import dataclasses
import hashlib
import inspect
import json
import pathlib
import sys
import types

import ray
from google.protobuf.json_format import MessageToDict
from google.protobuf.message import Message
from ray._common.ray_option_utils import validate_task_options
from ray.autoscaler.v2.instance_manager.config import AutoscalingConfig
from ray.autoscaler.v2 import scheduler as sched
from ray.autoscaler.v2.tests.test_scheduler import sched_request
from ray.autoscaler.v2.tests.util import make_autoscaler_instance
from ray.autoscaler.v2.utils import ResourceRequestUtil
from ray.core.generated.autoscaler_pb2 import NodeState, NodeStatus
from ray.core.generated.common_pb2 import LabelSelectorOperator
from ray.core.generated.instance_manager_pb2 import Instance, NodeKind

ROOT = pathlib.Path(__file__).parent
SOURCE = pathlib.Path(inspect.getsourcefile(sched)).resolve()
SOURCE_LINES = SOURCE.read_text().splitlines()
REVISION = json.loads((ROOT/'environment.json').read_text())['revisions']
PENDING = sched.ResourceRequestSource.PENDING_DEMAND


def plain(obj):
    if isinstance(obj, Message):
        return MessageToDict(obj, preserving_proto_field_name=True)
    if dataclasses.is_dataclass(obj):
        return {f.name: plain(getattr(obj, f.name)) for f in dataclasses.fields(obj)}
    if isinstance(obj, dict):
        return {str(k): plain(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [plain(v) for v in obj]
    return obj


def node_info(n):
    if n is None:
        return None
    return {'node_type': n.node_type, 'status': n.status.value,
            'node_kind': NodeKind.Name(n.node_kind),
            'total': dict(n.total_resources), 'available': dict(n.get_available_resources(PENDING)),
            'labels': dict(n.labels), 'scheduled_requests': plain(n.get_sched_requests(PENDING))}


def semantic(reply):
    return {'to_launch': {r.instance_type: r.count for r in reply.to_launch},
            'remaining': plain(reply.infeasible_resource_requests),
            'cluster_resources': dict(reply.cluster_resources),
            'to_terminate': plain(reply.to_terminate), 'to_ippr': plain(reply.to_ippr),
            'infeasible_gang': plain(reply.infeasible_gang_resource_requests),
            'infeasible_constraints': plain(reply.infeasible_cluster_resource_constraints)}


def fixture(case):
    if case == 'B':
        worker_specs = [('A',2,{'accel':'A'}),('B',2,{'accel':'B'})]
        request_specs = [(1,[{'accel':'A'},{'accel':'B'}]),(1,[{'accel':'B'},{'accel':'A'}])]
        max_workers = 10
    else:
        worker_specs = [('S',2,{'x':'1'}),('L',3,{'x':'1','y':'1'})]
        request_specs = [(2,[{'x':'1'}]),(1,[{'y':'1'}])]
        max_workers = 1
    raw_config = {'cluster_name':'pr-67455-phase2', 'provider':{'type':'kuberay'},
                  'cluster_synced_files':[], 'auth':{}, 'max_workers':max_workers,
                  'head_node_type':'head', 'available_node_types':{
                      'head':{'resources':{'CPU':0},'labels':{},'node_config':{},'min_workers':0,'max_workers':0}
                  }}
    for name, cpu, labels in worker_specs:
        raw_config['available_node_types'][name] = {'resources':{'CPU':cpu},'labels':labels,'node_config':{},'min_workers':0,'max_workers':10,'priority':0}
    cfg = AutoscalingConfig(copy.deepcopy(raw_config), skip_content_hash=True)
    node_types = cfg.get_node_type_configs()
    validated_options = []
    requests = []
    for cpu, selectors in request_specs:
        options = {'num_cpus':cpu, 'label_selector':selectors[0]}
        if len(selectors)>1:
            options['fallback_strategy']=[{'label_selector':s} for s in selectors[1:]]
        validate_task_options(options, in_options=True)
        validated_options.append(options)
        requests.append(ResourceRequestUtil.make({'CPU':cpu}, label_selectors=[[(k,LabelSelectorOperator.LABEL_OPERATOR_IN,[v]) for k,v in s.items()] for s in selectors]))
    head = make_autoscaler_instance(
        im_instance=Instance(instance_id='head-im', instance_type='head', status=Instance.RAY_RUNNING,
                             node_kind=NodeKind.HEAD, node_id='01'*28, launch_config_hash=node_types['head'].launch_config_hash),
        ray_node=NodeState(node_id=bytes.fromhex('01'*28),ray_node_type_name='head',total_resources={'CPU':0},
                           available_resources={'CPU':0},labels={},status=NodeStatus.RUNNING),cloud_instance_id='head-cloud')
    request = sched_request(node_type_configs=node_types, max_num_nodes=cfg.get_max_num_nodes(), resource_requests=requests,instances=[head])
    if case == 'C':
        request.max_num_nodes = None
    return raw_config, validated_options, request, requests


def production_sort(requests):
    code = next(c for c in sched.ResourceDemandScheduler._try_schedule.__code__.co_consts
                if isinstance(c,types.CodeType) and c.co_name=='_sort_resource_request')
    actual_key = types.FunctionType(code, sched.__dict__)
    return sorted(requests,key=actual_key,reverse=True), [actual_key(r) for r in requests]


def preflight(case, raw, options, request, requests):
    ctx = sched.ResourceDemandScheduler.ScheduleContext.from_schedule_request(request)
    ordered, keys = production_sort(requests)
    data = {'config_validation':'PASS', 'task_option_validation':'PASS',
            'raw_config':raw,'task_options':options,'scheduling_request':plain(request),
            'production_sort_keys':keys,'production_sorted_requests':plain(ordered),
            'node_type_quotas':ctx.get_node_type_available()}
    assert len(ctx.get_nodes())==1 and ctx.get_nodes()[0].node_kind==NodeKind.HEAD
    assert all(not r.placement_constraints for r in requests)
    assert request.ippr_specs is None and not request.ippr_statuses
    if case in ('A','C'):
        assert ordered == requests and [dict(r.resources_bundle) for r in ordered] == [{'CPU':2.0},{'CPU':1.0}]
        assert all(len(r.label_selectors)==1 for r in requests)
        assert ctx.get_node_type_available()=={'head':0,'S':10,'L':10}
        witness=sched.SchedulingNode.from_node_config(request.node_type_configs['L'],sched.SchedulingNodeStatus.TO_LAUNCH,NodeKind.WORKER)
        remaining,score=witness.try_schedule(copy.deepcopy(ordered),PENDING)
        assert not remaining and witness.get_available_resources(PENDING)['CPU']==0
        small=sched.SchedulingNode.from_node_config(request.node_type_configs['S'],sched.SchedulingNodeStatus.TO_LAUNCH,NodeKind.WORKER)
        denied,denied_score=small.try_schedule([copy.deepcopy(requests[1])],PENDING)
        assert denied == [requests[1]] and small.get_available_resources(PENDING)['CPU']==2
        data.update({'complete_placement_witness':node_info(witness),'witness_score':score,
                     'S_rejects_R2':plain(denied),'S_after_R2_attempt':node_info(small)})
        if case == 'A': assert request.max_num_nodes-len(ctx.get_nodes())==1
        else:
            a=fixture('A')[2]
            c=copy.deepcopy(request); c.max_num_nodes=a.max_num_nodes
            assert plain(c)==plain(a)
            data['only_change_from_A']='max_num_nodes: 2 -> None'
    return data


class Observer:
    def __init__(self): self.events=[]
    def trace(self,frame,event,arg):
        if frame.f_code.co_filename != str(SOURCE) or frame.f_code.co_name not in ('_try_schedule','_sched_best_node'):
            return None
        loc=frame.f_locals
        name=frame.f_code.co_name
        line=SOURCE_LINES[frame.f_lineno-1].strip()
        entry=None
        if event=='line' and name=='_try_schedule':
            if line=='shape_keys = {}':
                entry={'event':'sorted_requests','requests':plain(loc['requests_to_sched'])}
            elif line=='for node in existing_nodes:' and 'node_type_available' in loc:
                entry={'event':'groups_and_initial_state','groups':plain(loc.get('request_groups',[loc['requests_to_sched']])),
                       'quotas':dict(loc['node_type_available']),'nodes':[node_info(n) for n in loc['existing_nodes']]}
            elif line=='fresh_nodes = fresh_node_pools if can_launch_new else []':
                entry={'event':'global_cap','count':loc['num_nodes'],'max':loc['max_num_nodes'],'can_launch':loc['can_launch_new'],'quotas':dict(loc['node_type_available'])}
            elif line=='if max_num_nodes is not None and len(target_nodes) >= max_num_nodes:':
                entry={'event':'global_cap','count':len(loc['target_nodes']),'max':loc['max_num_nodes'],'can_launch':loc['max_num_nodes'] is None or len(loc['target_nodes'])<loc['max_num_nodes'],'quotas':dict(loc['node_type_available'])}
        elif event=='line' and name=='_sched_best_node' and line=='if len(remaining) == len(requests):':
            entry={'event':'candidate','requests':plain(loc['requests']),'before':node_info(loc['node']),
                   'after':node_info(loc['node_copy']),'score':plain(loc['score']),'remaining':plain(loc['remaining'])}
        elif event=='return' and name=='_sched_best_node' and arg:
            entry={'event':'selected','node':node_info(arg[0]),'remaining':plain(arg[1]),'other_candidates':[node_info(n) for n in arg[2]]}
        elif event=='return' and name=='_try_schedule' and arg:
            entry={'event':'planning_result','nodes':[node_info(n) for n in arg[0]],'remaining':plain(arg[1])}
        if entry:
            entry['line']=frame.f_lineno
            self.events.append(entry)
        return self.trace


def run(label,case):
    raw, options, request, requests=fixture(case)
    data={'case':case,'revision':REVISION[label]['sha'],'comparison_merge_base':REVISION['parent']['sha'],
          'scheduler_path':str(SOURCE),'scheduler_sha256':hashlib.sha256(SOURCE.read_bytes()).hexdigest(),
          'python':sys.version,'executable':sys.executable,'ray_path':ray.__file__}
    assert data['scheduler_sha256']==REVISION[label]['scheduler_sha256']
    data['preflight']=preflight(case,raw,options,request,requests)
    # Baseline execution has no observer, patch, altered sorting, or altered scorer.
    reply=sched.ResourceDemandScheduler().schedule(copy.deepcopy(request))
    data['raw_reply']=plain(reply)
    data['raw_proto_text']={'to_launch':[str(x) for x in reply.to_launch],
                            'infeasible_resource_requests':[str(x) for x in reply.infeasible_resource_requests]}
    data['semantic_reply']=semantic(reply)
    observer=Observer()
    sys.settrace(observer.trace)
    try:
        traced=sched.ResourceDemandScheduler().schedule(copy.deepcopy(request))
    finally:
        sys.settrace(None)
    data['trace']=observer.events
    data['assertions']={'observer_did_not_change_semantic_result':semantic(reply)==semantic(traced),
                        'no_termination':not reply.to_terminate,'no_resize':not reply.to_ippr,
                        'production_sort_matches_observation':next(e['requests'] for e in observer.events if e['event']=='sorted_requests')==data['preflight']['production_sorted_requests']}
    predictions={'parent':{'A':({'L':1},[]),'B':({'A':1},[]),'C':({'L':1},[])},
                 'head':{'A':({'S':1},[plain(requests[1])]),'B':({'A':1,'B':1},[]),'C':({'S':1,'L':1},[])}}
    launches,remaining=predictions[label][case]
    data['assertions']['prediction_launches']=data['semantic_reply']['to_launch']==launches
    data['assertions']['prediction_remaining']=data['semantic_reply']['remaining']==remaining
    data['all_demands_satisfied']=not reply.infeasible_resource_requests
    data['planned_worker_CPU']=sum(request.node_type_configs[r.instance_type].resources['CPU']*r.count for r in reply.to_launch)
    (ROOT/f'{label}-{case}.json').write_text(json.dumps(data,indent=2,sort_keys=True)+'\n')
    print(json.dumps(data,indent=2,sort_keys=True),flush=True)
    return all(data['assertions'].values())


if __name__=='__main__':
    parser=argparse.ArgumentParser(); parser.add_argument('--revision',choices=['parent','head'],required=True); parser.add_argument('--cases',default='A,B,C')
    args=parser.parse_args()
    success=True
    for case in args.cases.split(','):
        success=run(args.revision,case) and success
    raise SystemExit(0 if success else 1)
