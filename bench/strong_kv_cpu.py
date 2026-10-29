#!/usr/bin/env python3
"""Owned read-only CPU observer. No services, mutations or signals.

Darwin: public libproc proc_pid_rusage(V2), Mach ticks converted using public
mach_timebase_info. Linux: /proc utime/stime and CLK_TCK. Process and TX-thread
scopes overlap and MUST NOT be added together.
"""
import ctypes
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import time

OWNER = 'root-tarn-issue181-matched-measurement'
RUSAGE_FIELDS = ['ri_user_time','ri_system_time','ri_pkg_idle_wkups','ri_interrupt_wkups',
 'ri_pageins','ri_wired_size','ri_resident_size','ri_phys_footprint','ri_proc_start_abstime',
 'ri_proc_exit_abstime','ri_child_user_time','ri_child_system_time','ri_child_pkg_idle_wkups',
 'ri_child_interrupt_wkups','ri_child_pageins','ri_child_elapsed_abstime','ri_diskio_bytesread','ri_diskio_byteswritten']

class RusageV2(ctypes.Structure):
    _fields_ = [('ri_uuid', ctypes.c_uint8 * 16)] + [(n, ctypes.c_uint64) for n in RUSAGE_FIELDS]

class Timebase(ctypes.Structure):
    _fields_ = [('numer',ctypes.c_uint32),('denom',ctypes.c_uint32)]

def call(argv, **kw):
    return subprocess.run(argv, capture_output=True, text=True, check=True, timeout=kw.pop('timeout',5), **kw)

def parse_stat(raw):
    head, tail = raw.rsplit(') ',1)
    fields = tail.split()
    if len(fields)<20:
        raise ValueError('short /proc stat')
    return dict(pid=int(head.split(' (',1)[0]),ppid=int(fields[1]),starttime=int(fields[19]),
                user_ticks=int(fields[11]),system_ticks=int(fields[12]))

def darwin_usage(pid):
    """Return raw counters plus exact integer conversion, or explicit null/error."""
    if sys.platform != 'darwin':
        return dict(cpu_ns=None,starttime=None,error='not Darwin')
    try:
        if ctypes.sizeof(RusageV2)!=160 or RusageV2.ri_user_time.offset!=16 or RusageV2.ri_proc_start_abstime.offset!=80:
            raise ValueError('rusage_info_v2 ABI mismatch')
        lib=ctypes.CDLL('/usr/lib/libproc.dylib',use_errno=True)
        fn=lib.proc_pid_rusage
        fn.argtypes=[ctypes.c_int,ctypes.c_int,ctypes.c_void_p]
        fn.restype=ctypes.c_int
        value=RusageV2()
        ctypes.set_errno(0)
        result=fn(pid,2,ctypes.byref(value))
        if result!=0:
            return dict(cpu_ns=None,starttime=None,returncode=result,errno=ctypes.get_errno(),error='proc_pid_rusage failed')
        system=ctypes.CDLL('/usr/lib/libSystem.B.dylib',use_errno=True)
        system.mach_timebase_info.argtypes=[ctypes.POINTER(Timebase)]
        system.mach_timebase_info.restype=ctypes.c_int
        scale=Timebase()
        if system.mach_timebase_info(ctypes.byref(scale))!=0 or not scale.numer or not scale.denom:
            raise ValueError('mach_timebase_info unavailable')
        raw=int(value.ri_user_time)+int(value.ri_system_time)
        return dict(cpu_ns=raw*int(scale.numer)//int(scale.denom),
                    starttime=int(value.ri_proc_start_abstime),user_ticks=int(value.ri_user_time),
                    system_ticks=int(value.ri_system_time),raw_units='Mach absolute time',
                    timebase_numer=int(scale.numer),timebase_denom=int(scale.denom),
                    conversion='floor((user_ticks+system_ticks)*numer/denom)',struct_bytes=160)
    except (OSError,AttributeError,ValueError) as e:
        return dict(cpu_ns=None,starttime=None,error=type(e).__name__+': '+str(e))

def host_identity(pid, runner=call, usage=darwin_usage):
    if type(pid) is not int or pid<=0:
        raise ValueError('invalid host pid')
    result={'pid':pid}
    for field in ('ppid','lstart','command'):
        raw=runner(['/bin/ps','-ww','-p',str(pid),'-o',field+'=']).stdout.strip()
        if not raw:
            raise ValueError('host process missing')
        result[field]=int(raw) if field=='ppid' else raw
    if sys.platform=='darwin':
        u=usage(pid)
        result['kernel_birth']=u.get('starttime')
    else:
        result['kernel_birth']=parse_stat(Path('/proc',str(pid),'stat').read_text())['starttime']
    return result

def host_sample(expected):
    before=host_identity(expected['pid'])
    if before!=expected:
        raise ValueError('host identity changed')
    if sys.platform=='darwin':
        sample=darwin_usage(expected['pid'])
    else:
        stat=parse_stat(Path('/proc',str(expected['pid']),'stat').read_text())
        hz=os.sysconf('SC_CLK_TCK')
        sample=dict(cpu_ns=(stat['user_ticks']+stat['system_ticks'])*1_000_000_000//hz,
                    clk_tck=hz,**stat)
    after=host_identity(expected['pid'])
    if after!=expected or sample.get('starttime')!=expected['kernel_birth']:
        raise ValueError('host birth changed/unavailable across CPU read')
    return sample

def checked_container(config, runner=call):
    obj=json.loads(runner(['docker','inspect',config['container_id']]).stdout)[0]
    if obj['Id']!=config['container_id'] or obj['Config']['Labels'].get('codex.owner')!=config.get('owner',OWNER) or obj['Config']['Labels'].get('codex.owner_nonce')!=config['nonce'] or not obj['State']['Running']:
        raise ValueError('container ownership mismatch/not running')
    return obj

PROC_SCRIPT = r"""set -eu
hz=$(getconf CLK_TCK); printf 'HZ %s\n' "$hz"
while [ "$#" -gt 0 ]; do
  pid=$1; ppid=$2; birth=$3; tid=$4; tbirth=$5; shift 5
  before=$(cat "/proc/$pid/stat")
  rest=${before##*) }; set -- "$@" # preserve the node argument vector
  values=$(printf '%s\n' "$rest" | awk '{print $2 " " $20}')
  [ "$values" = "$ppid $birth" ] || exit 41
  thread=$(cat "/proc/$pid/task/$tid/stat")
  trest=${thread##*) }
  [ "$(printf '%s\n' "$trest" | awk '{print $20}')" = "$tbirth" ] || exit 42
  printf 'PROCESS %s\nTHREAD %s\n' "$before" "$thread"
  after=$(cat "/proc/$pid/stat")
  arest=${after##*) }
  [ "$(printf '%s\n' "$arest" | awk '{print $2 " " $20}')" = "$ppid $birth" ] || exit 43
done
"""

def checked_host_process(node, nonce, proc_root=Path('/proc')):
    p=proc_root/str(node['pid'])
    before=parse_stat((p/'stat').read_text())
    if (before['pid'],before['ppid'],before['starttime'])!=(node['pid'],node['ppid'],node['starttime']):
        raise ValueError('owned process identity changed')
    if os.readlink(p/'cwd')!=node['workdir'] or os.readlink(p/'exe')!=node['exe']:
        raise ValueError('owned process cwd/executable changed')
    env=(p/'environ').read_bytes().split(b'\0')
    for name,value in [('TARN_KV_OWNER_NONCE',nonce),('TARN_KV_NODE_ID',node['node_id']),('TARN_KV_WORKDIR',node['workdir'])]:
        if (name+'='+value).encode() not in env:
            raise ValueError('owned process nonce/node/workdir changed')
    after=parse_stat((p/'stat').read_text())
    if (after['pid'],after['ppid'],after['starttime'])!=(before['pid'],before['ppid'],before['starttime']):
        raise ValueError('process changed across identity inspection')
    return after

def checked_launcher(config):
    launcher=config['launcher']
    if config['backend']=='host':
        p=parse_stat(Path('/proc',str(launcher['pid']),'stat').read_text())
        if p['starttime']!=launcher['starttime']:
            raise ValueError('launcher birth changed')
    elif host_identity(launcher['pid'])!=launcher['identity']:
        raise ValueError('fixture helper identity changed')

def storage_samples(config, runner=call):
    if config['backend']=='host':
        hz=os.sysconf('SC_CLK_TCK');out=[]
        for node in config['nodes']:
            p=checked_host_process(node,config['nonce'])
            t=parse_stat(Path('/proc',str(node['pid']),'task',str(node['tx_tid']),'stat').read_text())
            if (t['pid'],t['starttime'])!=(node['tx_tid'],node['tx_starttime']):
                raise ValueError('TX thread identity changed')
            checked_host_process(node,config['nonce'])
            out.append(dict(node_id=node['node_id'],process=p,tx_thread=t,clk_tck=hz,
                            process_cpu_ns=(p['user_ticks']+p['system_ticks'])*1_000_000_000//hz,
                            tx_cpu_ns=(t['user_ticks']+t['system_ticks'])*1_000_000_000//hz))
        return out
    checked_container(config,runner)
    args=[]
    for n in config['nodes']:
        args += [str(n[k]) for k in ('pid','ppid','starttime','tx_tid','tx_starttime')]
    r=runner(['docker','exec',config['container_id'],'sh','-c',PROC_SCRIPT,'cpu-observer',*args],timeout=5)
    lines=r.stdout.splitlines()
    if not lines or not lines[0].startswith('HZ '):
        raise ValueError('missing CLK_TCK')
    hz=int(lines[0][3:])
    if hz<=0 or len(lines)!=1+2*len(config['nodes']):
        raise ValueError('incomplete /proc samples')
    out=[]
    for i,node in enumerate(config['nodes']):
        p,t=lines[1+2*i:3+2*i]
        if not p.startswith('PROCESS ') or not t.startswith('THREAD '):
            raise ValueError('invalid /proc envelope')
        p,t=parse_stat(p[8:]),parse_stat(t[7:])
        if (p['pid'],p['ppid'],p['starttime'])!=(node['pid'],node['ppid'],node['starttime']) or (t['pid'],t['starttime'])!=(node['tx_tid'],node['tx_starttime']):
            raise ValueError('process/thread identity mismatch')
        out.append(dict(node_id=node['node_id'],process=p,tx_thread=t,clk_tck=hz,
                        process_cpu_ns=(p['user_ticks']+p['system_ticks'])*1_000_000_000//hz,
                        tx_cpu_ns=(t['user_ticks']+t['system_ticks'])*1_000_000_000//hz))
    checked_container(config,runner)
    return out

def observe(config, request):
    if request.get('version')!=1 or request.get('stage') not in ('before','after') or request.get('mode') not in ('sql','strong','raw'):
        raise ValueError('invalid observer request')
    root=Path(config['root'])
    checked_launcher(config)
    controller=host_identity(config['controller']['pid'])
    if controller!=config['controller']:
        raise ValueError('measurement controller identity changed')
    client=host_identity(request['client_pid'])
    if request['mode']=='sql':
        if client!=controller:raise ValueError('SQL client is not owned controller')
    else:
        if config['driver_path'] not in client['command'] or client['ppid']!=controller['pid']:
            raise ValueError('client argv outside owned fixture')
    if controller['ppid']!=config['launcher']['pid'] or config['controller_path'] not in controller['command']:
        raise ValueError('client/controller lineage outside owned fixture')
    nodes=storage_samples(config)
    gateways=[]
    for g in config['gateways']:
        if config['backend']=='host':checked_host_process(g['process'],config['nonce'])
        gateways.append(dict(identity=g['identity'],sample=host_sample(g['identity'])))
    client_cpu=host_sample(client)
    identity={'container_id':config.get('container_id'),'nonce':config['nonce'],'nodes':config['nodes'],
              'gateways':config['gateways'],'client':client,'controller':controller}
    prefix='-'.join(str(request[k]) for k in ('trial','mode','batch','workers','seed'))
    if not all(c.isalnum() or c=='-' for c in prefix):
        raise ValueError('invalid observer state key')
    state=root/'cpu-observations';state.mkdir(exist_ok=True)
    prior=state/(prefix+'-before.json')
    if request['stage']=='after':
        if json.loads(prior.read_text())['identity']!=identity:
            raise ValueError('CPU identity changed within phase')
    process_total=sum(n['process_cpu_ns'] for n in nodes)
    tx_total=sum(n['tx_cpu_ns'] for n in nodes)
    gateway_values=[g['sample']['cpu_ns'] for g in gateways]
    result={'identity':identity,'cpu_counters_ns':{
        'storage_all_processes':process_total,'storage_all_tx_threads':tx_total,
        'hot_owner_leader_tx':next(n['tx_cpu_ns'] for n in nodes if n['node_id']==config['hot_leader']),
        'gateway_all_processes':sum(gateway_values) if all(v is not None for v in gateway_values) else None,
        'client_process':client_cpu['cpu_ns']},
        'raw':{'storage':nodes,'gateways':gateways,'client':client_cpu},
        'scope':'Process includes TX; do not add overlapping process/TX/hot-leader scopes. No aggregate double-counted CPU metric.',
        'linux_resolution_ns':1_000_000_000//nodes[0]['clk_tck'],
        'sampled_wall_ns':time.time_ns(),'request':request}
    target=state/(prefix+'-'+request['stage']+'.json')
    with target.open('x') as f:f.write(json.dumps(result,indent=2,sort_keys=True)+'\n')
    target.chmod(0o600)
    return result

def main():
    if len(sys.argv)!=2:
        raise ValueError('observer CONFIG')
    config=json.loads(Path(sys.argv[1]).read_text())
    print(json.dumps(observe(config,json.load(sys.stdin)),sort_keys=True))

if __name__=='__main__':
    main()
