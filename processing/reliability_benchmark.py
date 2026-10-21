"""Early resource screen for ADR 0009. The full experiment runner is pending.

This command cannot generate data, start Spark, run a matrix or authorize target
execution. Passing this screen alone is never a complete benchmark preflight.
"""
import importlib.metadata
from pathlib import Path
import platform
import re
import shutil
import subprocess
import sys
from publication_metadata import canonical_bytes
import reliability_format_metadata as m


def machine(root):
    """Read reviewed machine counters only; never dump paths or the environment."""
    root=m.root_path(root)
    m.need(sys.platform=='darwin','PREFLIGHT_BLOCKED')
    def command(argv):
        result=subprocess.run(argv,capture_output=True,text=True,check=True,timeout=10)
        return result.stdout.strip()
    values=command(['/usr/sbin/sysctl','-n','hw.memsize','hw.logicalcpu','hw.physicalcpu']).splitlines()
    m.need(len(values)==3 and all(v.isdecimal() for v in values),'PREFLIGHT_BLOCKED')
    total,logical,physical=map(int,values)
    vm=command(['/usr/bin/vm_stat'])
    page=re.search(r'page size of ([0-9]+) bytes',vm)
    def pages(name):
        found=re.search(r'^Pages '+name+r':\s+([0-9]+)[.]?$',vm,re.M)
        m.need(found is not None,'PREFLIGHT_BLOCKED');return int(found[1])
    m.need(page is not None,'PREFLIGHT_BLOCKED');size=int(page[1])
    free=pages('free')*size;speculative=pages('speculative')*size
    disk=shutil.disk_usage(root)
    return dict(captured_at=m.now(),os=platform.system(),release=platform.mac_ver()[0],architecture=platform.machine(),
        logical_cores=logical,physical_cores=physical,total_ram_bytes=total,free_ram_bytes=free,
        speculative_ram_bytes=speculative,available_ram_bytes=free+speculative,
        available_method='mac-free-speculative-v1',free_disk_bytes=disk.free,total_disk_bytes=disk.total,
        python=platform.python_version(),pyspark=importlib.metadata.version('pyspark'),
        duckdb=importlib.metadata.version('duckdb'))


def screen(environment,*,experiment_time_budget_seconds=None):
    """Fail closed on early prerequisites, without fabricating pilot/target estimates."""
    total=environment['total_ram_bytes'];available=environment['available_ram_bytes']
    m.integer(total,1);m.integer(available,0,total)
    if experiment_time_budget_seconds is not None:m.integer(experiment_time_budget_seconds,1)
    producer=3*1024**3;os_reserve=(total+3)//4
    checks=[dict(check_id='ram_total_reserve',required_bytes=producer,available_bytes=total-os_reserve,
                 passed=producer<=total-os_reserve,reason_code='PASS' if producer<=total-os_reserve else 'RAM_TOTAL_BUDGET'),
            dict(check_id='ram_available_reserve',required_bytes=producer+os_reserve,available_bytes=available,
                 passed=producer+os_reserve<=available,reason_code='PASS' if producer+os_reserve<=available else 'RAM_AVAILABLE_BUDGET'),
            dict(check_id='runtime_budget',required_bytes=None,available_bytes=None,
                 passed=experiment_time_budget_seconds is not None,
                 reason_code='PASS' if experiment_time_budget_seconds is not None else 'RUNTIME_UNKNOWN')]
    return dict(screen_version=1,stage='BEFORE_PILOT',state='SCREEN_PASS' if all(c['passed'] for c in checks) else 'PREFLIGHT_BLOCKED',
        checks=checks,experiment_time_budget_seconds=experiment_time_budget_seconds,
        complete_preflight=False,baseline_status='NOT_ATTEMPTED',target_execution_state='NOT_ATTEMPTED',
        target_execution_reason='OWNER_PENDING',target_preflight_state=None,
        missing_evidence=['PILOT_OBSERVATIONS','BASELINE_MATRIX','VERIFIER_BUDGET','ORACLE_BUDGET','TARGET_ESTIMATES'])


def main(argv=None):
    try:
        options=m.arguments(sys.argv[1:] if argv is None else argv,{'root'},{'time-budget-seconds'})
        if options is None:
            print('Reliability benchmark early screen: --root EXISTING_ROOT [--time-budget-seconds INTEGER]. Full plan/run/target preflight pending; this command cannot execute a workload.');return 0
        budget=options.get('time_budget_seconds')
        if budget is not None:
            m.need(re.fullmatch(r'[1-9][0-9]{0,8}',budget) is not None,'ARGUMENT_INVALID');budget=int(budget)
        environment=machine(options['root']);result=screen(environment,experiment_time_budget_seconds=budget)
        print(canonical_bytes(dict(status='SCREEN_ONLY',origin='synthetic',label=m.LABEL,
            environment=m.public(environment),admission=m.public(result))).decode())
        return 0 if result['state']=='SCREEN_PASS' else 1
    except m.ReliabilityFailure as e:return m.cli_error(e)
    except Exception:return m.cli_error(m.ReliabilityFailure('PREFLIGHT_BLOCKED'))


if __name__=='__main__':raise SystemExit(main())
