"""RYW experiments: normal, node failure, and network partition. See README.md."""
import argparse
import csv
import json
import subprocess
import re
import platform
import cassandra
import time
import uuid
from collections import Counter
from datetime import datetime
from pathlib import Path

from cassandra import ConsistencyLevel
from cassandra.cluster import Cluster, ExecutionProfile, EXEC_PROFILE_DEFAULT
from cassandra.policies import WhiteListRoundRobinPolicy, FallthroughRetryPolicy

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent.parent
RESULTS_DIR = PROJECT_ROOT / "results" / "read_your_writes"
PAIRS = [('ONE', 'ONE'), ('ONE', 'QUORUM'), ('QUORUM', 'ONE'),
         ('QUORUM', 'QUORUM'), ('ALL', 'ONE'), ('ONE', 'ALL')]
CHAIN = 'CMDD_RYW'
TRIAL_FIELDS = [
    'trial_id', 'model', 'scenario', 'client', 'operation', 'key',
    'version_written', 'version_observed', 'read_cl', 'write_cl',
    'target_node', 'success', 'violation', 'latency_ms', 'timestamp', 'error',
]
SUMMARY_FIELDS = [
    'write_cl', 'read_cl', 'trials', 'valid_reads', 'violations',
    'violation_rate', 'write_failed', 'read_failed',
]


def connect(port, level):
    # Pin each session to its localhost forwarded port.
    # Discovered Docker IPs are not directly reachable from Windows and must not be used as hosts.
    profile = ExecutionProfile(
        load_balancing_policy=WhiteListRoundRobinPolicy(['127.0.0.1']),
        retry_policy=FallthroughRetryPolicy(),
        consistency_level=level, request_timeout=15)
    cluster = Cluster(['127.0.0.1'], port=port, protocol_version=4,
                      execution_profiles={EXEC_PROFILE_DEFAULT: profile})
    try:
        return cluster, cluster.connect()
    except Exception:
        cluster.shutdown()
        raise

def check_ring():
    for node in (1, 2, 3):
        states, output = ring_view(node) # ring_view runs nodetool status of the node
        if states != Counter({"UN": 3}): # when it has three status, then the three nodes are ready.
            raise RuntimeError(f"node{node}: expected three UN nodes\n{output}")


def docker(*args, check=True):
    result = subprocess.run(['docker', *args], capture_output=True, text=True, timeout=60)
    if check and result.returncode:
        raise RuntimeError(result.stderr or result.stdout)
    return result


def firewall(*args, check=True):
    return docker('exec', '--user', 'root', 'cassandra-3',
                  'iptables', '-w', '5', *args, check=check)


def remove_partition():
    # Remove only this experiment's jumps and chain; never flush system rules.
    for hook in ('INPUT', 'OUTPUT'):
        while firewall('-C', hook, '-j', CHAIN, check=False).returncode == 0:
            firewall('-D', hook, '-j', CHAIN)
    exists = firewall('-S', CHAIN, check=False)
    if exists.returncode == 0:
        firewall('-F', CHAIN)
        firewall('-X', CHAIN)
    # Confirm firewall access even when no experiment rules existed.
    firewall('-S')


def add_partition():
    firewall('-N', CHAIN)
    for direction in ('--dports', '--sports'):
        firewall('-A', CHAIN, '-p', 'tcp', '-m', 'multiport', direction,
                 '7000,7001', '-j', 'DROP')
    for hook in ('INPUT', 'OUTPUT'):
        firewall('-I', hook, '1', '-j', CHAIN)


def ring_view(node):
    result = docker('exec', f'cassandra-{node}', 'nodetool', 'status')
    states = re.findall(r'^([UD][NLJM])\s', result.stdout, re.M)
    return Counter(states), result.stdout


def wait_topology(scenario, evidence):
    deadline = time.monotonic() + 180
    while True:
        views = {n: ring_view(n) for n in ((1, 2, 3) if scenario == 'partition' else (1, 2))}
        good = all(views[n][0] == Counter({'UN': 2, 'DN': 1}) for n in (1, 2))
        if scenario == 'partition':
            good = good and views[3][0] == Counter({'UN': 1, 'DN': 2})
        if good:
            evidence['fault_views'] = {str(n): v[1] for n, v in views.items()}
            return
        if time.monotonic() > deadline:
            raise RuntimeError('Fault topology not confirmed; experiment aborted')
        time.sleep(3)


def wait_recovery():
    deadline = time.monotonic() + 300
    while True:
        try:
            check_ring()
            for port in (10001, 10002, 10003):
                cluster, session = connect(port, ConsistencyLevel.ONE)
                try:
                    session.execute('SELECT release_version FROM system.local')
                finally:
                    cluster.shutdown()
            return
        except Exception:
            if time.monotonic() > deadline:
                raise RuntimeError('Recovery not confirmed; follow README recovery steps')
            time.sleep(3)


def classify(write_ok, read_ok, observed, expected=1):
    if not write_ok:
        return 'WRITE_FAILED'
    if not read_ok:
        return 'READ_FAILED'
    return 'VIOLATION' if observed is None or observed < expected else 'PASS'


def record_operation(session, statement, context, operation, target_node):
    """Record one measured request; blank violation means not evaluable."""
    row = dict(context, operation=operation, target_node=target_node,
               version_observed='', success=False, violation='', latency_ms=0,
               timestamp=datetime.now().astimezone().isoformat(), error='')
    started = time.perf_counter()
    try:
        result = session.execute(statement)
        if operation == 'READ':
            record = result.one()
            observed = record.version if record else None
            row['version_observed'] = observed if observed is not None else ''
            row['violation'] = classify(True, True, observed, row['version_written']) == 'VIOLATION'
        row['success'] = True
    except Exception as exc:
        row['error'] = f'{type(exc).__name__}: {exc}'
    finally:
        row['latency_ms'] = round((time.perf_counter() - started) * 1000, 3)
    return row


def summarize(rows):
    output = []
    for pair in dict.fromkeys((r['write_cl'], r['read_cl']) for r in rows):
        group = [r for r in rows if (r['write_cl'], r['read_cl']) == pair]
        writes = [r for r in group if r['operation'] == 'WRITE']
        reads = [r for r in group if r['operation'] == 'READ']
        valid = sum(r['success'] for r in reads)
        violations = sum(r['violation'] is True for r in reads)
        output.append(dict(write_cl=pair[0], read_cl=pair[1], trials=len(writes),
                           valid_reads=valid, violations=violations,
                           violation_rate=violations / valid if valid else None,
                           write_failed=sum(not r['success'] for r in writes),
                           read_failed=sum(not r['success'] for r in reads)))
    return output


def save(folder, rows, metadata):
    for name, data, fields in [('trials', rows, TRIAL_FIELDS),
                               ('summary', summarize(rows), SUMMARY_FIELDS)]:
        with (folder / f'{name}.csv').open('w', newline='', encoding='utf-8-sig') as f:
            writer = csv.DictWriter(f, fieldnames=fields)
            writer.writeheader()
            writer.writerows(data)
    (folder / 'metadata.json').write_text(json.dumps(metadata, indent=2, ensure_ascii=False), encoding='utf-8')


def run(args):
    run_id = str(uuid.uuid4())
    folder = RESULTS_DIR / f'{args.scenario}_{run_id}'
    folder.mkdir(parents=True)
    # Each measured operation has a row; a write/read pair shares a trial_id.
    rows, clusters, sessions = [], [], []
    metadata = dict(run_id=run_id, model='RYW', schema_version=2, row_grain='operation',
                    scenario=args.scenario, iterations=args.iterations,
                    started_at=datetime.now().astimezone().isoformat(), status='starting',
                    rf=3, fault_tool='node3 iptables (Dockerfile.cassandra)',
                    pairs=args.pairs, recovery='not_needed', fault_timing='before measured writes')
    fault_attempted = False
    try:
        docker('info') # check whether docker is available
        check_ring() # check whether the three nodes are normal
        metadata['docker_version'] = docker('version', '--format', '{{.Server.Version}}').stdout.strip()
        metadata['compose'] = docker('compose', 'config').stdout
        for port in (10001, 10002, 10003):
            cluster, session = connect(port, ConsistencyLevel.ONE)
            clusters.append(cluster)
            sessions.append(session)
        metadata['cassandra_versions'] = [str(s.execute('SELECT release_version FROM system.local').one().release_version) for s in sessions]
        metadata['python_version'] = platform.python_version()
        metadata['driver_version'] = cassandra.__version__
        writer = sessions[0]
        writer.execute("CREATE KEYSPACE IF NOT EXISTS ryw_matrix WITH replication = {'class':'NetworkTopologyStrategy','datacenter1':3}") # ryw_matrix is the dataset space
        replication = dict(writer.execute("SELECT replication FROM system_schema.keyspaces WHERE keyspace_name='ryw_matrix'").one().replication)
        if replication.get('datacenter1') != '3' or not replication['class'].endswith('NetworkTopologyStrategy'):
            raise RuntimeError('ryw_matrix replication must be NetworkTopologyStrategy, datacenter1=3')
        writer.execute('CREATE TABLE IF NOT EXISTS ryw_matrix.samples (id uuid PRIMARY KEY, version int)')

        inserts = [s.prepare('INSERT INTO ryw_matrix.samples (id,version) VALUES (?,?) USING TIMESTAMP ?') for s in sessions] # the ?s are accordingly the id, 0or1, timestamp
        selects = [s.prepare('SELECT version FROM ryw_matrix.samples WHERE id=?') for s in sessions]
        keys = {(w, r): [uuid.uuid4() for _ in range(args.iterations)] for w, r in args.pairs} # generate keys
        baseline_ts = time.time_ns() // 1000
        for group in keys.values():
            for key in group:
                stmt = inserts[0].bind((key, 0, baseline_ts))
                stmt.consistency_level = ConsistencyLevel.ALL  # initializaiton work
                writer.execute(stmt)
        if args.scenario == 'partition':
            firewall('-S')
            if firewall('-S', CHAIN, check=False).returncode == 0:
                raise RuntimeError('Previous partition rules exist; run --recover first')
        if args.scenario != 'normal':
            fault_attempted = True
            metadata['recovery'] = 'pending'
            if args.scenario == 'partition':
                add_partition()
            else:
                docker('stop', '--timeout', '15', 'cassandra-3') # simulate node failure
            wait_topology(args.scenario, metadata)
            if args.scenario == 'partition':
                metadata['firewall'] = firewall('-S').stdout
                sessions[2].execute('SELECT release_version FROM system.local')
        metadata['status'] = 'running'
        save(folder, rows, metadata)
        for w, r in args.pairs:
            print(f'{args.scenario}: WRITE={w}, READ={r}', flush=True)
            for index, key in enumerate(keys[w, r]):
                reader_index = 2 if args.scenario == 'partition' else (1 if args.scenario == 'node_failure' else 1 + index % 2)
                context = dict(trial_id=f'{run_id}:{w}:{r}:{index + 1}', model='RYW',
                               scenario=args.scenario, client='A', key=str(key),
                               version_written=1, read_cl=r, write_cl=w)
                stmt = inserts[0].bind((key, 1, baseline_ts + 1))
                stmt.consistency_level = getattr(ConsistencyLevel, w)
                write_row = record_operation(writer, stmt, context, 'WRITE', 'node1')
                rows.append(write_row)
                if write_row['success']:
                    stmt = selects[reader_index].bind((key,))
                    stmt.consistency_level = getattr(ConsistencyLevel, r)
                    rows.append(record_operation(sessions[reader_index], stmt, context,
                                                 'READ', f'node{reader_index + 1}'))
            save(folder, rows, metadata)
        metadata['status'] = 'completed'
    except BaseException as exc:
        metadata['status'] = 'aborted'
        metadata['error'] = f'{type(exc).__name__}: {exc}'
        raise
    finally:
        try:
            if fault_attempted:
                print('Restoring cluster...', flush=True)
                if args.scenario == 'partition':
                    remove_partition()
                else:
                    docker('start', 'cassandra-3')
                wait_recovery()
                metadata['recovery'] = 'verified: three UN nodes and CQL reachable'
        except BaseException as exc:
            metadata['recovery'] = f'FAILED: {exc}'
            metadata['status'] = 'recovery_failed'
            raise
        finally:
            for cluster in clusters:
                cluster.shutdown()
            metadata['finished_at'] = datetime.now().astimezone().isoformat()
            save(folder, rows, metadata)
            print(f'Results: {folder}', flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--scenario', choices=['normal', 'node_failure', 'partition'], default='normal')
    parser.add_argument('--iterations', type=int, default=30)
    parser.add_argument('--write-cl', choices=['ONE', 'QUORUM', 'ALL'])
    parser.add_argument('--read-cl', choices=['ONE', 'QUORUM', 'ALL'])
    parser.add_argument('--recover', action='store_true', help='Start node3 and remove only CMDD_RYW firewall rules')
    args = parser.parse_args()
    if args.iterations < 1:
        parser.error('--iterations must be positive')
    if bool(args.write_cl) != bool(args.read_cl):
        parser.error('Use --write-cl and --read-cl together, or omit both for all six pairs')
    args.pairs = [(args.write_cl, args.read_cl)] if args.write_cl else PAIRS
    if args.recover:
        docker('start', 'cassandra-3')
        remove_partition()
        wait_recovery()
        print('Recovery verified')
    else:
        run(args)


if __name__ == '__main__':
    main()
