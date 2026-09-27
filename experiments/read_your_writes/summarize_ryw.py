"""Aggregate completed RYW runs. No Cassandra connection or extra packages needed."""
import argparse
import csv
import json
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent.parent
RESULTS_DIR = PROJECT_ROOT / "results" / "read_your_writes"
SCENARIOS = {'normal': 'Normal', 'node_failure': 'Node failure', 'partition': 'Network partition'}
PAIRS = [('ONE', 'ONE'), ('ONE', 'QUORUM'), ('QUORUM', 'ONE'),
         ('QUORUM', 'QUORUM'), ('ALL', 'ONE'), ('ONE', 'ALL')]
COUNTS = ['trials', 'valid_reads', 'violations', 'write_failed', 'read_failed']
HEADERS = ['Scenario', 'Write CL', 'Read CL', 'Trials', 'Successful Reads',
           'RYW Violations', 'Violation Rate', 'Write Failures', 'Read Failures']


def collect(results):
    totals = {(s, w, r): dict.fromkeys(COUNTS, 0) for s in SCENARIOS for w, r in PAIRS}
    seen = set()
    repetitions = dict.fromkeys(SCENARIOS, 0)
    for path in sorted(results.glob('*/metadata.json')):
        meta = json.loads(path.read_text(encoding='utf-8-sig'))
        scenario = meta.get('scenario')
        if meta.get('model') != 'RYW' or scenario not in SCENARIOS:
            continue
        if meta.get('status') != 'completed' or (scenario != 'normal' and
                meta.get('recovery') != 'verified: three UN nodes and CQL reachable'):
            print(f'Skipping incomplete/unverified run: {path.parent.name}')
            continue
        if meta.get('schema_version') != 2 or meta.get('row_grain') != 'operation':
            raise ValueError(f'Unsupported historical format: {path.parent}')
        run_id = meta['run_id']
        if run_id in seen:
            raise ValueError(f'Duplicate run_id: {run_id}')
        seen.add(run_id)
        with (path.parent / 'summary.csv').open(encoding='utf-8-sig', newline='') as stream:
            rows = list(csv.DictReader(stream))
        pairs = [(row['write_cl'], row['read_cl']) for row in rows]
        if len(pairs) != len(PAIRS) or set(pairs) != set(PAIRS):
            raise ValueError(f'Expected all six configurations: {path.parent}')
        for row in rows:
            values = {name: int(row[name]) for name in COUNTS}
            if (any(value < 0 for value in values.values()) or
                    values['trials'] != meta['iterations'] or
                    values['violations'] > values['valid_reads'] or
                    values['trials'] != values['valid_reads'] + values['write_failed'] + values['read_failed']):
                raise ValueError(f'Inconsistent counts: {path.parent}, {row}')
            target = totals[scenario, row['write_cl'], row['read_cl']]
            for name in COUNTS:
                target[name] += values[name]
        repetitions[scenario] += 1
    missing = [s for s, count in repetitions.items() if not count]
    if missing:
        raise ValueError(f'No eligible runs for: {", ".join(missing)}')
    table = []
    for (scenario, write, read), counts in totals.items():
        rate = (f"{100 * counts['violations'] / counts['valid_reads']:.2f}%"
                if counts['valid_reads'] else 'N/A')
        table.append([SCENARIOS[scenario], write, read, counts['trials'],
                      counts['valid_reads'], counts['violations'], rate,
                      counts['write_failed'], counts['read_failed']])
    return table


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--results-dir', type=Path, default=RESULTS_DIR)
    parser.add_argument(
        '--output-dir',
        type=Path,
        default=RESULTS_DIR / 'aggregated'
    )
    args = parser.parse_args()
    try:
        table = collect(args.results_dir)
    except (ValueError, KeyError, OSError) as exc:
        parser.exit(1, f'Cannot generate table: {exc}\n')
    args.output_dir.mkdir(parents=True, exist_ok=True)
    csv_path = args.output_dir / 'ryw_complete_table.csv'
    with csv_path.open('w', encoding='utf-8-sig', newline='') as stream:
        writer = csv.writer(stream)
        writer.writerow(HEADERS)
        writer.writerows(table)
    print(f"CSV: {csv_path.resolve()}")


if __name__ == '__main__':
    main()

