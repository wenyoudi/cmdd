from pathlib import Path

import pandas as pd


# ============================================================
# Configuration
# ============================================================

RESULT_DIR = Path("results")

CONFIGS = [
    ("ONE", "ONE", "one_one"),
    ("ONE", "QUORUM", "one_quorum"),
    ("QUORUM", "ONE", "quorum_one"),
    ("QUORUM", "QUORUM", "quorum_quorum"),
    ("ALL", "ONE", "all_one"),
    ("ONE", "ALL", "one_all"),
]

TRIALS = [1, 2, 3]


# ============================================================
# Helpers
# ============================================================

def to_bool(series):
    """Convert CSV boolean-like values to bool."""
    return (
        series
        .astype(str)
        .str.strip()
        .str.lower()
        .eq("true")
    )


def latency_stats(series):
    """Return mean, median and p95 latency."""
    values = pd.to_numeric(
        series,
        errors="coerce",
    ).dropna()

    if values.empty:
        return {
            "mean": float("nan"),
            "median": float("nan"),
            "p95": float("nan"),
        }

    return {
        "mean": values.mean(),
        "median": values.median(),
        "p95": values.quantile(0.95),
    }


def analyse_trial(
    path,
    scenario,
    read_cl,
    write_cl,
    trial,
):
    """
    Analyse one experiment CSV.

    Returns:
        trial summary
        annotated dataframe
    """

    df = pd.read_csv(path)

    df["scenario_label"] = scenario
    df["trial"] = trial
    df["config_read_cl"] = read_cl
    df["config_write_cl"] = write_cl

    df["success_bool"] = to_bool(df["success"])
    df["violation_bool"] = to_bool(df["violation"])

    # --------------------------------------------------------
    # Reads
    # --------------------------------------------------------

    reads = df[
        df["operation"] == "read"
    ].copy()

    successful_reads_df = reads[
        reads["success_bool"]
    ].copy()

    total_reads = len(reads)
    successful_reads = len(successful_reads_df)
    failed_reads = total_reads - successful_reads

    violations = int(
        successful_reads_df["violation_bool"].sum()
    )

    failure_rate = (
        failed_reads / total_reads
        if total_reads
        else 0
    )

    violation_rate = (
        violations / successful_reads
        if successful_reads
        else 0
    )

    read_latency = latency_stats(
        successful_reads_df["latency_ms"]
    )

    # --------------------------------------------------------
    # Writes
    # --------------------------------------------------------

    writes = df[
        df["operation"] == "write"
    ].copy()

    successful_writes_df = writes[
        writes["success_bool"]
    ].copy()

    total_writes = len(writes)
    successful_writes = len(successful_writes_df)
    failed_writes = total_writes - successful_writes

    write_failure_rate = (
        failed_writes / total_writes
        if total_writes
        else 0
    )

    write_latency = latency_stats(
        successful_writes_df["latency_ms"]
    )

    # --------------------------------------------------------
    # Summary
    # --------------------------------------------------------

    summary = {
        "scenario": scenario,
        "read_cl": read_cl,
        "write_cl": write_cl,
        "trial": trial,

        "total_reads": total_reads,
        "successful_reads": successful_reads,
        "failed_reads": failed_reads,
        "read_failure_rate_pct": failure_rate * 100,

        "violations": violations,
        "violation_rate_pct": violation_rate * 100,

        "mean_read_latency_ms": read_latency["mean"],
        "median_read_latency_ms": read_latency["median"],
        "p95_read_latency_ms": read_latency["p95"],

        "total_writes": total_writes,
        "successful_writes": successful_writes,
        "failed_writes": failed_writes,
        "write_failure_rate_pct": write_failure_rate * 100,

        "mean_write_latency_ms": write_latency["mean"],
        "median_write_latency_ms": write_latency["median"],
        "p95_write_latency_ms": write_latency["p95"],
    }

    return summary, df


def print_section(title, dataframe):
    print()
    print("=" * 120)
    print(title)
    print("=" * 120)

    if dataframe.empty:
        print("No data available.")
    else:
        print(
            dataframe
            .round(4)
            .to_string(index=False)
        )


# ============================================================
# Load NORMAL trials
# ============================================================

normal_summaries = []
normal_rows = []

for read_cl, write_cl, filename_part in CONFIGS:

    for trial in TRIALS:

        path = (
            RESULT_DIR
            / f"mr_normal_{filename_part}_trial{trial}.csv"
        )

        if not path.exists():
            print(f"WARNING: missing {path}")
            continue

        summary, df = analyse_trial(
            path=path,
            scenario="normal",
            read_cl=read_cl,
            write_cl=write_cl,
            trial=trial,
        )

        normal_summaries.append(summary)
        normal_rows.append(df)


normal_summary = pd.DataFrame(
    normal_summaries
)


# ============================================================
# Load NETWORK PARTITION trials
# ============================================================

partition_summaries = []
partition_rows = []

for read_cl, write_cl, filename_part in CONFIGS:

    for trial in TRIALS:

        path = (
            RESULT_DIR
            / (
                "mr_network_partition_"
                f"{filename_part}_trial{trial}.csv"
            )
        )

        if not path.exists():
            print(f"WARNING: missing {path}")
            continue

        summary, df = analyse_trial(
            path=path,
            scenario="network_partition",
            read_cl=read_cl,
            write_cl=write_cl,
            trial=trial,
        )

        partition_summaries.append(summary)
        partition_rows.append(df)


partition_summary = pd.DataFrame(
    partition_summaries
)


# ============================================================
# Load NODE FAILURE trials
#
# Only ONE / ONE was deliberately tested for this scenario.
# ============================================================

node_failure_summaries = []
node_failure_rows = []

for trial in TRIALS:

    path = (
        RESULT_DIR
        / f"mr_node_failure_one_one_trial{trial}.csv"
    )

    if not path.exists():
        print(f"WARNING: missing {path}")
        continue

    summary, df = analyse_trial(
        path=path,
        scenario="node_failure",
        read_cl="ONE",
        write_cl="ONE",
        trial=trial,
    )

    node_failure_summaries.append(summary)
    node_failure_rows.append(df)


node_failure_summary = pd.DataFrame(
    node_failure_summaries
)


# ============================================================
# Combine all trial summaries
# ============================================================

all_summaries = pd.concat(
    [
        normal_summary,
        partition_summary,
        node_failure_summary,
    ],
    ignore_index=True,
)


# ============================================================
# 1. Trial-level results
# ============================================================

trial_columns = [
    "scenario",
    "read_cl",
    "write_cl",
    "trial",
    "total_reads",
    "successful_reads",
    "failed_reads",
    "read_failure_rate_pct",
    "violations",
    "violation_rate_pct",
    "median_read_latency_ms",
    "p95_read_latency_ms",
    "median_write_latency_ms",
    "p95_write_latency_ms",
]

print_section(
    "ALL SCENARIOS - TRIAL-LEVEL RESULTS",
    all_summaries[trial_columns],
)


# ============================================================
# 2. Normal-operation aggregate
# ============================================================

if not normal_summary.empty:

    normal_aggregate = (
        normal_summary
        .groupby(
            ["read_cl", "write_cl"],
            as_index=False,
        )
        .agg(
            trials=("trial", "count"),

            successful_reads=(
                "successful_reads",
                "sum",
            ),

            failed_reads=(
                "failed_reads",
                "sum",
            ),

            violations=(
                "violations",
                "sum",
            ),

            mean_violation_rate_pct=(
                "violation_rate_pct",
                "mean",
            ),

            mean_read_failure_rate_pct=(
                "read_failure_rate_pct",
                "mean",
            ),

            median_read_latency_ms=(
                "median_read_latency_ms",
                "mean",
            ),

            mean_p95_read_latency_ms=(
                "p95_read_latency_ms",
                "mean",
            ),

            median_write_latency_ms=(
                "median_write_latency_ms",
                "mean",
            ),

            mean_p95_write_latency_ms=(
                "p95_write_latency_ms",
                "mean",
            ),

            total_writes=(
                "total_writes",
                "sum",
            ),

            successful_writes=(
                "successful_writes",
                "sum",
            ),

            failed_writes=(
                "failed_writes",
                "sum",
            ),

            mean_write_failure_rate_pct=(
                "write_failure_rate_pct",
                "mean",
            ),
        )
    )

else:
    normal_aggregate = pd.DataFrame()


print_section(
    "NORMAL OPERATION - AGGREGATED RESULTS",
    normal_aggregate,
)


# ============================================================
# 3. Network-partition aggregate
# ============================================================

if not partition_summary.empty:

    partition_aggregate = (
        partition_summary
        .groupby(
            ["read_cl", "write_cl"],
            as_index=False,
        )
        .agg(
            trials=("trial", "count"),

            successful_reads=(
                "successful_reads",
                "sum",
            ),

            failed_reads=(
                "failed_reads",
                "sum",
            ),

            violations=(
                "violations",
                "sum",
            ),

            mean_violation_rate_pct=(
                "violation_rate_pct",
                "mean",
            ),

            std_violation_rate_pct=(
                "violation_rate_pct",
                "std",
            ),

            mean_read_failure_rate_pct=(
                "read_failure_rate_pct",
                "mean",
            ),

            median_read_latency_ms=(
                "median_read_latency_ms",
                "mean",
            ),

            mean_p95_read_latency_ms=(
                "p95_read_latency_ms",
                "mean",
            ),

            median_write_latency_ms=(
                "median_write_latency_ms",
                "mean",
            ),

            mean_p95_write_latency_ms=(
                "p95_write_latency_ms",
                "mean",
            ),

            total_writes=(
                "total_writes",
                "sum",
            ),

            successful_writes=(
                "successful_writes",
                "sum",
            ),

            failed_writes=(
                "failed_writes",
                "sum",
            ),

            mean_write_failure_rate_pct=(
                "write_failure_rate_pct",
                "mean",
            ),
        )
    )

    partition_aggregate[
        "overall_violation_rate_pct"
    ] = (
        partition_aggregate["violations"]
        / partition_aggregate["successful_reads"]
        * 100
    )

else:
    partition_aggregate = pd.DataFrame()


print_section(
    "NETWORK PARTITION - AGGREGATED RESULTS",
    partition_aggregate,
)


# ============================================================
# 4. Partition violations by target node
# ============================================================

if partition_rows:

    partition_combined = pd.concat(
        partition_rows,
        ignore_index=True,
    )

    partition_violations = partition_combined[
        (partition_combined["operation"] == "read")
        & partition_combined["violation_bool"]
    ]

    violations_by_node = (
        partition_violations
        .groupby(
            [
                "config_read_cl",
                "config_write_cl",
                "target_node",
            ]
        )
        .size()
        .reset_index(name="violations")
    )

else:
    violations_by_node = pd.DataFrame()


print_section(
    "NETWORK PARTITION - VIOLATIONS BY TARGET NODE",
    violations_by_node,
)


# ============================================================
# 5. Node-failure aggregate
# ============================================================

if not node_failure_summary.empty:

    node_failure_aggregate = pd.DataFrame(
        {
            "trials": [
                len(node_failure_summary)
            ],

            "total_reads": [
                node_failure_summary[
                    "total_reads"
                ].sum()
            ],

            "successful_reads": [
                node_failure_summary[
                    "successful_reads"
                ].sum()
            ],

            "failed_reads": [
                node_failure_summary[
                    "failed_reads"
                ].sum()
            ],

            "overall_read_failure_rate_pct": [
                (
                    node_failure_summary[
                        "failed_reads"
                    ].sum()
                    / node_failure_summary[
                        "total_reads"
                    ].sum()
                    * 100
                )
            ],

            "violations": [
                node_failure_summary[
                    "violations"
                ].sum()
            ],

            "overall_violation_rate_pct": [
                (
                    node_failure_summary[
                        "violations"
                    ].sum()
                    / node_failure_summary[
                        "successful_reads"
                    ].sum()
                    * 100
                )
            ],

            "median_read_latency_ms": [
                node_failure_summary[
                    "median_read_latency_ms"
                ].mean()
            ],

            "mean_p95_read_latency_ms": [
                node_failure_summary[
                    "p95_read_latency_ms"
                ].mean()
            ],
        }
    )

else:
    node_failure_aggregate = pd.DataFrame()


print_section(
    "NODE FAILURE / RECOVERY - AGGREGATED RESULTS",
    node_failure_aggregate,
)


# ============================================================
# 6. Node-failure recovery validation
#
# Find each False -> True transition for cassandra-3 and
# compare the first successfully observed post-recovery version
# with the client's previous high-water mark.
# ============================================================

recovery_records = []

for trial, df in enumerate(
    node_failure_rows,
    start=1,
):

    reads = df[
        df["operation"] == "read"
    ].copy()

    reads["version_observed_numeric"] = (
        pd.to_numeric(
            reads["version_observed"],
            errors="coerce",
        )
    )

    # --------------------------------------------------------
    # High-water mark based on successful reads
    # --------------------------------------------------------

    successful = reads[
        reads["success_bool"]
    ].copy()

    successful["previous_max"] = (
        successful[
            "version_observed_numeric"
        ]
        .cummax()
        .shift(1)
    )

    # --------------------------------------------------------
    # Cassandra-3 reads only
    # --------------------------------------------------------

    node3 = reads[
        reads["target_node"] == "cassandra-3"
    ].copy()

    node3["previous_node3_success"] = (
        node3["success_bool"]
        .shift(1, fill_value=True)
    )

    recovery_rows = node3[
        node3["success_bool"]
        & ~node3["previous_node3_success"]
    ]

    for index, recovery_row in (
        recovery_rows.iterrows()
    ):

        match = successful.loc[
            successful.index == index
        ]

        if match.empty:
            continue

        row = match.iloc[0]

        observed = row[
            "version_observed_numeric"
        ]

        previous_max = row[
            "previous_max"
        ]

        regression = (
            pd.notna(previous_max)
            and observed < previous_max
        )

        recovery_records.append(
            {
                "trial": trial,
                "target_node": "cassandra-3",
                "first_recovered_version": observed,
                "client_previous_max": previous_max,
                "regression": regression,
            }
        )


recovery_summary = pd.DataFrame(
    recovery_records
)


print_section(
    "NODE FAILURE / RECOVERY - FIRST SUCCESSFUL READ AFTER RECOVERY",
    recovery_summary,
)


# ============================================================
# 7. Compact cross-scenario comparison
#
# For an apples-to-apples scenario comparison, use ONE/ONE,
# because node failure was deliberately tested only at ONE/ONE.
# ============================================================

scenario_comparison_rows = []

for scenario_name, dataframe in [
    ("normal", normal_summary),
    ("network_partition", partition_summary),
    ("node_failure", node_failure_summary),
]:

    if dataframe.empty:
        continue

    subset = dataframe[
        (dataframe["read_cl"] == "ONE")
        & (dataframe["write_cl"] == "ONE")
    ]

    if subset.empty:
        continue

    total_reads = subset["total_reads"].sum()
    successful_reads = subset[
        "successful_reads"
    ].sum()

    failed_reads = subset[
        "failed_reads"
    ].sum()

    violations = subset[
        "violations"
    ].sum()

    scenario_comparison_rows.append(
        {
            "scenario": scenario_name,
            "trials": len(subset),

            "total_reads": total_reads,

            "successful_reads": successful_reads,

            "failed_reads": failed_reads,

            "overall_failure_rate_pct": (
                failed_reads
                / total_reads
                * 100
                if total_reads
                else 0
            ),

            "violations": violations,

            "overall_violation_rate_pct": (
                violations
                / successful_reads
                * 100
                if successful_reads
                else 0
            ),

            "median_read_latency_ms": (
                subset[
                    "median_read_latency_ms"
                ].mean()
            ),

            "median_write_latency_ms": (
                subset[
                    "median_write_latency_ms"
                ].mean()
            ),
        }
    )


scenario_comparison = pd.DataFrame(
    scenario_comparison_rows
)


print_section(
    "CROSS-SCENARIO COMPARISON - ONE / ONE",
    scenario_comparison,
)


# ============================================================
# 8. Key experiment checks
# ============================================================

print()
print("=" * 120)
print("KEY CHECKS")
print("=" * 120)

if not normal_summary.empty:

    normal_violations = int(
        normal_summary["violations"].sum()
    )

    normal_failures = int(
        normal_summary["failed_reads"].sum()
    )

    print(
        f"Normal operation: "
        f"{normal_violations} violations, "
        f"{normal_failures} failed reads."
    )


if not partition_summary.empty:

    read_one = partition_summary[
        partition_summary["read_cl"] == "ONE"
    ]

    read_quorum = partition_summary[
        partition_summary["read_cl"] == "QUORUM"
    ]

    print(
        "Network partition / READ ONE: "
        f"{int(read_one['violations'].sum())} "
        "violations."
    )

    print(
        "Network partition / READ QUORUM: "
        f"{int(read_quorum['violations'].sum())} "
        "violations."
    )

    if not violations_by_node.empty:

        node3_violations = int(
            violations_by_node.loc[
                violations_by_node[
                    "target_node"
                ] == "cassandra-3",
                "violations",
            ].sum()
        )

        total_partition_violations = int(
            violations_by_node[
                "violations"
            ].sum()
        )

        print(
            "Partition violations from cassandra-3: "
            f"{node3_violations}/"
            f"{total_partition_violations}."
        )


if not node_failure_summary.empty:

    node_failure_failed = int(
        node_failure_summary[
            "failed_reads"
        ].sum()
    )

    node_failure_violations = int(
        node_failure_summary[
            "violations"
        ].sum()
    )

    print(
        "Node failure / recovery: "
        f"{node_failure_failed} failed reads, "
        f"{node_failure_violations} violations."
    )

    if not recovery_summary.empty:

        regressions = int(
            recovery_summary[
                "regression"
            ].sum()
        )

        print(
            "Post-recovery Cassandra-3 regressions: "
            f"{regressions}/"
            f"{len(recovery_summary)}."
        )