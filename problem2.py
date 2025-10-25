#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Problem 2: Cluster Usage Analysis

Outputs (all written to local driver path: data/output/):
  - problem2_timeline.csv
  - problem2_cluster_summary.csv
  - problem2_stats.txt
  - problem2_bar_chart.png
  - problem2_density_plot.png

Usage:
  # Full Spark processing (10-20 minutes)
  python problem2.py spark://$MASTER_PRIVATE_IP:7077 --net-id YOUR-NET-ID

  # Skip Spark and regenerate visualizations from existing CSVs (fast)
  python problem2.py --skip-spark
"""

import argparse
import os
import sys
import shutil
import glob
from datetime import timedelta

# --- optional plotting deps ---
import matplotlib
matplotlib.use("Agg")  # no display
import matplotlib.pyplot as plt

try:
    import seaborn as sns
    _HAS_SEABORN = True
except Exception:
    _HAS_SEABORN = False

def ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)

def write_single_csv(df, final_path: str):
    """
    Write Spark DataFrame to a single CSV file (not a folder).
    Will write to `<final_path>.tmp/` then move part file to `final_path`.
    """
    import uuid
    parent = os.path.dirname(final_path)
    ensure_dir(parent if parent else ".")
    tmp_dir = f"{final_path}.tmp_{uuid.uuid4().hex}"

    # write as a single partition to a temp dir
    (df.coalesce(1)
       .write
       .option("header", True)
       .mode("overwrite")
       .csv(tmp_dir))

    # find the part file
    part_files = glob.glob(os.path.join(tmp_dir, "part-*.csv"))
    if not part_files:
        # Spark 4 sometimes writes '.csv' or no suffix; try broader match
        part_files = glob.glob(os.path.join(tmp_dir, "part-*"))
    if not part_files:
        raise RuntimeError(f"Could not find part file in {tmp_dir}")

    part_file = part_files[0]
    # move to the final_path
    if os.path.exists(final_path):
        os.remove(final_path)
    shutil.move(part_file, final_path)
    # cleanup
    shutil.rmtree(tmp_dir, ignore_errors=True)

def parse_args():
    p = argparse.ArgumentParser(description="Problem 2: Cluster Usage Analysis")
    p.add_argument("master", nargs="?", default=None,
                   help="Spark master URL (e.g., spark://host:7077). If omitted, use Spark default.")
    p.add_argument("--net-id", default="", help="Your NET ID (optional, unused in logic).")
    p.add_argument("--skip-spark", action="store_true",
                   help="Skip Spark processing and regenerate charts/stats from existing CSVs.")
    return p.parse_args()

def run_spark_job(master_url: str | None):
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F

    print("Initializing Spark session...")
    builder = (
        SparkSession.builder
        .appName("Problem2_ClusterUsageAnalysis")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.auth.IAMInstanceCredentialsProvider")
    )
    if master_url:
        builder = builder.master(master_url)

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    print("Spark session initialized.\n")

    # Input logs on S3 (read-only)
    input_path = "s3a://sw1451-spark-cluster-logs/data/*/*"
    print(f"Reading logs from: {input_path}")
    df = spark.read.text(input_path)

    # Attach file path for extracting IDs
    df = df.withColumn("path", F.input_file_name())

    # Extract fields from path:
    # path example:
    # s3a://.../data/application_1485248649253_0001/container_1485248649253_0001_01_000015.log
    df = df.withColumn(
        "application_id",
        F.regexp_extract(F.col("path"), r"(application_\d+_\d+)", 1)
    ).withColumn(
        "cluster_id",
        F.regexp_extract(F.col("application_id"), r"application_(\d+)_", 1)
    ).withColumn(
        "app_number",
        F.regexp_extract(F.col("application_id"), r"_(\d+)$", 1)
    )

    # Extract timestamp inside log line: e.g. 17/06/09 10:15:23
    df = df.withColumn(
        "timestamp_str",
        F.regexp_extract(F.col("value"), r"(\d{2}/\d{2}/\d{2}\s+\d{2}:\d{2}:\d{2})", 1)
    ).withColumn(
        "timestamp",
        F.to_timestamp(F.col("timestamp_str"), "yy/MM/dd HH:mm:ss")
    )

    # Compute start/end per application
    timeline = (
        df.groupBy("cluster_id", "application_id", "app_number")
          .agg(
              F.min("timestamp").alias("start_time"),
              F.max("timestamp").alias("end_time"),
          )
          .filter(F.col("start_time").isNotNull() & F.col("end_time").isNotNull())
    )

    # Format as string columns for CSV output
    timeline_csv = (
        timeline
        .select(
            "cluster_id",
            "application_id",
            "app_number",
            F.date_format("start_time", "yyyy-MM-dd HH:mm:ss").alias("start_time"),
            F.date_format("end_time",   "yyyy-MM-dd HH:mm:ss").alias("end_time"),
        )
        .orderBy("cluster_id", "app_number")
    )

    # Cluster summary: count + first/last application time
    cluster_summary = (
        timeline.groupBy("cluster_id")
        .agg(
            F.count("*").alias("num_applications"),
            F.date_format(F.min("start_time"), "yyyy-MM-dd HH:mm:ss").alias("cluster_first_app"),
            F.date_format(F.max("end_time"),   "yyyy-MM-dd HH:mm:ss").alias("cluster_last_app"),
        )
        .orderBy("cluster_id")
    )

    # Prepare output dir
    out_dir = "data/output"
    ensure_dir(out_dir)

    # Write CSVs as single files
    write_single_csv(timeline_csv, os.path.join(out_dir, "problem2_timeline.csv"))
    write_single_csv(cluster_summary, os.path.join(out_dir, "problem2_cluster_summary.csv"))

    print("\nSpark processing complete. CSVs written to data/output/.\n")

    # Collect to pandas for plotting & stats (this is small: one row per application / per cluster)
    timeline_pd = (
        timeline_csv
        .toPandas()
    )
    # duration seconds for plots
    from pandas import to_datetime
    timeline_pd["start_time"] = to_datetime(timeline_pd["start_time"])
    timeline_pd["end_time"]   = to_datetime(timeline_pd["end_time"])
    timeline_pd["duration_sec"] = (timeline_pd["end_time"] - timeline_pd["start_time"]).dt.total_seconds()

    cluster_pd = cluster_summary.toPandas()

    spark.stop()
    return timeline_pd, cluster_pd

def make_visuals_and_stats(timeline_pd, cluster_pd, out_dir="data/output"):
    ensure_dir(out_dir)

    # ===== Stats text =====
    total_clusters = cluster_pd["cluster_id"].nunique()
    total_apps = len(timeline_pd)
    avg_apps_per_cluster = (cluster_pd["num_applications"].mean() if len(cluster_pd) > 0 else 0.0)

    # Rank clusters by num_applications desc
    cluster_sorted = cluster_pd.sort_values("num_applications", ascending=False)

    stats_lines = []
    stats_lines.append(f"Total unique clusters: {total_clusters}")
    stats_lines.append(f"Total applications: {total_apps}")
    stats_lines.append(f"Average applications per cluster: {avg_apps_per_cluster:.2f}")
    stats_lines.append("")
    stats_lines.append("Most heavily used clusters:")
    for _, row in cluster_sorted.head(10).iterrows():
        stats_lines.append(f"  Cluster {row['cluster_id']}: {int(row['num_applications'])} applications")

    stats_path = os.path.join(out_dir, "problem2_stats.txt")
    with open(stats_path, "w") as f:
        f.write("\n".join(stats_lines))

    # ===== Bar chart: num apps per cluster =====
    fig = plt.figure(figsize=(10, 5))
    ax = fig.add_subplot(111)
    if _HAS_SEABORN:
        sns.barplot(data=cluster_sorted, x="cluster_id", y="num_applications", ax=ax)
    else:
        ax.bar(cluster_sorted["cluster_id"].astype(str), cluster_sorted["num_applications"])

    ax.set_xlabel("Cluster ID")
    ax.set_ylabel("Applications")
    ax.set_title("Applications per Cluster")

    # Add value labels
    for i, v in enumerate(cluster_sorted["num_applications"]):
        ax.text(i, v, str(int(v)), ha="center", va="bottom", fontsize=8)

    plt.xticks(rotation=45, ha="right")
    plt.tight_layout()
    bar_path = os.path.join(out_dir, "problem2_bar_chart.png")
    plt.savefig(bar_path, dpi=150)
    plt.close(fig)

    # ===== Density Plot: durations for largest cluster =====
    if len(cluster_sorted) > 0 and "duration_sec" in timeline_pd.columns:
        top_cluster = str(cluster_sorted.iloc[0]["cluster_id"])
        subset = timeline_pd[timeline_pd["cluster_id"] == top_cluster].copy()
        subset = subset[(subset["duration_sec"].notnull()) & (subset["duration_sec"] > 0)]
        n = len(subset)

        fig2 = plt.figure(figsize=(10, 5))
        ax2 = fig2.add_subplot(111)

        if n > 0:
            # Histogram + KDE if seaborn available
            if _HAS_SEABORN:
                sns.histplot(subset["duration_sec"], bins=50, kde=True, ax=ax2)
            else:
                ax2.hist(subset["duration_sec"], bins=50)

            ax2.set_xscale("log")  # handle skew
            ax2.set_xlabel("Duration (seconds, log scale)")
            ax2.set_ylabel("Frequency")
            ax2.set_title(f"Job Duration Distribution — Cluster {top_cluster} (n={n})")
            plt.tight_layout()
            density_path = os.path.join(out_dir, "problem2_density_plot.png")
            plt.savefig(density_path, dpi=150)
            plt.close(fig2)
        else:
            # create an empty figure with message
            ax2.text(0.5, 0.5, f"No valid durations for cluster {top_cluster}", ha="center", va="center")
            plt.tight_layout()
            density_path = os.path.join(out_dir, "problem2_density_plot.png")
            plt.savefig(density_path, dpi=150)
            plt.close(fig2)

def reload_from_csv_and_visualize(out_dir="data/output"):
    import pandas as pd
    # Load existing CSVs and regenerate plots/stats
    tl_path = os.path.join(out_dir, "problem2_timeline.csv")
    cs_path = os.path.join(out_dir, "problem2_cluster_summary.csv")
    if not (os.path.exists(tl_path) and os.path.exists(cs_path)):
        raise FileNotFoundError(
            f"Missing CSVs in {out_dir}. Expected problem2_timeline.csv and problem2_cluster_summary.csv"
        )
    timeline_pd = pd.read_csv(tl_path, dtype={"cluster_id": str, "application_id": str, "app_number": str})
    cluster_pd = pd.read_csv(cs_path, dtype={"cluster_id": str})
    # Rebuild duration column for the density plot
    timeline_pd["start_time"] = pd.to_datetime(timeline_pd["start_time"])
    timeline_pd["end_time"]   = pd.to_datetime(timeline_pd["end_time"])
    timeline_pd["duration_sec"] = (timeline_pd["end_time"] - timeline_pd["start_time"]).dt.total_seconds()
    make_visuals_and_stats(timeline_pd, cluster_pd, out_dir=out_dir)

def main():
    args = parse_args()
    out_dir = "data/output"
    ensure_dir(out_dir)

    if args.skip_spark:
        print("Skipping Spark. Regenerating visualizations/stats from existing CSVs...")
        reload_from_csv_and_visualize(out_dir=out_dir)
        print("Done. Files are in data/output/")
        return

    # Run Spark job (reads from S3, writes local CSVs), then do visuals & stats
    timeline_pd, cluster_pd = run_spark_job(args.master)
    make_visuals_and_stats(timeline_pd, cluster_pd, out_dir=out_dir)
    print("All outputs generated in data/output/")

if __name__ == "__main__":
    main()