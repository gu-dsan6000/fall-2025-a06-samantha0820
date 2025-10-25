from pyspark.sql import SparkSession
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import os
import re
from datetime import datetime
import argparse

# Parse command-line arguments
parser = argparse.ArgumentParser()
parser.add_argument("mode", help="local[*] or spark://<MASTER_IP>:7077")
parser.add_argument("--net-id", required=True)
parser.add_argument("--skip-spark", action="store_true")
args = parser.parse_args()

# Define input and output directories
base_dir = "data/raw" if "spark://" in args.mode else "data/sample"
out_dir = "data/output"
os.makedirs(out_dir, exist_ok=True)

# Initialize Spark session only when needed
if not args.skip_spark:
    spark = (
        SparkSession.builder
        .appName("Problem2_ClusterUsage")
        .config("spark.driver.memory", "8g")
        .config("spark.executor.memory", "4g")
        .getOrCreate()
    )
    df = spark.read.text(base_dir)
    df.limit(50).show(truncate=False)  # sanity check to confirm reading works
    spark.stop()

# Extract cluster ID, application number, and timestamps from logs
pattern = re.compile(r"application_(\d+)_(\d+)")
timeline = []

for root, _, files in os.walk(base_dir):
    m = pattern.search(root)
    if not m:
        continue
    cluster_id, app_num = m.groups()
    for f in files:
        if f.endswith(".log"):
            path = os.path.join(root, f)
            with open(path, "r", errors="ignore") as fh:
                lines = fh.readlines()
                if not lines:
                    continue
                timestamps = re.findall(r"\d{2}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}", "\n".join(lines))
                if timestamps:
                    start_dt = datetime.strptime(timestamps[0], "%y/%m/%d %H:%M:%S")
                    end_dt = datetime.strptime(timestamps[-1], "%y/%m/%d %H:%M:%S")
                    timeline.append([
                        cluster_id,
                        f"application_{cluster_id}_{app_num}",
                        app_num,
                        start_dt,
                        end_dt
                    ])

if not timeline:
    print("No valid log entries found. Please check the data/raw directory.")
    exit()

# Create timeline dataframe
timeline_df = pd.DataFrame(
    timeline,
    columns=["cluster_id", "application_id", "app_number", "start_time", "end_time"]
)
timeline_df.to_csv(os.path.join(out_dir, "problem2_timeline.csv"), index=False)

# Summarize by cluster
summary = (
    timeline_df.groupby("cluster_id")
    .agg(
        num_applications=("application_id", "count"),
        cluster_first_app=("start_time", "min"),
        cluster_last_app=("end_time", "max")
    )
    .reset_index()
)
summary.to_csv(os.path.join(out_dir, "problem2_cluster_summary.csv"), index=False)

# Write summary statistics
total_clusters = len(summary)
total_apps = len(timeline_df)
avg_apps = total_apps / total_clusters if total_clusters > 0 else 0
top_clusters = summary.sort_values("num_applications", ascending=False).head(2)

with open(os.path.join(out_dir, "problem2_stats.txt"), "w") as f:
    f.write(f"Total unique clusters: {total_clusters}\n")
    f.write(f"Total applications: {total_apps}\n")
    f.write(f"Average applications per cluster: {avg_apps:.2f}\n\n")
    f.write("Most heavily used clusters:\n")
    for _, row in top_clusters.iterrows():
        f.write(f"  Cluster {row['cluster_id']}: {row['num_applications']} applications\n")

# Generate bar chart
plt.figure(figsize=(6, 4))
sns.barplot(x="cluster_id", y="num_applications", data=summary, palette="Set2")
plt.xticks(rotation=45, ha="right")
plt.title("Applications per Cluster")
plt.tight_layout()
plt.savefig(os.path.join(out_dir, "problem2_bar_chart.png"))
plt.close()

# Generate density plot for the largest cluster
largest_cluster = top_clusters.iloc[0]["cluster_id"]
subset = timeline_df[timeline_df["cluster_id"] == largest_cluster].copy()
subset["duration"] = (subset["end_time"] - subset["start_time"]).dt.total_seconds()

plt.figure(figsize=(6, 4))
sns.histplot(subset["duration"], kde=True, log_scale=True)
plt.title(f"Job Duration Distribution (Cluster {largest_cluster})")
plt.tight_layout()
plt.savefig(os.path.join(out_dir, "problem2_density_plot.png"))
plt.close()
