from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import regexp_extract, col, rand
import subprocess

# === 1. Initialize Spark Session ===
print("Initializing Spark session...")
spark = (
    SparkSession.builder
    .appName("Problem1_LogLevelDistribution_Safe")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com")
    .config("spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.auth.IAMInstanceCredentialsProvider")
    .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
    .config("spark.hadoop.fs.s3a.committer.name", "directory")
    .config("spark.hadoop.fs.s3a.committer.magic.enabled", "true")
    .config("spark.driver.memory", "4g")
    .config("spark.executor.memory", "3g")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")
print("Spark session initialized.\n")

# === 2. List log files from S3 ===
print("Listing log files from S3 (this may take 5–15 seconds)...")
try:
    cmd = "aws s3 ls s3://sw1451-spark-cluster-logs/data/ --recursive | grep .log | awk '{print $4}'"
    result = subprocess.check_output(cmd, shell=True).decode().splitlines()
    if not result:
        raise Exception("No .log files found in S3 bucket path!")
    input_paths = [f"s3a://sw1451-spark-cluster-logs/{p}" for p in result]
    print(f"Total {len(input_paths)} log files found.\n")
except Exception as e:
    print("Error listing S3 files:", e)
    spark.stop()
    raise SystemExit

# === 3. Load log data in batches ===
print("Reading log data from S3 in batches...")
batch_size = 500
batches = [input_paths[i:i + batch_size] for i in range(0, len(input_paths), batch_size)]
dfs = []
total_lines = 0

for i, batch in enumerate(batches, start=1):
    print(f"Reading batch {i}/{len(batches)} ({len(batch)} files)...")
    df_batch = spark.read.text(batch)
    line_count = df_batch.count()
    total_lines += line_count
    dfs.append(df_batch)
    print(f"  Batch {i} read successfully with {line_count:,} lines.")

df = dfs[0]
for d in dfs[1:]:
    df = df.union(d)
print(f"\nTotal lines read across all batches: {total_lines:,}\n")

# === 4. Extract log levels ===
print("Extracting log levels...")
pattern = r"(?i)\b(INFO|WARN|ERROR|DEBUG)\b"
logs = df.withColumn("log_level", regexp_extract(col("value"), pattern, 1))
print("Log level extraction complete.\n")

# === 5. Count log levels ===
print("Counting log levels...")
counts = logs.groupBy("log_level").count().filter(col("log_level") != "")
counts.show(truncate=False)
print("Log level counting complete.\n")

# === 6. Get random samples ===
print("Selecting 10 random sample log entries...")
sample_logs = (
    logs.filter(col("log_level") != "")
        .orderBy(rand())
        .limit(10)
        .select(col("value").alias("log_entry"), "log_level")
)
sample_logs.show(truncate=False)
print("Random sampling complete.\n")

# === 7. Compute summary statistics ===
print("Computing summary statistics...")
labeled_lines = counts.agg({"count": "sum"}).first()[0]
unique_levels = counts.count()

summary_rows = counts.rdd.map(
    lambda r: f"{r['log_level']},{r['count']},{r['count']/labeled_lines*100:.2f}%"
).collect()

summary_text = f"""\
Total log lines processed: {total_lines}
Total lines with log levels: {labeled_lines}
Unique log levels found: {unique_levels}

Log level distribution:
""" + "\n".join(summary_rows)

print(summary_text)

# === 8. Write results to S3 ===
output_base = "s3a://sw1451-spark-cluster-logs/data/output/"
print(f"Writing outputs to {output_base} ...")

# 8.1 Counts (overwrite, coalesce to 1 file)
counts.coalesce(1).write.mode("overwrite").csv(output_base + "problem1_counts.csv", header=True)
print("  Counts written.")

# 8.2 Sample logs (overwrite, coalesce to 1 file)
sample_logs.coalesce(1).write.mode("overwrite").csv(output_base + "problem1_sample.csv", header=True)
print("  Sample logs written.")

# 8.3 Summary text (overwrite, single file)
summary_df = spark.createDataFrame([Row(summary=summary_text)])
summary_df.coalesce(1).write.mode("overwrite").text(output_base + "problem1_summary.txt")
print("  Summary written.")

print("\nAll outputs successfully written to S3.")
spark.stop()
print("Spark session stopped. Job complete.")