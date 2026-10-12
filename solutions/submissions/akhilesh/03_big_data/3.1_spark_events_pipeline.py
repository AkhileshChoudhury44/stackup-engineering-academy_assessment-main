"""
StackUp Engineering Academy — Data Engineering Assessment
Pillar 3: Big Data Processing — Task 3.1: Distributed Event Processing with PySpark
Trainee: Akhilesh
File: 3.1_spark_events_pipeline.py
"""

import os
import time
import logging
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType, MapType
)

# ── Logging Setup ─────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger(__name__)

# ── Paths ─────────────────────────────────────────────────────────────────────
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT   = os.path.abspath(os.path.join(CURRENT_DIR, "..", "..", "..", ".."))
EVENTS_DIR  = os.path.join(REPO_ROOT, "datasets", "events_stream")
OUTPUT_DIR  = os.path.join(REPO_ROOT, "outputs", "results", "Akhilesh", "03_big_data", "spark")
os.makedirs(OUTPUT_DIR, exist_ok=True)


def get_spark_session() -> SparkSession:
    """
    Initialize an optimized local SparkSession using all available cores.
    Explicitly enforces UTC timezone to eliminate environment-specific timestamp shifts.
    """
    logger.info("Initializing SparkSession...")
    spark = (
        SparkSession.builder
        .appName("PresightEventsProcessing")
        .master("local[*]")
        .config("spark.driver.memory", "4g")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def load_events(spark: SparkSession, events_dir: str):
    """
    Task 3.1a: Load all 12 monthly JSONL files using wildcard and explicit StructType schema.
    Avoids expensive schema inference over 100,000 JSON records.
    """
    logger.info("Loading event stream files from: %s", events_dir)
    
    # Explicit schema definition
    schema = StructType([
        StructField("event_id", StringType(), False),
        StructField("event_type", StringType(), False),
        StructField("project_id", StringType(), True),
        StructField("user_id", StringType(), False),
        StructField("timestamp", TimestampType(), False),
        StructField("payload", MapType(StringType(), StringType()), True)
    ])

    wildcard_path = os.path.join(events_dir, "events_*.jsonl")
    df = spark.read.schema(schema).json(wildcard_path)
    logger.info("Initial loaded event count: %d rows", df.count())
    return df


def validate_events(df):
    """
    Task 3.1b: Schema cleaning, deduplication, null removal, and temporal feature engineering.
    """
    logger.info("Validating and cleaning raw event stream...")
    initial_count = df.count()

    # 1. Drop rows where event_id or user_id is null
    df_valid = df.filter(F.col("event_id").isNotNull() & F.col("user_id").isNotNull())
    valid_count = df_valid.count()
    logger.info("Dropped %d rows with null IDs", initial_count - valid_count)

    # 2. Drop duplicate event_ids (keep first occurrence ordered by timestamp)
    window_spec = Window.partitionBy("event_id").orderBy(F.col("timestamp").asc())
    df_dedup = (
        df_valid.withColumn("row_num", F.row_number().over(window_spec))
        .filter(F.col("row_num") == 1)
        .drop("row_num")
    )
    dedup_count = df_dedup.count()
    logger.info("Dropped %d duplicate event records", valid_count - dedup_count)

    # 3. Add derived temporal columns
    df_cleaned = (
        df_dedup
        .withColumn("event_date", F.to_date(F.col("timestamp")))
        .withColumn("event_hour", F.hour(F.col("timestamp")))
        .withColumn("event_month", F.date_format(F.col("timestamp"), "yyyy-MM"))
    )

    return df_cleaned


def build_project_activity_summary(df):
    """
    Task 3.1c - Table 1: Per-project activity aggregation.
    """
    logger.info("Generating project_activity_summary...")
    return (
        df.filter(F.col("project_id").isNotNull())
        .groupBy("project_id")
        .agg(
            F.count("*").alias("total_events"),
            F.count(F.when(F.col("event_type") == "escalation_raised", 1)).alias("escalation_count"),
            F.count(F.when(F.col("event_type") == "task_completed", 1)).alias("task_completions"),
            F.count(F.when(F.col("event_type") == "document_uploaded", 1)).alias("document_uploads"),
            F.max("timestamp").alias("last_event_timestamp"),
            F.countDistinct("user_id").alias("unique_users"),
            F.countDistinct("event_type").alias("unique_event_types")
        )
        .orderBy(F.col("total_events").desc())
    )


def build_user_activity_summary(df):
    """
    Task 3.1c - Table 2: Per-user platform engagement metrics.
    """
    logger.info("Generating user_activity_summary...")
    return (
        df.groupBy("user_id")
        .agg(
            F.count(F.when(F.col("event_type") == "login", 1)).alias("login_count"),
            F.count(F.when(F.col("event_type") == "logout", 1)).alias("logout_count"),
            F.count(F.when(~F.col("event_type").isin("login", "logout"), 1)).alias("actions_taken"),
            F.countDistinct("project_id").alias("projects_touched"),
            F.min("timestamp").alias("first_active"),
            F.max("timestamp").alias("last_active"),
            F.countDistinct("event_date").alias("active_days")
        )
        .orderBy(F.col("actions_taken").desc())
    )


def build_escalation_log(df):
    """
    Task 3.1c - Table 3: Lifecycle matching between escalation_raised and escalation_resolved.
    Enforces sequential 1-to-1 matching so one resolution is never matched to multiple escalations.
    """
    logger.info("Generating escalation_log (with 1-to-1 sequential resolution matching)...")
    raised = (
        df.filter(F.col("event_type") == "escalation_raised")
        .select(
            F.col("event_id").alias("event_id"),
            F.col("project_id"),
            F.col("user_id").alias("raised_by"),
            F.col("timestamp").alias("raised_at"),
            F.coalesce(F.col("payload")["severity"], F.lit("Medium")).alias("severity")
        )
    )

    resolved = (
        df.filter(F.col("event_type") == "escalation_resolved")
        .select(
            F.col("project_id"),
            F.coalesce(F.col("payload")["resolved_by"], F.col("user_id")).alias("resolved_by"),
            F.col("timestamp").alias("resolved_at"),
            F.col("payload")["resolution"].alias("resolution")
        )
    )

    # Assign chronological sequence numbers within each project to pair escalations 1-to-1 with resolutions
    raised_seq = Window.partitionBy("project_id").orderBy(F.col("raised_at").asc())
    raised_with_seq = raised.withColumn("seq_id", F.row_number().over(raised_seq))

    resolved_seq = Window.partitionBy("project_id").orderBy(F.col("resolved_at").asc())
    resolved_with_seq = resolved.withColumn("seq_id", F.row_number().over(resolved_seq))

    # Join on project_id AND seq_id (ensures each resolution is matched to exactly ONE escalation)
    joined = (
        raised_with_seq.join(
            resolved_with_seq,
            on=["project_id", "seq_id"],
            how="left"
        )
        .drop("seq_id")
        .withColumn("resolved", F.col("resolved_at").isNotNull())
        .withColumn(
            "resolution_time_hours",
            F.when(
                F.col("resolved") == True,
                F.round((F.unix_timestamp("resolved_at") - F.unix_timestamp("raised_at")) / 3600.0, 2)
            ).otherwise(F.lit(None))
        )
    )

    return joined


def build_daily_event_volume(df):
    """
    Task 3.1c - Table 4: Daily event volumes with cumulative window running totals.
    """
    logger.info("Generating daily_event_volume...")
    daily_base = (
        df.groupBy("event_date", "event_type")
        .agg(F.count("*").alias("event_count"))
    )

    window_cum = (
        Window.partitionBy("event_type")
        .orderBy("event_date")
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )

    return (
        daily_base
        .withColumn("cumulative_count", F.sum("event_count").over(window_cum))
        .orderBy(F.col("event_date").asc(), F.col("event_count").desc())
    )


def build_peak_usage_analysis(df):
    """
    Task 3.1c - Table 5: Hourly utilization and peak concurrency hotspots.
    """
    logger.info("Generating peak_usage_analysis...")
    return (
        df.groupBy("event_date", "event_hour")
        .agg(
            F.count("*").alias("total_events"),
            F.countDistinct("user_id").alias("unique_users"),
            F.countDistinct("event_type").alias("event_types_per_hour")
        )
        .orderBy(F.col("total_events").desc())
        .limit(20)
    )


def write_outputs(df, name: str, partition_cols=None):
    """
    Task 3.1d: Parquet serialization with overwrite semantics and partition layout.
    """
    target_path = os.path.join(OUTPUT_DIR, name)
    logger.info("Writing Parquet output for %s to %s", name, target_path)
    
    writer = df.coalesce(1).write.mode("overwrite")
    if partition_cols:
        writer = writer.partitionBy(*partition_cols)
    
    writer.parquet(target_path)
    logger.info("Successfully persisted %s (count: %d)", name, df.count())


def run_pipeline():
    total_start = time.time()
    spark = get_spark_session()

    try:
        # Load and clean
        raw_events = load_events(spark, EVENTS_DIR)
        clean_events = validate_events(raw_events).cache()

        # Generate aggregated analytical views with per-aggregation timing (Task 3.1e)
        timings = {}

        t_agg = time.time()
        proj_summary = build_project_activity_summary(clean_events)
        timings["project_activity_summary"] = time.time() - t_agg

        t_agg = time.time()
        user_summary = build_user_activity_summary(clean_events)
        timings["user_activity_summary"] = time.time() - t_agg

        t_agg = time.time()
        esc_log = build_escalation_log(clean_events)
        timings["escalation_log"] = time.time() - t_agg

        t_agg = time.time()
        daily_vol = build_daily_event_volume(clean_events)
        timings["daily_event_volume"] = time.time() - t_agg

        t_agg = time.time()
        peak_usage = build_peak_usage_analysis(clean_events)
        timings["peak_usage_analysis"] = time.time() - t_agg

        # Write Parquet tables
        write_outputs(proj_summary, "project_activity_summary")
        write_outputs(user_summary, "user_activity_summary")
        write_outputs(esc_log, "escalation_log", partition_cols=["severity"])
        write_outputs(daily_vol, "daily_event_volume", partition_cols=["event_date"])
        write_outputs(peak_usage, "peak_usage_analysis")

        total_elapsed = time.time() - total_start
        total_rows = clean_events.count()
        throughput = total_rows / total_elapsed if total_elapsed > 0 else 0

        logger.info("=" * 60)
        logger.info("Spark Big Data Pipeline Execution Summary")
        logger.info("=" * 60)
        logger.info("Total Events Processed: %d rows", total_rows)
        logger.info("Total Execution Time:   %.2f seconds", total_elapsed)
        logger.info("Throughput:             %.2f events/second", throughput)
        logger.info("-" * 60)
        logger.info("Per-Aggregation Execution Timings (Task 3.1e):")
        for tbl_name, duration in timings.items():
            logger.info("  • %-26s : %.4f seconds", tbl_name, duration)
        logger.info("-" * 60)
        logger.info("Parquet Outputs Saved:  %s", OUTPUT_DIR)

    finally:
        spark.stop()
        logger.info("Spark context stopped.")


if __name__ == "__main__":
    run_pipeline()