"""Gold: business-facing fraud analytics — four Delta tables a fraud-ops
team would actually consume.

  fraud_by_type           exposure and confirmed losses per transaction type
  hourly_fraud_pattern    when fraud happens (night-time concentration)
  high_risk_accounts      accounts flagged by the rule-based risk score,
                          ranked by money at risk — the daily review queue
  detection_performance   precision / recall / F1 of the rules vs. labels

The detector is deliberately rule-based, not ML: rules are explainable to a
compliance team, need no training pipeline, and set an honest baseline that
any future model has to beat. Labels (is_fraud) are used only for the
performance evaluation table, never as an input to the score.
"""
from datetime import datetime, timezone

from pyspark.sql import DataFrame, Row, SparkSession
from pyspark.sql import functions as F

from src import config
from src.spark_session import get_spark

# Weights sum to 100. Threshold of 70 requires the strongest signal (full
# drain) plus corroboration — single weak signals never flag on their own.
RISK_WEIGHTS = {
    "full_balance_drain": 40,
    "dest_balance_stale": 30,
    "is_high_risk_type": 15,
    "amount_above_p95": 15,
}
RISK_THRESHOLD = 70


def _score_transactions(silver: DataFrame, p95_amount: float) -> DataFrame:
    scored = silver.withColumn("amount_above_p95", F.col("amount") >= F.lit(p95_amount))
    risk_expr = sum(
        F.when(F.col(signal), weight).otherwise(0)
        for signal, weight in RISK_WEIGHTS.items()
    )
    return scored.withColumn("risk_score", risk_expr).withColumn(
        "is_alert", F.col("risk_score") >= RISK_THRESHOLD
    )


def _write_gold(df: DataFrame, table_name: str) -> None:
    df.write.format("delta").mode("overwrite").save(config.gold_path(table_name))


def run(spark: SparkSession | None = None) -> dict:
    """Airflow task entrypoint. Returns the detection metrics dict."""
    owns_spark = spark is None
    if owns_spark:
        spark = get_spark("gold_fraud_metrics")
    try:
        silver = spark.read.format("delta").load(config.silver_path())
        p95_amount = silver.approxQuantile("amount", [0.95], 0.001)[0]
        scored = _score_transactions(silver, p95_amount).cache()

        # ── Table 1: exposure per transaction type ──────────────────────
        fraud_by_type = (
            scored.groupBy("txn_type")
            .agg(
                F.count("*").alias("txn_count"),
                F.round(F.sum("amount"), 2).alias("total_amount"),
                F.sum("is_fraud").alias("fraud_count"),
                F.round(
                    F.sum(F.when(F.col("is_fraud") == 1, F.col("amount")).otherwise(0)),
                    2,
                ).alias("fraud_amount"),
            )
            .withColumn(
                "fraud_rate_pct",
                F.round(F.col("fraud_count") * 100.0 / F.col("txn_count"), 3),
            )
            .orderBy(F.desc("fraud_amount"))
        )
        _write_gold(fraud_by_type, "fraud_by_type")

        # ── Table 2: when does fraud happen ─────────────────────────────
        hourly_fraud = (
            scored.groupBy("hour_of_day")
            .agg(
                F.count("*").alias("txn_count"),
                F.sum("is_fraud").alias("fraud_count"),
            )
            .withColumn(
                "fraud_rate_pct",
                F.round(F.col("fraud_count") * 100.0 / F.col("txn_count"), 3),
            )
            .orderBy("hour_of_day")
        )
        _write_gold(hourly_fraud, "hourly_fraud_pattern")

        # ── Table 3: the review queue, ranked by money at risk ──────────
        high_risk_accounts = (
            scored.filter(F.col("is_alert"))
            .groupBy("orig_account")
            .agg(
                F.count("*").alias("alert_txns"),
                F.round(F.sum("amount"), 2).alias("amount_at_risk"),
                F.max("risk_score").alias("max_risk_score"),
                F.collect_set("txn_type").alias("txn_types"),
            )
            .orderBy(F.desc("amount_at_risk"))
        )
        _write_gold(high_risk_accounts, "high_risk_accounts")

        # ── Table 4: how good are the rules, honestly ───────────────────
        alert = F.col("is_alert")
        fraud = F.col("is_fraud") == 1
        cm = scored.agg(
            F.sum(F.when(alert & fraud, 1).otherwise(0)).alias("tp"),
            F.sum(F.when(alert & ~fraud, 1).otherwise(0)).alias("fp"),
            F.sum(F.when(~alert & fraud, 1).otherwise(0)).alias("fn"),
            F.sum(F.when(~alert & ~fraud, 1).otherwise(0)).alias("tn"),
        ).first()

        precision = cm.tp / (cm.tp + cm.fp) if (cm.tp + cm.fp) else 0.0
        recall = cm.tp / (cm.tp + cm.fn) if (cm.tp + cm.fn) else 0.0
        f1 = (
            2 * precision * recall / (precision + recall)
            if (precision + recall)
            else 0.0
        )
        metrics = {
            "model": "rule_based_v1",
            "risk_threshold": RISK_THRESHOLD,
            "true_positives": cm.tp,
            "false_positives": cm.fp,
            "false_negatives": cm.fn,
            "true_negatives": cm.tn,
            "precision": round(precision, 4),
            "recall": round(recall, 4),
            "f1_score": round(f1, 4),
        }
        perf_df = spark.createDataFrame(
            [Row(**metrics, scored_at=datetime.now(timezone.utc))]
        )
        _write_gold(perf_df, "detection_performance")

        print("\n=== GOLD: fraud exposure by transaction type ===")
        fraud_by_type.show(truncate=False)
        print("=== GOLD: hourly fraud pattern ===")
        hourly_fraud.show(24, truncate=False)
        print("=== GOLD: top high-risk accounts (review queue) ===")
        high_risk_accounts.show(10, truncate=False)
        print("=== GOLD: rule-based detection performance ===")
        perf_df.show(truncate=False)

        scored.unpersist()
        return metrics
    finally:
        if owns_spark:
            spark.stop()


if __name__ == "__main__":
    run()
