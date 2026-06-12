import pandas as pd
from pyspark.sql import functions as F

from src import config
from src.bronze.ingest_transactions import run as bronze_run
from src.generate_data import generate_transactions
from src.generate_data import run as generate_run
from src.gold.fraud_metrics import run as gold_run
from src.silver.clean_transactions import HIGH_RISK_TYPES
from src.silver.clean_transactions import run as silver_run


class TestGenerator:
    def test_deterministic_with_seed(self):
        df_a = generate_transactions(num_rows=2_000, seed=7)
        df_b = generate_transactions(num_rows=2_000, seed=7)
        pd.testing.assert_frame_equal(df_a, df_b)

    def test_fraud_profile_matches_paysim(self):
        df = generate_transactions(num_rows=10_000, fraud_rate=0.01, seed=7)
        fraud = df[df["isFraud"] == 1]

        assert len(df) == 10_000
        assert len(fraud) == 100
        # PaySim invariant: fraud only happens via TRANSFER / CASH_OUT.
        assert set(fraud["type"].unique()) <= set(HIGH_RISK_TYPES)
        # Most (but deliberately not all) fraud drains the full balance.
        full_drain = (fraud["amount"] == fraud["oldbalanceOrg"]).mean()
        assert 0.7 < full_drain < 1.0
        # Mule accounts never show the stolen funds.
        assert (fraud["newbalanceDest"] == 0).all()


class TestPipelineEndToEnd:
    def test_bronze_silver_gold(self, spark, lakehouse_env):
        generate_run()

        assert bronze_run(spark) == 3_000

        silver_count = silver_run(spark)
        assert 0 < silver_count <= 3_000
        silver = spark.read.format("delta").load(config.silver_path())
        assert silver.filter(F.col("amount") <= 0).count() == 0
        assert "full_balance_drain" in silver.columns
        assert "dest_balance_stale" in silver.columns

        metrics = gold_run(spark)
        # The rules must beat a coin flip by a wide margin on seeded data.
        assert metrics["precision"] > 0.5
        assert metrics["recall"] > 0.5
        assert metrics["true_positives"] > 0

        by_type = spark.read.format("delta").load(config.gold_path("fraud_by_type"))
        fraud_types = {
            row["txn_type"]
            for row in by_type.filter(F.col("fraud_count") > 0).collect()
        }
        assert fraud_types <= set(HIGH_RISK_TYPES)

        perf = spark.read.format("delta").load(
            config.gold_path("detection_performance")
        )
        assert perf.count() == 1
