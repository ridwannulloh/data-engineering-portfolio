"""Synthetic PaySim-style mobile-money transaction generator.

Mirrors the exact schema of the public PaySim dataset (Kaggle: ealaxi/paysim1)
so the pipeline can be pointed at the real 6.3M-row file with zero code
changes — while the built-in generator keeps the demo laptop-sized, seeded
(fully reproducible) and free of Kaggle credentials.

Fraud pattern modelled (same as PaySim): account takeover. The fraudster
TRANSFERs or CASH_OUTs the victim's balance — usually all of it — and the
stolen funds never appear on the destination (mule) account's recorded
balances. Two realistic confounders are injected on purpose so the Gold-layer
detection rules can't be trivially perfect: some fraudsters only take 80% of
the balance, and some honest customers legitimately empty their accounts.
"""
import os

import numpy as np
import pandas as pd

from src import config

TXN_TYPES = ["PAYMENT", "TRANSFER", "CASH_OUT", "CASH_IN", "DEBIT"]
TXN_TYPE_WEIGHTS = [0.36, 0.09, 0.33, 0.19, 0.03]
SIMULATION_HOURS = 744  # 31 days, one "step" per hour — PaySim convention
DEFAULT_NUM_ROWS = 50_000
DEFAULT_FRAUD_RATE = 0.008


def _account_ids(prefix: str, rng: np.random.Generator, n: int) -> np.ndarray:
    return np.array([f"{prefix}{i:09d}" for i in rng.integers(0, 1_000_000_000, n)])


def _legit_transactions(n: int, rng: np.random.Generator) -> pd.DataFrame:
    txn_type = rng.choice(TXN_TYPES, size=n, p=TXN_TYPE_WEIGHTS)
    amount = np.round(rng.lognormal(mean=6.0, sigma=1.3, size=n), 2)  # median ≈ $400
    old_orig = np.round(rng.lognormal(mean=7.5, sigma=1.6, size=n), 2)

    inflow = txn_type == "CASH_IN"
    new_orig = np.where(inflow, old_orig + amount, np.maximum(old_orig - amount, 0.0))

    # ~0.5% of honest customers empty their account in one go — keeps the
    # "full balance drain" signal from being a perfect fraud separator.
    drain = (~inflow) & (rng.random(n) < 0.005)
    amount = np.where(drain, old_orig, amount)
    new_orig = np.where(drain, 0.0, new_orig)

    to_merchant = np.isin(txn_type, ["PAYMENT", "DEBIT"])
    dest = np.where(to_merchant, _account_ids("M", rng, n), _account_ids("C", rng, n))
    # PaySim quirk preserved: merchant balances are not tracked (always 0).
    old_dest = np.where(to_merchant, 0.0, np.round(rng.lognormal(7.5, 1.6, n), 2))
    new_dest = np.where(to_merchant, 0.0, np.round(old_dest + amount, 2))

    return pd.DataFrame(
        {
            "step": rng.integers(1, SIMULATION_HOURS + 1, n),
            "type": txn_type,
            "amount": np.round(amount, 2),
            "nameOrig": _account_ids("C", rng, n),
            "oldbalanceOrg": old_orig,
            "newbalanceOrig": np.round(new_orig, 2),
            "nameDest": dest,
            "oldbalanceDest": old_dest,
            "newbalanceDest": new_dest,
            "isFraud": 0,
        }
    )


def _fraud_transactions(n: int, rng: np.random.Generator) -> pd.DataFrame:
    txn_type = rng.choice(["TRANSFER", "CASH_OUT"], size=n)
    # Victims skew wealthy — fraudsters pick targets. Median balance ≈ $80k.
    old_orig = np.round(rng.lognormal(mean=11.3, sigma=0.9, size=n), 2)

    # Most takeovers drain the full balance; ~15% grab 80% to vary the pattern.
    partial = rng.random(n) < 0.15
    amount = np.where(partial, np.round(old_orig * 0.8, 2), old_orig)
    new_orig = np.round(old_orig - amount, 2)

    # Fraud clusters in the small hours (00:00–05:59) — gives the hourly
    # Gold table a clear, demo-friendly story.
    day = rng.integers(0, SIMULATION_HOURS // 24, n)
    night_hour = rng.integers(0, 6, n)

    return pd.DataFrame(
        {
            "step": day * 24 + night_hour + 1,
            "type": txn_type,
            "amount": amount,
            "nameOrig": _account_ids("C", rng, n),
            "oldbalanceOrg": old_orig,
            "newbalanceOrig": new_orig,
            "nameDest": _account_ids("C", rng, n),
            # Mule accounts: stolen funds never show on recorded balances.
            "oldbalanceDest": 0.0,
            "newbalanceDest": 0.0,
            "isFraud": 1,
        }
    )


def generate_transactions(
    num_rows: int = DEFAULT_NUM_ROWS,
    fraud_rate: float = DEFAULT_FRAUD_RATE,
    seed: int = 42,
) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    n_fraud = max(int(num_rows * fraud_rate), 1)
    df = pd.concat(
        [_legit_transactions(num_rows - n_fraud, rng), _fraud_transactions(n_fraud, rng)],
        ignore_index=True,
    )
    # PaySim's built-in naive control: flag transfers over 200k. Kept so the
    # Gold layer can show how much better the rule-based score does.
    df["isFlaggedFraud"] = ((df["type"] == "TRANSFER") & (df["amount"] > 200_000)).astype(int)
    return df.sample(frac=1.0, random_state=seed).reset_index(drop=True)


def run() -> str:
    """Airflow task entrypoint: write the raw CSV that Bronze will ingest."""
    num_rows = int(os.environ.get("DATA_NUM_ROWS", DEFAULT_NUM_ROWS))
    df = generate_transactions(num_rows=num_rows)
    output_path = config.raw_transactions_path()
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df.to_csv(output_path, index=False)
    print(
        f"Wrote {len(df):,} transactions "
        f"({int(df['isFraud'].sum()):,} fraudulent) to {output_path}"
    )
    return output_path


if __name__ == "__main__":
    run()
