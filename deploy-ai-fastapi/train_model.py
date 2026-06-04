"""
Train XGBoost models to predict NYSE S&P 500 stock close prices.

Downloads the Kaggle NYSE dataset, engineers features, and trains
one model per prediction horizon (1d, 7d, 1m, 3m, 6m, 1y).

Usage:
    1. Download the dataset from https://www.kaggle.com/datasets/dgawlik/nyse/data
    2. Place `prices-split-adjusted.csv` and `securities.csv` in the `data/` folder
    3. Run: python train_model.py
"""

import os
import warnings

import joblib
import numpy as np
import pandas as pd
from sklearn.model_selection import TimeSeriesSplit
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from xgboost import XGBRegressor

warnings.filterwarnings("ignore", category=FutureWarning)

# Prediction horizons in trading days
HORIZONS = {
    "1d": 1,
    "7d": 5,
    "1m": 21,
    "3m": 63,
    "6m": 126,
    "1y": 252,
}

DATA_DIR = "data"
MODEL_DIR = "model"


def load_data() -> pd.DataFrame:
    """Load and validate the prices dataset."""
    path = os.path.join(DATA_DIR, "prices-split-adjusted.csv")
    if not os.path.exists(path):
        raise FileNotFoundError(
            f"'{path}' not found. Download the NYSE dataset from "
            "https://www.kaggle.com/datasets/dgawlik/nyse/data "
            "and place prices-split-adjusted.csv in the data/ folder."
        )

    df = pd.read_csv(path, parse_dates=["date"])
    df.sort_values(["symbol", "date"], inplace=True)
    df.reset_index(drop=True, inplace=True)
    print(f"Loaded {len(df):,} rows for {df['symbol'].nunique()} tickers")
    return df


def engineer_features(group: pd.DataFrame) -> pd.DataFrame:
    """Create technical indicator features for a single stock."""
    df = group.copy()
    close = df["close"]
    volume = df["volume"]

    # Price returns
    df["return_1d"] = close.pct_change(1)
    df["return_5d"] = close.pct_change(5)
    df["return_10d"] = close.pct_change(10)
    df["return_20d"] = close.pct_change(20)

    # Simple Moving Averages
    for window in [5, 10, 20, 50]:
        df[f"sma_{window}"] = close.rolling(window).mean()
        df[f"close_to_sma_{window}"] = close / df[f"sma_{window}"]

    # Exponential Moving Averages (for MACD)
    df["ema_12"] = close.ewm(span=12).mean()
    df["ema_26"] = close.ewm(span=26).mean()
    df["macd"] = df["ema_12"] - df["ema_26"]
    df["macd_signal"] = df["macd"].ewm(span=9).mean()

    # RSI (14-day)
    delta = close.diff()
    gain = delta.clip(lower=0).rolling(14).mean()
    loss = (-delta.clip(upper=0)).rolling(14).mean()
    rs = gain / loss.replace(0, np.nan)
    df["rsi_14"] = 100 - (100 / (1 + rs))

    # Volatility
    df["volatility_20"] = df["return_1d"].rolling(20).std()

    # Volume features
    df["volume_sma_20"] = volume.rolling(20).mean()
    df["volume_ratio"] = volume / df["volume_sma_20"].replace(0, np.nan)

    # High-Low range
    df["hl_range_pct"] = (df["high"] - df["low"]) / close

    # Lag features (close price)
    for lag in [1, 2, 3, 5, 10]:
        df[f"close_lag_{lag}"] = close.shift(lag)

    return df


FEATURE_COLS = [
    "return_1d", "return_5d", "return_10d", "return_20d",
    "close_to_sma_5", "close_to_sma_10", "close_to_sma_20", "close_to_sma_50",
    "macd", "macd_signal",
    "rsi_14",
    "volatility_20",
    "volume_ratio",
    "hl_range_pct",
    "close_lag_1", "close_lag_2", "close_lag_3", "close_lag_5", "close_lag_10",
]


def create_target(df: pd.DataFrame, horizon_days: int) -> pd.Series:
    """Target = future close price (shifted back by horizon)."""
    return df.groupby("symbol")["close"].shift(-horizon_days)


def train_and_save():
    df = load_data()

    # Engineer features per stock
    print("Engineering features...")
    df = df.groupby("symbol", group_keys=False).apply(engineer_features)

    # Save latest features per ticker for API inference
    latest = df.groupby("symbol").last().reset_index()
    latest_features = latest[["symbol", "close", "date"] + FEATURE_COLS].copy()

    os.makedirs(MODEL_DIR, exist_ok=True)

    # Train one model per horizon
    for horizon_name, horizon_days in HORIZONS.items():
        print(f"\n{'='*60}")
        print(f"Training model for horizon: {horizon_name} ({horizon_days} trading days)")
        print(f"{'='*60}")

        df["target"] = create_target(df, horizon_days)

        # Drop rows with NaN features or target
        train_df = df.dropna(subset=FEATURE_COLS + ["target"])
        if len(train_df) < 100:
            print(f"  Skipping {horizon_name}: not enough data ({len(train_df)} rows)")
            continue

        X = train_df[FEATURE_COLS].values
        y = train_df["target"].values

        # Time-series split (no random shuffle — respect temporal order)
        split_idx = int(len(X) * 0.8)
        X_train, X_test = X[:split_idx], X[split_idx:]
        y_train, y_test = y[:split_idx], y[split_idx:]

        model = XGBRegressor(
            n_estimators=200,
            max_depth=6,
            learning_rate=0.05,
            subsample=0.8,
            colsample_bytree=0.8,
            random_state=42,
            n_jobs=-1,
        )
        model.fit(
            X_train, y_train,
            eval_set=[(X_test, y_test)],
            verbose=False,
        )

        # Evaluate
        y_pred = model.predict(X_test)
        mae = mean_absolute_error(y_test, y_pred)
        rmse = np.sqrt(mean_squared_error(y_test, y_pred))
        r2 = r2_score(y_test, y_pred)
        print(f"  MAE:  ${mae:.2f}")
        print(f"  RMSE: ${rmse:.2f}")
        print(f"  R²:   {r2:.4f}")

        # Save model
        model_path = os.path.join(MODEL_DIR, f"stock_model_{horizon_name}.joblib")
        joblib.dump(model, model_path)
        print(f"  Saved: {model_path}")

    # Save metadata
    tickers = sorted(latest_features["symbol"].unique().tolist())
    metadata = {
        "feature_cols": FEATURE_COLS,
        "horizons": HORIZONS,
        "tickers": tickers,
        "n_features": len(FEATURE_COLS),
    }
    joblib.dump(metadata, os.path.join(MODEL_DIR, "stock_metadata.joblib"))
    print(f"\nMetadata saved ({len(tickers)} tickers)")

    # Save latest feature snapshot for each ticker (used at inference time)
    latest_features.to_parquet(
        os.path.join(MODEL_DIR, "latest_features.parquet"), index=False
    )
    print(f"Latest features saved for {len(latest_features)} tickers")

    print("\nDone! All models saved to model/ directory.")


if __name__ == "__main__":
    train_and_save()
