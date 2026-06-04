import os
import logging

import joblib
import numpy as np
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

MODEL_DIR = os.getenv("MODEL_DIR", "model")

# Prediction horizons in trading days
HORIZONS = {
    "1d": 1,
    "7d": 5,
    "1m": 21,
    "3m": 63,
    "6m": 126,
    "1y": 252,
}

_models: dict = {}          # horizon_name -> XGBRegressor
_metadata: dict | None = None
_latest_features: pd.DataFrame | None = None


def load_model():
    """Load all horizon models, metadata, and latest features from disk."""
    global _models, _metadata, _latest_features

    # Load metadata
    meta_path = os.path.join(MODEL_DIR, "stock_metadata.joblib")
    if not os.path.exists(meta_path):
        raise FileNotFoundError(
            f"Metadata not found at {meta_path}. Run 'python train_model.py' first."
        )
    _metadata = joblib.load(meta_path)
    logger.info("Metadata loaded: %d tickers, %d features", len(_metadata["tickers"]), _metadata["n_features"])

    # Load one model per horizon
    for horizon_name in HORIZONS:
        model_path = os.path.join(MODEL_DIR, f"stock_model_{horizon_name}.joblib")
        if os.path.exists(model_path):
            _models[horizon_name] = joblib.load(model_path)
            logger.info("Model loaded: %s", model_path)
        else:
            logger.warning("Model not found for horizon %s", horizon_name)

    # Load latest features snapshot
    features_path = os.path.join(MODEL_DIR, "latest_features.parquet")
    if os.path.exists(features_path):
        _latest_features = pd.read_parquet(features_path)
        logger.info("Latest features loaded for %d tickers", len(_latest_features))
    else:
        raise FileNotFoundError(f"Latest features not found at {features_path}")


def get_metadata() -> dict:
    if _metadata is None:
        load_model()
    return _metadata


def is_model_loaded() -> bool:
    return len(_models) > 0 and _metadata is not None


def get_available_tickers() -> list[dict]:
    """Return list of available tickers with their last close and date."""
    if _latest_features is None:
        return []
    result = []
    for _, row in _latest_features.iterrows():
        result.append({
            "ticker": row["symbol"],
            "last_close": round(float(row["close"]), 2),
            "last_date": str(row["date"].date()) if hasattr(row["date"], "date") else str(row["date"])[:10],
        })
    return result


async def predict(ticker: str, horizon: str) -> dict:
    """Predict the future close price for a given ticker and horizon."""
    if not is_model_loaded():
        raise RuntimeError("Models not loaded")

    ticker = ticker.upper()
    metadata = get_metadata()

    # Validate ticker
    if ticker not in metadata["tickers"]:
        raise ValueError(
            f"Ticker '{ticker}' not found. Use GET /stocks for available tickers."
        )

    # Validate horizon
    if horizon not in _models:
        raise ValueError(f"No model available for horizon '{horizon}'")

    # Get latest features for this ticker
    ticker_row = _latest_features[_latest_features["symbol"] == ticker]
    if ticker_row.empty:
        raise ValueError(f"No feature data available for '{ticker}'")

    feature_cols = metadata["feature_cols"]
    features = ticker_row[feature_cols].values.astype(np.float64)

    # Handle any NaN in features (fill with 0)
    features = np.nan_to_num(features, nan=0.0)

    current_close = float(ticker_row["close"].iloc[0])

    # Predict
    model = _models[horizon]
    predicted_close = float(model.predict(features)[0])
    predicted_close = round(predicted_close, 2)
    change_pct = round(((predicted_close - current_close) / current_close) * 100, 2)

    return {
        "ticker": ticker,
        "horizon": horizon,
        "horizon_trading_days": HORIZONS[horizon],
        "current_close": round(current_close, 2),
        "predicted_close": predicted_close,
        "predicted_change_pct": change_pct,
        "direction": "UP" if change_pct >= 0 else "DOWN",
    }
