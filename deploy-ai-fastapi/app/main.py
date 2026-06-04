import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, Depends, Request, HTTPException
from fastapi.responses import JSONResponse
from slowapi import _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded

from app.logging_config import setup_logging
from app.auth import verify_api_key
from app.rate_limiter import limiter, RATE_LIMIT
from app.model import load_model, is_model_loaded, predict, get_available_tickers, HORIZONS
from app.schemas import (
    PredictionRequest,
    PredictionResponse,
    StockListResponse,
    TickerInfo,
    HealthResponse,
    ErrorResponse,
)

# Configure logging before anything else
setup_logging()
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Load the ML models on startup."""
    logger.info("Starting up — loading stock prediction models...")
    try:
        load_model()
        logger.info("All models loaded successfully")
    except FileNotFoundError as e:
        logger.error("Model not found! %s", e)
    yield
    logger.info("Shutting down...")


app = FastAPI(
    title="NYSE Stock Price Prediction API",
    description=(
        "Predict S&P 500 stock close prices using XGBoost models trained on "
        "the NYSE Kaggle dataset. Supports multiple prediction horizons "
        "(1d, 7d, 1m, 3m, 6m, 1y) with API key authentication and rate limiting."
    ),
    version="2.0.0",
    lifespan=lifespan,
)

# Attach rate limiter
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)


# ──────────────────────────────── Health Check ────────────────────────────────


@app.get(
    "/health",
    response_model=HealthResponse,
    tags=["monitoring"],
    summary="Health check",
)
async def health_check():
    """Returns API health status and whether the models are loaded."""
    return HealthResponse(
        status="healthy",
        model_loaded=is_model_loaded(),
        available_horizons=list(HORIZONS.keys()),
        version="2.0.0",
    )


# ──────────────────────────────── Stock List ──────────────────────────────────


@app.get(
    "/stocks",
    response_model=StockListResponse,
    responses={401: {"model": ErrorResponse}, 403: {"model": ErrorResponse}},
    tags=["stocks"],
    summary="List available stock tickers",
)
@limiter.limit(RATE_LIMIT)
async def list_stocks(
    request: Request,
    api_key: str = Depends(verify_api_key),
):
    """Returns all available S&P 500 tickers with their last known close price."""
    tickers = get_available_tickers()
    return StockListResponse(
        count=len(tickers),
        tickers=[TickerInfo(**t) for t in tickers],
    )


# ──────────────────────────────── Prediction ──────────────────────────────────


@app.post(
    "/predict",
    response_model=PredictionResponse,
    responses={
        400: {"model": ErrorResponse},
        401: {"model": ErrorResponse},
        403: {"model": ErrorResponse},
        429: {"model": ErrorResponse},
        503: {"model": ErrorResponse},
    },
    tags=["prediction"],
    summary="Predict stock close price",
)
@limiter.limit(RATE_LIMIT)
async def predict_stock(
    request: Request,
    body: PredictionRequest,
    api_key: str = Depends(verify_api_key),
):
    """
    Predict the future close price for a given S&P 500 stock ticker.

    - **ticker**: Stock symbol (e.g. AAPL, GOOGL, MSFT)
    - **horizon**: Prediction window — 1d, 7d, 1m, 3m, 6m, or 1y

    Requires a valid API key via `X-API-Key` header.
    Rate-limited to prevent abuse.
    """
    if not is_model_loaded():
        logger.error("Prediction requested but models are not loaded")
        return JSONResponse(
            status_code=503,
            content={"detail": "Models not available. Please try again later."},
        )

    logger.info(
        "Prediction request from key=%s... | ticker=%s horizon=%s",
        api_key[:8],
        body.ticker,
        body.horizon.value,
    )

    try:
        result = await predict(body.ticker, body.horizon.value)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    logger.info(
        "Prediction: %s %s → $%.2f (%+.2f%%)",
        result["ticker"],
        result["horizon"],
        result["predicted_close"],
        result["predicted_change_pct"],
    )

    return PredictionResponse(**result)
