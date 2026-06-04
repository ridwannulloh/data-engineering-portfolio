from enum import Enum
from pydantic import BaseModel, Field


class HorizonEnum(str, Enum):
    """Supported prediction horizons."""

    d1 = "1d"
    d7 = "7d"
    m1 = "1m"
    m3 = "3m"
    m6 = "6m"
    y1 = "1y"


class PredictionRequest(BaseModel):
    """Request body for stock close price prediction."""

    ticker: str = Field(
        ...,
        min_length=1,
        max_length=10,
        description="Stock ticker symbol (e.g. AAPL, GOOGL)",
        examples=["AAPL"],
    )
    horizon: HorizonEnum = Field(
        ...,
        description="Prediction horizon: 1d, 7d, 1m, 3m, 6m, or 1y",
        examples=["1m"],
    )


class PredictionResponse(BaseModel):
    """Response body for stock close price prediction."""

    ticker: str = Field(..., description="Stock ticker symbol")
    horizon: str = Field(..., description="Prediction horizon")
    horizon_trading_days: int = Field(
        ..., description="Number of trading days in the horizon"
    )
    current_close: float = Field(
        ..., description="Last known close price (USD)"
    )
    predicted_close: float = Field(
        ..., description="Predicted close price (USD)"
    )
    predicted_change_pct: float = Field(
        ..., description="Predicted percentage change"
    )
    direction: str = Field(
        ..., description="Predicted direction: UP or DOWN"
    )


class TickerInfo(BaseModel):
    ticker: str
    last_close: float
    last_date: str


class StockListResponse(BaseModel):
    """Response for the /stocks endpoint."""

    count: int
    tickers: list[TickerInfo]


class HealthResponse(BaseModel):
    """Response body for health check."""

    model_config = {"protected_namespaces": ()}

    status: str
    model_loaded: bool
    available_horizons: list[str]
    version: str


class ErrorResponse(BaseModel):
    """Standard error response."""

    detail: str
