# NYSE Stock Price Prediction API

A production-ready FastAPI application that predicts **S&P 500 stock close prices** using XGBoost models trained on the [NYSE Kaggle dataset](https://www.kaggle.com/datasets/dgawlik/nyse/data). Features async endpoints, API key authentication, rate limiting, and multiple prediction horizons.

## Project Structure

```
deploy-ai-fastapi/
├── app/
│   ├── __init__.py
│   ├── main.py                        # FastAPI app (routes, lifespan)
│   ├── auth.py                        # API key authentication
│   ├── rate_limiter.py                # Rate limiting (slowapi)
│   ├── schemas.py                     # Pydantic request/response models
│   ├── model.py                       # Model loading & prediction
│   └── logging_config.py             # Structured logging setup
├── data/                              # Kaggle dataset CSVs (you download)
│   ├── prices-split-adjusted.csv
│   └── securities.csv
├── model/                             # Generated model files (gitignored)
│   ├── stock_model_1d.joblib
│   ├── stock_model_7d.joblib
│   ├── stock_model_1m.joblib
│   ├── stock_model_3m.joblib
│   ├── stock_model_6m.joblib
│   ├── stock_model_1y.joblib
│   ├── stock_metadata.joblib
│   └── latest_features.parquet
├── train_model.py                     # Feature engineering + train XGBoost models
├── Dockerfile                         # Full build: copies data/ + retrains models
├── Dockerfile.prebuilt                # Fast build: uses existing model/ folder
├── Dockerfile.prebuilt.dockerignore   # Excludes data/, includes model/
├── docker-compose.yml                 # Compose for full retrain deployment
├── docker-compose.prebuilt.yml        # Compose for prebuilt model deployment
├── .env.example
├── .env                               # Your local environment config (gitignored)
├── requirements.txt
└── README.md
```

## Features

| Feature             | Details                                                     |
|---------------------|-------------------------------------------------------------|
| Model               | XGBoost regressors (one per horizon) trained on NYSE data   |
| Target              | **Close price** prediction                                  |
| Horizons            | `1d`, `7d`, `1m`, `3m`, `6m`, `1y`                         |
| Features            | 19 technical indicators (SMA, EMA, MACD, RSI, volatility)  |
| Async               | Fully async endpoints with FastAPI                          |
| Authentication      | API key via `X-API-Key` header                              |
| Rate Limiting       | 10 requests/minute per API key (configurable)               |
| Validation          | Pydantic v2 with enum horizon + ticker constraints          |
| Health Check        | `GET /health` — no auth required                            |
| Stock List          | `GET /stocks` — list available tickers                      |
| Logging             | Structured logging with timestamps                          |
| Docs                | Auto-generated at `/docs` (Swagger UI)                      |

## Quick Start (Local — Without Docker)

### 1. Download the dataset

Go to [https://www.kaggle.com/datasets/dgawlik/nyse/data](https://www.kaggle.com/datasets/dgawlik/nyse/data) and download the dataset. Place both files in the `data/` folder:

```
data/
├── prices-split-adjusted.csv
└── securities.csv
```

### 2. Install dependencies

```bash
cd deploy-ai-fastapi
pip install -r requirements.txt
```

### 3. Train the models

```bash
python train_model.py
```

This engineers 19 technical indicators, trains 6 XGBoost models (one per horizon), and saves everything to `model/`. Expected output:

```
Loaded 851,264 rows for 501 tickers
Engineering features...
Training model for horizon: 1d → MAE: $0.74  R²: 0.9988  ✓
Training model for horizon: 7d → MAE: $1.57  R²: 0.9954  ✓
Training model for horizon: 1m → MAE: $3.17  R²: 0.9833  ✓
Training model for horizon: 3m → MAE: $5.40  R²: 0.9546  ✓
Training model for horizon: 6m → MAE: $7.59  R²: 0.9102  ✓
Training model for horizon: 1y → MAE: $10.65 R²: 0.8239  ✓
Done! All models saved to model/ directory.
```

> **Only needs to be run once.** Re-run only when you want to retrain with new data.

### 4. Configure environment

```bash
cp .env.example .env
# Edit .env — set your API keys
```

`.env` format:
```env
API_KEYS=your-secret-key-1,your-secret-key-2
RATE_LIMIT=10/minute
MODEL_DIR=model
```

### 5. Run the server

```bash
uvicorn app.main:app --reload
```

API: `http://localhost:8000` | Swagger docs: `http://localhost:8000/docs`

## API Endpoints

### Health Check (no auth required)

```bash
curl http://localhost:8000/health
```

```json
{
  "status": "healthy",
  "model_loaded": true,
  "available_horizons": ["1d", "7d", "1m", "3m", "6m", "1y"],
  "version": "2.0.0"
}
```

### List Available Stocks (auth required)

```bash
curl http://localhost:8000/stocks \
  -H "X-API-Key: dev-api-key-123"
```

### Predict Stock Close Price (auth required)

```bash
curl -X POST http://localhost:8000/predict \
  -H "Content-Type: application/json" \
  -H "X-API-Key: dev-api-key-123" \
  -d '{"ticker": "AAPL", "horizon": "1m"}'
```

**Response:**
```json
{
  "ticker": "AAPL",
  "horizon": "1m",
  "horizon_trading_days": 21,
  "current_close": 115.82,
  "predicted_close": 118.45,
  "predicted_change_pct": 2.27,
  "direction": "UP"
}
```

### Prediction Horizons

| Horizon | Trading Days | Description      |
|---------|-------------|-------------------|
| `1d`    | 1           | Next trading day  |
| `7d`    | 5           | ~1 week ahead     |
| `1m`    | 21          | ~1 month ahead    |
| `3m`    | 63          | ~3 months ahead   |
| `6m`    | 126         | ~6 months ahead   |
| `1y`    | 252         | ~1 year ahead     |

## Technical Indicators Used

The model uses 19 engineered features per stock:

- **Price returns**: 1d, 5d, 10d, 20d percentage changes
- **SMA ratios**: Close/SMA for 5, 10, 20, 50-day windows
- **MACD**: 12/26 EMA crossover + signal line
- **RSI**: 14-day Relative Strength Index
- **Volatility**: 20-day rolling standard deviation of returns
- **Volume**: Volume/SMA-20 ratio
- **Range**: Daily high-low range as percentage of close
- **Lag features**: Close price at t-1, t-2, t-3, t-5, t-10

## Docker Deployment

There are **two Docker strategies** depending on whether you want to retrain models at build time or use pre-trained models.

---

### Strategy A — Prebuilt Models (Recommended for fast deploys)

Uses the `model/` folder you already trained locally. **No retraining during build (~2 min).**

**Prerequisites:** `model/` folder must exist locally (run `python train_model.py` first).

**With Docker Compose (recommended):**
```bash
docker-compose -f docker-compose.prebuilt.yml up --build -d
```

**With plain Docker:**
```bash
docker build -f Dockerfile.prebuilt -t stock-prediction-api .
docker run -d --name stock-api -p 8000:8000 --env-file .env stock-prediction-api
```

---

### Strategy B — Full Retrain on Build

Copies the `data/` folder into the image and **retrains all models from scratch (~10 min)**. Useful for fresh deployments or CI/CD pipelines where you always want the latest trained model baked in.

**Prerequisites:** `data/prices-split-adjusted.csv` and `data/securities.csv` must exist.

**With Docker Compose (recommended):**
```bash
docker-compose up --build -d
```

**With plain Docker:**
```bash
docker build -t stock-prediction-api .
docker run -d --name stock-api -p 8000:8000 --env-file .env stock-prediction-api
```

---

### Comparison

| | Strategy A (Prebuilt) | Strategy B (Retrain) |
|--|--|--|
| Build time | ~2 min | ~10 min |
| Requires `model/` locally | ✅ Yes | ❌ No |
| Requires `data/` locally | ❌ No | ✅ Yes |
| Image size | Smaller (no dataset) | Larger |
| Best for | Fast iteration & redeployment | Clean CI/CD, fresh environments |

---

### Common Docker Commands

```bash
# Stop and remove container
docker-compose -f docker-compose.prebuilt.yml down

# View logs
docker logs stock-api -f

# Rebuild after code changes (prebuilt)
docker-compose -f docker-compose.prebuilt.yml up --build -d

# Rebuild after retraining models (retrain)
docker-compose up --build -d
```

After running, visit:
- **API:** `http://localhost:8000`
- **Swagger UI:** `http://localhost:8000/docs`
- **Health check:** `http://localhost:8000/health`

---

## Deployment Guide

### Option A: GCP Cloud Run (Recommended — Simplest)

```bash
# Build and push
gcloud auth configure-docker us-central1-docker.pkg.dev
docker tag stock-api us-central1-docker.pkg.dev/<project-id>/stock-api/stock-api:latest
docker push us-central1-docker.pkg.dev/<project-id>/stock-api/stock-api:latest

# Deploy
gcloud run deploy stock-api \
  --image us-central1-docker.pkg.dev/<project-id>/stock-api/stock-api:latest \
  --port 8000 \
  --region us-central1 \
  --allow-unauthenticated \
  --set-env-vars "API_KEYS=your-prod-key" \
  --set-env-vars "RATE_LIMIT=10/minute" \
  --memory 1Gi \
  --cpu 1 \
  --max-instances 3
```

**Cost:** Pay-per-request. Free tier covers ~2M requests/month.

### Option B: AWS ECS + Fargate

```bash
# Push to ECR
aws ecr create-repository --repository-name stock-api
aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin <account-id>.dkr.ecr.us-east-1.amazonaws.com
docker tag stock-api:latest <account-id>.dkr.ecr.us-east-1.amazonaws.com/stock-api:latest
docker push <account-id>.dkr.ecr.us-east-1.amazonaws.com/stock-api:latest

# Create cluster and deploy
aws ecs create-cluster --cluster-name stock-api-cluster
aws ecs register-task-definition --cli-input-json file://task-def.json
aws ecs create-service \
  --cluster stock-api-cluster \
  --service-name stock-api-service \
  --task-definition stock-api \
  --desired-count 1 \
  --launch-type FARGATE \
  --network-configuration "awsvpcConfiguration={subnets=[subnet-xxx],securityGroups=[sg-xxx],assignPublicIp=ENABLED}"
```

**Cost:** ~$10-15/month for a small Fargate task.

### Option C: AWS Lambda (Serverless)

Add `mangum` to requirements and a handler:
```python
from mangum import Mangum
handler = Mangum(app)
```

**Cost:** Extremely cheap for low traffic. Free tier covers 1M requests/month.

---

## Production Checklist

- [ ] Use **secrets manager** (AWS Secrets Manager / GCP Secret Manager) for API keys
- [ ] Add **HTTPS** via load balancer or Cloud Run (auto)
- [ ] Set up **monitoring** (CloudWatch / Cloud Monitoring)
- [ ] Add **CI/CD** pipeline (GitHub Actions → build → push → deploy)
- [ ] Consider **model retraining pipeline** with fresh price data
- [ ] Add request **tracing** (OpenTelemetry) for production debugging

## Disclaimer

> This API is for **educational and portfolio purposes only**. Stock predictions from historical data are inherently unreliable. Do not use this for actual trading decisions. The dataset covers 2010-2016 and models reflect patterns from that era.
