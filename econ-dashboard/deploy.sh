#!/usr/bin/env bash
# =============================================================================
# deploy.sh
# One-command build + push + deploy to GCP Cloud Run
#
# Usage:
#   chmod +x deploy.sh
#   ./deploy.sh                          # deploy with defaults
#   ./deploy.sh --project my-project-id  # specify project
# =============================================================================

set -euo pipefail  # exit on error, undefined var, pipe failure

# ── Defaults (override with flags or env vars) ────────────────────────────────
PROJECT_ID="${GCP_PROJECT_ID:-YOUR_PROJECT_ID}"
REGION="${GCP_REGION:-us-central1}"
SERVICE_NAME="econ-dashboard"
IMAGE="gcr.io/${PROJECT_ID}/${SERVICE_NAME}"
MEMORY="512Mi"
CPU="1"
MIN_INSTANCES="0"   # scale to zero when idle (saves money)
MAX_INSTANCES="3"

# ── Parse flags ───────────────────────────────────────────────────────────────
while [[ $# -gt 0 ]]; do
    case $1 in
        --project|-p)  PROJECT_ID="$2"; IMAGE="gcr.io/${PROJECT_ID}/${SERVICE_NAME}"; shift 2 ;;
        --region|-r)   REGION="$2"; shift 2 ;;
        --help|-h)
            echo "Usage: ./deploy.sh [--project PROJECT_ID] [--region REGION]"
            exit 0
            ;;
        *) echo "Unknown flag: $1"; exit 1 ;;
    esac
done

echo ""
echo "=============================================="
echo "  Deploying: ${SERVICE_NAME}"
echo "  Project  : ${PROJECT_ID}"
echo "  Region   : ${REGION}"
echo "  Image    : ${IMAGE}"
echo "=============================================="
echo ""

# ── Step 1: Authenticate Docker with GCR ─────────────────────────────────────
echo "[1/4] Configuring Docker auth for GCR..."
gcloud auth configure-docker --quiet

# ── Step 2: Build image ───────────────────────────────────────────────────────
echo ""
echo "[2/4] Building Docker image..."
docker build \
    --tag "${IMAGE}:latest" \
    --label "git-commit=$(git rev-parse --short HEAD 2>/dev/null || echo 'unknown')" \
    --label "built-at=$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
    .

echo "  ✓ Image built: ${IMAGE}:latest"

# ── Step 3: Push to Container Registry ───────────────────────────────────────
echo ""
echo "[3/4] Pushing image to GCR..."
docker push "${IMAGE}:latest"
echo "  ✓ Image pushed"

# ── Step 4: Deploy to Cloud Run ───────────────────────────────────────────────
echo ""
echo "[4/4] Deploying to Cloud Run..."
gcloud run deploy "${SERVICE_NAME}" \
    --image "${IMAGE}:latest" \
    --platform managed \
    --region "${REGION}" \
    --allow-unauthenticated \
    --memory "${MEMORY}" \
    --cpu "${CPU}" \
    --min-instances "${MIN_INSTANCES}" \
    --max-instances "${MAX_INSTANCES}" \
    --port 8080 \
    --set-env-vars "GCP_PROJECT_ID=${PROJECT_ID}" \
    --quiet

# ── Get service URL ───────────────────────────────────────────────────────────
echo ""
SERVICE_URL=$(gcloud run services describe "${SERVICE_NAME}" \
    --platform managed \
    --region "${REGION}" \
    --format "value(status.url)")

echo "=============================================="
echo "  ✅ DEPLOYED SUCCESSFULLY"
echo ""
echo "  🌐 URL: ${SERVICE_URL}"
echo ""
echo "  Share this URL in your Upwork profile!"
echo "=============================================="
echo ""
