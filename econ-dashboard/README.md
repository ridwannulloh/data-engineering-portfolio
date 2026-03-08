# 🌐 Global Economic Indicators Dashboard

> **Live economic data pipeline** — World Bank API → BigQuery → Streamlit → GCP Cloud Run

[![Deploy to Cloud Run](https://img.shields.io/badge/Deploy-Cloud%20Run-4285F4?logo=google-cloud&logoColor=white)](https://cloud.google.com/run)
[![Python 3.11](https://img.shields.io/badge/Python-3.11-3776AB?logo=python&logoColor=white)](https://python.org)
[![Streamlit](https://img.shields.io/badge/Streamlit-1.32-FF4B4B?logo=streamlit&logoColor=white)](https://streamlit.io)
[![BigQuery](https://img.shields.io/badge/BigQuery-Data%20Warehouse-4285F4?logo=google-cloud)](https://cloud.google.com/bigquery)

---

## 📺 Live Demo

🔗 **[View Dashboard →](https://econ-dashboard-986403815263.us-central1.run.app)**

![Dashboard Preview](https://via.placeholder.com/900x500/0f172a/ffffff?text=Economic+Dashboard+-+Dark+Theme)

---

## 🏗️ Architecture

```
World Bank API  ──►  ingest.py  ──►  BigQuery  ──►  Streamlit App  ──►  Cloud Run (public URL)
    (G20 data)      (truncate +       (full          (filtered to
                     load fresh)      dataset)        G7 + Indonesia)
                         ▲
                         │
                  Cloud Scheduler
                  (monthly: 1st day at 06:00 UTC)
```

**Stack:**
- **Data Source:** [World Bank Open Data API](https://data.worldbank.org) (free, no key needed)
- **Pipeline:** Python (`requests`, `google-cloud-bigquery`)
- **Warehouse:** Google BigQuery (partitioned + clustered for cost efficiency)
- **Dashboard:** Streamlit + Plotly
- **Hosting:** GCP Cloud Run (scales to zero — ~$0.10/month)
- **Scheduling:** Cloud Scheduler (daily ingestion)
- **CI/CD:** GitHub Actions (auto-deploy on push to `main`)

---

## 📊 Indicators Tracked

| Code | Indicator | Unit |
|------|-----------|------|
| `NY.GDP.MKTP.CD` | GDP | Current USD |
| `FP.CPI.TOTL.ZG` | Inflation Rate | Annual % |
| `SL.UEM.TOTL.ZS` | Unemployment Rate | % |
| `NE.EXP.GNFS.ZS` | Exports | % of GDP |
| `GC.DOD.TOTL.GD.ZS` | Government Debt | % of GDP |

**Data Ingestion:** All 19 G20 countries (Argentina, Australia, Brazil, Canada, China, France, Germany, India, Indonesia, Italy, Japan, Mexico, Russia, Saudi Arabia, South Africa, South Korea, Turkey, United Kingdom, United States)

**Dashboard Display:** Filtered to G7 + Indonesia (Canada, France, Germany, Italy, Japan, United Kingdom, United States, Indonesia)

---

## 🚀 Quick Start

### Prerequisites
- Python 3.11+
- Docker
- GCP project with BigQuery, Cloud Run, and Artifact Registry APIs enabled
- `gcloud` CLI authenticated

### 1. Clone & install

```bash
git clone https://github.com/ridwannulloh/data-engineering-portfolio.git
cd data-engineering-portfolio/econ-dashboard
pip install -r requirements.txt
```

### 2. Set environment variable

```bash
export GCP_PROJECT_ID="your-gcp-project-id"
```

### 3. Create BigQuery table (one-time)

```bash
python setup_bigquery.py --project $GCP_PROJECT_ID
```

### 4. Run initial ingestion

```bash
# Dry run first (no BigQuery writes)
python ingest.py --dry-run

# Full ingestion
python ingest.py
```

### 5. Run dashboard locally

```bash
streamlit run app.py
# Open http://localhost:8501
```

### 6. Deploy to Cloud Run

```bash
chmod +x deploy.sh
./deploy.sh --project $GCP_PROJECT_ID
```

### 7. Set up monthly scheduling (optional)

```bash
python schedule_ingestion.py --project $GCP_PROJECT_ID
# Runs on 1st of each month at 06:00 UTC
# Automatically truncates old data and loads fresh data
```

---

## 📁 Project Structure

```
econ-dashboard/
├── app.py                        # Streamlit dashboard
├── ingest.py                     # World Bank API → BigQuery pipeline
├── setup_bigquery.py             # One-time BQ dataset/table creation
├── requirements.txt              # Python dependencies
├── Dockerfile                    # Multi-stage Docker build
├── deploy.sh                     # One-command deploy script
├── schedule_ingestion.py         # Cloud Scheduler setup (monthly)
└── .github/
    └── workflows/
        └── deploy.yml            # CI/CD — auto deploy on push to main
```

---

## 💰 GCP Cost Estimate

| Service | Free Tier | This Project |
|---------|-----------|-------------|
| BigQuery Storage | 10 GB/month | ~15 MB (G20 data) |
| BigQuery Queries | 1 TB/month | ~1 MB/month |
| Cloud Run | 2M requests/month | <1K/month |
| Cloud Scheduler | 3 jobs free | 1 job (monthly) |
| **Total** | | **~$0.10–0.20/month** |

> ✅ Well within the GCP free tier limits.
> 💡 Monthly schedule (vs daily) keeps costs minimal while data stays fresh.

---

## 🔧 CI/CD Setup

GitHub Actions auto-deploys on every push to `main`. Add these secrets to your repo:

1. Go to **Settings → Secrets → Actions**
2. Add `GCP_PROJECT_ID` — your GCP project ID
3. Add `GCP_SA_KEY` — base64-encoded service account JSON:

```bash
base64 -i service-account-key.json | pbcopy   # macOS
base64 -w 0 service-account-key.json           # Linux
```

---

## 📬 Contact

Built by **ridwannulloh** — Data Engineer  
🔗 [Upwork Profile](https://https://www.upwork.com/freelancers/~019bf10d5b188b2e3a) · [LinkedIn](https://linkedin.com/in/ridwannulloh) · [GitHub](https://github.com/ridwannulloh)
