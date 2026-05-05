from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

app = FastAPI(title="Personal Portfolio")

app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")

PROFILE = {
    "name": "Ridwannulloh (Ridwan)",
    "title": "Data Engineer",
    "location": "Jakarta, Indonesia",
    "tagline": (
        "Data Engineer with 2+ years specializing in modern data warehousing "
        "and Lambda Architecture, backed by 6 years in the data field."
    ),
    "about": (
        "Skilled in building scalable ETL/ELT pipelines, designing optimized data models, "
        "and managing cloud-based data platforms (AWS, GCP). Proficient in SQL, Python, "
        "Redshift, BigQuery, and MySQL/PostgreSQL, with hands-on experience orchestrating "
        "reliable streaming and batch workflows. Strong background in delivering "
        "analytics-ready datasets and supporting BI tools including PowerBI, AWS QuickSight, "
        "Metabase, and Tableau."
    ),
    "email": "nulloh.ridwan0320@gmail.com",
    "github": "https://github.com/ridwannulloh",
    "linkedin": "https://linkedin.com/in/ridwannulloh",
}

EXPERIENCE = [
    {
        "role": "Data Engineer",
        "company": "Koltiva",
        "location": "Jakarta, Indonesia",
        "period": "May 2024 — Present",
        "bullets": [
            "Design, build, and maintain ETL/ELT pipelines to transform raw, multi-source data into analytics-ready datasets.",
            "Initiate and develop datamarts using dimensional modeling, including SCD Type 1 & Type 2 for historical tracking.",
            "Architect and optimize data warehousing structures in Redshift — schema design, partitioning, performance tuning, and cost optimization.",
            "Manage real-time CDC with Debezium + Kafka (MSK), event-driven pipelines via AWS Lambda, and data replication with AWS DMS.",
            "Develop and orchestrate workflows in Apache Airflow for automated, reliable data movement across systems.",
            "Implement data quality checks enforcing schema validation, completeness, accuracy, and consistency.",
        ],
    },
    {
        "role": "Data Officer",
        "company": "Koltiva",
        "location": "Jakarta, Indonesia",
        "period": "Mar 2022 — May 2024",
        "bullets": [
            "Optimized data infrastructure to support agritech operations and improve supply chain visibility.",
            "Managed the full data lifecycle — collection, validation, integration, and transformation of field survey data.",
            "Verified and validated data to ensure accuracy, completeness, and consistency across multiple sources.",
            "Built and maintained dashboards and reports using MySQL, PostgreSQL, Redshift, AWS QuickSight, Metabase, and Power BI.",
        ],
    },
    {
        "role": "Marketing Data Analyst",
        "company": "Java Villas Boutique Hotel & Resto",
        "location": "Yogyakarta, Indonesia",
        "period": "Sep 2018 — Sep 2020",
        "bullets": [
            "Collected and analyzed guest data to identify trends, preferences, and behaviors for targeted marketing.",
            "Designed, executed, and optimized marketing campaigns on Facebook Ads, achieving higher engagement and improved ROI.",
            "Developed predictive models to forecast occupancy, revenue, and marketing outcomes.",
            "Built dashboards and reports providing real-time insights for operational and marketing decisions.",
        ],
    },
]

EDUCATION = {
    "degree": "Bachelor Degree, Economics",
    "university": "UIN Sunan Kalijaga",
    "location": "Yogyakarta, Indonesia",
    "period": "Aug 2014 — May 2018",
    "gpa": "3.68 / 4.00",
}

SKILLS = [
    {"category": "Programming", "items": ["Python", "SQL", "Bash"]},
    {"category": "Data Engineering", "items": ["Apache Spark", "Airflow", "Kafka", "Debezium", "AWS Lambda", "AWS DMS"]},
    {"category": "Cloud Platforms", "items": ["AWS (Redshift, RDS, MSK, Lambda, DMS)", "Google Cloud Platform"]},
    {"category": "Databases", "items": ["MySQL", "PostgreSQL", "Redshift", "BigQuery", "MongoDB"]},
    {"category": "Visualization", "items": ["PowerBI", "Tableau", "AWS QuickSight", "Metabase", "Looker"]},
    {"category": "Others", "items": ["Docker", "Git", "Delta Lake", "DuckDB", "KNIME"]},
]

PROJECTS = [
    {
        "title": "LogiStream — Real-Time Supply Chain Pipeline",
        "description": (
            "Production-grade real-time streaming pipeline that tracks shipment events "
            "across carriers and warehouses, detects SLA breaches, and surfaces delay alerts "
            "via a REST API. Built with Kafka + Spark Structured Streaming + Delta Lake "
            "Medallion Architecture. Fully containerised — one docker-compose up --build "
            "starts the entire pipeline."
        ),
        "tech": ["Apache Kafka", "PySpark", "Spark Structured Streaming", "Delta Lake", "FastAPI", "Airflow", "Docker"],
        "github": "https://github.com/ridwannulloh/data-engineering-portfolio/tree/main/logistream",
        "highlights": [
            "3 Kafka topics → Bronze / Silver / Gold Delta Lake layers",
            "SLA breach detection with configurable per-tier thresholds",
            "Carrier KPI aggregations using 60-second tumbling windows",
            "FastAPI alert service with live Swagger UI at /api/docs",
        ],
    },
    {
        "title": "Olist E-commerce ETL Pipeline",
        "description": (
            "End-to-end ETL pipeline processing Brazilian e-commerce data from Olist. "
            "Implements a medallion architecture (Bronze → Silver → Gold) using Apache Spark "
            "and Delta Lake, orchestrated with Apache Airflow. Produces Gold-layer aggregations "
            "for customer RFM analysis, monthly revenue, and category performance."
        ),
        "tech": ["Apache Spark", "Delta Lake", "Apache Airflow", "DuckDB", "Docker", "Python"],
        "github": "https://github.com/ridwannulloh/data-engineering-portfolio/tree/main/olist-etl-pipeline",
        "highlights": [
            "Medallion architecture (Bronze / Silver / Gold)",
            "Partitioned Delta tables for optimized reads",
            "Dockerized Airflow DAG for full orchestration",
            "DuckDB for lightweight analytical queries on results",
        ],
    },
    {
        "title": "Economics Dashboard",
        "description": (
            "Automated data ingestion and visualization platform for macroeconomic indicators. "
            "Ingests data into Google BigQuery on a scheduled basis and serves a Streamlit "
            "dashboard deployed on Google Cloud Run."
        ),
        "tech": ["Python", "Google BigQuery", "Streamlit", "Plotly", "Cloud Run", "Docker"],
        "github": "https://github.com/ridwannulloh/data-engineering-portfolio/tree/main/econ-dashboard",
        "highlights": [
            "Automated ingestion pipeline into BigQuery",
            "Scheduled data refresh via Cloud Scheduler",
            "Interactive Streamlit dashboard with Plotly charts",
            "Containerized & deployed on Google Cloud Run",
        ],
    },
]


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    return templates.TemplateResponse(
        request=request,
        name="index.html",
        context={
            "profile": PROFILE,
            "experience": EXPERIENCE,
            "education": EDUCATION,
            "skills": SKILLS,
            "projects": PROJECTS,
        },
    )
