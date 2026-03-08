"""
schedule_ingestion.py
----------------------
Sets up a Cloud Scheduler job to run the ingestion pipeline monthly.
Run this ONCE after deploying your Cloud Run service.

Usage:
    python schedule_ingestion.py
    python schedule_ingestion.py --project YOUR_PROJECT_ID --region us-central1
"""

import os
import argparse
import subprocess
import logging
import sys

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

# Determine gcloud command based on platform
GCLOUD_CMD = "gcloud.cmd" if sys.platform == "win32" else "gcloud"


def run(cmd: list[str]) -> str:
    """Run a shell command and return stdout."""
    log.info(f"  $ {' '.join(cmd)}")
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(f"Command failed:\n{result.stderr}")
    return result.stdout.strip()


def get_project_number(project_id: str) -> str:
    return run([GCLOUD_CMD, "projects", "describe", project_id, "--format=value(projectNumber)"])


def setup_scheduler(project_id: str, region: str, service_name: str):
    log.info("\n=== Setting up Cloud Scheduler ===\n")

    # 1. Get project number (needed for service account email)
    project_number = get_project_number(project_id)
    sa_email = f"{project_number}-compute@developer.gserviceaccount.com"
    log.info(f"  Service account: {sa_email}")

    # 2. Get Cloud Run service URL
    service_url = run([
        GCLOUD_CMD, "run", "services", "describe", service_name,
        "--platform", "managed",
        "--region", region,
        "--format", "value(status.url)",
    ])
    log.info(f"  Cloud Run URL: {service_url}")

    # 3. The ingestion endpoint — Cloud Run Jobs alternative
    # We'll trigger a separate Cloud Run Job for ingestion
    job_name = f"{service_name}-ingest"

    # 4. Create Cloud Run Job for ingestion (separate from dashboard)
    log.info(f"\n[1/3] Creating Cloud Run Job: {job_name}")
    image = f"gcr.io/{project_id}/{service_name}:latest"
    run([
        GCLOUD_CMD, "run", "jobs", "create", job_name,
        "--image", image,
        "--region", region,
        "--command", "python",
        "--args", "ingest.py",
        "--set-env-vars", f"GCP_PROJECT_ID={project_id}",
        "--memory", "512Mi",
        "--cpu", "1",
        "--task-timeout", "600",   # 10 min max per run
        "--max-retries", "2",
    ])
    log.info(f"  ✓ Cloud Run Job created: {job_name}")

    # 5. Construct job execution URL
    job_uri = (
        f"https://{region}-run.googleapis.com/apis/run.googleapis.com/v1"
        f"/namespaces/{project_id}/jobs/{job_name}:run"
    )

    # 6. Create Cloud Scheduler job
    log.info(f"\n[2/3] Creating Cloud Scheduler job...")
    scheduler_name = f"{service_name}-monthly-ingest"
    run([
        GCLOUD_CMD, "scheduler", "jobs", "create", "http", scheduler_name,
        "--schedule", "0 6 1 * *",          # 1st day of every month at 06:00 UTC
        "--uri", job_uri,
        "--message-body", "{}",
        "--oauth-service-account-email", sa_email,
        "--location", region,
        "--time-zone", "UTC",
        "--description", "Monthly World Bank data ingestion into BigQuery",
        "--attempt-deadline", "600s",
    ])
    log.info(f"  ✓ Scheduler created: {scheduler_name} (runs monthly on 1st at 06:00 UTC)")

    # 7. Test run immediately
    log.info(f"\n[3/3] Triggering a test run now...")
    run([
        GCLOUD_CMD, "scheduler", "jobs", "run", scheduler_name,
        "--location", region,
    ])
    log.info(f"  ✓ Test run triggered — check Cloud Run logs in ~30 seconds")

    log.info("\n" + "=" * 50)
    log.info("  ✅ Scheduler setup complete!")
    log.info(f"  Job name    : {scheduler_name}")
    log.info(f"  Schedule    : Monthly on 1st at 06:00 UTC")
    log.info(f"  Monitor at  : https://console.cloud.google.com/cloudscheduler")
    log.info("=" * 50 + "\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--project",  "-p", default=os.environ.get("GCP_PROJECT_ID", "YOUR_PROJECT_ID"))
    parser.add_argument("--region",   "-r", default="us-central1")
    parser.add_argument("--service",  "-s", default="econ-dashboard")
    args = parser.parse_args()

    setup_scheduler(args.project, args.region, args.service)
