import numpy as np
import pandas as pd
from datetime import datetime, timedelta
import os
import gzip
import shutil
import requests
from io import BytesIO, TextIOWrapper



BASE_URL = "https://dumps.wikimedia.org/other/pageviews"
# local storage inside worker — choose a shared/ephemeral path depending on your infra
BASE_WORKDIR = "/tmp/wikipedia_pipeline"
RAW_DIR = os.path.join(BASE_WORKDIR, "raw")
EXTRACTED_DIR = os.path.join(BASE_WORKDIR, "extracted")
OUTPUT_DIR = os.path.join(BASE_WORKDIR, "output")

# Companies to track (page titles exactly as in dumps)
COMPANIES = ["Apple_Inc.", "Microsoft", "Tesla,_Inc.", "Amazon.com", "Meta_Platforms"]

# Airflow connections/variables (set these in Airflow UI -> Admin -> Connections / Variables)
POSTGRES_CONN_ID = "postgres_core_sentiment"  # must be configured in Airflow connections
ALERT_EMAILS = ["talktoitopa@gmail.com"]      # configure as you wish (list)

# Ensure directories exist
for d in (RAW_DIR, EXTRACTED_DIR, OUTPUT_DIR):
    os.makedirs(d, exist_ok=True)


# Utility functions
def _format_hour_filename(dt: datetime):
    """
    Wikimedia file pattern: pageviews-{YYYY}{MM}{DD}-{HH}0000.gz
    Example: pageviews-20251001-000000.gz for 2025-10-01 00:00:00 -> covers 23:00-00:00? (follow docs)
    We'll use dt.hour formatted as two digits and append 0000.
    """
    return f"pageviews-{dt.strftime('%Y%m%d')}-{dt.strftime('%H')}0000.gz"

def _download_url(url: str) -> bytes:
    """Download content and return bytes. Raises exception if non-200."""
    resp = requests.get(url, stream=True, timeout=60)
    if resp.status_code != 200:
        raise AirflowFailException(f"Failed to download {url} (status {resp.status_code})")
    return resp.content

# Task implementations
def download_data(ds, **context):
    """
    Download pageviews file for the execution date/hour.
    By default this DAG expects the logical execution time (ds) and an `execution_hour` variable in params
    (or default to 00).
    """
    # ds is YYYY-MM-DD ; optionally we allow the hour via dag_run.conf or Airflow params
    dag_run = context.get("dag_run")
    conf = getattr(dag_run, "conf", {}) or {}
    hour = conf.get("hour", 0)  # integer 0-23; allow override at runtime
    # Use ds + hour as the target datetime
    dt = datetime.strptime(ds, "%Y-%m-%d").replace(hour=int(hour))
    filename = _format_hour_filename(dt)
    url = f"{BASE_URL}/{dt.strftime('%Y')}/{dt.strftime('%Y')}-{dt.strftime('%m')}/{filename}"
    local_path = os.path.join(RAW_DIR, filename)
    # If file already downloaded, skip (idempotency)
    if os.path.exists(local_path):
        print(f"[download_data] File already exists, skipping download: {local_path}")
        return local_path

    print(f"[download_data] Downloading {url}")
    try:
        content = _download_url(url)
    except Exception as e:
        # bubble up so Airflow marks task failed and retries if configured
        raise

    with open(local_path, "wb") as f:
        f.write(content)
    print(f"[download_data] Saved to {local_path}")
    return local_path

