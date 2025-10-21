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