





def load_data_to_postgres(ti, **context):
    """
    Upsert aggregated CSV rows into Postgres.
    Table schema expected:
      CREATE TABLE IF NOT EXISTS pageviews_aggregated (
        page TEXT NOT NULL,
        date_hour TIMESTAMP NOT NULL,
        views BIGINT NOT NULL,
        PRIMARY KEY (page, date_hour)
      );
    Upsert uses ON CONFLICT (page, date_hour) DO UPDATE SET views = EXCLUDED.views
    """
    csv_path = ti.xcom_pull(task_ids="transform_data")
    if not csv_path or not os.path.exists(csv_path):
        raise AirflowFailException(f"No aggregated CSV available at {csv_path}")

    df = pd.read_csv(csv_path)
    if df.empty:
        print("[load_data_to_postgres] No rows to insert.")
        return

    pg = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    # Prepare rows as tuples
    # Ensure date_hour castable to timestamp; PostgresHook will handle param substitution
    rows = []
    for _, r in df.iterrows():
        # Normalize page and ensure types
        page = str(r["page"])
        views = int(r["views"])
        date_hour = str(r["date_hour"])  # 'YYYY-MM-DD HH:00:00'
        rows.append((page, date_hour, views))

    upsert_sql = """
    INSERT INTO pageviews_aggregated (page, date_hour, views)
    VALUES (%s, %s, %s)
    ON CONFLICT (page, date_hour)
    DO UPDATE SET views = EXCLUDED.views;
    """

    conn = pg.get_conn()
    cur = conn.cursor()
    try:
        cur.executemany(upsert_sql, rows)
        conn.commit()
        print(f"[load_data_to_postgres] Upserted {len(rows)} rows into pageviews_aggregated")
    except Exception as e:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()
