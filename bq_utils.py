from google.cloud import bigquery


def charger_dataframe_vers_bigquery(df, nom_table, mode_ecrasement=True):
    client = bigquery.Client(project="tp-donnees-gp1")
    table_id = f"tp-donnees-gp1.pollution_data.{nom_table}"

    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE" if mode_ecrasement else "WRITE_APPEND",
    )

    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()
    print(f"✅ Table {nom_table} mise à jour dans BigQuery ({len(df)} lignes).")