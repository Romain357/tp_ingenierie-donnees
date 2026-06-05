from google.cloud import bigquery
import pandas as pd


def _sanitize_for_bigquery(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    # Ensure numeric measurement column is float
    if "valeur" in df.columns:
        df["valeur"] = pd.to_numeric(df["valeur"], errors="coerce").astype(float)

    # Common integer columns - use pandas nullable integer to preserve NA
    for col in ("id", "insee_com", "id_poll_ue"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    # Date/time
    if "date_mesure" in df.columns:
        df["date_mesure"] = pd.to_datetime(df["date_mesure"], errors="coerce")

    # Boolean
    if "validite" in df.columns:
        try:
            df["validite"] = df["validite"].astype("boolean")
        except Exception:
            df["validite"] = df["validite"].map({True: True, False: False}).astype("boolean")

    return df


def charger_dataframe_vers_bigquery(df, nom_table, mode_ecrasement=True):
    client = bigquery.Client(project="tp-donnees-gp1")
    table_id = f"tp-donnees-gp1.pollution_data.{nom_table}"

    df_to_load = _sanitize_for_bigquery(df)

    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE" if mode_ecrasement else "WRITE_APPEND",
    )

    job = client.load_table_from_dataframe(df_to_load, table_id, job_config=job_config)
    job.result()
    print(f"✅ Table {nom_table} mise à jour dans BigQuery ({len(df_to_load)} lignes).")