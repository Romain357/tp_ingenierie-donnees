from google.api_core.exceptions import NotFound
from google.cloud import bigquery
import pandas as pd


def _sanitize_for_bigquery(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    # Ensure numeric measurement column is float
    if "valeur" in df.columns:
        df["valeur"] = pd.to_numeric(df["valeur"], errors="coerce").astype(float)

    if "id" in df.columns:
        df["id"] = df["id"].astype("string")

    # Common integer columns - use pandas nullable integer to preserve NA
    for col in ("insee_com", "id_poll_ue"):
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


def supprimer_lignes_pour_dates(client: bigquery.Client, table_id: str, dates_cibles, colonne_date: str = "date_mesure"):
    dates_normalisees = pd.to_datetime(pd.Series(list(dates_cibles)), errors="coerce").dropna().dt.date.unique().tolist()

    if not dates_normalisees:
        return

    requete = f"DELETE FROM `{table_id}` WHERE DATE({colonne_date}) IN UNNEST(@dates_cibles)"
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ArrayQueryParameter("dates_cibles", "DATE", dates_normalisees)
        ]
    )

    try:
        job = client.query(requete, job_config=job_config)
        job.result()
        print(f"🧹 Nettoyage préalable effectué sur {len(dates_normalisees)} date(s) dans {table_id}.")
    except NotFound:
        print(f"ℹ️ Table introuvable, nettoyage ignoré pour {table_id}.")


def charger_dataframe_vers_bigquery(df, nom_table, mode_ecrasement=True, nettoyage_prealable=False, colonne_date="date_mesure"):
    client = bigquery.Client(project="tp-donnees-gp1")
    table_id = f"tp-donnees-gp1.pollution_data.{nom_table}"

    df_to_load = _sanitize_for_bigquery(df)

    if nettoyage_prealable and not mode_ecrasement and colonne_date in df_to_load.columns:
        dates_cibles = df_to_load[colonne_date].dropna().dt.date.unique().tolist()
        supprimer_lignes_pour_dates(client, table_id, dates_cibles, colonne_date=colonne_date)

    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE" if mode_ecrasement else "WRITE_APPEND",
    )

    job = client.load_table_from_dataframe(df_to_load, table_id, job_config=job_config)
    job.result()
    print(f"✅ Table {nom_table} mise à jour dans BigQuery ({len(df_to_load)} lignes).")
