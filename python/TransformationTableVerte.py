import pandas as pd
import os
import logging
from pandas.errors import EmptyDataError
from pathlib import Path
from bq_utils import charger_dataframe_vers_bigquery
from google.cloud import bigquery


def transformer_mesures():
    dossier_entree = Path("/tmp/data")
    fichiers_mesures = list(dossier_entree.glob("mesures_airpl*.csv"))
    liste_df = []

    for fichier in fichiers_mesures:
        try:
            df_temp = pd.read_csv(fichier)
            if not df_temp.empty:
                liste_df.append(df_temp)
        except EmptyDataError:
            continue

    if not liste_df:
        return

    df = pd.concat(liste_df, ignore_index=True)

    df_mesures = df[
        ["id", "code_station", "code_polluant", "code_commune", "valeur", "date_heure_tu", "validite"]].copy()
    df_mesures = df_mesures.rename(columns={
        "code_polluant": "id_poll_ue",
        "code_commune": "insee_com",
        "date_heure_tu": "date_mesure"
    })
    df_mesures = df_mesures.dropna(subset=["code_station", "id_poll_ue", "insee_com"])

    print(f"{len(df_mesures)} mesures récupérées")

    mode = os.environ.get("ETL_MODE", "INCREMENTAL")
    doit_ecraser = True if mode == "FULL" else False

    charger_dataframe_vers_bigquery(df_mesures, "fait_mesures", mode_ecrasement=doit_ecraser)
def creer_agregats():
    client = bigquery.Client(project="tp-donnees-gp1")

    requetes_sql = [
        (
            "agregat_jour",
            """
            CREATE OR REPLACE TABLE `tp-donnees-gp1.pollution_data.fait_mesures_jour` AS
            SELECT
              DATE(date_mesure) AS date_jour,
              code_station,
              id_poll_ue,
              insee_com,
              AVG(CAST(valeur AS FLOAT64)) AS valeur
            FROM `tp-donnees-gp1.pollution_data.fait_mesures`
            WHERE code_station IS NOT NULL
              AND id_poll_ue IS NOT NULL
              AND insee_com IS NOT NULL
              AND valeur IS NOT NULL
              AND date_mesure IS NOT NULL
            GROUP BY
              date_jour,
              code_station,
              id_poll_ue,
              insee_com;
            """,
        ),
        (
            "agregat_mois",
            """
            CREATE OR REPLACE TABLE `tp-donnees-gp1.pollution_data.fait_mesures_mois` AS
            SELECT
              FORMAT_DATE('%Y-%m', DATE(date_mesure)) AS date_mois,
              code_station,
              id_poll_ue,
              insee_com,
              AVG(CAST(valeur AS FLOAT64)) AS valeur
            FROM `tp-donnees-gp1.pollution_data.fait_mesures`
            WHERE code_station IS NOT NULL
              AND id_poll_ue IS NOT NULL
              AND insee_com IS NOT NULL
              AND valeur IS NOT NULL
              AND date_mesure IS NOT NULL
            GROUP BY
              date_mois,
              code_station,
              id_poll_ue,
              insee_com;
            """,
        ),
    ]

    for nom_table, requete in requetes_sql:
        logging.info("Exécution de la requête SQL pour %s", nom_table)
        job = client.query(requete)
        job.result()
        logging.info("Table %s mise à jour dans BigQuery", nom_table)


if __name__ == "__main__":
    transformer_mesures()
    creer_agregats()
