import pandas as pd
import os
import logging
from pandas.errors import EmptyDataError
from pathlib import Path
from google.cloud import bigquery
from bq_utils import charger_dataframe_vers_bigquery


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
        logging.warning("Aucun fichier de mesures trouvé.")
        return

    df = pd.concat(liste_df, ignore_index=True)

    df_mesures = df[
        [
            "id",
            "code_station",
            "code_polluant",
            "code_commune",
            "valeur",
            "date_heure_tu",
            "validite",
        ]
    ].copy()

    df_mesures = df_mesures.rename(
        columns={
            "code_commune": "insee_com",
            "date_heure_tu": "date_mesure",
        }
    )

    df_mesures = df_mesures.dropna(
        subset=["code_station", "code_polluant", "insee_com"]
    )

    df_mesures = df_mesures[
        df_mesures["validite"].astype(str).str.lower() == "true"
    ].copy()

    df_mesures["code_polluant"] = pd.to_numeric(
        df_mesures["code_polluant"],
        errors="coerce"
    )

    polluants_autorises = [1, 3, 8, 24, 39]

    df_mesures = df_mesures[
        df_mesures["code_polluant"].isin(polluants_autorises)
    ].copy()

    df_mesures["code_polluant"] = df_mesures["code_polluant"].astype(str)

    df_mesures = df_mesures.drop(columns=["validite"])

    df_mesures["date_mesure"] = pd.to_datetime(
        df_mesures["date_mesure"],
        errors="coerce"
    )

    df_mesures = df_mesures.dropna(subset=["date_mesure"])

    print(
        f"{len(df_mesures)} mesures valides récupérées "
        f"pour les polluants {polluants_autorises}"
    )

    mode = os.environ.get("ETL_MODE", "INCREMENTAL")

    doit_ecraser = mode == "FULL"
    nettoyage_prealable = mode != "FULL"

    charger_dataframe_vers_bigquery(
    df_mesures,
    "fait_mesures_heure",
    mode_ecrasement=doit_ecraser,
    nettoyage_prealable=nettoyage_prealable,
)


def creer_agregats():
    client = bigquery.Client(project="tp-donnees-gp1")

    requetes_sql = [
        (
            "agregat_jour",
            """
            CREATE OR REPLACE TABLE `tp-donnees-gp1.pollution_data.agregat_jour` AS
            SELECT
                ROW_NUMBER() OVER (
                    ORDER BY
                        DATETIME(DATE(date_mesure)),
                        code_station,
                        code_polluant,
                        insee_com
                ) AS id,

                DATETIME(DATE(date_mesure)) AS date_jour,
                AVG(CAST(valeur AS FLOAT64)) AS valeur,
                CAST(code_station AS STRING) AS code_station,
                CAST(code_polluant AS STRING) AS code_polluant,
                CAST(insee_com AS STRING) AS insee_com

            FROM `tp-donnees-gp1.pollution_data.fait_mesures_heure`

            WHERE code_station IS NOT NULL
              AND code_polluant IS NOT NULL
              AND insee_com IS NOT NULL
              AND valeur IS NOT NULL
              AND date_mesure IS NOT NULL
              AND SAFE_CAST(code_polluant AS INT64) IN (1, 3, 8, 24, 39)

            GROUP BY
                date_jour,
                code_station,
                code_polluant,
                insee_com
            ;
            """,
        ),
        (
            "agregat_mois",
            """
            CREATE OR REPLACE TABLE `tp-donnees-gp1.pollution_data.agregat_mois` AS
            SELECT
                ROW_NUMBER() OVER (
                    ORDER BY
                        DATETIME(DATE_TRUNC(DATE(date_mesure), MONTH)),
                        code_station,
                        code_polluant,
                        insee_com
                ) AS id,

                DATETIME(DATE_TRUNC(DATE(date_mesure), MONTH)) AS date_mois,
                AVG(CAST(valeur AS FLOAT64)) AS valeur,
                CAST(code_station AS STRING) AS code_station,
                CAST(code_polluant AS STRING) AS code_polluant,
                CAST(insee_com AS STRING) AS insee_com

            FROM `tp-donnees-gp1.pollution_data.fait_mesures_heure`

            WHERE code_station IS NOT NULL
              AND code_polluant IS NOT NULL
              AND insee_com IS NOT NULL
              AND valeur IS NOT NULL
              AND date_mesure IS NOT NULL
              AND SAFE_CAST(code_polluant AS INT64) IN (1, 3, 8, 24, 39)

            GROUP BY
                date_mois,
                code_station,
                code_polluant,
                insee_com
            ;
            """,
        ),
    ]

    for nom_table, requete in requetes_sql:
        logging.info("Exécution de la requête SQL pour %s", nom_table)
        job = client.query(requete)
        job.result()
        logging.info("Table %s mise à jour dans BigQuery", nom_table)

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    transformer_mesures()
    creer_agregats()