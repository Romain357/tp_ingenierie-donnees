import pandas as pd
import os
from pandas.errors import EmptyDataError
from pathlib import Path
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

import pandas as pd
from google.cloud import bigquery
from bq_utils import charger_dataframe_vers_bigquery


def creer_agregats():

    client = bigquery.Client()

    query = """
        SELECT
            code_station,
            id_poll_ue,
            insee_com,
            valeur,
            date_mesure
        FROM `tp-donnees-gp1.pollution_data.fait_mesures`
        WHERE code_station IS NOT NULL
          AND id_poll_ue IS NOT NULL
          AND insee_com IS NOT NULL
          AND valeur IS NOT NULL
          AND date_mesure IS NOT NULL
    """

    df = client.query(query).to_dataframe()

    if df.empty:
        print("Aucune mesure trouvée")
        return

    df["date_mesure"] = pd.to_datetime(df["date_mesure"])
    df["valeur"] = pd.to_numeric(df["valeur"], errors="coerce")

    # ==========================
    # AGRÉGAT JOUR
    # ==========================
    df["date_jour"] = df["date_mesure"].dt.date

    agregat_jour = (
        df.groupby(
            ["date_jour", "code_station", "id_poll_ue", "insee_com"],
            as_index=False
        )
        .agg(valeur=("valeur", "mean"))
    )

    print(f"{len(agregat_jour)} agrégats journaliers générés")

    charger_dataframe_vers_bigquery(
        agregat_jour,
        "agregat_jour",
        mode_ecrasement=True
    )

    # ==========================
    # AGRÉGAT MOIS
    # ==========================
    df["date_mois"] = df["date_mesure"].dt.to_period("M").astype(str)

    agregat_mois = (
        df.groupby(
            ["date_mois", "code_station", "id_poll_ue", "insee_com"],
            as_index=False
        )
        .agg(valeur=("valeur", "mean"))
    )

    print(f"{len(agregat_mois)} agrégats mensuels générés")

    charger_dataframe_vers_bigquery(
        agregat_mois,
        "agregat_mois",
        mode_ecrasement=True
    )


if __name__ == "__main__":
    transformer_mesures()
    creer_agregats()
