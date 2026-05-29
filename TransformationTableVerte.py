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

def creer_agregats():
    dossier_entree = Path("data")
    dossier_sortie = Path("data/transform")

    dossier_sortie.mkdir(parents=True, exist_ok=True)

    fichiers_mesures = list(
        dossier_entree.glob("mesures_airpl*.csv")
    )

    liste_df = []

    for fichier in fichiers_mesures:
        print(f"Lecture : {fichier.name}")

        try:
            df_temp = pd.read_csv(fichier)

            if df_temp.empty:
                continue

            liste_df.append(df_temp)

        except EmptyDataError:
            continue

    if not liste_df:
        print("Aucun fichier trouvÃ©")
        return

    # Fusion de tous les fichiers
    df = pd.concat(liste_df, ignore_index=True)

    # Colonnes utiles
    df = df[
        [
            "code_station",
            "code_polluant",
            "code_commune",
            "valeur",
            "date_heure_tu"
        ]
    ].copy()

    # Renommage
    df = df.rename(columns={
        "code_polluant": "id_poll_ue",
        "code_commune": "insee_com",
        "date_heure_tu": "date_mesure"
    })

    # Types
    df["date_mesure"] = pd.to_datetime(df["date_mesure"])
    df["valeur"] = pd.to_numeric(df["valeur"], errors="coerce")

    # Suppression des lignes inexploitables
    df = df.dropna(
        subset=[
            "code_station",
            "id_poll_ue",
            "insee_com",
            "date_mesure",
            "valeur"
        ]
    )

    # ==========================
    # TABLE JOUR
    # ==========================
    df["date_jour"] = df["date_mesure"].dt.date

    mesures_jour = (
        df.groupby(
            [
                "date_jour",
                "code_station",
                "id_poll_ue",
                "insee_com"
            ],
            as_index=False
        )
        .agg(
            valeur=("valeur", "mean")
        )
    )

    # ==========================
    # TABLE MOIS
    # ==========================
    df["date_mois"] = (
        df["date_mesure"]
        .dt.to_period("M")
        .astype(str)
    )

    mesures_mois = (
        df.groupby(
            [
                "date_mois",
                "code_station",
                "id_poll_ue",
                "insee_com"
            ],
            as_index=False
        )
        .agg(
            valeur=("valeur", "mean")
        )
    )



if __name__ == "__main__":
    transformer_mesures()
    creer_agregats()
