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
        print("Aucun fichier trouvé pour les agrégats")
        return

    df = pd.concat(liste_df, ignore_index=True)

    df = df[["code_station", "code_polluant", "code_commune", "valeur", "date_heure_tu"]].copy()
    df = df.rename(columns={
        "code_polluant": "id_poll_ue",
        "code_commune": "insee_com",
        "date_heure_tu": "date_mesure"
    })

    df["date_mesure"] = pd.to_datetime(df["date_mesure"])
    df["valeur"] = pd.to_numeric(df["valeur"], errors="coerce")
    df = df.dropna(subset=["code_station", "id_poll_ue", "insee_com", "date_mesure", "valeur"])

    # ==========================
    # TABLE JOUR
    # ==========================
    df["date_jour"] = df["date_mesure"].dt.date
    mesures_jour = (
        df.groupby(["date_jour", "code_station", "id_poll_ue", "insee_com"], as_index=False)
        .agg(valeur=("valeur", "mean"))
    )

    # ==========================
    # TABLE MOIS
    # ==========================
    df["date_mois"] = df["date_mesure"].dt.to_period("M").astype(str)
    mesures_mois = (
        df.groupby(["date_mois", "code_station", "id_poll_ue", "insee_com"], as_index=False)
        .agg(valeur=("valeur", "mean"))
    )

    # ==========================
    # 2 & 3. EXPORT VERS BIGQUERY
    # ==========================
    mode = os.environ.get("ETL_MODE", "INCREMENTAL")
    doit_ecraser = True if mode == "FULL" else False

    print(f"{len(mesures_jour)} agrégats journaliers générés.")
    charger_dataframe_vers_bigquery(mesures_jour, "agregat_jour", mode_ecrasement=doit_ecraser)

    print(f"{len(mesures_mois)} agrégats mensuels générés.")
    charger_dataframe_vers_bigquery(mesures_mois, "agregat_mois", mode_ecrasement=doit_ecraser)


if __name__ == "__main__":
    transformer_mesures()
    creer_agregats()
