import pandas as pd
from pathlib import Path


def transformer_stations():
    dossier_entree = Path("tmp/data")
    dossier_sortie = Path("tmp/data/transform")

    dossier_sortie.mkdir(parents=True, exist_ok=True)

    # Lecture du fichier source
    df = pd.read_csv(dossier_entree / "stations.csv")

    # Sélection des colonnes utiles
    df_stations = df[
        [
            "code",
            "nom",
            "typologie_com_airpl"
        ]
    ].drop_duplicates()

    # Renommage des colonnes
    df_stations = df_stations.rename(columns={
        "code": "code_station",
        "nom": "nom_station",
        "typologie_com_airpl": "influence"
    })

    # Suppression des lignes sans code station
    df_stations = df_stations.dropna(subset=["code_station"])

    # Export CSV
    fichier_sortie = dossier_sortie / "stations.csv"

    df_stations.to_csv(
        fichier_sortie,
        index=False,
        encoding="utf-8"
    )

    print(f"Fichier créé : {fichier_sortie}")
    print(f"{len(df_stations)} stations récupérées")
    print(df_stations.head())


if __name__ == "__main__":
    transformer_stations()