import pandas as pd
from pandas.errors import EmptyDataError
from pathlib import Path


def transformer_mesures():
    dossier_entree = Path("tmp/data")
    dossier_sortie = Path("tmp/data/transform")

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
                print(f"Fichier vide ignoré : {fichier.name}")
                continue

            liste_df.append(df_temp)

        except EmptyDataError:
            print(f"Fichier vide ignoré : {fichier.name}")
            continue

    if not liste_df:
        print("Aucun fichier valide trouvé.")
        return

    # fusion de tous les fichiers
    df = pd.concat(liste_df, ignore_index=True)

    # garder uniquement les colonnes utiles
    df_mesures = df[
        [
            "id",
            "code_station",
            "code_polluant",
            "code_commune",
            "valeur",
            "date_heure_tu",
            "validite"
        ]
    ].copy()

    # renommage pour correspondre au schéma SQL
    df_mesures = df_mesures.rename(columns={
        "code_polluant": "id_poll_ue",
        "code_commune": "insee_com",
        "date_heure_tu": "date_mesure"
    })

    # suppression des lignes invalides
    df_mesures = df_mesures.dropna(
        subset=[
            "code_station",
            "id_poll_ue",
            "insee_com"
        ]
    )

    fichier_sortie = dossier_sortie / "mesures.csv"

    df_mesures.to_csv(
        fichier_sortie,
        index=False,
        encoding="utf-8"
    )

    print(f"Fichier créé : {fichier_sortie}")
    print(f"{len(df_mesures)} mesures récupérées")
    print(df_mesures.head())


if __name__ == "__main__":
        transformer_mesures()