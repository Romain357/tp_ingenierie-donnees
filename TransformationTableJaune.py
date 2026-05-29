import pandas as pd
from pathlib import Path


def transformer_communes():
    dossier_entree = Path("tmp/data")
    dossier_sortie = Path("tmp/data/transform")
    dossier_sortie.mkdir(parents=True, exist_ok=True)

    df = pd.read_csv(dossier_entree / "stations.csv")

    df_communes = df[
        [
            "code_commune",
            "commune_nom",
            "code_departement"
        ]
    ].drop_duplicates()

    df_communes = df_communes.dropna(subset=["code_commune"])

    df_communes = df_communes.rename(columns={
        "code_commune": "insee_com",
        "commune_nom": "nom_com",
        "code_departement": "code_dept"
    })

    fichier_sortie = dossier_sortie / "communes.csv"
    df_communes.to_csv(fichier_sortie, index=False, encoding="utf-8")

    print(f"Fichier créé : {fichier_sortie}")
    print(f"{len(df_communes)} communes récupérées")
    print(df_communes.head())


if __name__ == "__main__":
    transformer_communes()