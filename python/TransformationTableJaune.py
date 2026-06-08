import pandas as pd
from pathlib import Path
from bq_utils import charger_dataframe_vers_bigquery


def transformer_communes():
    dossier_entree = Path("/tmp/data")

    df = pd.read_csv(dossier_entree / "stations.csv")

    df_communes = df[["code_commune", "commune_nom", "code_departement"]].drop_duplicates()
    df_communes = df_communes.dropna(subset=["code_commune"])
    df_communes = df_communes.rename(columns={
        "code_commune": "insee_com",
        "commune_nom": "nom_com",
        "code_departement": "code_dept"
    })

    print(f"{len(df_communes)} communes récupérées")
    charger_dataframe_vers_bigquery(df_communes,"communes", mode_ecrasement=True)


if __name__ == "__main__":
    transformer_communes()