import pandas as pd
from pathlib import Path
from bq_utils import charger_dataframe_vers_bigquery


def transformer_polluants():
    dossier_entree = Path("/tmp/data")

    df = pd.read_csv(dossier_entree / "polluants.csv")

    df_polluants = df[["codeue", "code", "libelle_fr", "code_unite_concentration"]].drop_duplicates()
    df_polluants = df_polluants.rename(columns={
        "codeue": "id_poll_ue",
        "code": "code_poll",
        "libelle_fr": "nom_poll",
        "code_unite_concentration": "unite"
    })
    df_polluants = df_polluants.dropna(subset=["id_poll_ue"])

    print(f"{len(df_polluants)} polluants récupérés")
    charger_dataframe_vers_bigquery(df_polluants, "dim_polluants", mode_ecrasement=True)


if __name__ == "__main__":
    transformer_polluants()