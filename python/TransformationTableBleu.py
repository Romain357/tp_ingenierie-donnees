import pandas as pd
from pathlib import Path
from bq_utils import charger_dataframe_vers_bigquery


def transformer_polluants():
    dossier_entree = Path("/tmp/data")


    df = pd.read_csv(dossier_entree / "polluants.csv")

    # Convertir la colonne code en nombre
    df["code"] = pd.to_numeric(df["code"], errors="coerce")

    codes_polluants = [8, 24, 39, 3, 1]

    df_polluants = (
        df.loc[
            df["code"].isin(codes_polluants),
            ["code", "notation", "code_unite_concentration"]
        ]
        .drop_duplicates()
        .rename(columns={
            "code": "code_poll",
            "code_unite_concentration": "unite"
        })
        .sort_values("code_poll")
    )

    df_polluants["code_poll"] = df_polluants["code_poll"].astype(int)


if __name__ == "__main__":
    transformer_polluants()
