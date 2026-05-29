import pandas as pd
from pathlib import Path
from bq_utils import charger_dataframe_vers_bigquery


def transformer_stations():
    dossier_entree = Path("/tmp/data")

    df = pd.read_csv(dossier_entree / "stations.csv")

    df_stations = df[["code", "nom", "typologie_com_airpl"]].drop_duplicates()
    df_stations = df_stations.rename(columns={
        "code": "code_station",
        "nom": "nom_station",
        "typologie_com_airpl": "influence"
    })
    df_stations = df_stations.dropna(subset=["code_station"])

    print(f"{len(df_stations)} stations récupérées")
    charger_dataframe_vers_bigquery(df_stations, "dim_stations", mode_ecrasement=True)


if __name__ == "__main__":
    transformer_stations()