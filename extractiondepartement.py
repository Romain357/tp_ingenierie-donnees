import requests
import pandas as pd

URL_DEPARTEMENTS = "https://data.airpl.org/api/v1/mesure/departement/"


def extraire_departements():
    url = URL_DEPARTEMENTS
    all_results = []

    while url:
        response = requests.get(url, timeout=60)
        response.raise_for_status()

        data = response.json()

        all_results.extend(data.get("results", []))

        # pagination automatique
        url = data.get("next")

        print(f"{len(all_results)} départements récupérés...")

    return pd.DataFrame(all_results)


df_departements = extraire_departements()

df_departements.to_csv(
    "tmp/data/departements.csv",
    index=False,
    encoding="utf-8"
)

print(df_departements.shape)
print(df_departements.head())
print(df_departements.columns.tolist())