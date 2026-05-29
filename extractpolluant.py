import requests
import pandas as pd
from pathlib import Path

URL_POLLUANTS = "https://data.airpl.org/api/v1/mesure/polluant/"


def extraire_polluants() -> pd.DataFrame:
    url = URL_POLLUANTS
    params = {"format": "json", "limit": 1000}
    all_results = []

    while url:
        response = requests.get(url, params=params, timeout=60)
        response.raise_for_status()

        data = response.json()
        all_results.extend(data.get("results", []))

        url = data.get("next")
        params = None

    return pd.DataFrame(all_results)


def executer_extraction_polluants():
    dossier_sortie = Path("/tmp/data") # MODIFIÉ
    dossier_sortie.mkdir(parents=True, exist_ok=True)

    df_polluants = extraire_polluants()

    fichier_sortie = dossier_sortie / "polluants.csv"
    df_polluants.to_csv(fichier_sortie, index=False, encoding="utf-8")
    print(f"Fichier créé : {fichier_sortie}")