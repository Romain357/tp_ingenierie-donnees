# extraction.py
import requests
import pandas as pd
from datetime import date, timedelta
from pathlib import Path
import os

URL_MESURES_HORAIRES = "https://data.airpl.org/api/v1/mesure/horaire/"


def extraire_mesures_jour(date_jour: str) -> pd.DataFrame:
    url = URL_MESURES_HORAIRES
    debut = f"{date_jour}T00:00:00Z"
    fin = f"{date_jour}T23:59:59Z"

    params = {
        "format": "json",
        "limit": 1000,
        "date_heure_tu__gte": debut,
        "date_heure_tu__lte": fin
    }

    all_results = []
    page = 1

    while url:
        print(f"    -> Récupération page {page} depuis l'API...")

        response = requests.get(url, params=params, timeout=60)
        response.raise_for_status()
        data = response.json()

        results = data.get("results", [])

        if not results:
            break

        all_results.extend(results)

        url = data.get("next")
        params = None
        page += 1

    return pd.DataFrame(all_results)


def extraire_mesures():

    mode = os.environ.get("ETL_MODE", "INCREMENTAL")

    hier = date.today() - timedelta(days=1)
    date_cible = hier.isoformat()

    print(f"Extraction des données du {date_cible} (Mode: {mode})")
    df = extraire_mesures_jour(date_cible)

    dossier_sortie = Path("/tmp/data")
    dossier_sortie.mkdir(parents=True, exist_ok=True)
    fichier_sortie = dossier_sortie / f"mesures_airpl_{date_cible}.csv"

    df.to_csv(fichier_sortie, index=False, encoding="utf-8")
    print(f"Fichier créé : {fichier_sortie}")