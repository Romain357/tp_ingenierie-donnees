# extraction.py
import requests
import pandas as pd
from datetime import date, datetime, timedelta
from pathlib import Path
import os

URL_MESURES_HORAIRES = "https://data.airpl.org/api/v1/mesure/horaire/"


def extraire_mesures_jour(date_jour: str) -> pd.DataFrame:
    url = URL_MESURES_HORAIRES
    params = {
        "format": "json",
        "limit": 1000,
    }

    date_cible = datetime.strptime(date_jour, "%Y-%m-%d").date()

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

        for mesure in results:
            date_mesure = mesure.get("date_heure_tu")
            if not date_mesure:
                continue

            try:
                dt_mesure = datetime.strptime(date_mesure, "%Y-%m-%dT%H:%M:%SZ").date()
            except ValueError:
                continue

            if dt_mesure == date_cible:
                all_results.append(mesure)
            elif dt_mesure < date_cible:
                return pd.DataFrame(all_results)

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
    print(f"{len(df)} mesures récupérées")

    dossier_sortie = Path("/tmp/data")
    dossier_sortie.mkdir(parents=True, exist_ok=True)
    fichier_sortie = dossier_sortie / f"mesures_airpl_{date_cible}.csv"

    df.to_csv(fichier_sortie, index=False, encoding="utf-8")
    print(f"Fichier créé : {fichier_sortie}")