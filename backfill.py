import pandas as pd
import requests
import time
from datetime import datetime, date, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from bq_utils import charger_dataframe_vers_bigquery

URL_MESURES_HORAIRES = "https://data.airpl.org/api/v1/mesure/horaire/"
MAX_WORKERS = 8   # requêtes API simultanées
BATCH_SIZE = 32   # jours par lot avant envoi BigQuery


def _extraire_jour(date_jour: str):
    """Récupère toutes les mesures d'un jour donné via des filtres de date."""
    url = URL_MESURES_HORAIRES
    params = {
        "format": "json",
        "limit": 1000,
        "date_heure_tu__gte": f"{date_jour}T00:00:00Z",
        "date_heure_tu__lte": f"{date_jour}T23:59:59Z",
    }
    all_results = []
    tentatives = 0

    while url:
        try:
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()
            all_results.extend(data.get("results", []))
            url = data.get("next")
            params = None
            tentatives = 0
        except Exception as e:
            tentatives += 1
            if tentatives > 3:
                print(f"  ⚠️  Abandon {date_jour} après 3 tentatives : {e}")
                break
            time.sleep(5 * tentatives)

    return date_jour, all_results


def _nettoyer(lignes: list) -> pd.DataFrame | None:
    if not lignes:
        return None
    df = pd.DataFrame(lignes)
    cols = ["id", "code_station", "code_polluant", "code_commune", "valeur", "date_heure_tu", "validite"]
    df = df[[c for c in cols if c in df.columns]].copy()
    df = df.rename(columns={
        "code_polluant": "id_poll_ue",
        "code_commune": "insee_com",
        "date_heure_tu": "date_mesure",
    })
    return df.dropna(subset=["code_station", "id_poll_ue", "insee_com"])


def lancer_backfill_massif(date_debut_str: str, date_fin_str: str | None = None):
    date_debut = datetime.strptime(date_debut_str, "%Y-%m-%dT%H:%M:%SZ").date()
    date_fin = date.today() if date_fin_str is None else datetime.strptime(date_fin_str, "%Y-%m-%dT%H:%M:%SZ").date()

    jours = []
    cur = date_debut
    while cur <= date_fin:
        jours.append(cur.isoformat())
        cur += timedelta(days=1)

    total = len(jours)
    print(f"Backfill du {date_debut} au {date_fin} : {total} jours")
    print(f"Parallelisme : {MAX_WORKERS} requetes / lots de {BATCH_SIZE} jours")

    premier_lot = True
    traites = 0

    for i in range(0, total, BATCH_SIZE):
        lot = jours[i: i + BATCH_SIZE]
        toutes_lignes = []

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {executor.submit(_extraire_jour, j): j for j in lot}
            for future in as_completed(futures):
                jour, lignes = future.result()
                traites += 1
                print(f"  [{traites:4d}/{total}] {jour} -> {len(lignes)} lignes")
                toutes_lignes.extend(lignes)

        df = _nettoyer(toutes_lignes)
        if df is not None and len(df) > 0:
            charger_dataframe_vers_bigquery(df, "fait_mesures", mode_ecrasement=premier_lot)
            premier_lot = False

    print(f"Backfill termine : {traites} jours traites.")


if __name__ == "__main__":
    lancer_backfill_massif("2025-01-01T00:00:00Z")
