import pandas as pd
import requests
import time
from datetime import datetime, timezone
from bq_utils import charger_dataframe_vers_bigquery

URL_MESURES_HORAIRES = "https://data.airpl.org/api/v1/mesure/horaire/"


def lancer_backfill_massif(date_limite_str):
    url = URL_MESURES_HORAIRES
    params = {"format": "json", "limit": 1000}
    liste_lignes = []
    page = 1

    # On convertit la limite en objet datetime pour une comparaison fiable
    date_limite = datetime.strptime(date_limite_str, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)

    print(f"🚀 Lancement du Backfill massif. Remontée dans le temps jusqu'au : {date_limite_str}")
    print("⏳ Aspiration de l'historique en cours (cela peut prendre du temps)...")

    while url:
        print(f"    -> Traitement de la page {page}...")

        try:
            response = requests.get(url, params=params, timeout=15)
            response.raise_for_status()
            data = response.json()

        except Exception as e:
            print(f"⚠️ Erreur de connexion sur la page {page} : {e}")
            print("🔄 Nouvelle tentative dans 5 secondes...")
            time.sleep(5)
            continue

        results = data.get("results", [])
        if not results:
            print("Fin des données atteinte sur l'API.")
            break

        stop_recherche = False
        lignes_ajoutees = 0

        for ligne in results:
            date_mesure_str = ligne.get("date_heure_tu")
            if not date_mesure_str:
                continue

            try:
                date_mesure = datetime.strptime(date_mesure_str, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
            except ValueError:
                continue  # Ignore les dates mal formatées

            if date_mesure >= date_limite:
                liste_lignes.append(ligne)
                lignes_ajoutees += 1
            else:
                stop_recherche = True
                break

        # print(f"       ({lignes_ajoutees} lignes conservées sur cette page)")

        if stop_recherche:
            print(f"🛑 Date limite {date_limite_str} atteinte ! On coupe l'aspirateur.")
            break

        url = data.get("next")
        params = None
        page += 1

    if not liste_lignes:
        print("Aucune donnée récupérée.")
        return

    print("🔄 Conversion en DataFrame et nettoyage...")
    df_complet = pd.DataFrame(liste_lignes)

    df_complet = df_complet[
        ["id", "code_station", "code_polluant", "code_commune", "valeur", "date_heure_tu", "validite"]
    ].copy()

    df_complet = df_complet.rename(columns={
        "code_polluant": "id_poll_ue",
        "code_commune": "insee_com",
        "date_heure_tu": "date_mesure"
    })

    df_complet = df_complet.dropna(subset=["code_station", "id_poll_ue", "insee_com"])

    print(f"✅ {len(df_complet)} lignes historiques prêtes. Envoi vers BigQuery (Écrasement de la table)...")
    charger_dataframe_vers_bigquery(df_complet, "fait_mesures", mode_ecrasement=True)
    print("🎉 Backfill massif terminé avec succès !")


if __name__ == "__main__":
    lancer_backfill_massif("2025-01-01T00:00:00Z")