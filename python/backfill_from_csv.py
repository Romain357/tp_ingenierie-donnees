"""
backfill_from_csv.py
--------------------
Prend en entrée un ou plusieurs fichiers CSV de mesures AirPL transformées,
extrait les dates distinctes, vérifie dans BigQuery si ces journées sont déjà
présentes, et n'envoie les données que pour les journées absentes.

Usage :
    python backfill_from_csv.py fichier1.csv fichier2.csv
    python backfill_from_csv.py *.csv
    python backfill_from_csv.py fichier.csv --dry-run
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path
from typing import Optional

import pandas as pd
from google.cloud import bigquery

from bq_utils import charger_dataframe_vers_bigquery

# ── Configuration (alignée sur bq_utils.py) ───────────────────────────────────

PROJECT_ID = "tp-donnees-gp1"
DATASET_ID = "pollution_data"
TABLE_ID   = "fait_mesures_heure"
TABLE_REF  = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

DATE_COL   = "date_mesure"

CSV_COLUMNS = ["id", "code_station", "code_polluant", "insee_com", "valeur", DATE_COL]


# ── BigQuery : vérification des journées déjà présentes ───────────────────────

def dates_deja_en_base(dates: set[str]) -> set[str]:
    """
    Interroge BigQuery pour savoir quelles journées (YYYY-MM-DD) parmi
    `dates` ont déjà au moins une ligne dans la table.
    Réutilise le même projet/client que bq_utils.
    """
    if not dates:
        return set()

    client = bigquery.Client(project=PROJECT_ID)

    # Paramètre typé DATE pour éviter l'injection SQL
    dates_as_date = [
        pd.Timestamp(d).date() for d in sorted(dates)
    ]

    query = f"""
        SELECT DISTINCT DATE({DATE_COL}) AS jour
        FROM `{TABLE_REF}`
        WHERE DATE({DATE_COL}) IN UNNEST(@dates_cibles)
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ArrayQueryParameter("dates_cibles", "DATE", dates_as_date)
        ]
    )

    logging.info("Vérification en base pour : %s", sorted(dates))
    try:
        result = client.query(query, job_config=job_config).result()
        presentes = {str(row["jour"]) for row in result}
        logging.info("Dates déjà présentes en base : %s", presentes or "aucune")
        return presentes
    except Exception:
        logging.exception(
            "Erreur lors de la vérification des dates en base. "
            "Aucune donnée ne sera envoyée par sécurité."
        )
        raise


# ── Lecture & nettoyage du CSV ─────────────────────────────────────────────────

def charger_csv(chemin: Path) -> pd.DataFrame:
    """Charge le CSV et normalise les colonnes."""
    df = pd.read_csv(chemin, dtype=str)

    manquantes = [c for c in CSV_COLUMNS if c not in df.columns]
    if manquantes:
        raise ValueError(
            f"Colonnes manquantes dans {chemin.name} : {manquantes}\n"
            f"Colonnes présentes : {list(df.columns)}"
        )

    return df[CSV_COLUMNS].copy()


def extraire_dates(df: pd.DataFrame) -> set[str]:
    """Retourne l'ensemble des journées YYYY-MM-DD présentes dans le DataFrame."""
    dates = pd.to_datetime(df[DATE_COL], errors="coerce", utc=True)
    return set(dates.dt.date.astype(str).dropna().unique())


# ── Logique principale ─────────────────────────────────────────────────────────

def traiter_fichiers(chemins: list[Path], dry_run: bool = False) -> None:
    """
    Pipeline complet :
    1. Lit tous les CSV → regroupe par journée
    2. Une seule requête BQ pour vérifier les dates déjà présentes
    3. Envoie en une seule fois toutes les lignes des dates absentes
    """

    # ── Étape 1 : lecture ────────────────────────────────────────────────────
    frames_par_date: dict[str, list[pd.DataFrame]] = {}

    for chemin in chemins:
        logging.info("Lecture de %s …", chemin.name)
        try:
            df = charger_csv(chemin)
        except Exception:
            logging.exception("Impossible de lire %s → fichier ignoré.", chemin)
            continue

        dates_fichier = extraire_dates(df)
        logging.info(
            "  → %d lignes | %d journée(s) : %s",
            len(df), len(dates_fichier), sorted(dates_fichier),
        )

        for date_str in dates_fichier:
            dates_parsed = pd.to_datetime(df[DATE_COL], errors="coerce", utc=True)
            mask = dates_parsed.dt.date.astype(str) == date_str
            frames_par_date.setdefault(date_str, []).append(df[mask].copy())

    if not frames_par_date:
        logging.warning("Aucune donnée valide trouvée dans les fichiers fournis.")
        return

    toutes_dates = set(frames_par_date.keys())
    logging.info("Dates candidates au backfill : %s", sorted(toutes_dates))

    # ── Étape 2 : vérification BigQuery ─────────────────────────────────────
    dates_presentes = dates_deja_en_base(toutes_dates)
    dates_a_envoyer = toutes_dates - dates_presentes

    if dates_presentes:
        logging.info("⏭  Déjà en base (ignorées) : %s", sorted(dates_presentes))

    if not dates_a_envoyer:
        logging.info("✅ Toutes les journées sont déjà présentes en base. Rien à envoyer.")
        return

    logging.info("📤 Dates à backfiller : %s", sorted(dates_a_envoyer))

    # ── Étape 3 : concat + upload ────────────────────────────────────────────
    df_final = pd.concat(
        [f for d in sorted(dates_a_envoyer) for f in frames_par_date[d]],
        ignore_index=True,
    )
    logging.info(
        "Total à envoyer : %d lignes pour %d journée(s).",
        len(df_final), len(dates_a_envoyer),
    )

    if dry_run:
        logging.info("[DRY-RUN] Aucune écriture en base. Aperçu :")
        print(df_final.to_string(max_rows=20))
        return

    # Utilise charger_dataframe_vers_bigquery de bq_utils (mode APPEND)
    charger_dataframe_vers_bigquery(df_final, TABLE_ID, mode_ecrasement=False)
    logging.info("🎉 Backfill terminé avec succès.")


# ── CLI ────────────────────────────────────────────────────────────────────────

def _parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Backfill CSV → BigQuery (idempotent par journée)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemples :
  python backfill_from_csv.py mesures_2026-06-10.csv
  python backfill_from_csv.py *.csv
  python backfill_from_csv.py fichier1.csv fichier2.csv --dry-run --verbose
        """,
    )
    p.add_argument(
        "fichiers",
        nargs="+",
        type=Path,
        metavar="FICHIER.csv",
        help="Un ou plusieurs fichiers CSV de mesures transformées",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Simule l'envoi sans écrire en base (affiche un aperçu des données)",
    )
    p.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Active les logs DEBUG",
    )
    return p.parse_args(argv)


def main(argv: Optional[list[str]] = None) -> None:
    args = _parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)-8s %(message)s",
        stream=sys.stdout,
    )

    chemins_valides = []
    for p in args.fichiers:
        if not p.exists():
            logging.error("Fichier introuvable : %s", p)
        else:
            chemins_valides.append(p)

    if not chemins_valides:
        logging.error("Aucun fichier valide fourni. Abandon.")
        sys.exit(1)

    traiter_fichiers(chemins_valides, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
