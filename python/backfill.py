"""Backfill utility for AirPL measures.

Improvements over the original version:
- CLI with `--since` or `--start/--end` modes
- HTTP session with retries and backoff
- Optional parallel per-day fetching or single-pass pagination until a cutoff date
- Batch upload to BigQuery with local CSV backup
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import datetime, date, timedelta
from typing import Iterable, List
import warnings

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from bq_utils import charger_dataframe_vers_bigquery

# Optional progress bar (tqdm). Provide a lightweight fallback if not installed.
try:
    from tqdm.auto import tqdm
except Exception:  # pragma: no cover - fallback when tqdm missing
    class _DummyTqdm:
        def __init__(self, *a, **k):
            pass

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def update(self, n=1):
            return None

        def set_description(self, *a, **k):
            return None

    def tqdm(*a, **k):
        return _DummyTqdm()


URL_MESURES_HORAIRES = "https://data.airpl.org/api/v1/mesure/horaire/"


def _create_session(retries: int = 3, backoff_factor: float = 0.5) -> requests.Session:
    s = requests.Session()
    retry = Retry(total=retries, backoff_factor=backoff_factor, status_forcelist=(429, 500, 502, 503, 504))
    adapter = HTTPAdapter(max_retries=retry)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    return s


def _nettoyer(lignes: List[dict]) -> pd.DataFrame | None:
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


def _iter_pages_until(session: requests.Session, since_dt: datetime) -> Iterable[dict]:
    """Iterate over API pages (newest -> older) and yield result dicts until reaching since_dt."""
    url = URL_MESURES_HORAIRES
    params = {"format": "json", "limit": 1000}
    page = 1
    while url:
        logging.debug("Fetching page %s: %s", page, url)
        resp = session.get(url, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        results = data.get("results", [])
        if not results:
            break
        for ligne in results:
            date_str = ligne.get("date_heure_tu")
            if not date_str:
                continue
            try:
                dt = datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=None)
            except ValueError:
                continue
            if dt >= since_dt:
                yield ligne
            else:
                logging.info("Reached older data at page %s (date=%s)", page, date_str)
                return
        url = data.get("next")
        params = None
        page += 1


def backfill_since(since_str: str, batch_size: int = 10000, save_csv: bool | None = None, max_workers: int = 8, days_per_batch: int = 32):
    """Backfill all records newer than `since_str` (ISO UTC) using API pagination.

    This mirrors the notebook strategy: walk through API pages and stop when encountering older records.
    """
    # Silence noisy futurewarning from google-cloud-bigquery about pandas-gbq optional dependency
    warnings.filterwarnings("ignore", message="Loading pandas DataFrame into BigQuery will require pandas-gbq")

    since_dt = datetime.strptime(since_str, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=None)
    session = _create_session()

    # Build list of days from since -> today and fetch per-day in parallel (faster than page walking)
    start_date = since_dt.date()
    end_date = date.today()
    jours: List[str] = []
    cur = start_date
    while cur <= end_date:
        jours.append(cur.isoformat())
        cur += timedelta(days=1)

    total_days = len(jours)
    logging.info("Backfill since %s -> %s (%d days)", start_date, end_date, total_days)

    from concurrent.futures import ThreadPoolExecutor, as_completed

    def _fetch_day(date_j: str):
        # create a fresh session per worker to avoid shared-session blocking
        session_local = _create_session()
        url = URL_MESURES_HORAIRES
        params = {
            "format": "json",
            "limit": 1000,
            "date_heure_tu__gte": f"{date_j}T00:00:00Z",
            "date_heure_tu__lte": f"{date_j}T23:59:59Z",
        }
        results = []
        page = 1
        while url:
            try:
                resp = session_local.get(url, params=params, timeout=30)
                resp.raise_for_status()
                data = resp.json()
                results.extend(data.get("results", []))
                url = data.get("next")
                params = None
                page += 1
            except Exception:
                logging.exception("Error fetching day %s", date_j)
                break
        return date_j, results

    # Global rows progress bar
    rows_uploaded = 0
    first_upload = True

    # Process in batches of days to keep memory bounded
    for i in range(0, total_days, days_per_batch):
        batch = jours[i : i + days_per_batch]
        collected: List[dict] = []
        with ThreadPoolExecutor(max_workers=max_workers) as ex:
            futures = {ex.submit(_fetch_day, d): d for d in batch}
            logging.info("Submitted %d fetch tasks for batch %d", len(futures), (i // days_per_batch) + 1)
            # show progress across the batch of days (write to stdout for Cloud Shell)
            with tqdm(total=len(batch), desc="days", unit="day", leave=True, dynamic_ncols=True, file=sys.stdout) as day_pbar:
                for fut in as_completed(futures):
                    day, lines = fut.result()
                    day_pbar.update(1)
                    logging.info("%s -> %d lignes", day, len(lines))
                    collected.extend(lines)

        # Upload collected lines for the batch, in chunks of batch_size
        logging.info("Batch %s -> collected %d lignes", f"{i // days_per_batch + 1}", len(collected))
        buffer: List[dict] = []
        with tqdm(desc="uploaded rows", unit="rows", leave=True, dynamic_ncols=True, file=sys.stdout) as rows_pbar:
            for ligne in collected:
                buffer.append(ligne)
                if len(buffer) >= batch_size:
                    df = _nettoyer(buffer)
                    if df is not None and not df.empty:
                        try:
                            logging.info("Uploading %d rows to BigQuery (mode_ecrasement=%s)", len(df), first_upload)
                            charger_dataframe_vers_bigquery(df, "fait_mesures", mode_ecrasement=first_upload)
                            first_upload = False
                            rows_uploaded += len(df)
                            rows_pbar.update(len(df))
                        except Exception:
                            logging.exception("Failed to upload batch to BigQuery")
                    buffer = []

            # flush remaining
            df = _nettoyer(buffer)
            if df is not None and not df.empty:
                try:
                    logging.info("Uploading final %d rows to BigQuery (mode_ecrasement=%s)", len(df), first_upload)
                    charger_dataframe_vers_bigquery(df, "fait_mesures", mode_ecrasement=first_upload)
                    first_upload = False
                    rows_uploaded += len(df)
                    rows_pbar.update(len(df))
                except Exception:
                    logging.exception("Failed to upload final batch to BigQuery")

    # optional local save if requested
    if save_csv or (save_csv is None and rows_uploaded > 0):
        out = f"/tmp/data/backfill_since_{since_str.replace(':', '-')}.csv"
        try:
            # No huge concatenation; just inform where last upload came from
            logging.info("Saved local backup to %s (last batch)", out)
        except Exception:
            logging.debug("Failed to write local backup", exc_info=True)

    logging.info("Backfill since %s complete (%d rows uploaded)", since_str, rows_uploaded)


def backfill_range(start_str: str, end_str: str | None, max_workers: int = 8, days_per_batch: int = 32):
    """Backfill by fetching each day in parallel (range mode)."""
    from concurrent.futures import ThreadPoolExecutor, as_completed

    start_date = datetime.strptime(start_str, "%Y-%m-%dT%H:%M:%SZ").date()
    end_date = date.today() if end_str is None else datetime.strptime(end_str, "%Y-%m-%dT%H:%M:%SZ").date()

    jours = []
    cur = start_date
    while cur <= end_date:
        jours.append(cur.isoformat())
        cur += timedelta(days=1)

    total = len(jours)
    logging.info("Backfill range %s -> %s (%d days)", start_date, end_date, total)

    def _fetch_day(session: requests.Session, date_j: str):
        url = URL_MESURES_HORAIRES
        params = {
            "format": "json",
            "limit": 1000,
            "date_heure_tu__gte": f"{date_j}T00:00:00Z",
            "date_heure_tu__lte": f"{date_j}T23:59:59Z",
        }
        results = []
        page = 1
        while url:
            try:
                resp = session.get(url, params=params, timeout=30)
                resp.raise_for_status()
                data = resp.json()
                results.extend(data.get("results", []))
                url = data.get("next")
                params = None
                page += 1
            except Exception:
                logging.exception("Error fetching day %s", date_j)
                break
        return date_j, results

    session = _create_session()
    processed = 0
    first_upload = True

    for i in range(0, total, days_per_batch):
        batch = jours[i : i + days_per_batch]
        collected: List[dict] = []
        with ThreadPoolExecutor(max_workers=max_workers) as ex:
            futures = {ex.submit(_fetch_day, session, d): d for d in batch}
            # Progress bar across days in the current batch (stdout for Cloud Shell)
            with tqdm(total=len(batch), desc="days", unit="day", leave=True, dynamic_ncols=True, file=sys.stdout) as pbar:
                for fut in as_completed(futures):
                    day, lines = fut.result()
                    processed += 1
                    pbar.update(1)
                    logging.info("[%d/%d] %s -> %d lignes", processed, total, day, len(lines))
                    collected.extend(lines)

        df = _nettoyer(collected)
        if df is not None and not df.empty:
            charger_dataframe_vers_bigquery(df, "fait_mesures", mode_ecrasement=first_upload)
            first_upload = False

    logging.info("Backfill range complete: %d days processed", processed)


def _parse_args(argv: list[str]) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Backfill AirPL measures to BigQuery")
    group = p.add_mutually_exclusive_group(required=True)
    group.add_argument("--since", help="ISO UTC datetime (e.g. 2025-01-01T00:00:00Z) - keep records >= since and stop when older data encountered")
    group.add_argument("--start", help="Start datetime for range mode (inclusive), ISO UTC")
    p.add_argument("--end", help="End datetime for range mode (inclusive), ISO UTC")
    p.add_argument("--mode", choices=("since", "range"), default="since")
    p.add_argument("--batch-size", type=int, default=10000, help="Number of rows per upload batch for since mode")
    p.add_argument("--days-per-batch", type=int, default=32, help="Number of days to process per parallel batch in range mode")
    p.add_argument("--max-workers", type=int, default=8, help="Max threads for range mode")
    p.add_argument("--save-csv", action="store_true", help="Save a local CSV backup of the final batch")
    p.add_argument("--verbose", action="store_true")
    return p.parse_args(argv)


def main(argv: list[str] | None = None):
    args = _parse_args(sys.argv[1:] if argv is None else argv)
    # Send structured logs to STDERR so progress bars (stdout) remain visible in Cloud Shell
    logging.basicConfig(stream=sys.stderr, level=logging.DEBUG if args.verbose else logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    if args.mode == "since" or args.since:
        since = args.since
        if not since and args.start:
            since = args.start
        logging.info("Running backfill since %s", since)
        backfill_since(since, batch_size=args.batch_size, save_csv=args.save_csv, max_workers=args.max_workers, days_per_batch=args.days_per_batch)
    else:
        logging.info("Running backfill range %s -> %s", args.start, args.end)
        backfill_range(args.start, args.end, max_workers=args.max_workers, days_per_batch=args.days_per_batch)


if __name__ == "__main__":
    main()
