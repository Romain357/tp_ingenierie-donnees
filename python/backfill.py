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
from typing import Iterable, List, Optional

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from bq_utils import charger_dataframe_vers_bigquery


URL_MESURES_HORAIRES = "https://data.airpl.org/api/v1/mesure/horaire/"


def _est_valide(mesure: dict) -> bool:
    valeur = mesure.get("validite")
    return valeur is True or str(valeur).strip().lower() == "true"


def _create_session(retries: int = 3, backoff_factor: float = 0.5, pool_size: int = 25) -> requests.Session:
    s = requests.Session()
    retry = Retry(total=retries, backoff_factor=backoff_factor, status_forcelist=(429, 500, 502, 503, 504))
    # tune connection pool for concurrent workers
    adapter = HTTPAdapter(max_retries=retry, pool_connections=pool_size, pool_maxsize=pool_size)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    return s


def _nettoyer(lignes: List[dict]) -> Optional[pd.DataFrame]:
    if not lignes:
        return None
    df = pd.DataFrame(lignes)
    cols = ["id", "code_station", "code_polluant", "code_commune", "valeur", "date_heure_tu"]
    df = df[[c for c in cols if c in df.columns]].copy()
    df = df.rename(columns={
        "code_commune": "insee_com",
        "date_heure_tu": "date_mesure",
    })
    return df.dropna(subset=["code_station", "code_polluant", "insee_com"])


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
                if _est_valide(ligne):
                    yield ligne
            else:
                logging.info("Reached older data at page %s (date=%s)", page, date_str)
                return
        url = data.get("next")
        params = None
        page += 1


def backfill_since(since_str: str, batch_size: int = 10000, save_csv: Optional[bool] = None, upload_workers: int = 4, http_pool_size: int = 25):
    """Backfill all records newer than `since_str` (ISO UTC) using API pagination.

    This mirrors the notebook strategy: walk through API pages and stop when encountering older records.
    """
    since_dt = datetime.strptime(since_str, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=None)
    session = _create_session(pool_size=http_pool_size)

    buffer: List[dict] = []
    first_upload = True
    total = 0
    from concurrent.futures import ThreadPoolExecutor, as_completed

    # executor for uploads
    upload_executor = ThreadPoolExecutor(max_workers=upload_workers)
    upload_futures = []

    for ligne in _iter_pages_until(session, since_dt):
        buffer.append(ligne)
        if len(buffer) >= batch_size:
            df = _nettoyer(buffer)
            if df is not None and not df.empty:
                mode_flag = first_upload
                first_upload = False
                # submit upload and continue fetching
                fut = upload_executor.submit(charger_dataframe_vers_bigquery, df, "fait_mesures_heure", mode_flag)
                upload_futures.append(fut)
                total += len(df)
            buffer = []

    # final flush
    df = _nettoyer(buffer)
    if df is not None and not df.empty:
        mode_flag = first_upload
        first_upload = False
        fut = upload_executor.submit(charger_dataframe_vers_bigquery, df, "fait_mesures_heure", mode_flag)
        upload_futures.append(fut)
        total += len(df)

    # wait for uploads to finish
    for fut in upload_futures:
        try:
            fut.result()
        except Exception:
            logging.exception("Upload task failed")

    upload_executor.shutdown(wait=True)

    # optional local save if requested or when running in dry-run style
    if save_csv or (save_csv is None and total > 0):
        out = f"/tmp/data/backfill_since_{since_str.replace(':', '-')}.csv"
        pd.concat([df]) if df is not None else None
        try:
            all_df = _nettoyer(buffer) if buffer else df
            if all_df is not None and not all_df.empty:
                all_df.to_csv(out, index=False, encoding="utf-8")
                logging.info("Saved local backup to %s", out)
        except Exception:
            logging.debug("Failed to write local backup", exc_info=True)

    logging.info("Backfill since %s complete (%d rows uploaded)", since_str, total)


def backfill_range(start_str: str, end_str: Optional[str], max_workers: int = 8, days_per_batch: int = 32, http_pool_size: int = 25):
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
                results.extend(ligne for ligne in data.get("results", []) if _est_valide(ligne))
                url = data.get("next")
                params = None
                page += 1
            except Exception:
                logging.exception("Error fetching day %s", date_j)
                break
        return date_j, results

    session = _create_session(pool_size=http_pool_size)
    processed = 0
    first_upload = True

    for i in range(0, total, days_per_batch):
        batch = jours[i : i + days_per_batch]
        collected: List[dict] = []
        with ThreadPoolExecutor(max_workers=max_workers) as ex:
            futures = {ex.submit(_fetch_day, session, d): d for d in batch}
            for fut in as_completed(futures):
                day, lines = fut.result()
                processed += 1
                logging.info("[%d/%d] %s -> %d lignes", processed, total, day, len(lines))
                collected.extend(lines)

        df = _nettoyer(collected)
        if df is not None and not df.empty:
            charger_dataframe_vers_bigquery(df, "fait_mesures_heure", mode_ecrasement=first_upload)
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
    p.add_argument("--upload-workers", type=int, default=4, help="Number of parallel upload workers")
    p.add_argument("--http-pool-size", type=int, default=25, help="HTTP connection pool size per session")
    p.add_argument("--save-csv", action="store_true", help="Save a local CSV backup of the final batch")
    p.add_argument("--verbose", action="store_true")
    return p.parse_args(argv)


def main(argv=None):
    args = _parse_args(sys.argv[1:] if argv is None else argv)
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    if args.mode == "since" or args.since:
        since = args.since
        if not since and args.start:
            since = args.start
        logging.info("Running backfill since %s", since)
        backfill_since(since, batch_size=args.batch_size, save_csv=args.save_csv, upload_workers=args.upload_workers, http_pool_size=args.http_pool_size)
    else:
        logging.info("Running backfill range %s -> %s", args.start, args.end)
        backfill_range(args.start, args.end, max_workers=args.max_workers, days_per_batch=args.days_per_batch, http_pool_size=args.http_pool_size)


if __name__ == "__main__":
    main()
