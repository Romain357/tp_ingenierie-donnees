"""Backfill utility for AirPL measures.

Improvements over the original version:
- CLI with `--since` or `--start/--end` modes
- HTTP session with retries and backoff
- Optional parallel per-day fetching or single-pass pagination until a cutoff date
- Batch upload to BigQuery
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import datetime, date, timedelta
from functools import lru_cache
from urllib.parse import parse_qs, urlparse
from typing import Iterable, List, Optional

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from bq_utils import charger_dataframe_vers_bigquery


URL_MESURES_HORAIRES = "https://data.airpl.org/api/v1/mesure/horaire/"
POLLUANTS_AUTORISES = {1, 3, 8, 24, 39}
DEPARTEMENTS_CIBLES = {44, 49, 53, 72, 85}


def _est_valide(mesure: dict) -> bool:
    valeur = mesure.get("validite")
    return valeur is True or str(valeur).strip().lower() == "true"


def _create_session(retries: int = 6, backoff_factor: float = 1.0, pool_size: int = 25) -> requests.Session:
    s = requests.Session()
    retry = Retry(
        total=retries,
        connect=retries,
        read=retries,
        backoff_factor=backoff_factor,
        status_forcelist=(429, 500, 502, 503, 504),
        respect_retry_after_header=True,
    )
    # tune connection pool for concurrent workers and block when the pool is exhausted
    adapter = HTTPAdapter(max_retries=retry, pool_connections=pool_size, pool_maxsize=pool_size, pool_block=True)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    return s


@lru_cache(maxsize=1)
def _polluants_query_string() -> str:
    return ",".join(str(x) for x in sorted(POLLUANTS_AUTORISES))


@lru_cache(maxsize=1)
def _departements_query_string() -> str:
    return ",".join(str(x) for x in sorted(DEPARTEMENTS_CIBLES))


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
    # Filtrer uniquement les mesures valides
    if "validite" in df.columns:
        df = df[df["validite"].astype(str).str.lower() == "true"].copy()
        df = df.drop(columns=[c for c in ("validite",) if c in df.columns])
    # Filtrer uniquement les polluants autorisés et normaliser le format
    if "code_polluant" in df.columns:
        df["code_polluant"] = pd.to_numeric(df["code_polluant"], errors="coerce")
        df = df[df["code_polluant"].isin(POLLUANTS_AUTORISES)].copy()
        # stocker comme string propre pour BigQuery
        df["code_polluant"] = df["code_polluant"].astype("Int64").astype("string")
    return df.dropna(subset=["code_station", "code_polluant", "insee_com"])


def _offset_from_next(next_url: Optional[str]) -> int:
    if not next_url:
        return 0
    try:
        query = parse_qs(urlparse(next_url).query)
        raw = query.get("offset", ["0"])[0]
        return int(raw)
    except Exception:
        return 0


def _fetch_day_window(
    pool_size: int,
    day: str,
    start_iso: str,
    end_iso: str,
    max_pages_per_day: int,
    max_offset_guard: int,
) -> tuple[str, List[dict]]:
    session = _create_session(pool_size=pool_size)
    url = URL_MESURES_HORAIRES
    params = {
        "format": "json",
        "limit": 5000,
        "date_heure_tu__range": f"{start_iso[:10]},{end_iso[:10]}",
        "code_polluant__in": _polluants_query_string(),
        "validite": "true",
        "code_configuration_de_mesure__code_point_de_prelevement__code_station__code_commune__code_departement__in": _departements_query_string(),
    }
    results: List[dict] = []
    page = 1
    while url:
        try:
            if page > max_pages_per_day:
                logging.warning("Stopping day %s: reached max_pages_per_day=%d", day, max_pages_per_day)
                break
            resp = session.get(url, params=params, timeout=30)
            resp.raise_for_status()
            data = resp.json()
            page_results = data.get("results", [])
            results.extend(ligne for ligne in page_results if _est_valide(ligne))
            url = data.get("next")
            next_offset = _offset_from_next(url)
            if next_offset > max_offset_guard:
                logging.warning(
                    "Stopping day %s: next offset %d exceeds guard %d (filters likely too broad)",
                    day,
                    next_offset,
                    max_offset_guard,
                )
                break
            params = None
            page += 1
        except Exception:
            logging.exception("Error fetching day %s", day)
            break
    return day, results


def _iter_since_pages(
    session: requests.Session,
    since_dt: datetime,
    max_pages: int,
    max_offset_guard: int,
) -> Iterable[dict]:
    url = URL_MESURES_HORAIRES
    params = {
        "format": "json",
        "limit": 5000,
        "date_heure_tu__gte": since_dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "code_polluant__in": _polluants_query_string(),
        "validite": "true",
        "code_configuration_de_mesure__code_point_de_prelevement__code_station__code_commune__code_departement__in": _departements_query_string(),
    }

    page = 1
    while url:
        if page > max_pages:
            logging.warning("Stopping since-mode scan: reached max pages=%d", max_pages)
            break

        resp = session.get(url, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        results = data.get("results", [])
        if not results:
            break

        oldest_dt = None
        for ligne in results:
            date_str = ligne.get("date_heure_tu")
            if not date_str:
                continue
            try:
                dt = datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=None)
            except ValueError:
                continue

            if oldest_dt is None or dt < oldest_dt:
                oldest_dt = dt

            if dt >= since_dt and _est_valide(ligne):
                yield ligne

        if oldest_dt is not None and oldest_dt < since_dt:
            logging.info("Reached cutoff date at page %d (oldest=%s)", page, oldest_dt.isoformat())
            break

        url = data.get("next")
        next_offset = _offset_from_next(url)
        if next_offset > max_offset_guard:
            logging.warning(
                "Stopping since-mode scan: next offset %d exceeds guard %d",
                next_offset,
                max_offset_guard,
            )
            break

        params = None
        page += 1


def backfill_since(
    since_str: str,
    batch_size: int = 10000,
    max_workers: int = 8,
    days_per_batch: int = 32,
    upload_workers: int = 4,
    http_pool_size: int = 25,
    max_pages_per_day: int = 200,
    max_offset_guard: int = 300000,
):
    """Backfill all records newer than `since_str` (ISO UTC) with a single paginated scan.

    Using one descending scan avoids restarting from offset 0 for each day.
    """
    since_dt = datetime.strptime(since_str, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=None)
    effective_upload_workers = min(upload_workers, 8)
    session = _create_session(pool_size=http_pool_size)

    if max_workers != 8 or days_per_batch != 32:
        logging.info("Since-mode now uses single-pass scan; --max-workers and --days-per-batch are ignored")
    if effective_upload_workers != upload_workers:
        logging.info("Capping upload-workers from %d to %d to avoid oversaturating BigQuery uploads", upload_workers, effective_upload_workers)
    logging.info("Backfill since %s (single-pass)", since_dt.isoformat())

    from concurrent.futures import ThreadPoolExecutor, as_completed

    upload_executor = ThreadPoolExecutor(max_workers=effective_upload_workers)
    upload_futures = []
    first_upload = True
    total_rows = 0
    buffer: List[dict] = []

    for ligne in _iter_since_pages(session, since_dt, max_pages=max_pages_per_day * 50, max_offset_guard=max_offset_guard * 20):
        buffer.append(ligne)
        if len(buffer) < batch_size:
            continue

        df = _nettoyer(buffer)
        if df is not None and not df.empty:
            mode_flag = first_upload
            first_upload = False
            fut = upload_executor.submit(charger_dataframe_vers_bigquery, df, "fait_mesures_heure", mode_flag)
            upload_futures.append(fut)
            total_rows += len(df)
            logging.info("Queued upload batch: %d rows (total queued=%d)", len(df), total_rows)
        buffer = []

    # flush tail
    df = _nettoyer(buffer)
    if df is not None and not df.empty:
        mode_flag = first_upload
        first_upload = False
        fut = upload_executor.submit(charger_dataframe_vers_bigquery, df, "fait_mesures_heure", mode_flag)
        upload_futures.append(fut)
        total_rows += len(df)
        logging.info("Queued final upload batch: %d rows (total queued=%d)", len(df), total_rows)

    # wait for uploads to finish
    for fut in upload_futures:
        try:
            fut.result()
        except Exception:
            logging.exception("Upload task failed")

    upload_executor.shutdown(wait=True)

    logging.info("Backfill since %s complete (%d rows queued for upload)", since_str, total_rows)


def backfill_range(
    start_str: str,
    end_str: Optional[str],
    max_workers: int = 8,
    days_per_batch: int = 32,
    http_pool_size: int = 25,
    max_pages_per_day: int = 200,
    max_offset_guard: int = 300000,
):
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
    effective_max_workers = min(max_workers, 12)
    if effective_max_workers != max_workers:
        logging.info("Capping max-workers from %d to %d to reduce API contention", max_workers, effective_max_workers)

    processed = 0
    first_upload = True

    effective_days_per_batch = min(days_per_batch, effective_max_workers)
    for i in range(0, total, effective_days_per_batch):
        batch = jours[i : i + effective_days_per_batch]
        collected: List[dict] = []
        with ThreadPoolExecutor(max_workers=effective_max_workers) as ex:
            futures = {
                ex.submit(
                    _fetch_day_window,
                    http_pool_size,
                    d,
                    f"{d}T00:00:00Z",
                    f"{d}T23:59:59Z",
                    max_pages_per_day,
                    max_offset_guard,
                ): d
                for d in batch
            }
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
    p.add_argument("--max-pages-per-day", type=int, default=200, help="Safety cap on API pages fetched per day")
    p.add_argument("--max-offset-guard", type=int, default=300000, help="Safety guard: stop a day if next offset exceeds this value")
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
        backfill_since(
            since,
            batch_size=args.batch_size,
            max_workers=args.max_workers,
            days_per_batch=args.days_per_batch,
            upload_workers=args.upload_workers,
            http_pool_size=args.http_pool_size,
            max_pages_per_day=args.max_pages_per_day,
            max_offset_guard=args.max_offset_guard,
        )
    else:
        logging.info("Running backfill range %s -> %s", args.start, args.end)
        backfill_range(
            args.start,
            args.end,
            max_workers=args.max_workers,
            days_per_batch=args.days_per_batch,
            http_pool_size=args.http_pool_size,
            max_pages_per_day=args.max_pages_per_day,
            max_offset_guard=args.max_offset_guard,
        )


if __name__ == "__main__":
    main()
