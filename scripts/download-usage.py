#!/usr/bin/env python3
"""
Download CircleCI Usage API data for the last N days.

Requires:
  - CIRCLECI_TOKEN env var (API token with org read scope)
  - CIRCLECI_ORG_ID env var

Writes CSV file(s) to --output-dir.
"""

import gzip
import os
import sys
import time
import argparse
import logging
from datetime import datetime, timedelta, timezone

import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

BASE_URL = "https://circleci.com/api/v2"


def start_export(token: str, org_id: str, start: str, end: str) -> str:
    r = requests.post(
        f"{BASE_URL}/organizations/{org_id}/usage_export_job",
        headers={"Circle-Token": token},
        json={"start": start, "end": end},
    )
    r.raise_for_status()
    job_id = r.json()["usage_export_job_id"]
    logger.info(f"Export job started: {job_id} ({start} to {end})")
    return job_id


def poll_until_ready(token: str, org_id: str, job_id: str, timeout: int = 600) -> dict:
    deadline = time.time() + timeout
    attempt = 0
    while time.time() < deadline:
        attempt += 1
        time.sleep(min(10, 2 ** min(attempt, 5)))
        r = requests.get(
            f"{BASE_URL}/organizations/{org_id}/usage_export_job/{job_id}",
            headers={"Circle-Token": token},
        )
        r.raise_for_status()
        data = r.json()
        state = data.get("state")
        logger.info(f"  Poll {attempt}: {state}")
        if state == "completed":
            return data
        if state in ("failed", "error"):
            raise RuntimeError(f"Export job failed: {data}")
    raise TimeoutError(f"Export job {job_id} did not complete within {timeout}s")


def download_csv(urls: list, output_dir: str, prefix: str) -> list:
    paths = []
    for i, url in enumerate(urls):
        r = requests.get(url)
        r.raise_for_status()
        fname = f"{prefix}-{i}.csv" if len(urls) > 1 else f"{prefix}.csv"
        path = os.path.join(output_dir, fname)
        content = r.content
        if content[:2] == b'\x1f\x8b':
            content = gzip.decompress(content)
            logger.info(f"Decompressed gzip response ({len(r.content):,} -> {len(content):,} bytes)")
        with open(path, "wb") as f:
            f.write(content)
        logger.info(f"Downloaded {path} ({len(content):,} bytes)")
        paths.append(path)
    return paths


def main():
    parser = argparse.ArgumentParser(description="Download CircleCI Usage API data")
    parser.add_argument("--lookback-days", type=int, default=3,
                        help="Number of days to look back (default: 3)")
    parser.add_argument("--output-dir", default="./data",
                        help="Directory to write CSV files")
    parser.add_argument("--timeout", type=int, default=600,
                        help="Max seconds to wait for export job")
    args = parser.parse_args()

    token = os.environ.get("CIRCLECI_TOKEN")
    org_id = os.environ.get("CIRCLECI_ORG_ID")
    if not token or not org_id:
        logger.error("CIRCLECI_TOKEN and CIRCLECI_ORG_ID env vars are required")
        sys.exit(1)

    os.makedirs(args.output_dir, exist_ok=True)

    end_date = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    start_date = (datetime.now(timezone.utc) - timedelta(days=args.lookback_days)).strftime("%Y-%m-%dT%H:%M:%SZ")

    job_id = start_export(token, org_id, start_date, end_date)
    result = poll_until_ready(token, org_id, job_id, args.timeout)
    urls = result.get("download_urls", [])
    if not urls:
        logger.warning("Export completed but returned no download URLs (no data in range?)")
        sys.exit(0)

    paths = download_csv(urls, args.output_dir, f"usage-{start_date}-to-{end_date}")
    logger.info(f"Done — {len(paths)} file(s) in {args.output_dir}")


if __name__ == "__main__":
    main()
