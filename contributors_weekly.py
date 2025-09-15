#!/usr/bin/env python3
# contributors_weekly.py
# Count unique contributors per week using the GitHub Commits API (accurate, no snapshots needed).

import os
import csv
import time
import argparse
from datetime import datetime, timedelta, timezone
from collections import defaultdict

import requests

COMMITS_URL = "https://api.github.com/repos/{owner}/{repo}/commits"

def gh_headers(token: str | None):
    h = {"Accept": "application/vnd.github+json"}
    if token:
        h["Authorization"] = f"Bearer {token}"
    return h

def monday_bucket(dt: datetime) -> datetime:
    dt = dt.astimezone(timezone.utc)
    monday = dt - timedelta(days=dt.weekday())
    return datetime(monday.year, monday.month, monday.day, tzinfo=timezone.utc)

def iso_z(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

def fetch_commits(owner, repo, since_iso, until_iso, token, per_page=100, max_pages=100, pause=0.2):
    session = requests.Session()
    headers = gh_headers(token)
    url = COMMITS_URL.format(owner=owner, repo=repo)
    all_commits = []
    for page in range(1, max_pages + 1):
        r = session.get(
            url,
            headers=headers,
            params={"since": since_iso, "until": until_iso, "per_page": per_page, "page": page},
            timeout=30,
        )
        if r.status_code != 200:
            try:
                err = r.json()
            except Exception:
                err = r.text
            raise SystemExit(f"GitHub commits error {r.status_code} page {page}:\n{err}")
        items = r.json()
        if not isinstance(items, list) or not items:
            break
        all_commits.extend(items)
        if len(items) < per_page:
            break
        time.sleep(pause)
    return all_commits

def main():
    ap = argparse.ArgumentParser(description="Generate weekly unique contributors CSV from GitHub commits.")
    ap.add_argument("--owner", required=True, help="Repository owner/org (e.g., valkey-io)")
    ap.add_argument("--repo", required=True, help="Repository name (e.g., valkey)")
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--since-days", type=int, help="Look back this many days from today (UTC)")
    g.add_argument("--since", help="Start date YYYY-MM-DD (UTC)")
    ap.add_argument("--until", help="End date YYYY-MM-DD (UTC). Defaults to today.")
    ap.add_argument("--out", required=True, help="Output CSV path (e.g., contributors_weekly.csv)")
    ap.add_argument("--token", default=os.getenv("GITHUB_TOKEN"), help="GitHub token (or set GITHUB_TOKEN)")
    ap.add_argument("--exclude-bots", action="store_true", help="Exclude authors whose login ends with [bot]")
    args = ap.parse_args()

    today = datetime.now(timezone.utc).date()
    start_date = (today - timedelta(days=args.since_days)) if args.since_days is not None else datetime.strptime(args.since, "%Y-%m-%d").date()
    end_date = datetime.strptime(args.until, "%Y-%m-%d").date() if args.until else today
    if start_date > end_date:
        raise SystemExit(f"--since/--since-days must be <= --until. Got start={start_date} end={end_date}")

    # API expects full timestamps
    since_iso = datetime(start_date.year, start_date.month, start_date.day, tzinfo=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    until_iso_dt = datetime(end_date.year, end_date.month, end_date.day, tzinfo=timezone.utc) + timedelta(days=1)
    until_iso = until_iso_dt.strftime("%Y-%m-%dT%H:%M:%SZ")

    commits = fetch_commits(args.owner, args.repo, since_iso, until_iso, args.token)

    # Aggregate unique contributors (login/email) per Monday bucket (UTC)
    buckets: dict[str, set] = defaultdict(set)
    for c in commits:
        # commit timestamp
        try:
            ts = c["commit"]["author"]["date"]
            dt = datetime.strptime(ts, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
        except Exception:
            continue

        # contributor identity: prefer GitHub login; fallback to commit email
        login = (c.get("author") or {}).get("login")
        email = (c.get("commit") or {}).get("author", {}).get("email")
        ident = login or email
        if not ident:
            continue

        # optional bot filter
        if args.exclude_bots and isinstance(login, str) and login.endswith("[bot]"):
            continue

        key = iso_z(monday_bucket(dt))
        buckets[key].add(ident)

    # Write CSV
    rows = sorted((k, len(v)) for k, v in buckets.items())
    with open(args.out, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["time", "count"])
        w.writerows(rows)

    print(f"Wrote {len(rows)} rows to {args.out}")

if __name__ == "__main__":
    main()
