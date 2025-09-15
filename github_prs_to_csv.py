#!/usr/bin/env python3
"""
github_prs_to_csv.py — Export GitHub PR metrics to a time-bucketed CSV for Grafana.

Usage:
  python github_prs_to_csv.py --owner valkey-io --repo valkey --since-days 90 \
      --metric opened --bucket weekly --out prs_opened_weekly.csv

Notes:
- Uses the Search API (`/search/issues`) with correct space-separated qualifiers.
- For `--metric merged`, this script fetches each PR to read `merged_at`.
- Respects GITHUB_TOKEN env var if --token is not supplied (recommended to increase rate limit).
"""
import os
import csv
import time
import argparse
from datetime import datetime, timedelta, timezone
from collections import defaultdict

import requests

SEARCH_URL = "https://api.github.com/search/issues"
PULL_URL_TPL = "https://api.github.com/repos/{owner}/{repo}/pulls/{number}"

def parse_args():
    p = argparse.ArgumentParser(description="Export PR trends to CSV")
    p.add_argument("--owner", required=True, help="Repository owner/org (e.g. valkey-io)")
    p.add_argument("--repo", required=True, help="Repository name (e.g. valkey)")
    g = p.add_mutually_exclusive_group(required=True)
    g.add_argument("--since-days", type=int, help="Look back this many days from today (UTC)")
    g.add_argument("--since", help="Start date YYYY-MM-DD (UTC)")
    p.add_argument("--until", help="End date YYYY-MM-DD (UTC). Defaults to today.")
    p.add_argument("--metric", choices=["opened", "closed", "merged"], default="opened",
                   help="Which timestamp to count by")
    p.add_argument("--bucket", choices=["daily", "weekly", "monthly"], default="weekly",
                   help="Time bucket for aggregation")
    p.add_argument("--out", required=True, help="Output CSV path")
    p.add_argument("--token", default=os.getenv("GITHUB_TOKEN"), help="GitHub token (or set GITHUB_TOKEN)")
    p.add_argument("--max-pages", type=int, default=10, help="Max pages per search (100 results/page)")
    p.add_argument("--pause", type=float, default=0.2, help="Pause between paginated requests (seconds)")
    p.add_argument("--chunk-days", type=int, default=30,
                   help="Split the query range into N-day chunks to avoid Search API 1,000 result cap")
    return p.parse_args()

def to_date(s: str):
    return datetime.strptime(s, "%Y-%m-%d").date()

def calc_range(args):
    today = datetime.now(timezone.utc).date()
    start = today - timedelta(days=args.since_days) if args.since_days is not None else to_date(args.since)
    end = to_date(args.until) if args.until else today
    if start > end:
        raise SystemExit(f"--since/--since-days must be <= --until. Got start={start} end={end}")
    return start, end

def daterange_chunks(start_date, end_date, chunk_days):
    """Yield (chunk_start, chunk_end) inclusive, stepping by chunk_days."""
    cur = start_date
    one_day = timedelta(days=1)
    step = timedelta(days=chunk_days - 1)  # inclusive range
    while cur <= end_date:
        chunk_end = min(end_date, cur + step)
        yield cur, chunk_end
        cur = chunk_end + one_day

def build_query(owner, repo, metric, start, end):
    """
    Build a GitHub search query with *spaces* (do NOT insert '+').
    requests will encode spaces to '+' automatically.
    """
    base = f"repo:{owner}/{repo} is:pr"
    if metric == "opened":
        return f"{base} created:{start}..{end}"
    if metric == "closed":
        return f"{base} is:closed closed:{start}..{end}"
    # merged
    return f"{base} is:merged merged:{start}..{end}"

def bucket_key(dt: datetime, bucket: str) -> str:
    dt = dt.astimezone(timezone.utc)
    if bucket == "daily":
        b = datetime(dt.year, dt.month, dt.day, tzinfo=timezone.utc)
    elif bucket == "weekly":
        monday = dt - timedelta(days=dt.weekday())
        b = datetime(monday.year, monday.month, monday.day, tzinfo=timezone.utc)
    else:
        b = datetime(dt.year, dt.month, 1, tzinfo=timezone.utc)
    # Grafana-friendly ISO8601 with Z
    return b.strftime("%Y-%m-%dT%H:%M:%SZ")

def fetch_search(session, query, token, per_page=100, max_pages=10, pause=0.2):
    headers = {"Accept": "application/vnd.github+json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    all_items = []
    for page in range(1, max_pages + 1):
        r = session.get(SEARCH_URL, headers=headers,
                        params={"q": query, "per_page": per_page, "page": page},
                        timeout=30)
        if r.status_code != 200:
            # Surface GitHub's JSON error payload if available
            try:
                err = r.json()
            except Exception:
                err = r.text
            raise SystemExit(f"GitHub search error {r.status_code} for query `{query}` page {page}:\n{err}")
        data = r.json()
        items = data.get("items", [])
        all_items += items
        if len(items) < per_page:
            break
        time.sleep(pause)
    return all_items

def fetch_pull_merged_at(session, owner, repo, number, token):
    headers = {"Accept": "application/vnd.github+json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    url = PULL_URL_TPL.format(owner=owner, repo=repo, number=number)
    r = session.get(url, headers=headers, timeout=30)
    if r.status_code != 200:
        try:
            err = r.json()
        except Exception:
            err = r.text
        raise SystemExit(f"GitHub PR fetch error {r.status_code} for PR #{number}:\n{err}")
    return r.json().get("merged_at")

def main():
    a = parse_args()
    start_date, end_date = calc_range(a)

    counts = defaultdict(int)
    session = requests.Session()

    for chunk_start, chunk_end in daterange_chunks(start_date, end_date, a.chunk_days):
        q = build_query(a.owner, a.repo, a.metric, chunk_start, chunk_end)
        items = fetch_search(session, q, a.token, max_pages=a.max_pages, pause=a.pause)
        # Process items
        for it in items:
            if a.metric == "opened":
                ts = it.get("created_at")
            elif a.metric == "closed":
                ts = it.get("closed_at")
            else:  # merged
                num = it.get("number")
                ts = fetch_pull_merged_at(session, a.owner, a.repo, num, a.token) if num is not None else None
            if not ts:
                continue
            dt = datetime.strptime(ts, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
            key = bucket_key(dt, a.bucket)
            counts[key] += 1

    rows = sorted(counts.items())
    with open(a.out, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["time", "count"])
        w.writerows(rows)
    print(f"Wrote {len(rows)} rows to {a.out}")

if __name__ == "__main__":
    main()
