#!/usr/bin/env python3
# major_decision_weekly.py
#
# Writes only two CSVs for the label "Major decision pending":
#   data/label-major-decision-pending_opened_weekly.csv
#   data/label-major-decision-pending_closed_weekly.csv

import os, csv, time, argparse, requests
from datetime import datetime, date, timedelta, timezone

LABEL_TEXT = "major-decision-pending"

def parse_iso_z(ts: str) -> datetime:
    return datetime.strptime(ts, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)

def monday_bucket(dt: datetime) -> date:
    d = dt.date()
    return d - timedelta(days=d.weekday())

def write_csv(path: str, counter: dict):
    rows = sorted(counter.items())
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["time", "count"])
        for k, v in rows:
            dt = datetime(k.year, k.month, k.day, 0, 0, 0, tzinfo=timezone.utc)
            w.writerow([dt.strftime("%Y-%m-%dT00:00:00Z"), v])
    print(f"Wrote {len(rows)} rows to {path}")

def search_label(owner: str, repo: str, which: str, start: date, end: date, token: str):
    base = f'repo:{owner}/{repo} is:issue label:"{LABEL_TEXT}"'
    if which == "opened":
        q = f"{base} created:{start.isoformat()}..{end.isoformat()}"
        ts_field = "created_at"
    else:
        q = f"{base} is:closed closed:{start.isoformat()}..{end.isoformat()}"
        ts_field = "closed_at"

    url = "https://api.github.com/search/issues"
    headers = {
        "Accept": "application/vnd.github+json",
        "User-Agent": "major-decision-weekly/1.0",
        "Authorization": f"Bearer {token}",
    }
    params = {"q": q, "per_page": 100, "page": 1}

    items = []
    while True:
        r = requests.get(url, headers=headers, params=params, timeout=30)
        if r.status_code == 403:
            print("Hit rate limit; pausing 5s…")
            time.sleep(5)
            continue
        r.raise_for_status()
        data = r.json()
        page_items = data.get("items", [])
        items.extend(page_items)
        if len(page_items) < 100:
            break
        params["page"] += 1
        time.sleep(0.5)
    return items, ts_field

def main():
    ap = argparse.ArgumentParser(description="Weekly CSVs for the 'Major decision pending' label")
    ap.add_argument("--owner", required=True)
    ap.add_argument("--repo", required=True)
    ap.add_argument("--since-days", type=int, default=180)
    ap.add_argument("--out-dir", default="data")
    args = ap.parse_args()

    token = os.getenv("GITHUB_TOKEN")
    if not token:
        raise SystemExit("GITHUB_TOKEN environment variable is required.")

    end = datetime.now(timezone.utc).date()
    start = end - timedelta(days=args.since_days)

    # Opened
    opened_items, opened_field = search_label(args.owner, args.repo, "opened", start, end, token)
    opened_weekly = {}
    for it in opened_items:
        ts = it.get(opened_field)
        if ts:
            dt = parse_iso_z(ts)
            wk = monday_bucket(dt)
            opened_weekly[wk] = opened_weekly.get(wk, 0) + 1
    write_csv(os.path.join(args.out_dir, "label-major-decision-pending_opened_weekly.csv"), opened_weekly)

    # Closed
    closed_items, closed_field = search_label(args.owner, args.repo, "closed", start, end, token)
    closed_weekly = {}
    for it in closed_items:
        ts = it.get(closed_field)
        if ts:
            dt = parse_iso_z(ts)
            wk = monday_bucket(dt)
            closed_weekly[wk] = closed_weekly.get(wk, 0) + 1
    write_csv(os.path.join(args.out_dir, "label-major-decision-pending_closed_weekly.csv"), closed_weekly)

if __name__ == "__main__":
    main()
