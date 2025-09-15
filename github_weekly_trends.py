#!/usr/bin/env python3
# github_weekly_trends.py
#
# Produces weekly/monthly CSVs for Grafana dashboards from a GitHub repo.
# Outputs clean, unambiguous bucket labels:
#  - weekly: Monday 00:00:00Z (YYYY-MM-DDT00:00:00Z)
#  - monthly: 1st of month 00:00:00Z (YYYY-MM-01T00:00:00Z)
#
# New in this version:
# - Multi-label support via --labels "label1,label2,Label With Spaces"
#   -> emits label-<slug>_opened_weekly.csv and label-<slug>_closed_weekly.csv for each label.
# - Backwards compatible: --enhancement-label still works if --labels is not provided.

import os
import sys
import csv
import time
import argparse
from collections import defaultdict
from datetime import datetime, timezone, date, timedelta
import requests
from typing import Optional, Tuple, Dict, Iterable

# -----------------------------
# HTTP helpers
# -----------------------------

def make_session(token: Optional[str]) -> requests.Session:
    s = requests.Session()
    headers = {
        "Accept": "application/vnd.github+json",
        "User-Agent": "github-weekly-trends/1.1",
    }
    if token:
        headers["Authorization"] = f"Bearer {token}"
    s.headers.update(headers)
    return s

def get_json(session: requests.Session, url: str, params: Optional[dict] = None, timeout: int = 30):
    r = session.get(url, params=params or {}, timeout=timeout)
    r.raise_for_status()
    return r.json(), r.links

# -----------------------------
# Date helpers
# -----------------------------

def today_utc_date() -> date:
    return datetime.now(timezone.utc).date()

def monday_bucket(dtime: datetime) -> date:
    """Return the Monday (as date) for the week containing dtime."""
    d = dtime.date()
    return d - timedelta(days=d.weekday())

def month_bucket(dtime: datetime) -> date:
    """Return the first day of the month (as date) for dtime."""
    d = dtime.date()
    return date(d.year, d.month, 1)

def parse_iso_z(ts: str) -> datetime:
    # ts like "2025-09-12T12:35:18Z"
    return datetime.strptime(ts, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)

# -----------------------------
# Writing CSVs (robust to date/datetime/str keys)
# -----------------------------

def write_csv(path: str, counter: Dict) -> None:
    rows = sorted(counter.items())
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["time", "count"])
        for k, v in rows:
            # Accept keys as date, datetime, or ISO string
            if isinstance(k, date) and not isinstance(k, datetime):
                dt = datetime(k.year, k.month, k.day, 0, 0, 0, tzinfo=timezone.utc)
            elif isinstance(k, datetime):
                dt = k.astimezone(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
            elif isinstance(k, str):
                # Try strict ISO first, then YYYY-MM-DD
                try:
                    parsed = datetime.fromisoformat(k.replace("Z", "+00:00"))
                except Exception:
                    parsed = datetime.strptime(k, "%Y-%m-%d")
                dt = parsed.replace(tzinfo=timezone.utc, hour=0, minute=0, second=0, microsecond=0)
            else:
                raise TypeError(f"Unsupported key type in write_csv: {type(k)}")

            iso_label = dt.strftime("%Y-%m-%dT00:00:00Z")
            w.writerow([iso_label, v])
    print(f"Wrote {len(rows)} rows to {path}")

# -----------------------------
# Range & chunking
# -----------------------------

def compute_window(since_days: int) -> Tuple[date, date]:
    end = today_utc_date()
    start = end - timedelta(days=since_days)
    return start, end

def daterange_chunks(start: date, end: date, chunk_days: int) -> Iterable[Tuple[str, str]]:
    """
    Yield [start, end] inclusive chunks as ISO date strings for GitHub search.
    """
    s = start
    one_day = timedelta(days=1)
    while s <= end:
        e = min(s + timedelta(days=chunk_days - 1), end)
        yield (s.isoformat(), e.isoformat())
        s = e + one_day

# -----------------------------
# Search builders
# -----------------------------

def build_query_pr(owner, repo, which, start_date, end_date) -> str:
    """
    which in {"opened","closed","merged"}
    """
    base = f"repo:{owner}/{repo} is:pr"
    if which == "opened":
        return f"{base} created:{start_date}..{end_date}"
    elif which == "closed":
        return f"{base} is:closed closed:{start_date}..{end_date}"
    elif which == "merged":
        return f"{base} is:merged merged:{start_date}..{end_date}"
    else:
        raise ValueError("which must be opened|closed|merged")

def build_query_issue(owner, repo, which, start_date, end_date) -> str:
    """
    which in {"opened","closed"}
    """
    base = f"repo:{owner}/{repo} is:issue"
    if which == "opened":
        return f"{base} created:{start_date}..{end_date}"
    elif which == "closed":
        return f"{base} is:closed closed:{start_date}..{end_date}"
    else:
        raise ValueError("which must be opened|closed")

def build_query_labeled_issue(owner, repo, label, which, start_date, end_date) -> str:
    """
    Label-filtered issues (e.g., enhancement, Major decision pending).
    Label text must match exactly as shown in GitHub.
    """
    base = f'repo:{owner}/{repo} is:issue label:"{label}"'
    if which == "opened":
        return f"{base} created:{start_date}..{end_date}"
    elif which == "closed":
        return f"{base} is:closed closed:{start_date}..{end_date}"
    else:
        raise ValueError("which must be opened|closed")

# -----------------------------
# GitHub queries (search/issues, commits, releases)
# -----------------------------

def search_items(session: requests.Session, q: str, token: Optional[str], max_pages: int = 10, pause: float = 0.5):
    """
    Use GitHub Search API (issues/PRs). Returns list of items.
    Honours 1000 results cap via max_pages (100 per page).
    """
    url = "https://api.github.com/search/issues"
    params = {"q": q, "per_page": 100, "page": 1}
    items = []
    for page in range(1, max_pages + 1):
        params["page"] = page
        r = session.get(url, params=params, timeout=30)
        if r.status_code == 422:
            r.raise_for_status()
        r.raise_for_status()
        data = r.json()
        batch = data.get("items", [])
        items.extend(batch)
        if len(batch) < 100:
            break
        time.sleep(pause)
    return items

def list_commits(session: requests.Session, owner: str, repo: str, since_iso: str, until_iso: str):
    """
    List commits via REST (not search), paginated.
    since/until are ISO timestamps like 'YYYY-MM-DDT00:00:00Z'
    """
    url = f"https://api.github.com/repos/{owner}/{repo}/commits"
    params = {"per_page": 100, "since": since_iso, "until": until_iso}
    commits = []
    while True:
        data, links = get_json(session, url, params=params)
        if not data:
            break
        commits.extend(data)
        if "next" not in links:
            break
        url = links["next"]["url"]
        params = None  # next url already has params
        time.sleep(0.3)
    return commits

def list_releases(session: requests.Session, owner: str, repo: str, since_d: date, until_d: date):
    """
    List releases; bucket by month if within window.
    """
    url = f"https://api.github.com/repos/{owner}/{repo}/releases"
    params = {"per_page": 100, "page": 1}
    releases = []
    while True:
        r = session.get(url, params=params, timeout=30)
        r.raise_for_status()
        data = r.json()
        if not isinstance(data, list):
            break
        releases.extend(data)
        link = r.links
        if "next" in link:
            params = None
            url = link["next"]["url"]
        else:
            break
        time.sleep(0.3)
    # filter to window using published_at or created_at
    out = []
    for rel in releases:
        ts = rel.get("published_at") or rel.get("created_at")
        if not ts:
            continue
        dt = parse_iso_z(ts)
        d = dt.date()
        if since_d <= d <= until_d:
            out.append(rel)
    return out

# -----------------------------
# Utilities
# -----------------------------

def slugify_label(label: str) -> str:
    """
    Convert a label into a safe, readable filename slug:
    "Major decision pending" -> "major-decision-pending"
    """
    return "".join(ch.lower() if ch.isalnum() else "-" for ch in label).strip("-")

# -----------------------------
# Main aggregation
# -----------------------------

def main():
    ap = argparse.ArgumentParser(description="Generate weekly/monthly GitHub CSVs for Grafana.")
    ap.add_argument("--owner", required=True)
    ap.add_argument("--repo", required=True)
    ap.add_argument("--since-days", type=int, default=90, help="Lookback window in days (default 90)")
    ap.add_argument("--out-dir", default=".", help="Directory to write CSVs")
    ap.add_argument("--chunk-days", type=int, default=30, help="Search chunk size in days (default 30)")
    ap.add_argument("--max-pages", type=int, default=10, help="Max search pages (100 results each)")
    ap.add_argument("--pause", type=float, default=0.5, help="Pause between search pages (seconds)")

    # Back-compat: old single-label arg (used if --labels not provided)
    ap.add_argument("--enhancement-label", default="enhancement",
                    help="(Back-compat) Single label text to export if --labels is not set.")
    # New: multiple labels (comma-separated exact label text as in GitHub)
    ap.add_argument("--labels", default="",
                    help="Comma-separated list of labels to export (exact label text). Example: \"enhancement,Major decision pending\"")

    args = ap.parse_args()

    token = os.getenv("GITHUB_TOKEN")
    if not os.path.isdir(args.out_dir):
        os.makedirs(args.out_dir, exist_ok=True)

    start_d, end_d = compute_window(args.since_days)
    # for commits API we need ISO times
    since_iso = f"{start_d}T00:00:00Z"
    until_iso = f"{end_d}T23:59:59Z"

    with make_session(token) as session:
        # ----- PRs: opened/closed/merged (weekly) -----
        for which in ("opened", "closed", "merged"):
            weekly = defaultdict(int)
            for s, e in daterange_chunks(start_d, end_d, args.chunk_days):
                q = build_query_pr(args.owner, args.repo, which, s, e)
                items = search_items(session, q, token, max_pages=args.max_pages, pause=args.pause)
                for it in items:
                    if which == "opened":
                        ts = it.get("created_at")
                    elif which == "closed":
                        ts = it.get("closed_at")
                    else:  # merged
                        # merged_at can be on top-level or under pull_request
                        ts = it.get("merged_at") or it.get("pull_request", {}).get("merged_at") or it.get("closed_at")
                    if not ts:
                        continue
                    dt = parse_iso_z(ts)
                    key = monday_bucket(dt)
                    weekly[key] += 1
            write_csv(os.path.join(args.out_dir, f"prs_{which}_weekly.csv"), weekly)

        # ----- Issues: opened/closed (weekly) -----
        for which in ("opened", "closed"):
            weekly = defaultdict(int)
            for s, e in daterange_chunks(start_d, end_d, args.chunk_days):
                q = build_query_issue(args.owner, args.repo, which, s, e)
                items = search_items(session, q, token, max_pages=args.max_pages, pause=args.pause)
                for it in items:
                    ts = it.get("created_at") if which == "opened" else it.get("closed_at")
                    if not ts:
                        continue
                    dt = parse_iso_z(ts)
                    key = monday_bucket(dt)
                    weekly[key] += 1
            write_csv(os.path.join(args.out_dir, f"issues_{which}_weekly.csv"), weekly)

        # ----- Label-specific issues (weekly for each label) -----
        labels_arg = args.labels.strip()
        labels = []
        if labels_arg:
            labels = [x.strip() for x in labels_arg.split(",") if x.strip()]
        else:
            # Back-compat path: just use the single enhancement label
            labels = [args.enhancement_label]

        for label in labels:
            for which in ("opened", "closed"):
                weekly = defaultdict(int)
                for s, e in daterange_chunks(start_d, end_d, args.chunk_days):
                    q = build_query_labeled_issue(args.owner, args.repo, label, which, s, e)
                    items = search_items(session, q, token, max_pages=args.max_pages, pause=args.pause)
                    for it in items:
                        ts = it.get("created_at") if which == "opened" else it.get("closed_at")
                        if not ts:
                            continue
                        dt = parse_iso_z(ts)
                        key = monday_bucket(dt)
                        weekly[key] += 1
                # Write one CSV per label+which with clean Monday-UTC labels
                fname = f"label-{slugify_label(label)}_{which}_weekly.csv"
                write_csv(os.path.join(args.out_dir, fname), weekly)

        # ----- Commits + Contributors (weekly) -----
        commits = list_commits(session, args.owner, args.repo, since_iso, until_iso)

        # Commits per week
        commits_weekly = defaultdict(int)
        # Unique contributors per week (by author.login or commit author email)
        contributors_weekly_sets = defaultdict(set)

        for c in commits:
            ts = (c.get("commit", {}) or {}).get("author", {}).get("date")
            if not ts:
                continue
            dt = parse_iso_z(ts)
            if not (start_d <= dt.date() <= end_d):
                continue
            wk = monday_bucket(dt)
            commits_weekly[wk] += 1

            # contributor identity
            login = (c.get("author") or {}).get("login")
            if login:
                ident = f"login:{login}"
            else:
                email = (c.get("commit", {}) or {}).get("author", {}).get("email")
                ident = f"email:{email}" if email else "unknown"
            contributors_weekly_sets[wk].add(ident)

        write_csv(os.path.join(args.out_dir, "commits_weekly.csv"), commits_weekly)

        # Convert contributor sets -> counts
        contributors_weekly = {k: len(v) for k, v in contributors_weekly_sets.items()}
        write_csv(os.path.join(args.out_dir, "contributors_weekly.csv"), contributors_weekly)

        # ----- Releases (monthly) -----
        rels = list_releases(session, args.owner, args.repo, start_d, end_d)
        releases_monthly = defaultdict(int)
        for rel in rels:
            ts = rel.get("published_at") or rel.get("created_at")
            if not ts:
                continue
            dt = parse_iso_z(ts)
            key = month_bucket(dt)
            releases_monthly[key] += 1
        write_csv(os.path.join(args.out_dir, "releases_monthly.csv"), releases_monthly)


if __name__ == "__main__":
    try:
        main()
    except requests.HTTPError as e:
        # Print useful context for GitHub API errors
        print(f"HTTPError: {e} - Response text: {getattr(e.response, 'text', '')}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
