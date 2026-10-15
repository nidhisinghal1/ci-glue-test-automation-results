#!/usr/bin/env python3
"""
CI Glue Monthly Metrics Reporter

Calculates three quality metrics per pipeline, per month:

  1. Total Active Tests     — unique test IDs (tid-NNN) that actually executed
                              (state != WAITING / ABORTED) in the month.

  2. Failing-Test Backlog   — unique tests that failed in ≥50 % of their
                              executions that month. Month-over-month delta
                              shows whether the backlog is growing or shrinking.

  3. Feature Coverage       — percentage of distinct pipeline stages that had
                              at least one PASSED job in the month.

Usage examples
--------------
  # Metrics for the current month across all pipelines:
  python ci_glue_metrics.py

  # Specific month:
  python ci_glue_metrics.py --month 2026-10

  # Compare two consecutive months (shows backlog delta):
  python ci_glue_metrics.py --month 2026-10 --compare 2026-09

  # Limit to specific pipelines:
  python ci_glue_metrics.py --month 2026-10 \\
      --pipelines Dev-Bur-SlowTrack QATermSimSlowTrack

  # Force a full re-sync from the API (ignores local cache):
  python ci_glue_metrics.py --month 2026-10 --sync

  # Show per-test backlog detail:
  python ci_glue_metrics.py --month 2026-10 --detail

Cache
-----
  Pipeline run and job data are cached in a local SQLite database at
  ~/.cache/ci-glue-metrics.db so that repeated runs are fast.
  Use --sync to refresh.
"""

import argparse
import json
import sqlite3
import sys
import urllib.request
from collections import defaultdict
from datetime import datetime, date, timedelta
from pathlib import Path

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

TOKEN_FILE = Path.home() / ".config" / "ci-glue" / "token"
BASE_URL   = "https://ci-glue.fleetedgeportal.com/api"
DB_PATH    = Path.home() / ".cache" / "ci-glue-metrics.db"
PAGE_SIZE  = 50

# States that represent a test actually running
FAIL_STATES   = {"FAILED", "FAIL-IMG-DEPLOY", "FAIL-INFRA", "FAIL-PROVISION"}
ACTIVE_STATES = {"PASSED"} | FAIL_STATES

# A test is considered "in the failing backlog" if it fails this fraction
# of the time or more.
BACKLOG_FAIL_THRESHOLD = 0.50

# ---------------------------------------------------------------------------
# API helpers
# ---------------------------------------------------------------------------

def load_token() -> str:
    if not TOKEN_FILE.exists():
        sys.exit(f"ERROR: Token file not found at {TOKEN_FILE}\n"
                 "Run the CI Glue login flow to create it.")
    return TOKEN_FILE.read_text().strip()


def fetch_json(url: str, token: str) -> dict:
    req = urllib.request.Request(url, headers={"Authorization": f"Token {token}"})
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read())

# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------

def init_db(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS pipeline_runs (
            id           TEXT PRIMARY KEY,
            pipeline     TEXT NOT NULL,
            state        TEXT,
            created_at   TEXT NOT NULL,
            edge_version TEXT,
            track_id     INTEGER
        );
        CREATE INDEX IF NOT EXISTS idx_runs_pipeline ON pipeline_runs (pipeline);
        CREATE INDEX IF NOT EXISTS idx_runs_date     ON pipeline_runs (created_at);

        CREATE TABLE IF NOT EXISTS pipeline_jobs (
            id           TEXT PRIMARY KEY,
            pipeline_run TEXT NOT NULL,
            stage        TEXT,
            name         TEXT,
            state        TEXT,
            annotation   TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_jobs_run   ON pipeline_jobs (pipeline_run);
        CREATE INDEX IF NOT EXISTS idx_jobs_name  ON pipeline_jobs (name);
        CREATE INDEX IF NOT EXISTS idx_jobs_stage ON pipeline_jobs (stage);

        CREATE TABLE IF NOT EXISTS synced_runs (
            run_id TEXT PRIMARY KEY
        );
    """)
    conn.commit()
    return conn


def _run_count(conn: sqlite3.Connection) -> int:
    return conn.execute("SELECT COUNT(*) FROM pipeline_runs").fetchone()[0]

# ---------------------------------------------------------------------------
# Sync: pipeline runs
# ---------------------------------------------------------------------------

def sync_runs(conn: sqlite3.Connection, token: str, verbose: bool = True) -> int:
    """Fetch every pipeline run from the API and upsert into the local DB."""
    cursor = conn.cursor()
    url = f"{BASE_URL}/pipeline-runs/?limit={PAGE_SIZE}"
    page, new_runs = 0, 0

    if verbose:
        print("Syncing pipeline runs from API …", flush=True)

    while url:
        data = fetch_json(url, token)
        for r in data["results"]:
            meta = r.get("metadata") or {}
            cursor.execute(
                "INSERT OR IGNORE INTO pipeline_runs VALUES (?,?,?,?,?,?)",
                (r["id"], r["pipeline"], r["state"], r["created_at"],
                 meta.get("edge_version"), meta.get("track_id")),
            )
            if cursor.rowcount:
                new_runs += 1
        conn.commit()
        page += 1
        url = data.get("next")
        if verbose and page % 20 == 0:
            total = data["count"]
            pct = min(100, round(100 * page * PAGE_SIZE / total))
            print(f"  … {page * PAGE_SIZE}/{total} ({pct}%) — {new_runs} new runs",
                  flush=True)

    if verbose:
        print(f"Run sync complete. {new_runs} new runs added to cache.\n")
    return new_runs


# ---------------------------------------------------------------------------
# Sync: jobs for a specific run
# ---------------------------------------------------------------------------

def sync_jobs_for_run(conn: sqlite3.Connection, token: str, run_id: str) -> int:
    """Fetch and cache all jobs for one pipeline run (skips if already cached)."""
    already = conn.execute(
        "SELECT 1 FROM synced_runs WHERE run_id=?", (run_id,)
    ).fetchone()
    if already:
        return 0

    url = f"{BASE_URL}/pipeline-jobs/?pipeline_run={run_id}&limit=200"
    data = fetch_json(url, token)
    cursor = conn.cursor()
    new_jobs = 0
    for j in data["results"]:
        cursor.execute(
            "INSERT OR IGNORE INTO pipeline_jobs VALUES (?,?,?,?,?,?)",
            (j["id"], j["pipeline_run"], j["stage"], j["name"],
             j["state"], j.get("annotation") or ""),
        )
        if cursor.rowcount:
            new_jobs += 1
    cursor.execute("INSERT OR IGNORE INTO synced_runs VALUES (?)", (run_id,))
    conn.commit()
    return new_jobs

# ---------------------------------------------------------------------------
# Query helpers
# ---------------------------------------------------------------------------

def month_bounds(year: int, month: int):
    start = f"{year:04d}-{month:02d}-01"
    if month == 12:
        end = f"{year+1:04d}-01-01"
    else:
        end = f"{year:04d}-{month+1:02d}-01"
    return start, end


def get_runs_for_month(
    conn: sqlite3.Connection,
    year: int,
    month: int,
    pipelines: list | None = None,
) -> list[tuple[str, str]]:
    """Return [(run_id, pipeline_name)] for the given month."""
    start, end = month_bounds(year, month)
    if pipelines:
        ph = ",".join("?" * len(pipelines))
        rows = conn.execute(
            f"SELECT id, pipeline FROM pipeline_runs "
            f"WHERE created_at >= ? AND created_at < ? AND pipeline IN ({ph})",
            [start, end] + list(pipelines),
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT id, pipeline FROM pipeline_runs "
            "WHERE created_at >= ? AND created_at < ?",
            (start, end),
        ).fetchall()
    return rows

# ---------------------------------------------------------------------------
# Core metrics calculation
# ---------------------------------------------------------------------------

def calculate_metrics(
    conn: sqlite3.Connection,
    token: str,
    year: int,
    month: int,
    pipelines: list | None = None,
    verbose: bool = True,
) -> dict | None:
    month_label = f"{year:04d}-{month:02d}"

    if verbose:
        print(f"{'='*60}")
        print(f"Processing {month_label} …")
        print(f"{'='*60}")

    runs = get_runs_for_month(conn, year, month, pipelines)
    if not runs:
        if verbose:
            print(f"  No pipeline runs found for {month_label}.\n")
        return None

    if verbose:
        print(f"  {len(runs)} pipeline runs found. Fetching job data …")

    run_ids = [r[0] for r in runs]
    for i, run_id in enumerate(run_ids):
        sync_jobs_for_run(conn, token, run_id)
        if verbose and (i + 1) % 10 == 0:
            print(f"  … {i + 1}/{len(run_ids)} runs fetched", flush=True)

    if verbose:
        print()

    ph = ",".join("?" * len(run_ids))
    jobs = conn.execute(
        f"SELECT name, stage, state FROM pipeline_jobs WHERE pipeline_run IN ({ph})",
        run_ids,
    ).fetchall()

    # ------------------------------------------------------------------
    # Metric 1 — Total Active Tests
    # A test (tid-NNN) is "active" in the month if it was executed at
    # least once (state ∈ ACTIVE_STATES). We count unique test IDs.
    # ------------------------------------------------------------------
    active_tids: set[str] = set()
    all_tids: set[str] = set()
    for name, stage, state in jobs:
        if name and name.startswith("tid-"):
            all_tids.add(name)
            if state in ACTIVE_STATES:
                active_tids.add(name)

    # ------------------------------------------------------------------
    # Metric 2 — Failing-Test Backlog
    # For each tid that actually ran, tally pass vs fail executions.
    # The "backlog" = tests failing ≥ BACKLOG_FAIL_THRESHOLD of the time.
    # ------------------------------------------------------------------
    tid_run_stats: dict[str, dict] = defaultdict(lambda: {"pass": 0, "fail": 0})

    for name, stage, state in jobs:
        if name and name.startswith("tid-") and state in ACTIVE_STATES:
            if state == "PASSED":
                tid_run_stats[name]["pass"] += 1
            else:
                tid_run_stats[name]["fail"] += 1

    backlog_tids: set[str] = set()
    for tid, s in tid_run_stats.items():
        total = s["pass"] + s["fail"]
        if total > 0 and (s["fail"] / total) >= BACKLOG_FAIL_THRESHOLD:
            backlog_tids.add(tid)

    # ------------------------------------------------------------------
    # Metric 3 — Feature Coverage
    # Stage = feature area (remote-management, voice, provisioning, …).
    # Coverage = stages that had ≥1 PASSED job / stages that ran at all.
    # ------------------------------------------------------------------
    stages_with_any_run:    set[str] = set()
    stages_with_pass:       set[str] = set()
    stage_job_counts:       dict[str, dict] = defaultdict(lambda: {"pass": 0, "fail": 0, "total": 0})

    for name, stage, state in jobs:
        if not stage:
            continue
        if state in ACTIVE_STATES:
            stages_with_any_run.add(stage)
            stage_job_counts[stage]["total"] += 1
            if state == "PASSED":
                stages_with_pass.add(stage)
                stage_job_counts[stage]["pass"] += 1
            else:
                stage_job_counts[stage]["fail"] += 1

    feature_coverage_pct = (
        round(100 * len(stages_with_pass) / len(stages_with_any_run))
        if stages_with_any_run else 0
    )

    return {
        "month":                month_label,
        "runs":                 len(runs),
        "total_jobs":           len(jobs),
        # Metric 1
        "active_tests":         len(active_tids),
        "all_tids":             len(all_tids),
        # Metric 2
        "failing_backlog":      len(backlog_tids),
        "backlog_tids":         backlog_tids,
        "tid_run_stats":        tid_run_stats,
        # Metric 3
        "stages_ran":           stages_with_any_run,
        "stages_passed":        stages_with_pass,
        "feature_coverage_pct": feature_coverage_pct,
        "stage_job_counts":     dict(stage_job_counts),
    }

# ---------------------------------------------------------------------------
# Report rendering
# ---------------------------------------------------------------------------

def print_metric_header(title: str, width: int = 60) -> None:
    print(f"\n{'-' * width}")
    print(f"  {title}")
    print(f"{'-' * width}")


def render_report(
    current: dict,
    previous: dict | None = None,
    detail: bool = False,
) -> None:
    label = current["month"]
    print(f"\n{'=' * 62}")
    print(f"=  CI Glue Monthly Metrics - {label}")
    print(f"{'=' * 62}")
    print(f"  Pipeline runs : {current['runs']}")
    print(f"  Total jobs    : {current['total_jobs']}")

    # ------------------------------------------------------------------
    # Metric 1 — Total Active Tests
    # ------------------------------------------------------------------
    print_metric_header("METRIC 1 — Total Active Tests")
    print(f"  Active tests (executed this month) : {current['active_tests']}")
    print(f"  Unique tids seen (incl. WAITING)   : {current['all_tids']}")

    if previous:
        delta = current["active_tests"] - previous["active_tests"]
        sign  = "+" if delta >= 0 else ""
        print(f"  Change vs {previous['month']}               : {sign}{delta}")

    # ------------------------------------------------------------------
    # Metric 2 — Failing-Test Backlog
    # ------------------------------------------------------------------
    print_metric_header("METRIC 2 — Failing-Test Backlog")
    backlog = current["failing_backlog"]
    print(f"  Tests in backlog (fail ≥50% of runs) : {backlog}")

    if previous:
        prev_backlog = previous["failing_backlog"]
        delta = backlog - prev_backlog
        sign  = "+" if delta >= 0 else ""
        reduction = prev_backlog - backlog
        trend = "REDUCED" if reduction > 0 else ("GROWN" if reduction < 0 else "UNCHANGED")
        print(f"  Prior month backlog ({previous['month']})  : {prev_backlog}")
        print(f"  Month-over-month change              : {sign}{delta}  [{trend}]")
        if prev_backlog > 0:
            pct = round(100 * abs(reduction) / prev_backlog)
            print(f"  Backlog reduction rate               : {pct}%")

    if detail and current["backlog_tids"]:
        print(f"\n  Backlog tests (fail-rate ≥ 50%):")
        stats = current["tid_run_stats"]
        rows = sorted(
            current["backlog_tids"],
            key=lambda t: stats[t]["fail"] / max(1, stats[t]["pass"] + stats[t]["fail"]),
            reverse=True,
        )
        print(f"  {'Test ID':<12}  {'Pass':>6}  {'Fail':>6}  {'Total':>7}  {'Fail%':>6}")
        print(f"  {'-'*12}  {'-'*6}  {'-'*6}  {'-'*7}  {'-'*6}")
        for tid in rows[:30]:
            s = stats[tid]
            tot = s["pass"] + s["fail"]
            pct = round(100 * s["fail"] / tot) if tot else 0
            print(f"  {tid:<12}  {s['pass']:>6}  {s['fail']:>6}  {tot:>7}  {pct:>5}%")
        if len(rows) > 30:
            print(f"  … and {len(rows) - 30} more")

    # ------------------------------------------------------------------
    # Metric 3 — Feature Coverage
    # ------------------------------------------------------------------
    print_metric_header("METRIC 3 — Feature Coverage")
    print(f"  Stages with ≥1 PASSED job : {len(current['stages_passed'])}  /  "
          f"{len(current['stages_ran'])} stages ran  =  "
          f"{current['feature_coverage_pct']}%")

    if previous:
        delta = current["feature_coverage_pct"] - previous["feature_coverage_pct"]
        sign  = "+" if delta >= 0 else ""
        print(f"  Change vs {previous['month']}             : {sign}{delta}%")

    if detail and current["stage_job_counts"]:
        sc = current["stage_job_counts"]
        print(f"\n  Stage breakdown:")
        print(f"  {'Stage':<26}  {'Pass':>6}  {'Fail':>6}  {'Total':>7}  {'Pass%':>6}")
        print(f"  {'-'*26}  {'-'*6}  {'-'*6}  {'-'*7}  {'-'*6}")
        for stage in sorted(sc):
            s = sc[stage]
            tot = s["total"]
            pct = round(100 * s["pass"] / tot) if tot else 0
            covered = "[PASS]" if stage in current["stages_passed"] else "[ -- ]"
            print(f"  {covered} {stage:<24}  {s['pass']:>6}  {s['fail']:>6}  {tot:>7}  {pct:>5}%")

    print()

# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_ym(value: str) -> tuple[int, int]:
    try:
        dt = datetime.strptime(value, "%Y-%m")
        return dt.year, dt.month
    except ValueError:
        raise argparse.ArgumentTypeError(f"Invalid month format '{value}'. Use YYYY-MM.")


def main() -> None:
    # Ensure UTF-8 output on Windows (avoids CP1252 encoding errors)
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")

    parser = argparse.ArgumentParser(
        description="CI Glue Monthly Metrics Reporter",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--month", type=parse_ym, default=None,
        metavar="YYYY-MM",
        help="Month to report on (default: current month).",
    )
    parser.add_argument(
        "--compare", type=parse_ym, default=None,
        metavar="YYYY-MM",
        help="Prior month to compare against for delta metrics.",
    )
    parser.add_argument(
        "--pipelines", nargs="+", default=None,
        metavar="NAME",
        help="Limit to specific pipeline names (default: all).",
    )
    parser.add_argument(
        "--sync", action="store_true",
        help="Force a full re-sync of pipeline runs from the API "
             "before calculating (ignores existing cache).",
    )
    parser.add_argument(
        "--detail", action="store_true",
        help="Show per-test backlog list and per-stage coverage breakdown.",
    )
    parser.add_argument(
        "--db", type=Path, default=DB_PATH,
        metavar="PATH",
        help=f"SQLite cache path (default: {DB_PATH}).",
    )
    args = parser.parse_args()

    token = load_token()
    conn  = init_db(args.db)

    # ------------------------------------------------------------------
    # Determine months to process
    # ------------------------------------------------------------------
    today = date.today()
    if args.month:
        c_year, c_month = args.month
    else:
        c_year, c_month = today.year, today.month

    p_year = p_month = None
    if args.compare:
        p_year, p_month = args.compare

    # ------------------------------------------------------------------
    # Sync
    # ------------------------------------------------------------------
    db_count = _run_count(conn)
    if args.sync or db_count == 0:
        if db_count == 0:
            print("Local cache is empty. Performing initial sync …\n")
        sync_runs(conn, token, verbose=True)
    else:
        print(f"Using cached data ({db_count} runs in local DB). "
              f"Pass --sync to refresh.\n")

    # ------------------------------------------------------------------
    # Calculate
    # ------------------------------------------------------------------
    current_metrics  = calculate_metrics(conn, token, c_year,  c_month,  args.pipelines)
    previous_metrics = None
    if p_year is not None:
        previous_metrics = calculate_metrics(conn, token, p_year, p_month, args.pipelines)

    # ------------------------------------------------------------------
    # Render
    # ------------------------------------------------------------------
    if current_metrics is None:
        print(f"No data found for {c_year:04d}-{c_month:02d}. "
              "Try --sync to fetch the latest data.")
        sys.exit(1)

    render_report(current_metrics, previous_metrics, detail=args.detail)


if __name__ == "__main__":
    main()
