# CI Glue Test Automation Results

Monthly quality metrics for Fleet Edge CI/CD pipelines, calculated from the
CI Glue API.

## Metrics

| # | Metric | Definition |
| - | ------ | ---------- |
| 1 | **Total Active Tests** | Unique test IDs (`tid-NNN`) that actually executed in the month — i.e. reached a terminal state rather than `WAITING` / `ABORTED`. |
| 2 | **Failing-Test Backlog** | Unique tests that failed in ≥50% of their executions that month. The month-over-month delta shows whether the backlog is growing or shrinking. |
| 3 | **Feature Coverage** | Percentage of distinct pipeline stages that had at least one `PASSED` job in the month. |

## Requirements

- Python 3.10+ (uses `X | None` type syntax)
- A CI Glue API token at `~/.config/ci-glue/token`

No third-party dependencies — standard library only.

## Usage

```bash
# Current month, all pipelines
python ci_glue_metrics.py

# A specific month
python ci_glue_metrics.py --month 2026-10

# Compare two months to get the backlog delta
python ci_glue_metrics.py --month 2026-10 --compare 2026-09

# Limit to specific pipelines
python ci_glue_metrics.py --month 2026-10 \
    --pipelines Dev-Bur-SlowTrack QATermSimSlowTrack

# Per-test backlog list and per-stage coverage breakdown
python ci_glue_metrics.py --month 2026-10 --detail

# Force a full re-sync, ignoring the local cache
python ci_glue_metrics.py --month 2026-10 --sync
```

## Caching

Pipeline runs and jobs are cached in a local SQLite database at
`~/.cache/ci-glue-metrics.db`, so repeated runs are fast. The first run
performs a full sync and will take a while. Pass `--sync` to refresh.

Job data is fetched lazily, per run, and only once — a run already recorded in
`synced_runs` is skipped on later invocations.

## Configuration

Tunable constants at the top of `ci_glue_metrics.py`:

| Constant | Default | Purpose |
| -------- | ------- | ------- |
| `BASE_URL` | `https://ci-glue.fleetedgeportal.com/api` | CI Glue API root |
| `PAGE_SIZE` | `50` | Pagination size when syncing runs |
| `BACKLOG_FAIL_THRESHOLD` | `0.50` | Fail-rate at which a test joins the backlog |
| `FAIL_STATES` | `FAILED`, `FAIL-IMG-DEPLOY`, `FAIL-INFRA`, `FAIL-PROVISION` | States counted as failures |

Note that `FAIL_STATES` distinguishes genuine test failures from
infrastructure and provisioning failures, but Metric 2 currently counts all of
them toward the backlog. Narrowing it to `FAILED` only would measure test
quality rather than overall pipeline reliability.
