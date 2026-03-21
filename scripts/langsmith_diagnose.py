#!/usr/bin/env python3
"""CLI tool to diagnose LangSmith traces for CaelynAI.

Usage:
    # Set your API key
    export LANGCHAIN_API_KEY="lsv2_pt_..."

    # Full diagnostic (last 24h)
    python scripts/langsmith_diagnose.py

    # Errors only, last 6 hours
    python scripts/langsmith_diagnose.py --hours 6 --errors-only

    # Full detail for a specific run
    python scripts/langsmith_diagnose.py --run-id <run-uuid>

    # Output raw JSON
    python scripts/langsmith_diagnose.py --json
"""

import argparse
import json
import sys
import os

# Add project root to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from data.langsmith_diagnostics import diagnose, get_run_detail, get_recent_runs


def _print_summary(report: dict):
    s = report.get("summary", {})
    print(f"\n{'='*60}")
    print(f"  LangSmith Diagnostics — {report['project']}")
    print(f"  Window: last {report['window_hours']}h | Generated: {report['generated_at']}")
    print(f"{'='*60}")

    if report.get("_diagnostic_error"):
        print(f"\n  !! DIAGNOSTIC ERROR: {report['_diagnostic_error']}")
        return

    print(f"\n  Total runs:    {s.get('total_runs', 0)}")
    print(f"  Errors:        {s.get('error_count', 0)} ({s.get('error_rate', 'N/A')})")
    print(f"  Total tokens:  {s.get('total_tokens', 0):,}")
    print(f"  Avg latency:   {s.get('avg_latency_s', 'N/A')}s")
    print(f"  Max latency:   {s.get('max_latency_s', 'N/A')}s")

    errors = report.get("errors", [])
    if errors:
        print(f"\n  {'─'*56}")
        print(f"  ERRORS ({len(errors)}):")
        for e in errors:
            print(f"\n    Run: {e.get('name')} [{e.get('id', '')[:8]}...]")
            print(f"    Time: {e.get('start_time')}")
            err_msg = e.get("error", "")
            if len(err_msg) > 200:
                err_msg = err_msg[:200] + "..."
            print(f"    Error: {err_msg}")

    runs = report.get("recent_runs", [])
    if runs:
        print(f"\n  {'─'*56}")
        print(f"  RECENT RUNS ({len(runs)}):")
        for r in runs:
            status = "ERR" if r.get("error") else "OK "
            tokens = r.get("total_tokens") or 0
            latency = r.get("latency_s") or "?"
            name = (r.get("name") or "unnamed")[:30]
            print(f"    [{status}] {name:<30} {tokens:>6} tok  {latency}s")

    print()


def main():
    parser = argparse.ArgumentParser(description="LangSmith diagnostics for CaelynAI")
    parser.add_argument("--hours", type=int, default=24, help="Lookback window in hours")
    parser.add_argument("--limit", type=int, default=20, help="Max runs to fetch")
    parser.add_argument("--errors-only", action="store_true", help="Only show error runs")
    parser.add_argument("--run-id", type=str, help="Get full detail for a specific run")
    parser.add_argument("--json", action="store_true", help="Output raw JSON")
    parser.add_argument("--project", type=str, default="CaelynAI", help="LangSmith project name")
    args = parser.parse_args()

    if not (os.environ.get("LANGCHAIN_API_KEY") or os.environ.get("LANGSMITH_API_KEY")):
        print("ERROR: Set LANGCHAIN_API_KEY or LANGSMITH_API_KEY environment variable")
        sys.exit(1)

    if args.run_id:
        result = get_run_detail(args.run_id)
        print(json.dumps(result, indent=2, default=str))
        return

    if args.errors_only:
        runs = get_recent_runs(args.project, hours=args.hours, error_only=True, limit=args.limit)
        if args.json:
            print(json.dumps(runs, indent=2, default=str))
        else:
            for r in runs:
                print(f"[{r.get('id', '')[:8]}] {r.get('name')} — {r.get('error', '')[:100]}")
        return

    report = diagnose(hours=args.hours, limit=args.limit, project_name=args.project)
    if args.json:
        print(json.dumps(report, indent=2, default=str))
    else:
        _print_summary(report)


if __name__ == "__main__":
    main()
