from __future__ import annotations

import argparse
import json
from dataclasses import asdict
from pathlib import Path

from .config import load_channel_master, load_playbooks, load_runtime_config
from .orchestrator import build_jobs, execute_jobs, summarize_jobs


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="OpenClaw sales pipeline helper")
    parser.add_argument(
        "--config",
        default="config/runtime.example.json",
        help="Path to runtime config JSON",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    plan_parser = subparsers.add_parser("plan", help="Show execution plan")
    plan_parser.add_argument("--date", required=True, help="Business date (YYYY-MM-DD)")

    run_parser = subparsers.add_parser("run", help="Run execution plan")
    run_parser.add_argument("--date", required=True, help="Business date (YYYY-MM-DD)")
    run_parser.add_argument("--dry-run", action="store_true", help="Skip real collectors")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    config_path = Path(args.config).expanduser().resolve()
    cfg = load_runtime_config(config_path)
    channels = load_channel_master(Path(cfg.master_path).expanduser())
    playbooks = load_playbooks(Path(cfg.playbook_dir).expanduser())
    jobs = build_jobs(
        channels=channels,
        playbooks=playbooks,
        business_date=args.date,
        artifact_root=Path(cfg.artifact_root).expanduser(),
        default_strategy=cfg.default_strategy,
    )

    if args.command == "plan":
        summary = summarize_jobs(jobs)
        print(json.dumps({"summary": summary, "jobs": [asdict(job) for job in jobs]}, ensure_ascii=False, indent=2))
        return

    results = execute_jobs(jobs, cfg, dry_run=bool(args.dry_run))
    summary = summarize_jobs(jobs)
    print(
        json.dumps(
            {"summary": summary, "results": [asdict(result) for result in results]},
            ensure_ascii=False,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
