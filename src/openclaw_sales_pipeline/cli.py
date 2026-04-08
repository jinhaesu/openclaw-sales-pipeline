from __future__ import annotations

import argparse
import json
from dataclasses import asdict
from pathlib import Path

from .config import load_channel_master, load_playbooks, load_runtime_config
from .excel_analysis import analyze_sales_file, write_analysis
from .orchestrator import build_jobs, execute_jobs, summarize_jobs
from .workflow_knowledge import build_workflow_knowledge, write_workflow_knowledge


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
    plan_parser.add_argument("--channel", action="append", default=[], help="Only include matching vendor names")

    run_parser = subparsers.add_parser("run", help="Run execution plan")
    run_parser.add_argument("--date", required=True, help="Business date (YYYY-MM-DD)")
    run_parser.add_argument("--dry-run", action="store_true", help="Skip real collectors")
    run_parser.add_argument("--channel", action="append", default=[], help="Only include matching vendor names")

    validate_parser = subparsers.add_parser("validate", help="Validate config, playbooks, and secrets coverage")
    validate_parser.add_argument("--date", default="today", help="Logical date label for validation output")
    validate_parser.add_argument("--channel", action="append", default=[], help="Only include matching vendor names")

    knowledge_parser = subparsers.add_parser("build-knowledge", help="Build workflow knowledge from OpenClaw channel master")
    knowledge_parser.add_argument(
        "--output",
        default="artifacts/workflow_knowledge.json",
        help="Output JSON path",
    )

    analyze_parser = subparsers.add_parser("analyze-file", help="Analyze downloaded sales Excel/CSV file")
    analyze_parser.add_argument("--file", required=True, help="Input file path")
    analyze_parser.add_argument(
        "--output",
        default="artifacts/file_analysis.json",
        help="Output JSON path",
    )
    return parser.parse_args()


def filter_jobs(jobs, channel_filters):
    if not channel_filters:
        return jobs
    wanted = set(channel_filters)
    return [job for job in jobs if job.vendor_name in wanted]


def main() -> None:
    args = parse_args()
    config_path = Path(args.config).expanduser().resolve()
    cfg = load_runtime_config(config_path)
    channels = load_channel_master(Path(cfg.master_path).expanduser())
    playbooks = load_playbooks(Path(cfg.playbook_dir).expanduser())

    if args.command == "build-knowledge":
        knowledge = build_workflow_knowledge(
            master_path=Path(cfg.master_path).expanduser(),
            playbook_dir=Path(cfg.playbook_dir).expanduser(),
        )
        output_path = Path(args.output).expanduser()
        write_workflow_knowledge(output_path, knowledge)
        print(json.dumps({"output": str(output_path.resolve()), "channel_count": knowledge["channel_count"]}, ensure_ascii=False, indent=2))
        return

    if args.command == "analyze-file":
        input_path = Path(args.file).expanduser()
        analysis = analyze_sales_file(input_path)
        output_path = Path(args.output).expanduser()
        write_analysis(output_path, analysis)
        print(json.dumps({"output": str(output_path.resolve()), "row_count": analysis["row_count"], "product_count": analysis["product_count"]}, ensure_ascii=False, indent=2))
        return

    jobs = build_jobs(
        channels=channels,
        playbooks=playbooks,
        business_date=args.date,
        artifact_root=Path(cfg.artifact_root).expanduser(),
        default_strategy=cfg.default_strategy,
    )
    jobs = filter_jobs(jobs, getattr(args, "channel", []))

    if args.command == "plan":
        summary = summarize_jobs(jobs)
        print(json.dumps({"summary": summary, "jobs": [asdict(job) for job in jobs]}, ensure_ascii=False, indent=2))
        return

    if args.command == "validate":
        from .secrets import SecretStore

        secrets = SecretStore(Path(cfg.secrets_path).expanduser())
        report = []
        for job in jobs:
            report.append(
                {
                    "vendor_name": job.vendor_name,
                    "strategy": job.strategy,
                    "run_mode": job.run_mode,
                    "has_playbook": job.playbook is not None,
                    "credential_key": job.playbook.credential_key if job.playbook else None,
                    "has_credentials": secrets.has(job.playbook.credential_key) if job.playbook else False,
                    "browser_actions": len(job.playbook.browser_actions) if job.playbook else 0,
                    "has_video": job.has_video,
                }
            )
        print(json.dumps({"count": len(report), "items": report}, ensure_ascii=False, indent=2))
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
