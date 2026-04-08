from __future__ import annotations

import argparse
import json
from dataclasses import asdict
from pathlib import Path

from .browser_discovery import discover_channel
from .channel_credentials import ChannelCredentialStore
from .config import load_channel_master, load_playbooks, load_runtime_config
from .excel_analysis import analyze_sales_file, write_analysis
from .orchestrator import build_jobs, execute_jobs, summarize_jobs
from .reporting import build_report_bundle
from .secrets import SecretStore
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

    report_parser = subparsers.add_parser("report-bundle", help="Build consolidated sales workbook, summary, and email draft")
    report_parser.add_argument("--input-root", default="run_outputs", help="Root directory to scan for downloaded files")
    report_parser.add_argument("--manifest", default="", help="Optional source manifest JSON path")
    report_parser.add_argument("--file", action="append", default=[], help="Explicit raw sales file or analysis JSON path")
    report_parser.add_argument("--date-from", default="", help="Start business date (YYYY-MM-DD)")
    report_parser.add_argument("--date-to", default="", help="End business date (YYYY-MM-DD)")
    report_parser.add_argument("--channel", action="append", default=[], help="Only include matching vendor names")
    report_parser.add_argument("--output-dir", default="artifacts/report_bundles/latest", help="Output directory for workbook/summary/email")
    report_parser.add_argument("--label", default="", help="Optional report label")
    report_parser.add_argument("--email-to", action="append", default=[], help="Recipient email address")
    report_parser.add_argument("--email-cc", action="append", default=[], help="CC email address")
    report_parser.add_argument("--email-subject", default="", help="Optional subject override")
    report_parser.add_argument("--send-email", action="store_true", help="Send email using SMTP profile from secrets file")
    report_parser.add_argument("--smtp-profile", default="smtp", help="Secret profile name for SMTP settings")

    discover_parser = subparsers.add_parser("discover-browser", help="Discover browser menu/frame structure after playbook actions")
    discover_parser.add_argument("--date", required=True, help="Business date (YYYY-MM-DD)")
    discover_parser.add_argument("--channel", action="append", default=[], help="Only include matching vendor names")
    discover_parser.add_argument("--output-root", default="artifacts/browser_discovery", help="Output directory for discovery dumps")
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

    if args.command == "report-bundle":
        secrets = SecretStore(Path(cfg.secrets_path).expanduser())
        bundle = build_report_bundle(
            channels=channels,
            playbooks=playbooks,
            secrets=secrets,
            input_root=Path(args.input_root).expanduser(),
            output_dir=Path(args.output_dir).expanduser(),
            date_from=args.date_from or None,
            date_to=args.date_to or None,
            channel_filters=list(args.channel),
            explicit_files=list(args.file),
            manifest_path=args.manifest or None,
            label=args.label or None,
            email_to=list(args.email_to),
            email_cc=list(args.email_cc),
            email_subject=args.email_subject or None,
            send_email=bool(args.send_email),
            smtp_profile=args.smtp_profile,
        )
        print(json.dumps(bundle, ensure_ascii=False, indent=2))
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

    if args.command == "discover-browser":
        jobs = build_jobs(
            channels=channels,
            playbooks=playbooks,
            business_date=args.date,
            artifact_root=Path(cfg.artifact_root).expanduser(),
            default_strategy=cfg.default_strategy,
        )
        jobs = filter_jobs(jobs, getattr(args, "channel", []))
        secrets = SecretStore(Path(cfg.secrets_path).expanduser())
        channel_credentials = ChannelCredentialStore(Path(cfg.channel_credentials_path).expanduser())
        output_root = Path(args.output_root).expanduser()
        report = []
        for job in jobs:
            if job.run_mode != "browser":
                continue
            channel_dir = output_root / job.vendor_name.replace(" ", "_")
            try:
                result = discover_channel(cfg, secrets, channel_credentials, job, channel_dir)
                report.append({"vendor_name": job.vendor_name, "status": "ok", **result})
            except Exception as exc:
                report.append({"vendor_name": job.vendor_name, "status": "failed", "error": type(exc).__name__, "message": str(exc)})
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
