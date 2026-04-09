from __future__ import annotations

import json
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict
from pathlib import Path

from .channel_credentials import ChannelCredentialStore
from .collectors.registry import get_collector
from .models import ChannelRecord, Job, JobResult, Playbook, RuntimeConfig
from .secrets import SecretStore


API_FIRST_CHANNELS = {
    "스마트스토어",
    "카페24",
    "카페24 공동구매",
    "파트너스몰(카페24)",
    "쿠팡 WING",
    "쿠팡 로켓프레시",
    "11번가",
    "G마켓",
    "옥션",
}


def infer_strategy(channel: ChannelRecord, playbooks: dict[str, Playbook], default_strategy: str) -> str:
    if channel.vendor_name in playbooks:
        return playbooks[channel.vendor_name].strategy
    if channel.vendor_name in API_FIRST_CHANNELS:
        return "api"
    if channel.mentions_excel_download:
        return "browser_download"
    if channel.requires_verification:
        return "browser_verified"
    return default_strategy


def infer_run_mode(strategy: str) -> str:
    if strategy == "api":
        return "api"
    if strategy in {"browser_download", "browser_verified", "browser"}:
        return "browser"
    return "manual"


def build_jobs(
    channels: list[ChannelRecord],
    playbooks: dict[str, Playbook],
    business_date: str,
    artifact_root: Path,
    default_strategy: str,
) -> list[Job]:
    jobs: list[Job] = []
    for channel in channels:
        strategy = infer_strategy(channel, playbooks, default_strategy)
        run_mode = infer_run_mode(strategy)
        safe_vendor = (
            channel.vendor_name.replace(" ", "_")
            .replace("/", "_")
            .replace("(", "")
            .replace(")", "")
        )
        output_dir = artifact_root / business_date / safe_vendor
        notes = []
        if channel.vendor_name in playbooks:
            notes.extend(playbooks[channel.vendor_name].notes)
        if channel.has_video:
            notes.append(f"video_support:{channel.video_count}")
        if channel.special_notes:
            notes.append(channel.special_notes)
        jobs.append(
            Job(
                vendor_name=channel.vendor_name,
                strategy=strategy,
                run_mode=run_mode,
                business_date=business_date,
                output_dir=str(output_dir),
                auth_type_meaning=channel.auth_type_meaning,
                collection_path=channel.collection_path,
                login_url=channel.login_url,
                manager=channel.manager,
                channel_group=channel.channel_group,
                requires_verification=channel.requires_verification,
                has_video=channel.has_video,
                playbook=playbooks.get(channel.vendor_name),
                notes=notes,
            )
        )
    return jobs


def summarize_jobs(jobs: list[Job]) -> dict[str, int]:
    summary = {"api": 0, "browser": 0, "manual": 0}
    for job in jobs:
        summary[job.run_mode] = summary.get(job.run_mode, 0) + 1
    return summary


def execute_jobs(jobs: list[Job], cfg: RuntimeConfig, dry_run: bool) -> list[JobResult]:
    grouped: dict[str, list[Job]] = {"api": [], "browser": [], "manual": []}
    secrets = SecretStore(Path(cfg.secrets_path).expanduser())
    channel_credentials = ChannelCredentialStore(Path(cfg.channel_credentials_path).expanduser())
    for job in jobs:
        grouped.setdefault(job.run_mode, []).append(job)

    results: list[JobResult] = []
    limits = {
        "api": cfg.api_concurrency,
        "browser": cfg.browser_concurrency,
        "manual": cfg.manual_concurrency,
    }

    for run_mode, bucket in grouped.items():
        if not bucket:
            continue
        with ThreadPoolExecutor(max_workers=limits[run_mode]) as executor:
            futures = [executor.submit(_run_job, job, cfg, secrets, channel_credentials, dry_run) for job in bucket]
            for future in as_completed(futures):
                results.append(future.result())
    return sorted(results, key=lambda item: item.vendor_name)


def _run_job(
    job: Job,
    cfg: RuntimeConfig,
    secrets: SecretStore,
    channel_credentials: ChannelCredentialStore,
    dry_run: bool,
) -> JobResult:
    output_dir = Path(job.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    payload = {
        "vendor_name": job.vendor_name,
        "strategy": job.strategy,
        "run_mode": job.run_mode,
        "business_date": job.business_date,
        "auth_type_meaning": job.auth_type_meaning,
        "requires_verification": job.requires_verification,
        "collection_path": job.collection_path,
        "notes": job.notes,
    }
    (output_dir / "job.json").write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )

    time.sleep(0.02)
    collector = get_collector(job, cfg, secrets, channel_credentials)
    result = collector.collect(job, dry_run=dry_run)
    (output_dir / "result.json").write_text(
        json.dumps(asdict(result), ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    return result
