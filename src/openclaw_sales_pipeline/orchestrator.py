from __future__ import annotations

import json
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict
from pathlib import Path

from .channel_credentials import ChannelCredentialStore
from .collectors.registry import get_collector
from .models import ChannelRecord, Job, JobResult, Playbook, RuntimeConfig
from .operations import build_channel_operation_profile
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
        playbook = playbooks.get(channel.vendor_name)
        operation_profile = build_channel_operation_profile(asdict(channel), asdict(playbook) if playbook else {})
        safe_vendor = (
            channel.vendor_name.replace(" ", "_")
            .replace("/", "_")
            .replace("(", "")
            .replace(")", "")
        )
        output_dir = artifact_root / business_date / safe_vendor
        notes = []
        if playbook:
            notes.extend(playbook.notes)
        if channel.has_video:
            notes.append(f"video_support:{channel.video_count}")
        if channel.special_notes:
            notes.append(channel.special_notes)
        notes.append(f"queue:{operation_profile['queue_id']}")
        notes.append(f"collection_mode:{operation_profile['collection_mode']}")
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
                queue_id=operation_profile["queue_id"],
                queue_label=operation_profile["queue_label"],
                concurrency_policy=operation_profile["concurrency_policy"],
                browser_policy=operation_profile["browser_policy"],
                session_strategy=operation_profile["session_strategy"],
                collection_mode=operation_profile["collection_mode"],
                revenue_basis=operation_profile["revenue_basis"],
                revenue_metric_key=operation_profile["revenue_metric_key"],
                date_basis=operation_profile["date_basis"],
                validation_mode=operation_profile["validation_mode"],
                verification_mode=operation_profile["verification_mode"],
                auth_priority=operation_profile["auth_priority"],
                user_browser_preferred=operation_profile["user_browser_preferred"],
                playbook=playbook,
                notes=notes,
            )
        )
    return jobs


def summarize_jobs(jobs: list[Job]) -> dict[str, int]:
    summary = {"api": 0, "browser": 0, "manual": 0}
    for job in jobs:
        summary[job.run_mode] = summary.get(job.run_mode, 0) + 1
        queue_key = f"queue:{job.queue_id}"
        summary[queue_key] = summary.get(queue_key, 0) + 1
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
        for queue_id, queue_bucket in _bucket_jobs_by_queue(bucket).items():
            if not queue_bucket:
                continue
            max_workers = _resolve_max_workers(run_mode, queue_id, limits)
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = [executor.submit(_run_job, job, cfg, secrets, channel_credentials, dry_run) for job in queue_bucket]
                for future in as_completed(futures):
                    results.append(future.result())
    return sorted(results, key=lambda item: item.vendor_name)


def _bucket_jobs_by_queue(jobs: list[Job]) -> dict[str, list[Job]]:
    queue_order = ["stable", "legacy", "auth_wait", "environment_special"]
    buckets: dict[str, list[Job]] = {queue_id: [] for queue_id in queue_order}
    for job in sorted(jobs, key=lambda item: (queue_order.index(item.queue_id) if item.queue_id in queue_order else 999, item.auth_priority, item.vendor_name)):
        buckets.setdefault(job.queue_id, []).append(job)
    return buckets


def _resolve_max_workers(run_mode: str, queue_id: str, limits: dict[str, int]) -> int:
    default_limit = max(1, limits[run_mode])
    if run_mode == "manual":
        return 1
    if run_mode == "api" and queue_id == "auth_wait":
        return 1
    if run_mode != "browser":
        return default_limit
    if queue_id == "stable":
        return default_limit
    if queue_id == "legacy":
        return max(1, min(default_limit, 2))
    if queue_id in {"auth_wait", "environment_special"}:
        return 1
    return default_limit


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
        "queue_id": job.queue_id,
        "queue_label": job.queue_label,
        "concurrency_policy": job.concurrency_policy,
        "browser_policy": job.browser_policy,
        "session_strategy": job.session_strategy,
        "collection_mode": job.collection_mode,
        "revenue_basis": job.revenue_basis,
        "revenue_metric_key": job.revenue_metric_key,
        "date_basis": job.date_basis,
        "validation_mode": job.validation_mode,
        "verification_mode": job.verification_mode,
        "auth_priority": job.auth_priority,
        "user_browser_preferred": job.user_browser_preferred,
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
