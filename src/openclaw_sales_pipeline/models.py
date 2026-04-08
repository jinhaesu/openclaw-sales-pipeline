from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class ChannelRecord:
    vendor_name: str
    channel_group: str
    manager: str
    login_url: str
    auth_type: str
    auth_type_meaning: str
    special_notes: str
    collection_path: str
    has_video: bool
    video_count: int
    requires_verification: bool
    mentions_excel_download: bool


@dataclass
class RuntimeConfig:
    master_path: str
    artifact_root: str
    api_concurrency: int
    browser_concurrency: int
    manual_concurrency: int
    default_strategy: str
    playbook_dir: str


@dataclass
class Playbook:
    vendor_name: str
    strategy: str
    api_provider: str | None = None
    preferred_dataset: list[str] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)


@dataclass
class Job:
    vendor_name: str
    strategy: str
    run_mode: str
    business_date: str
    output_dir: str
    auth_type_meaning: str
    collection_path: str
    notes: list[str] = field(default_factory=list)


@dataclass
class JobResult:
    vendor_name: str
    strategy: str
    status: str
    output_dir: str
    detail: str
    metadata: dict[str, Any] = field(default_factory=dict)
