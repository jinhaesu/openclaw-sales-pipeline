from __future__ import annotations

import json
from pathlib import Path

from .models import ChannelRecord, Playbook, RuntimeConfig


def load_runtime_config(path: Path) -> RuntimeConfig:
    with path.open("r", encoding="utf-8") as handle:
        raw = json.load(handle)
    return RuntimeConfig(
        master_path=raw["master_path"],
        artifact_root=raw["artifact_root"],
        secrets_path=raw["secrets_path"],
        session_state_root=raw["session_state_root"],
        api_concurrency=int(raw["api_concurrency"]),
        browser_concurrency=int(raw["browser_concurrency"]),
        manual_concurrency=int(raw["manual_concurrency"]),
        default_strategy=raw["default_strategy"],
        playbook_dir=raw["playbook_dir"],
    )


def load_channel_master(path: Path) -> list[ChannelRecord]:
    with path.open("r", encoding="utf-8") as handle:
        raw = json.load(handle)
    rows = raw.get("master", [])
    channels: list[ChannelRecord] = []
    for row in rows:
        flags = row.get("workflow_flags", {})
        video_support = row.get("video_support", {})
        channels.append(
            ChannelRecord(
                vendor_name=row.get("vendor_name", ""),
                channel_group=row.get("channel_group", ""),
                manager=row.get("manager", ""),
                login_url=row.get("login_url", ""),
                auth_type=row.get("auth_type", ""),
                auth_type_meaning=row.get("auth_type_meaning", ""),
                special_notes=row.get("special_notes", ""),
                collection_path=row.get("collection_path", ""),
                has_video=bool(video_support.get("has_video", False)),
                video_count=int(flags.get("video_count", 0)),
                requires_verification=bool(flags.get("requires_verification", False)),
                mentions_excel_download=bool(flags.get("mentions_excel_download", False)),
            )
        )
    return channels


def load_playbooks(directory: Path) -> dict[str, Playbook]:
    playbooks: dict[str, Playbook] = {}
    if not directory.exists():
        return playbooks
    for path in sorted(directory.glob("*.json")):
        with path.open("r", encoding="utf-8") as handle:
            raw = json.load(handle)
        playbook = Playbook(
            vendor_name=raw["vendor_name"],
            strategy=raw["strategy"],
            api_provider=raw.get("api_provider"),
            credential_key=raw.get("credential_key"),
            preferred_dataset=list(raw.get("preferred_dataset", [])),
            notes=list(raw.get("notes", [])),
        )
        playbooks[playbook.vendor_name] = playbook
    return playbooks
