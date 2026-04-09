from __future__ import annotations

import json
import re
import shutil
import unicodedata
from pathlib import Path
from typing import Any

from .excel_analysis import analyze_sales_file
from .models import ChannelRecord, Playbook
from .reporting import safe_vendor_name


SUPPORTED_DOWNLOAD_SUFFIXES = {".xlsx", ".xlsm", ".csv", ".txt"}
SKIP_NAME_KEYWORDS = ["로그인정보", "로그인 정보", "openclaw", "codex", ".ds_store"]
MANUAL_VENDOR_ALIASES = {
    "쿠팡 WING": ["쿠팡wing", "쿠팡po", "쿠팡_po", "쿠팡"],
    "컬리": ["컬리", "kurly"],
    "B마트": ["b마트", "bmart"],
    "베네피아": ["베네피아"],
    "카페24 공동구매": ["카페24공동구매"],
    "카페24": ["카페24", "cafe24"],
    "스마트스토어": ["스마트스토어", "smartstore"],
    "SSG": ["ssg"],
}


def ingest_downloads(
    downloads_root: Path,
    output_root: Path,
    channels: list[ChannelRecord],
    playbooks: dict[str, Playbook],
    business_date: str,
    channel_filters: list[str] | None = None,
    analyze: bool = True,
    move_files: bool = False,
    manifest_path: Path | None = None,
) -> dict[str, Any]:
    downloads_root = downloads_root.expanduser()
    output_root = output_root.expanduser()
    output_root.mkdir(parents=True, exist_ok=True)
    channel_filters = channel_filters or []
    matcher = build_vendor_matcher(channels, channel_filters)
    channel_lookup = {channel.vendor_name: channel for channel in channels}

    items: list[dict[str, Any]] = []
    unmatched: list[str] = []
    scanned = 0

    for path in sorted(downloads_root.rglob("*")):
        if not path.is_file():
            continue
        scanned += 1
        if path.suffix.lower() not in SUPPORTED_DOWNLOAD_SUFFIXES:
            continue
        if should_skip_file(path):
            continue
        vendor_name = infer_vendor_name(path, matcher)
        if not vendor_name:
            unmatched.append(str(path))
            continue
        target_dir = output_root / business_date / safe_vendor_name(vendor_name) / "downloads"
        target_dir.mkdir(parents=True, exist_ok=True)
        target_path = make_target_path(target_dir, path)
        if move_files:
            shutil.move(str(path), str(target_path))
            action = "moved"
        else:
            shutil.copy2(path, target_path)
            action = "copied"

        item: dict[str, Any] = {
            "vendor_name": vendor_name,
            "business_date": business_date,
            "source_path": str(path),
            "stored_path": str(target_path),
            "action": action,
        }
        if analyze:
            channel = channel_lookup.get(vendor_name)
            playbook = playbooks.get(vendor_name)
            context = {
                "vendor_name": vendor_name,
                "business_date": business_date,
                "channel_group": channel.channel_group if channel else "",
                "manager": channel.manager if channel else "",
            }
            analysis = analyze_sales_file(
                path=target_path,
                profile=playbook.analysis_profile if playbook else {},
                context=context,
            )
            analysis_path = target_path.with_name(f"{target_path.stem}_analysis.json")
            analysis_path.write_text(json.dumps(analysis, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
            item["analysis_path"] = str(analysis_path)
            item["row_count"] = analysis["row_count"]
            item["product_count"] = analysis["product_count"]
            item["sales"] = analysis["totals"]["sales"]
            item["qty"] = analysis["totals"]["qty"]
        items.append(item)

    manifest = {
        "business_date": business_date,
        "downloads_root": str(downloads_root),
        "output_root": str(output_root),
        "move_files": move_files,
        "analyze": analyze,
        "scanned_files": scanned,
        "matched_count": len(items),
        "unmatched_count": len(unmatched),
        "items": items,
        "unmatched": unmatched,
    }
    if manifest_path:
        manifest_path = manifest_path.expanduser()
        manifest_path.parent.mkdir(parents=True, exist_ok=True)
        manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return manifest


def build_vendor_matcher(channels: list[ChannelRecord], channel_filters: list[str]) -> list[tuple[str, str]]:
    allowed = set(channel_filters)
    entries: dict[str, str] = {}
    for channel in channels:
        if allowed and channel.vendor_name not in allowed:
            continue
        vendor_name = channel.vendor_name
        aliases = {vendor_name, safe_vendor_name(vendor_name), normalize_token(vendor_name)}
        aliases.update(MANUAL_VENDOR_ALIASES.get(vendor_name, []))
        for alias in aliases:
            normalized = normalize_token(alias)
            if normalized:
                entries[normalized] = vendor_name
    return sorted(entries.items(), key=lambda item: len(item[0]), reverse=True)


def infer_vendor_name(path: Path, matcher: list[tuple[str, str]]) -> str:
    normalized = normalize_token(str(path))
    for alias, vendor_name in matcher:
        if alias and alias in normalized:
            return vendor_name
    return ""


def normalize_token(value: str) -> str:
    lowered = unicodedata.normalize("NFC", value).lower()
    return re.sub(r"[^0-9a-z가-힣]+", "", lowered)


def should_skip_file(path: Path) -> bool:
    lowered = normalize_token(path.name)
    return any(normalize_token(keyword) in lowered for keyword in SKIP_NAME_KEYWORDS)


def make_target_path(target_dir: Path, source_path: Path) -> Path:
    base = source_path.stem
    suffix = source_path.suffix
    candidate = target_dir / source_path.name
    index = 1
    while candidate.exists():
        candidate = target_dir / f"{base}_{index}{suffix}"
        index += 1
    return candidate
