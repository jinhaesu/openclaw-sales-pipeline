from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from .standards import build_channel_output_contract, build_standards_bundle, merge_postprocess_rules


def build_workflow_knowledge(master_path: Path, playbook_dir: Path) -> dict[str, Any]:
    with master_path.open("r", encoding="utf-8") as handle:
        master = json.load(handle)

    playbooks: dict[str, dict[str, Any]] = {}
    if playbook_dir.exists():
        for path in sorted(playbook_dir.glob("*.json")):
            with path.open("r", encoding="utf-8") as handle:
                raw = json.load(handle)
            playbooks[raw["vendor_name"]] = raw

    items = []
    for row in master.get("master", []):
        vendor_name = row.get("vendor_name", "")
        workflow_flags = row.get("workflow_flags", {})
        video_support = row.get("video_support", {})
        playbook = playbooks.get(vendor_name, {})
        items.append(
            {
                "vendor_name": vendor_name,
                "channel_group": row.get("channel_group"),
                "manager": row.get("manager"),
                "login_url": row.get("login_url"),
                "auth_type_meaning": row.get("auth_type_meaning"),
                "collection_path": row.get("collection_path"),
                "special_notes": row.get("special_notes"),
                "requires_verification": workflow_flags.get("requires_verification", False),
                "mentions_excel_download": workflow_flags.get("mentions_excel_download", False),
                "has_video": video_support.get("has_video", False),
                "video_files": [item.get("file_path") for item in video_support.get("files", [])],
                "playbook_strategy": playbook.get("strategy"),
                "playbook_actions": len(playbook.get("browser_actions", [])),
                "analysis_profile": playbook.get("analysis_profile", {}),
                "postprocess_rules": merge_postprocess_rules(
                    playbook.get("analysis_profile", {}),
                    playbook.get("postprocess_rules", {}),
                ),
                "channel_output_contract": build_channel_output_contract(),
                "optimization_hints": build_hints(row, playbook),
            }
        )

    return {
        "standards": build_standards_bundle(),
        "channel_count": master.get("channel_count", len(items)),
        "video_supported_count": master.get("video_supported_count", 0),
        "items": items,
    }


def build_hints(row: dict[str, Any], playbook: dict[str, Any]) -> list[str]:
    hints: list[str] = []
    path = row.get("collection_path", "") or ""
    notes = row.get("special_notes", "") or ""
    if "엑셀" in path or "다운로드" in path:
        hints.append("prefer_download_then_parse")
    if row.get("auth_type_meaning") in {"sms_verification_required", "email_verification_required"}:
        hints.append("persist_session_state")
    if playbook.get("browser_actions"):
        hints.append("playbook_available")
    if row.get("video_support", {}).get("has_video"):
        hints.append("video_reference_available")
    if playbook.get("analysis_profile", {}).get("mode") == "download_then_analyze":
        hints.append("auto_file_analysis_ready")
        hints.append("standardized_channel_output")
    if "OTP" in notes or "인증" in notes:
        hints.append("authentication_step_present")
    return hints


def write_workflow_knowledge(output_path: Path, knowledge: dict[str, Any]) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(knowledge, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
