from __future__ import annotations

import json
from collections import Counter
from dataclasses import asdict, is_dataclass
from pathlib import Path
from typing import Any


OPERATIONS_VERSION = "2026-04-14"
CHANNEL_OPERATING_MODEL_ID = "channel_operating_model_v1"


QUEUE_DEFINITIONS: dict[str, dict[str, Any]] = {
    "stable": {
        "label": "안정 채널",
        "objective": "세션이 살아 있을 때 빠르게 숫자와 원본 파일을 확보한다.",
        "concurrency": "aggressive_parallel",
        "default_browser_policy": "isolated_browser_or_api",
        "operator_mode": "run_first_collect_first",
    },
    "legacy": {
        "label": "레거시/우회 채널",
        "objective": "frame, popup, SSO, hidden download route를 우선 추적한다.",
        "concurrency": "limited_parallel",
        "default_browser_policy": "user_browser_preferred",
        "operator_mode": "single_browser_focus",
    },
    "auth_wait": {
        "label": "인증 필요 채널",
        "objective": "평소에는 대기 큐로 두고 인증이 들어오면 즉시 재개한다.",
        "concurrency": "serial_auth_queue",
        "default_browser_policy": "session_resume_after_verification",
        "operator_mode": "wait_for_code_then_resume",
    },
    "environment_special": {
        "label": "환경 특수 채널",
        "objective": "IE/호환모드/특수 브라우저 같은 별도 환경에서만 처리한다.",
        "concurrency": "dedicated_single",
        "default_browser_policy": "manual_legacy_environment",
        "operator_mode": "manual_or_remote_desktop",
    },
}


QUEUE_OVERRIDES: dict[str, str] = {
    "GS25": "stable",
    "CU": "stable",
    "카페24": "stable",
    "카페24 공동구매": "stable",
    "스마트스토어": "auth_wait",
    "카카오선물하기": "auth_wait",
    "카카오톡스토어": "auth_wait",
    "올리브영": "auth_wait",
    "NS mall": "auth_wait",
    "신세계TV쇼핑": "auth_wait",
    "CJ온스타일": "auth_wait",
    "알리익스프레스": "auth_wait",
    "홈플러스": "legacy",
    "CJ프레시웨이": "legacy",
    "T딜": "legacy",
    "아워홈": "environment_special",
}


USER_BROWSER_PREFERRED = {
    "CJ프레시웨이",
    "홈플러스",
    "G마켓",
    "옥션",
    "파트너스몰(카페24)",
}


AUTH_PRIORITY = {
    "스마트스토어": 10,
    "카카오선물하기": 20,
    "카카오톡스토어": 21,
    "올리브영": 30,
    "SSG": 40,
    "CJ온스타일": 50,
    "알리익스프레스": 60,
    "NS mall": 70,
    "신세계TV쇼핑": 80,
}


REVENUE_BASIS_OVERRIDES: dict[str, dict[str, Any]] = {
    "카페24": {
        "metric_key": "net_sales",
        "basis_name": "실결제금액 - 실환불금액",
        "date_basis": "결제일",
        "validation_mode": "download_or_admin_sales_screen",
    },
    "카페24 공동구매": {
        "metric_key": "net_sales",
        "basis_name": "실결제금액 - 실환불금액",
        "date_basis": "결제일",
        "validation_mode": "download_or_admin_sales_screen",
    },
    "GS25": {
        "metric_key": "delivery_amount",
        "basis_name": "납품금액",
        "date_basis": "납품일",
        "validation_mode": "legacy_excel_download",
    },
    "CU": {
        "metric_key": "delivery_amount",
        "basis_name": "납품금액",
        "date_basis": "납품일",
        "validation_mode": "download_or_legacy_screen",
    },
    "CJ프레시웨이": {
        "metric_key": "supply_amount",
        "basis_name": "입고금액",
        "date_basis": "입고일",
        "validation_mode": "screen_then_export",
    },
    "홈플러스": {
        "metric_key": "delivery_amount",
        "basis_name": "납품정보 기준 금액",
        "date_basis": "입고일자",
        "validation_mode": "download_post_chain",
    },
}


def normalize_text(value: str | None) -> str:
    return (value or "").strip().lower()


def normalize_playbook(playbook: Any) -> dict[str, Any]:
    if playbook is None:
        return {}
    if isinstance(playbook, dict):
        return playbook
    if is_dataclass(playbook):
        return asdict(playbook)
    return {
        "vendor_name": getattr(playbook, "vendor_name", ""),
        "strategy": getattr(playbook, "strategy", ""),
        "notes": list(getattr(playbook, "notes", []) or []),
        "analysis_profile": dict(getattr(playbook, "analysis_profile", {}) or {}),
        "postprocess_rules": dict(getattr(playbook, "postprocess_rules", {}) or {}),
    }


def has_any(text: str, keywords: list[str]) -> bool:
    lowered = normalize_text(text)
    return any(keyword in lowered for keyword in keywords)


def infer_queue_id(row: dict[str, Any], playbook: dict[str, Any]) -> str:
    playbook = normalize_playbook(playbook)
    vendor_name = row.get("vendor_name", "")
    if vendor_name in QUEUE_OVERRIDES:
        return QUEUE_OVERRIDES[vendor_name]

    notes = " ".join(
        [
            str(row.get("special_notes", "") or ""),
            str(row.get("collection_path", "") or ""),
            " ".join(playbook.get("notes", [])),
        ]
    )
    workflow_flags = row.get("workflow_flags", {}) or {}
    auth_type_meaning = row.get("auth_type_meaning", "")

    if has_any(notes, ["internet explore", "internet explorer", "explore", "ie", "webkit", "호환모드", "익스플로러"]):
        return "environment_special"
    if workflow_flags.get("requires_verification") or auth_type_meaning in {"sms_verification_required", "email_verification_required"}:
        return "auth_wait"
    if has_any(notes, ["frame", "iframe", "popup", "sso", "portal", "레거시", "숨김", "우회", "다운로드 인계"]):
        return "legacy"
    if playbook.get("strategy") == "api":
        return "stable"
    if workflow_flags.get("mentions_excel_download"):
        return "stable"
    return "stable"


def infer_collection_mode(row: dict[str, Any], playbook: dict[str, Any]) -> str:
    playbook = normalize_playbook(playbook)
    vendor_name = row.get("vendor_name", "")
    if playbook.get("strategy") == "api":
        return "api"
    if vendor_name in {"GS25", "홈플러스"}:
        return "legacy_download_route"
    if playbook.get("analysis_profile", {}).get("mode") == "download_then_analyze":
        return "download_then_analyze"
    if row.get("workflow_flags", {}).get("mentions_excel_download"):
        return "download"
    if has_any(row.get("collection_path", ""), ["api"]):
        return "api"
    return "screen_or_internal_route"


def infer_revenue_basis(row: dict[str, Any]) -> dict[str, Any]:
    vendor_name = row.get("vendor_name", "")
    if vendor_name in REVENUE_BASIS_OVERRIDES:
        return dict(REVENUE_BASIS_OVERRIDES[vendor_name])

    path = str(row.get("collection_path", "") or "")
    if "입고" in path:
        return {
            "metric_key": "supply_amount",
            "basis_name": "입고금액 추정",
            "date_basis": "입고일",
            "validation_mode": "download_or_screen",
        }
    if "납품" in path:
        return {
            "metric_key": "delivery_amount",
            "basis_name": "납품금액 추정",
            "date_basis": "납품일",
            "validation_mode": "download_or_screen",
        }
    if "결제" in path:
        return {
            "metric_key": "net_sales",
            "basis_name": "결제금액 추정",
            "date_basis": "결제일",
            "validation_mode": "download_or_screen",
        }
    if "주문" in path:
        return {
            "metric_key": "gross_sales",
            "basis_name": "주문금액 추정",
            "date_basis": "주문일",
            "validation_mode": "download_or_screen",
        }
    return {
        "metric_key": "sales",
        "basis_name": "채널 대표 매출 컬럼 수동 확정 필요",
        "date_basis": "채널 기준일 수동 확정 필요",
        "validation_mode": "manual_definition_required",
    }


def infer_browser_policy(queue_id: str, row: dict[str, Any]) -> str:
    vendor_name = row.get("vendor_name", "")
    notes = " ".join(
        [
            str(row.get("special_notes", "") or ""),
            str(row.get("collection_path", "") or ""),
        ]
    )
    if queue_id == "environment_special":
        return "manual_legacy_environment"
    if vendor_name in USER_BROWSER_PREFERRED or has_any(notes, ["sso", "popup", "portal", "서드파티", "보안", "예외"]):
        return "user_browser_preferred"
    if queue_id == "auth_wait":
        return "session_resume_after_verification"
    if queue_id == "legacy":
        return "isolated_browser_with_saved_session"
    return "isolated_browser_or_api"


def infer_session_strategy(queue_id: str, row: dict[str, Any]) -> str:
    if queue_id == "auth_wait":
        return "pause_and_resume_after_code"
    if queue_id == "legacy":
        return "checkpoint_download_first_then_explore"
    if queue_id == "environment_special":
        return "manual_only"
    if row.get("workflow_flags", {}).get("mentions_excel_download"):
        return "reuse_session_and_download_immediately"
    return "reuse_session_first"


def infer_verification_mode(row: dict[str, Any]) -> str:
    auth_type_meaning = row.get("auth_type_meaning", "")
    if auth_type_meaning == "sms_verification_required":
        return "sms"
    if auth_type_meaning == "email_verification_required":
        return "email"
    return "none"


def build_channel_operation_profile(row: dict[str, Any], playbook: dict[str, Any]) -> dict[str, Any]:
    playbook = normalize_playbook(playbook)
    queue_id = infer_queue_id(row, playbook)
    queue_def = QUEUE_DEFINITIONS[queue_id]
    basis = infer_revenue_basis(row)
    verification_mode = infer_verification_mode(row)
    vendor_name = row.get("vendor_name", "")
    return {
        "queue_id": queue_id,
        "queue_label": queue_def["label"],
        "queue_objective": queue_def["objective"],
        "concurrency_policy": queue_def["concurrency"],
        "browser_policy": infer_browser_policy(queue_id, row),
        "session_strategy": infer_session_strategy(queue_id, row),
        "collection_mode": infer_collection_mode(row, playbook),
        "revenue_basis": basis["basis_name"],
        "revenue_metric_key": basis["metric_key"],
        "date_basis": basis["date_basis"],
        "validation_mode": basis["validation_mode"],
        "verification_mode": verification_mode,
        "user_browser_preferred": infer_browser_policy(queue_id, row) == "user_browser_preferred",
        "auth_priority": AUTH_PRIORITY.get(vendor_name, 999 if queue_id == "auth_wait" else 0),
        "session_hot_path": [
            "period_query",
            "aggregate_capture",
            "download_original_file",
            "save_evidence",
            "analyze_file",
        ],
    }


def build_channel_operating_model() -> dict[str, Any]:
    return {
        "id": CHANNEL_OPERATING_MODEL_ID,
        "version": OPERATIONS_VERSION,
        "principles": [
            "숫자 확보를 화면 탐색보다 우선한다.",
            "인증 채널은 평소 실행 큐가 아니라 대기 큐로 분리한다.",
            "레거시 채널은 frame, popup, hidden download route를 기본 전제로 둔다.",
            "다운로드 파일은 즉시 표준 후처리 규칙을 적용해 품목 분석 파이프라인으로 넘긴다.",
            "0원 또는 무데이터도 오류가 아니라 근거가 있는 상태로 기록한다.",
        ],
        "queue_definitions": QUEUE_DEFINITIONS,
        "required_channel_definition_fields": [
            "revenue_basis",
            "date_basis",
            "collection_mode",
            "validation_mode",
        ],
        "download_postprocess_required_fields": [
            "date",
            "channel",
            "sku",
            "product_name",
            "qty",
            "gross_sales",
            "net_sales",
            "supply_amount",
            "delivery_amount",
            "refund_flag",
            "raw_file_name",
        ],
    }


def build_operations_bundle(master_rows: list[dict[str, Any]], playbooks: dict[str, dict[str, Any]]) -> dict[str, Any]:
    channel_profiles = []
    for row in master_rows:
        vendor_name = row.get("vendor_name", "")
        profile = build_channel_operation_profile(row, normalize_playbook(playbooks.get(vendor_name, {})))
        channel_profiles.append(
            {
                "vendor_name": vendor_name,
                "channel_group": row.get("channel_group"),
                "manager": row.get("manager"),
                "login_url": row.get("login_url"),
                "collection_path": row.get("collection_path"),
                "special_notes": row.get("special_notes"),
                **profile,
            }
        )

    queue_counts = Counter(item["queue_id"] for item in channel_profiles)
    auth_waitlist = sorted(
        [item for item in channel_profiles if item["queue_id"] == "auth_wait"],
        key=lambda item: (item["auth_priority"], item["vendor_name"]),
    )
    legacy_routes = [
        item
        for item in channel_profiles
        if item["queue_id"] in {"legacy", "environment_special"}
    ]

    return {
        "version": OPERATIONS_VERSION,
        "channel_operating_model": build_channel_operating_model(),
        "queue_counts": dict(queue_counts),
        "channel_profiles": channel_profiles,
        "auth_waitlist": auth_waitlist,
        "legacy_channel_routes": legacy_routes,
    }


def build_operations_markdown(bundle: dict[str, Any]) -> str:
    lines = [
        "# Channel Operations Guide",
        "",
        "## Queue Counts",
        "",
    ]
    for queue_id, count in sorted(bundle.get("queue_counts", {}).items()):
        label = QUEUE_DEFINITIONS.get(queue_id, {}).get("label", queue_id)
        lines.append(f"- `{queue_id}` ({label}): {count}")

    lines.extend(
        [
            "",
            "## Authentication Queue",
            "",
            "| Priority | Vendor | Verification | Browser Policy | Revenue Basis | Date Basis |",
            "| --- | --- | --- | --- | --- | --- |",
        ]
    )
    for item in bundle.get("auth_waitlist", []):
        lines.append(
            f"| {item['auth_priority']} | {item['vendor_name']} | {item['verification_mode']} | {item['browser_policy']} | {item['revenue_basis']} | {item['date_basis']} |"
        )

    lines.extend(
        [
            "",
            "## Legacy / Special Routes",
            "",
            "| Vendor | Queue | Browser Policy | Collection Mode | Collection Path |",
            "| --- | --- | --- | --- | --- |",
        ]
    )
    for item in bundle.get("legacy_channel_routes", []):
        lines.append(
            f"| {item['vendor_name']} | {item['queue_label']} | {item['browser_policy']} | {item['collection_mode']} | {item['collection_path']} |"
        )

    return "\n".join(lines) + "\n"


def write_operations_bundle(output_dir: Path, bundle: dict[str, Any]) -> dict[str, str]:
    output_dir.mkdir(parents=True, exist_ok=True)
    outputs: dict[str, str] = {}

    files = {
        "operations_bundle": bundle,
        "channel_operating_model": bundle["channel_operating_model"],
        "channel_queue_matrix": bundle["channel_profiles"],
        "auth_waitlist": bundle["auth_waitlist"],
        "legacy_channel_routes": bundle["legacy_channel_routes"],
    }
    for name, payload in files.items():
        path = output_dir / f"{name}.json"
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        outputs[name] = str(path.resolve())

    md_path = output_dir / "channel_operations_guide.md"
    md_path.write_text(build_operations_markdown(bundle), encoding="utf-8")
    outputs["channel_operations_guide"] = str(md_path.resolve())
    return outputs
