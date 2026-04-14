from __future__ import annotations

import json
from collections import Counter
from pathlib import Path
from typing import Any

from .operations import AUTH_PRIORITY, QUEUE_OVERRIDES, REVENUE_BASIS_OVERRIDES, USER_BROWSER_PREFERRED


def summarize_runs(
    input_root: Path,
    date_from: str | None = None,
    date_to: str | None = None,
    channel_filters: list[str] | None = None,
    output_dir: Path | None = None,
) -> dict[str, Any]:
    channel_filters = channel_filters or []
    items: list[dict[str, Any]] = []
    for date_dir in sorted(path for path in input_root.iterdir() if path.is_dir()):
        business_date = date_dir.name
        if date_from and business_date < date_from:
            continue
        if date_to and business_date > date_to:
            continue
        for vendor_dir in sorted(path for path in date_dir.iterdir() if path.is_dir()):
            item = inspect_vendor_run(vendor_dir, business_date)
            if channel_filters and item["vendor_name"] not in channel_filters:
                continue
            items.append(item)

    summary = {
        "run_count": len(items),
        "counts_by_category": dict(Counter(item["category"] for item in items)),
        "counts_by_status": dict(Counter(item["status"] for item in items)),
        "counts_by_queue": dict(Counter(item["queue_id"] for item in items if item.get("queue_id"))),
        "auth_queue": [item for item in items if item["category"] == "auth_required"],
        "login_queue": [item for item in items if item["category"] == "login_required"],
        "captcha_queue": [item for item in items if item["category"] == "captcha_required"],
        "relogin_queue": [item for item in items if item["category"] == "session_expired"],
        "selector_fix_queue": [item for item in items if item["category"] == "selector_fix_needed"],
        "environment_queue": [item for item in items if item["category"] == "environment_blocked"],
        "credentials_queue": [item for item in items if item["category"] in {"credentials_missing", "api_auth_failed"}],
        "ready_or_collected": [item for item in items if item["category"] in {"collected", "session_ready"}],
        "items": items,
    }
    summary["recommendations"] = build_recommendations(items)
    if output_dir:
        output_dir.mkdir(parents=True, exist_ok=True)
        json_path = output_dir / "run_status_summary.json"
        md_path = output_dir / "run_status_summary.md"
        json_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        md_path.write_text(build_summary_markdown(summary), encoding="utf-8")
        summary["outputs"] = {"json": str(json_path.resolve()), "markdown": str(md_path.resolve())}
    return summary


def inspect_vendor_run(vendor_dir: Path, business_date: str) -> dict[str, Any]:
    result_path = vendor_dir / "result.json"
    job_path = vendor_dir / "job.json"
    browser_error_path = vendor_dir / "browser_error.json"
    api_error_path = vendor_dir / "api_error.json"
    api_results_path = vendor_dir / "api_results_summary.json"

    job = load_json(job_path) if job_path.exists() else {}
    result = load_json(result_path) if result_path.exists() else {}
    browser_error = load_json(browser_error_path) if browser_error_path.exists() else {}
    api_error = load_json(api_error_path) if api_error_path.exists() else {}
    has_download = any(
        path.is_file() and (path.suffix.lower() in {".xlsx", ".xlsm", ".csv"} or path.name.endswith("_analysis.json"))
        for path in vendor_dir.rglob("*")
    )

    vendor_name = result.get("vendor_name") or job.get("vendor_name") or vendor_dir.name.replace("_", " ")
    if has_download or api_results_path.exists():
        category = "collected"
        next_action = "analyze_and_merge"
        status = "executed"
    else:
        category = result.get("category") or infer_category(
            job=job,
            browser_error=browser_error,
            api_error=api_error,
            has_download=has_download,
            has_api_results=api_results_path.exists(),
        )
        next_action = result.get("next_action") or infer_next_action(category)
        status = result.get("status") or infer_status(category, has_download, api_results_path.exists())
    detail = browser_error.get("message") or api_error.get("message") or result.get("detail") or ""
    return {
        "business_date": business_date,
        "vendor_name": vendor_name,
        "strategy": result.get("strategy") or job.get("strategy", ""),
        "queue_id": job.get("queue_id", infer_queue_from_job(job, vendor_name)),
        "queue_label": job.get("queue_label", ""),
        "browser_policy": job.get("browser_policy", infer_browser_policy(vendor_name)),
        "session_strategy": job.get("session_strategy", ""),
        "collection_mode": job.get("collection_mode", infer_collection_mode(vendor_name)),
        "revenue_basis": job.get("revenue_basis", infer_revenue_basis(vendor_name).get("basis_name", "")),
        "date_basis": job.get("date_basis", infer_revenue_basis(vendor_name).get("date_basis", "")),
        "validation_mode": job.get("validation_mode", infer_revenue_basis(vendor_name).get("validation_mode", "")),
        "verification_mode": job.get("verification_mode", ""),
        "auth_priority": int(job.get("auth_priority", AUTH_PRIORITY.get(vendor_name, 0)) or 0),
        "status": status,
        "category": category,
        "next_action": next_action,
        "data_ready": bool(result.get("data_ready", has_download or api_results_path.exists())),
        "has_download": has_download,
        "has_api_results": api_results_path.exists(),
        "detail": detail[:240],
        "output_dir": str(vendor_dir.resolve()),
    }


def infer_category(
    job: dict[str, Any],
    browser_error: dict[str, Any],
    api_error: dict[str, Any],
    has_download: bool,
    has_api_results: bool,
) -> str:
    if has_download or has_api_results:
        return "collected"
    message = f"{browser_error.get('message', '')} {api_error.get('message', '')}".lower()
    notes = " ".join(job.get("notes", [])).lower()
    auth_type = str(job.get("auth_type_meaning", ""))
    strategy = str(job.get("strategy", ""))
    requires_verification = bool(job.get("requires_verification", False))
    if "missing api credentials" in message:
        return "credentials_missing"
    if "401" in message or "403" in message or "auth" in message or "token" in message:
        return "api_auth_failed"
    if "session" in message or "expired" in message or "만료" in message:
        return "session_expired"
    if requires_verification or strategy == "browser_verified":
        return "auth_required"
    if auth_type in {"sms_verification_required", "email_verification_required"} and browser_error:
        return "auth_required"
    if "ie" in notes or "activex" in notes or "webkit" in notes or "호환모드" in notes:
        return "environment_blocked"
    if "timeout" in message or "frame not found" in message or "locator" in message:
        return "selector_fix_needed"
    if browser_error or api_error:
        return "retry_needed"
    return "not_started"


def infer_next_action(category: str) -> str:
    mapping = {
        "collected": "analyze_and_merge",
        "session_ready": "reuse_session_for_download",
        "auth_required": "request_verification_and_resume",
        "login_required": "recheck_credentials_and_login_flow",
        "captcha_required": "solve_captcha_or_switch_browser",
        "session_expired": "relogin_and_rerun_immediately",
        "selector_fix_needed": "run_discovery_and_adjust_playbook",
        "environment_blocked": "use_required_browser_environment",
        "credentials_missing": "add_credentials",
        "api_auth_failed": "refresh_api_auth",
        "retry_needed": "rerun_with_diagnostics",
        "not_started": "run_collection",
    }
    return mapping.get(category, "inspect_run")


def infer_queue_from_job(job: dict[str, Any], vendor_name: str) -> str:
    if vendor_name in QUEUE_OVERRIDES:
        return QUEUE_OVERRIDES[vendor_name]
    auth_type = str(job.get("auth_type_meaning", ""))
    notes = " ".join(job.get("notes", [])).lower()
    if auth_type in {"sms_verification_required", "email_verification_required"}:
        return "auth_wait"
    if any(keyword in notes for keyword in ["ie", "webkit", "호환모드"]):
        return "environment_special"
    if any(keyword in notes for keyword in ["sso", "frame", "popup", "레거시", "portal"]):
        return "legacy"
    return "stable"


def infer_browser_policy(vendor_name: str) -> str:
    if vendor_name in USER_BROWSER_PREFERRED:
        return "user_browser_preferred"
    return ""


def infer_revenue_basis(vendor_name: str) -> dict[str, Any]:
    return REVENUE_BASIS_OVERRIDES.get(vendor_name, {})


def infer_collection_mode(vendor_name: str) -> str:
    if vendor_name in {"GS25", "홈플러스"}:
        return "legacy_download_route"
    if vendor_name in USER_BROWSER_PREFERRED:
        return "screen_or_internal_route"
    return ""


def infer_status(category: str, has_download: bool, has_api_results: bool) -> str:
    if has_download or has_api_results or category == "collected":
        return "executed"
    if category == "not_started":
        return "pending"
    if category in {"auth_required", "login_required", "captcha_required", "session_expired", "selector_fix_needed", "environment_blocked", "credentials_missing", "api_auth_failed"}:
        return "blocked"
    return "failed"


def load_json(path: Path) -> dict[str, Any]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def build_recommendations(items: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    latest_items = latest_by_vendor(items)
    today_run_channels = sorted(
        [
            enrich_recommendation(item, recommend_reason(item))
            for item in latest_items
            if item.get("queue_id") == "stable" and item.get("category") in {"not_started", "retry_needed", "session_expired", "selector_fix_needed", "login_required", "captcha_required", "browser_failed"}
        ],
        key=lambda item: (today_run_priority(item), item["vendor_name"]),
    )
    auth_wait_channels = sorted(
        [
            enrich_recommendation(item, "인증이 들어오면 바로 재개")
            for item in latest_items
            if item.get("queue_id") == "auth_wait" or item.get("category") == "auth_required"
        ],
        key=lambda item: (int(item.get("auth_priority", 999) or 999), item["vendor_name"]),
    )
    legacy_focus_channels = sorted(
        [
            enrich_recommendation(item, recommend_reason(item))
            for item in latest_items
            if item.get("queue_id") in {"legacy", "environment_special"}
        ],
        key=lambda item: (legacy_priority(item), item["vendor_name"]),
    )
    return {
        "today_run_channels": today_run_channels,
        "auth_wait_channels": auth_wait_channels,
        "legacy_focus_channels": legacy_focus_channels,
    }


def latest_by_vendor(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    latest: dict[str, dict[str, Any]] = {}
    for item in sorted(items, key=lambda row: (row["business_date"], row["vendor_name"])):
        latest[item["vendor_name"]] = item
    return list(latest.values())


def enrich_recommendation(item: dict[str, Any], reason: str) -> dict[str, Any]:
    return {
        "vendor_name": item["vendor_name"],
        "business_date": item["business_date"],
        "queue_id": item.get("queue_id", ""),
        "category": item["category"],
        "next_action": item["next_action"],
        "reason": reason,
        "revenue_basis": item.get("revenue_basis", ""),
        "date_basis": item.get("date_basis", ""),
        "collection_mode": item.get("collection_mode", ""),
        "browser_policy": item.get("browser_policy", ""),
        "verification_mode": item.get("verification_mode", ""),
        "auth_priority": item.get("auth_priority", 0),
        "output_dir": item["output_dir"],
    }


def today_run_priority(item: dict[str, Any]) -> int:
    order = {
        "session_expired": 10,
        "login_required": 20,
        "selector_fix_needed": 30,
        "browser_failed": 40,
        "retry_needed": 50,
        "captcha_required": 60,
        "not_started": 70,
    }
    return order.get(item["category"], 999)


def legacy_priority(item: dict[str, Any]) -> int:
    order = {
        "session_expired": 10,
        "selector_fix_needed": 20,
        "login_required": 30,
        "browser_failed": 40,
        "retry_needed": 50,
        "environment_blocked": 60,
        "not_started": 70,
    }
    return order.get(item["category"], 999)


def recommend_reason(item: dict[str, Any]) -> str:
    category = item.get("category", "")
    collection_mode = item.get("collection_mode", "")
    if category == "session_expired":
        return "세션만 복구되면 재실행 가능"
    if category == "selector_fix_needed":
        return "화면 구조 보정 후 바로 재수집 가능"
    if category == "login_required":
        return "로그인 재확인 후 같은 날 수집 재개 가능"
    if category == "retry_needed":
        return "재시도 가치가 높은 채널"
    if category == "browser_failed":
        return "브라우저 진단 후 바로 재실행 가치가 높음"
    if category == "environment_blocked":
        return "전용 브라우저/환경에서 집중 처리 필요"
    if collection_mode == "legacy_download_route":
        return "다운로드 경로가 확인된 채널이라 집중 가치가 높음"
    return "오늘 재점검 가치가 높은 채널"


def build_summary_markdown(summary: dict[str, Any]) -> str:
    lines = [
        "# Run Status Summary",
        "",
        f"- run_count: {summary['run_count']}",
        f"- counts_by_category: {json.dumps(summary['counts_by_category'], ensure_ascii=False)}",
        f"- counts_by_queue: {json.dumps(summary['counts_by_queue'], ensure_ascii=False)}",
        "",
        "## Today Run Channels",
    ]
    for item in summary["recommendations"]["today_run_channels"]:
        lines.append(
            f"- {item['vendor_name']}: {item['reason']} / next={item['next_action']} / basis={item.get('revenue_basis', '') or '미정'} / date_basis={item.get('date_basis', '') or '미정'}"
        )
    lines.extend(
        [
            "",
            "## Auth Wait Channels",
        ]
    )
    for item in summary["recommendations"]["auth_wait_channels"]:
        lines.append(
            f"- {item['vendor_name']}: priority={item.get('auth_priority', 0)} / verification={item.get('verification_mode', '') or 'unknown'} / next={item['next_action']}"
        )
    lines.extend(
        [
            "",
            "## Legacy Focus Channels",
        ]
    )
    for item in summary["recommendations"]["legacy_focus_channels"]:
        lines.append(
            f"- {item['vendor_name']}: {item['reason']} / collection_mode={item.get('collection_mode', '') or 'unknown'} / browser_policy={item.get('browser_policy', '') or 'unknown'}"
        )
    lines.extend(
        [
        "",
        "## Auth Queue",
        ]
    )
    for item in summary["auth_queue"]:
        lines.append(f"- {item['business_date']} {item['vendor_name']}: {item['next_action']}")
    lines.extend(["", "## Login Queue"])
    for item in summary["login_queue"]:
        lines.append(f"- {item['business_date']} {item['vendor_name']}: {item['detail']}")
    lines.extend(["", "## Captcha Queue"])
    for item in summary["captcha_queue"]:
        lines.append(f"- {item['business_date']} {item['vendor_name']}: {item['next_action']}")
    lines.extend(["", "## Relogin Queue"])
    for item in summary["relogin_queue"]:
        lines.append(f"- {item['business_date']} {item['vendor_name']}: {item['next_action']}")
    lines.extend(["", "## Selector Fix Queue"])
    for item in summary["selector_fix_queue"]:
        lines.append(f"- {item['business_date']} {item['vendor_name']}: {item['detail']}")
    lines.extend(["", "## Ready Or Collected"])
    for item in summary["ready_or_collected"]:
        lines.append(f"- {item['business_date']} {item['vendor_name']}: {item['category']}")
    return "\n".join(lines) + "\n"
