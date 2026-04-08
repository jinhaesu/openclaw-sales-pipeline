from __future__ import annotations

import json
from pathlib import Path
from urllib.parse import parse_qsl

from .channel_credentials import ChannelCredentialStore
from .collectors.browser import BrowserCollector
from .models import Job, Playbook, RuntimeConfig
from .secrets import SecretStore


def discover_channel(
    cfg: RuntimeConfig,
    secrets: SecretStore,
    credentials: ChannelCredentialStore,
    job: Job,
    output_dir: Path,
) -> dict:
    collector = BrowserCollector(cfg, secrets, credentials)
    output_dir.mkdir(parents=True, exist_ok=True)
    state_root = Path(cfg.session_state_root).expanduser()
    state_root.mkdir(parents=True, exist_ok=True)
    safe_vendor = (
        job.vendor_name.replace(" ", "_")
        .replace("/", "_")
        .replace("(", "")
        .replace(")", "")
    )
    session_state_path = state_root / f"{safe_vendor}.json"
    reuse_session_state = bool(job.playbook.metadata.get("reuse_session_state", True)) if job.playbook else True

    from playwright.sync_api import sync_playwright  # type: ignore

    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=True)
        context = browser.new_context(
            storage_state=str(session_state_path) if reuse_session_state and session_state_path.exists() else None
        )
        page = context.new_page()
        request_log: list[dict] = []

        def handle_request(request) -> None:
            try:
                post_data = request.post_data or ""
                post_data_note = "text"
            except Exception:
                post_data = ""
                post_data_note = "binary_or_unreadable"
            post_keys: list[str] = []
            non_empty_keys: list[str] = []
            if post_data and "=" in post_data:
                for key, value in parse_qsl(post_data, keep_blank_values=True):
                    post_keys.append(key)
                    if value:
                        non_empty_keys.append(key)
            request_log.append(
                {
                    "method": request.method,
                    "url": request.url,
                    "resource_type": request.resource_type,
                    "has_post_data": bool(post_data),
                    "post_data_length": len(post_data),
                    "post_data_note": post_data_note,
                    "post_keys": post_keys[:50],
                    "non_empty_post_keys": non_empty_keys[:50],
                }
            )

        page.on("request", handle_request)
        actions = collector.run_actions(page, output_dir, job)
        summary = collector.dump_page_summary(page, output_dir, "discovery_summary.json")

        frame_summaries = []
        for frame in page.frames:
            if frame == page.main_frame:
                continue
            try:
                data = frame.evaluate(
                    """() => ({
                      url: location.href,
                      title: document.title,
                      links: Array.from(document.querySelectorAll('a'))
                        .map((el, i) => ({i, text: (el.textContent || '').trim(), href: el.getAttribute('href') || '', onclick: el.getAttribute('onclick') || ''}))
                        .filter(x => x.text)
                        .slice(0, 120),
                      texts: Array.from(document.querySelectorAll('td, span, li'))
                        .map((el, i) => ({i, text: (el.textContent || '').trim()}))
                        .filter(x => x.text && x.text.length < 60)
                        .slice(0, 160)
                    })"""
                )
            except Exception as exc:
                data = {"error": type(exc).__name__, "message": str(exc)}
            frame_summaries.append({"name": frame.name, "url": frame.url, "summary": data})

        (output_dir / "frame_summaries.json").write_text(
            json.dumps(frame_summaries, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
        (output_dir / "discovery_actions.json").write_text(
            json.dumps(actions, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
        (output_dir / "network_requests.json").write_text(
            json.dumps(request_log, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
        if reuse_session_state:
            context.storage_state(path=str(session_state_path))
        context.close()
        browser.close()

    return {
        "actions": len(actions),
        "frames": len(frame_summaries),
        "url": summary.get("url"),
        "session_state_path": str(session_state_path) if reuse_session_state else "",
    }
