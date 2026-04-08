from __future__ import annotations

import json
from pathlib import Path

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

    from playwright.sync_api import sync_playwright  # type: ignore

    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=True)
        page = browser.new_page()
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
        browser.close()

    return {"actions": len(actions), "frames": len(frame_summaries), "url": summary.get("url")}
