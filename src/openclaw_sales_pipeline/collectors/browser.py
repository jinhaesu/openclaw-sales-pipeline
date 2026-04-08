from __future__ import annotations

import json
from pathlib import Path

from ..models import Job, JobResult
from .base import BaseCollector


class BrowserCollector(BaseCollector):
    def collect(self, job: Job, dry_run: bool) -> JobResult:
        output_dir = self.ensure_output_dir(job)
        state_root = Path(self.cfg.session_state_root).expanduser()
        state_root.mkdir(parents=True, exist_ok=True)
        safe_vendor = (
            job.vendor_name.replace(" ", "_")
            .replace("/", "_")
            .replace("(", "")
            .replace(")", "")
        )
        session_state_path = state_root / f"{safe_vendor}.json"

        payload = {
            "vendor_name": job.vendor_name,
            "strategy": job.strategy,
            "business_date": job.business_date,
            "login_url": job.login_url,
            "collection_path": job.collection_path,
            "auth_type_meaning": job.auth_type_meaning,
            "requires_verification": job.requires_verification,
            "session_state_path": str(session_state_path),
            "has_video": job.has_video,
            "playbook": {
                "strategy": job.playbook.strategy if job.playbook else None,
                "notes": job.playbook.notes if job.playbook else [],
            },
        }
        (output_dir / "browser_plan.json").write_text(
            json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )

        if dry_run:
            status = "planned"
            detail = "browser collector planned"
        else:
            try:
                from playwright.sync_api import sync_playwright  # type: ignore
            except Exception:
                status = "missing_dependency"
                detail = "playwright not installed"
            else:
                with sync_playwright() as playwright:
                    browser = playwright.chromium.launch(headless=True)
                    context = browser.new_context(storage_state=str(session_state_path) if session_state_path.exists() else None)
                    page = context.new_page()
                    page.goto(job.login_url, wait_until="domcontentloaded", timeout=30000)
                    (output_dir / "last_url.txt").write_text(page.url + "\n", encoding="utf-8")
                    context.storage_state(path=str(session_state_path))
                    browser.close()
                status = "scaffolded"
                detail = "playwright session initialized"

        return JobResult(
            vendor_name=job.vendor_name,
            strategy=job.strategy,
            status=status,
            output_dir=str(output_dir),
            detail=detail,
            metadata={"run_mode": job.run_mode, "session_state_path": str(session_state_path)},
        )
