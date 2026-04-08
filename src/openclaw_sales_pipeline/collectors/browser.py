from __future__ import annotations

import json
from pathlib import Path

from ..models import Job, JobResult
from .base import BaseCollector


class BrowserCollector(BaseCollector):
    def run_actions(self, page, output_dir: Path, job: Job) -> list[dict]:
        actions = job.playbook.browser_actions if job.playbook else []
        credentials = self.channel_credentials.get(job.vendor_name)
        executed: list[dict] = []
        for action in actions:
            action_type = action.get("type")
            if action_type == "goto":
                target = action.get("url") or job.login_url
                page.goto(target, wait_until="domcontentloaded", timeout=30000)
                executed.append({"type": "goto", "url": target})
            elif action_type == "press":
                page.keyboard.press(action.get("key", "Escape"))
                executed.append({"type": "press", "key": action.get("key", "Escape")})
            elif action_type == "eval":
                expression = action.get("expression", "")
                page.evaluate(expression)
                executed.append({"type": "eval"})
            elif action_type == "click_text":
                text = action.get("text", "")
                exact = bool(action.get("exact", False))
                page.get_by_text(text, exact=exact).first.click(timeout=15000)
                executed.append({"type": "click_text", "text": text, "exact": exact})
            elif action_type == "click_role":
                role = action.get("role", "button")
                name = action.get("name", "")
                exact = bool(action.get("exact", False))
                page.get_by_role(role, name=name, exact=exact).first.click(timeout=15000)
                executed.append({"type": "click_role", "role": role, "name": name, "exact": exact})
            elif action_type == "fill_label":
                label = action.get("label", "")
                value = action.get("value", "")
                page.get_by_label(label, exact=bool(action.get("exact", False))).fill(value, timeout=15000)
                executed.append({"type": "fill_label", "label": label})
            elif action_type == "fill_name":
                name = action.get("name", "")
                value = action.get("value", "")
                page.locator(f'[name=\"{name}\"]').first.fill(value, timeout=15000)
                executed.append({"type": "fill_name", "name": name})
            elif action_type == "fill_credential":
                name = action.get("name", "")
                source = action.get("source", "")
                value = credentials.get(source, "")
                page.locator(f'[name=\"{name}\"]').first.fill(str(value), timeout=15000)
                executed.append({"type": "fill_credential", "name": name, "source": source, "present": bool(value)})
            elif action_type == "click_alt":
                alt = action.get("alt", "")
                page.get_by_alt_text(alt).first.click(timeout=15000)
                executed.append({"type": "click_alt", "alt": alt})
            elif action_type == "screenshot":
                path = output_dir / action.get("path", "page.png")
                page.screenshot(path=str(path), full_page=True)
                executed.append({"type": "screenshot", "path": str(path)})
            elif action_type == "note":
                executed.append({"type": "note", "message": action.get("message", "")})
            elif action_type == "wait_for_timeout":
                ms = int(action.get("ms", 1000))
                page.wait_for_timeout(ms)
                executed.append({"type": "wait_for_timeout", "ms": ms})
        return executed

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
                "browser_actions": job.playbook.browser_actions if job.playbook else [],
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
                try:
                    with sync_playwright() as playwright:
                        browser = playwright.chromium.launch(headless=True)
                        context = browser.new_context(
                            storage_state=str(session_state_path) if session_state_path.exists() else None
                        )
                        page = context.new_page()
                        executed_actions = self.run_actions(page, output_dir, job)
                        if not executed_actions:
                            page.goto(job.login_url, wait_until="domcontentloaded", timeout=30000)
                            executed_actions = [{"type": "goto", "url": job.login_url}]
                        (output_dir / "last_url.txt").write_text(page.url + "\n", encoding="utf-8")
                        (output_dir / "browser_actions_executed.json").write_text(
                            json.dumps(executed_actions, ensure_ascii=False, indent=2) + "\n",
                            encoding="utf-8",
                        )
                        context.storage_state(path=str(session_state_path))
                        browser.close()
                    status = "scaffolded"
                    detail = "playwright session initialized"
                except Exception as exc:
                    (output_dir / "browser_error.json").write_text(
                        json.dumps({"error": type(exc).__name__, "message": str(exc)}, ensure_ascii=False, indent=2)
                        + "\n",
                        encoding="utf-8",
                    )
                    status = "failed"
                    detail = f"browser collector failed: {type(exc).__name__}"

        return JobResult(
            vendor_name=job.vendor_name,
            strategy=job.strategy,
            status=status,
            output_dir=str(output_dir),
            detail=detail,
            metadata={"run_mode": job.run_mode, "session_state_path": str(session_state_path)},
        )
