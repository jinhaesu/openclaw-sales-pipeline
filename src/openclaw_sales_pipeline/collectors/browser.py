from __future__ import annotations

import cgi
import json
import re
from datetime import date, timedelta
from pathlib import Path
from typing import Any
from urllib.parse import urljoin
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from ..models import Job, JobResult
from .base import BaseCollector


class BrowserCollector(BaseCollector):
    def build_template_context(self, job: Job) -> dict[str, str]:
        business_date = date.fromisoformat(job.business_date)
        return {
            "{{business_date}}": job.business_date,
            "{{business_date_compact}}": business_date.strftime("%Y%m%d"),
            "{{business_date_minus_1}}": (business_date - timedelta(days=1)).isoformat(),
            "{{business_date_minus_1_compact}}": (business_date - timedelta(days=1)).strftime("%Y%m%d"),
            "{{login_url}}": job.login_url,
        }

    def render_templates(self, value: Any, context: dict[str, str]) -> Any:
        if isinstance(value, str):
            rendered = value
            for token, replacement in context.items():
                rendered = rendered.replace(token, replacement)
            return rendered
        if isinstance(value, list):
            return [self.render_templates(item, context) for item in value]
        if isinstance(value, dict):
            return {key: self.render_templates(item, context) for key, item in value.items()}
        return value

    def find_frame(self, page, action: dict[str, Any]):
        frame_name = action.get("frame_name")
        frame_url_contains = action.get("frame_url_contains")
        for frame in page.frames:
            if frame_name and frame.name == frame_name:
                return frame
            if frame_url_contains and frame_url_contains in frame.url:
                return frame
        return None

    def dump_page_summary(self, page, output_dir: Path, file_name: str = "page_summary.json") -> dict[str, Any]:
        summary = page.evaluate(
            """() => ({
              url: location.href,
              title: document.title,
              links: Array.from(document.querySelectorAll('a'))
                .map((el, i) => ({i, text: (el.textContent || '').trim(), href: el.getAttribute('href') || '', onclick: el.getAttribute('onclick') || ''}))
                .filter(x => x.text)
                .slice(0, 200),
              inputs: Array.from(document.querySelectorAll('input'))
                .map((el, i) => ({i, type: el.type || '', id: el.id || '', name: el.name || '', value: el.value || '', placeholder: el.getAttribute('placeholder') || ''}))
                .slice(0, 100),
              frames: Array.from(document.querySelectorAll('frame, iframe'))
                .map((el, i) => ({i, name: el.name || '', src: el.getAttribute('src') || ''})),
              texts: Array.from(document.querySelectorAll('td, span, li, strong, b'))
                .map((el, i) => ({i, text: (el.textContent || '').trim()}))
                .filter(x => x.text && x.text.length < 60)
                .slice(0, 250),
              flags: {
                has_recaptcha: !!document.querySelector('iframe[title*="reCAPTCHA"], .g-recaptcha, textarea[name="g-recaptcha-response"]'),
                has_password_input: !!document.querySelector('input[type="password"]'),
                has_login_button: Array.from(document.querySelectorAll('button, a, input[type="button"], input[type="submit"]'))
                  .some((el) => /로그인|login/i.test((el.textContent || el.value || el.getAttribute('title') || '').trim()))
              }
            })"""
        )
        (output_dir / file_name).write_text(
            json.dumps(summary, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
        (output_dir / file_name.replace(".json", ".html")).write_text(page.content(), encoding="utf-8")
        return summary

    def build_action_variants(self, action: dict[str, Any]) -> list[dict[str, Any]]:
        variants = [dict(action)]
        action_type = action.get("type")
        if action_type in {"click_text", "click_text_in_frame"}:
            for text in action.get("fallback_texts", []):
                variants.append({**action, "text": text})
        if action_type in {"click_selector", "fill_selector", "type_selector", "download_click_selector"}:
            for selector in action.get("fallback_selectors", []):
                variants.append({**action, "selector": selector})
        return variants

    def resolve_output_path(self, output_dir: Path, raw_path: str | None, default_name: str) -> Path:
        relative = raw_path or default_name
        path = Path(relative)
        if not path.is_absolute():
            path = output_dir / path
        path.parent.mkdir(parents=True, exist_ok=True)
        return path

    def infer_download_name(self, headers, fallback: str) -> str:
        content_disposition = headers.get("Content-Disposition", "")
        if not content_disposition:
            return fallback
        _value, params = cgi.parse_header(content_disposition)
        filename = params.get("filename")
        return filename or fallback

    def decode_payload_text(self, payload: bytes) -> str:
        for encoding in ("utf-8", "cp949", "latin1"):
            try:
                return payload.decode(encoding)
            except Exception:
                continue
        return payload.decode("latin1", errors="ignore")

    def no_data_path(self, path: Path) -> Path:
        return path.with_suffix(".no_data.html")

    def is_html_interstitial(self, payload: bytes) -> bool:
        lowered = self.decode_payload_text(payload[:4096]).lower()
        return "<script" in lowered and "downloadfrm" in lowered

    def extract_match(self, pattern: str, text: str, default: str = "") -> str:
        match = re.search(pattern, text, re.MULTILINE)
        if not match:
            return default
        return match.group(1)

    def perform_action(self, page, output_dir: Path, action: dict[str, Any], credentials: dict[str, Any]) -> dict[str, Any]:
        action_type = action.get("type")
        if action_type == "goto":
            target = action.get("url", "")
            page.goto(target, wait_until="domcontentloaded", timeout=30000)
            return {"type": "goto", "url": target}
        if action_type == "press":
            key = action.get("key", "Escape")
            page.keyboard.press(key)
            return {"type": "press", "key": key}
        if action_type == "eval":
            page.evaluate(action.get("expression", ""))
            return {"type": "eval"}
        if action_type == "eval_in_frame":
            target_frame = self.find_frame(page, action)
            if target_frame is None:
                raise RuntimeError(f"frame not found for action: {action}")
            target_frame.evaluate(action.get("expression", ""))
            return {
                "type": "eval_in_frame",
                "frame_name": action.get("frame_name"),
                "frame_url_contains": action.get("frame_url_contains"),
            }
        if action_type == "eval_dump":
            path = output_dir / action.get("path", "eval_dump.json")
            data = page.evaluate(action.get("expression", ""))
            path.write_text(json.dumps(data, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
            return {"type": "eval_dump", "path": str(path)}
        if action_type == "click_selector":
            selector = action.get("selector", "")
            try:
                page.locator(selector).first.click(timeout=int(action.get("timeout_ms", 15000)))
            except Exception:
                if action.get("optional"):
                    return {"type": "click_selector", "selector": selector, "skipped": True}
                raise
            return {"type": "click_selector", "selector": selector}
        if action_type == "download_click_selector":
            selector = action.get("selector", "")
            with page.expect_download(timeout=int(action.get("timeout_ms", 30000))) as download_info:
                page.locator(selector).first.click(timeout=15000)
            download = download_info.value
            path = self.resolve_output_path(output_dir, action.get("path"), download.suggested_filename or "download.bin")
            download.save_as(str(path))
            return {"type": "download_click_selector", "selector": selector, "path": str(path)}
        if action_type == "click_text":
            text = action.get("text", "")
            exact = bool(action.get("exact", False))
            page.get_by_text(text, exact=exact).first.click(timeout=15000)
            return {"type": "click_text", "text": text, "exact": exact}
        if action_type == "click_text_in_frame":
            text = action.get("text", "")
            exact = bool(action.get("exact", False))
            frame_name = action.get("frame_name")
            frame_url_contains = action.get("frame_url_contains")
            target_frame = self.find_frame(page, action)
            if target_frame is None and bool(action.get("search_all_frames", True)):
                for frame in page.frames:
                    try:
                        frame.get_by_text(text, exact=exact).first.click(timeout=3000)
                        return {
                            "type": "click_text_in_frame",
                            "text": text,
                            "exact": exact,
                            "frame_name": frame.name,
                            "frame_url_contains": frame.url,
                            "frame_search_mode": "all_frames",
                        }
                    except Exception:
                        continue
            if target_frame is None:
                raise RuntimeError(f"frame not found for action: {action}")
            target_frame.get_by_text(text, exact=exact).first.click(timeout=15000)
            return {
                "type": "click_text_in_frame",
                "text": text,
                "exact": exact,
                "frame_name": frame_name,
                "frame_url_contains": frame_url_contains,
            }
        if action_type == "click_role":
            role = action.get("role", "button")
            name = action.get("name", "")
            exact = bool(action.get("exact", False))
            page.get_by_role(role, name=name, exact=exact).first.click(timeout=15000)
            return {"type": "click_role", "role": role, "name": name, "exact": exact}
        if action_type == "fill_selector":
            selector = action.get("selector", "")
            value = action.get("value", "")
            source = action.get("source", "")
            if source:
                value = credentials.get(source, "")
            try:
                page.locator(selector).first.fill(str(value), timeout=int(action.get("timeout_ms", 15000)))
            except Exception:
                if action.get("optional"):
                    return {"type": "fill_selector", "selector": selector, "source": source or None, "present": bool(value), "skipped": True}
                raise
            return {"type": "fill_selector", "selector": selector, "source": source or None, "present": bool(value)}
        if action_type == "type_selector":
            selector = action.get("selector", "")
            value = action.get("value", "")
            source = action.get("source", "")
            delay_ms = float(action.get("delay_ms", 40))
            if source:
                value = credentials.get(source, "")
            locator = page.locator(selector).first
            locator.click(timeout=15000)
            locator.fill("", timeout=15000)
            locator.type(str(value), delay=delay_ms, timeout=15000)
            return {
                "type": "type_selector",
                "selector": selector,
                "source": source or None,
                "present": bool(value),
                "delay_ms": delay_ms,
            }
        if action_type == "fill_label":
            label = action.get("label", "")
            page.get_by_label(label, exact=bool(action.get("exact", False))).fill(action.get("value", ""), timeout=15000)
            return {"type": "fill_label", "label": label}
        if action_type == "fill_name":
            name = action.get("name", "")
            page.locator(f'[name="{name}"]').first.fill(action.get("value", ""), timeout=15000)
            return {"type": "fill_name", "name": name}
        if action_type == "fill_credential":
            name = action.get("name", "")
            source = action.get("source", "")
            value = credentials.get(source, "")
            page.locator(f'[name="{name}"]').first.fill(str(value), timeout=15000)
            return {"type": "fill_credential", "name": name, "source": source, "present": bool(value)}
        if action_type == "click_alt":
            alt = action.get("alt", "")
            page.get_by_alt_text(alt).first.click(timeout=15000)
            return {"type": "click_alt", "alt": alt}
        if action_type == "screenshot":
            path = output_dir / action.get("path", "page.png")
            page.screenshot(path=str(path), full_page=True)
            return {"type": "screenshot", "path": str(path)}
        if action_type == "note":
            return {"type": "note", "message": action.get("message", "")}
        if action_type == "wait_for_timeout":
            ms = int(action.get("ms", 1000))
            page.wait_for_timeout(ms)
            return {"type": "wait_for_timeout", "ms": ms}
        if action_type == "download_url_from_frame_expression":
            target_frame = self.find_frame(page, action)
            if target_frame is None:
                raise RuntimeError(f"frame not found for action: {action}")
            relative_url = target_frame.evaluate(action.get("expression", ""))
            if not relative_url:
                raise RuntimeError("download url expression returned empty value")
            download_url = urljoin(target_frame.url, str(relative_url))
            cookies = page.context.cookies()
            cookie_header = "; ".join(f"{cookie['name']}={cookie['value']}" for cookie in cookies)
            request = Request(
                download_url,
                headers={
                    "Cookie": cookie_header,
                    "Referer": target_frame.url,
                    "User-Agent": page.evaluate("() => navigator.userAgent"),
                },
            )
            with urlopen(request, timeout=int(action.get("timeout_seconds", 30))) as response:
                payload = response.read()
                default_name = self.infer_download_name(response.headers, "download.bin")
                path = self.resolve_output_path(output_dir, action.get("path"), default_name)
                lowered = self.decode_payload_text(payload[:512]).lower()
                if path.suffix.lower() == ".xls" and "<script" in lowered and "alert(" in lowered:
                    no_data_path = self.no_data_path(path)
                    no_data_path.write_bytes(payload)
                    return {
                        "type": "download_url_from_frame_expression",
                        "url": download_url,
                        "path": str(no_data_path),
                        "no_data_message": self.decode_payload_text(payload),
                    }
                path.write_bytes(payload)
            return {"type": "download_url_from_frame_expression", "url": download_url, "path": str(path)}
        if action_type == "download_post_from_page_expression":
            payload = page.evaluate(action.get("expression", ""))
            if not isinstance(payload, dict):
                raise RuntimeError("download post expression must return an object")
            download_url = urljoin(page.url, str(payload.get("url", "")))
            form_payload = payload.get("form", {})
            if not download_url or not isinstance(form_payload, dict):
                raise RuntimeError("download post expression returned invalid url/form payload")
            encoded = urlencode({key: "" if value is None else str(value) for key, value in form_payload.items()}).encode("utf-8")
            cookies = page.context.cookies()
            cookie_header = "; ".join(f"{cookie['name']}={cookie['value']}" for cookie in cookies)
            request = Request(
                download_url,
                data=encoded,
                headers={
                    "Cookie": cookie_header,
                    "Referer": page.url,
                    "User-Agent": page.evaluate("() => navigator.userAgent"),
                    "Content-Type": "application/x-www-form-urlencoded",
                },
            )
            with urlopen(request, timeout=int(action.get("timeout_seconds", 30))) as response:
                response_payload = response.read()
                default_name = self.infer_download_name(response.headers, "download.bin")
                path = self.resolve_output_path(output_dir, action.get("path"), default_name)
                if self.is_html_interstitial(response_payload):
                    interstitial = self.decode_payload_text(response_payload)
                    msg = self.extract_match(r'var msg\s*=\s*"([^"]*)";', interstitial)
                    if msg:
                        no_data_path = self.no_data_path(path)
                        no_data_path.write_bytes(response_payload)
                        return {
                            "type": "download_post_from_page_expression",
                            "url": download_url,
                            "path": str(no_data_path),
                            "no_data_message": msg,
                        }
                    follow_url = urljoin(
                        page.url,
                        self.extract_match(r'url\s*=\s*"([^"]*fileDown[^"]*)";', interstitial, "/Homeplus/filedown/fileDown.do"),
                    )
                    follow_form = {
                        "deleteFile": self.extract_match(r'#deleteFile"\)\.val\("([^"]*)"\)', interstitial, "true") or "true",
                        "fileUrl": self.extract_match(r'#fileUrl"\)\.val\("([^"]*)"\)', interstitial, ""),
                        "fileName": self.extract_match(r'#fileName"\)\.val\("([^"]*)"\)', interstitial, ""),
                    }
                    follow_request = Request(
                        follow_url,
                        data=urlencode(follow_form).encode("utf-8"),
                        headers={
                            "Cookie": cookie_header,
                            "Referer": download_url,
                            "User-Agent": page.evaluate("() => navigator.userAgent"),
                            "Content-Type": "application/x-www-form-urlencoded",
                        },
                    )
                    with urlopen(follow_request, timeout=int(action.get("timeout_seconds", 30))) as follow_response:
                        response_payload = follow_response.read()
                        default_name = self.infer_download_name(follow_response.headers, follow_form.get("fileName") or default_name)
                path.write_bytes(response_payload)
            return {"type": "download_post_from_page_expression", "url": download_url, "path": str(path)}
        if action_type == "assert_frame_url_contains":
            url_contains = action.get("url_contains", "")
            target_frame = self.find_frame(page, {"frame_url_contains": url_contains})
            if target_frame is None:
                raise RuntimeError(f"frame url not found: {url_contains}")
            return {"type": "assert_frame_url_contains", "url_contains": url_contains}
        raise RuntimeError(f"unsupported browser action: {action_type}")

    def run_action_with_retry(self, page, output_dir: Path, action: dict[str, Any], credentials: dict[str, Any]) -> dict[str, Any]:
        action_type = action.get("type")
        default_retries = 2 if action_type in {"click_text", "click_text_in_frame", "click_selector", "click_role", "assert_frame_url_contains"} else 1
        retries = int(action.get("retries", default_retries))
        retry_wait_ms = int(action.get("retry_wait_ms", 1200))
        last_exc: Exception | None = None
        for variant_index, variant in enumerate(self.build_action_variants(action), start=1):
            for attempt in range(1, retries + 2):
                try:
                    result = self.perform_action(page, output_dir, variant, credentials)
                    result["attempt"] = attempt
                    if variant_index > 1:
                        result["fallback_variant"] = variant_index
                    return result
                except Exception as exc:
                    last_exc = exc
                    try:
                        page.keyboard.press("Escape")
                    except Exception:
                        pass
                    page.wait_for_timeout(retry_wait_ms)
        assert last_exc is not None
        raise last_exc

    def capture_failure_diagnostics(
        self,
        page,
        output_dir: Path,
        exc: Exception,
        current_action: dict[str, Any] | None,
        executed: list[dict[str, Any]],
    ) -> dict[str, Any]:
        screenshot_path = output_dir / "failure.png"
        try:
            page.screenshot(path=str(screenshot_path), full_page=True)
        except Exception:
            pass
        try:
            summary = self.dump_page_summary(page, output_dir, "failure_page_summary.json")
        except Exception:
            summary = {"url": "", "title": ""}
        error_payload = {
            "error": type(exc).__name__,
            "message": str(exc),
            "current_url": summary.get("url", ""),
            "current_title": summary.get("title", ""),
            "current_action": current_action or {},
            "executed_actions_count": len(executed),
            "executed_actions_tail": executed[-10:],
            "failure_screenshot": str(screenshot_path),
        }
        (output_dir / "browser_error.json").write_text(
            json.dumps(error_payload, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
        return error_payload

    def classify_browser_failure(self, job: Job, error_payload: dict[str, Any]) -> tuple[str, str]:
        message = f"{error_payload.get('message', '')} {error_payload.get('current_url', '')}".lower()
        notes = " ".join(job.notes).lower()
        if "ie" in notes or "activex" in notes or "webkit" in notes or "호환모드" in notes:
            return "environment_blocked", "use_required_browser_environment"
        if "session" in message or "expired" in message or "만료" in message:
            return "session_expired", "relogin_and_rerun_immediately"
        if job.requires_verification or job.auth_type_meaning in {"sms_verification_required", "email_verification_required"}:
            return "auth_required", "request_verification_and_resume"
        if "timeout" in message or "frame not found" in message or "locator" in message:
            return "selector_fix_needed", "run_discovery_and_adjust_playbook"
        return "browser_failed", "inspect_browser_diagnostics"

    def classify_post_action_page_state(self, job: Job, summary: dict[str, Any]) -> tuple[str, str, str] | None:
        flags = summary.get("flags", {}) or {}
        texts = " ".join(item.get("text", "") for item in summary.get("texts", []))
        inputs = " ".join(
            " ".join(
                [
                    str(item.get("id", "")),
                    str(item.get("name", "")),
                    str(item.get("placeholder", "")),
                    str(item.get("value", "")),
                ]
            )
            for item in summary.get("inputs", [])
        )
        blob = f"{summary.get('url', '')} {summary.get('title', '')} {texts} {inputs}".lower()
        if flags.get("has_recaptcha"):
            return "captcha_required", "solve_captcha_or_switch_browser", "captcha is present on login page"
        login_markers = (
            "login",
            "로그인",
            "partner portal",
            "partner login",
            "쇼핑몰관리자 로그인",
            "서비스 로그인",
        )
        is_login_page = bool(flags.get("has_password_input") and flags.get("has_login_button")) and any(
            marker in blob for marker in login_markers
        )
        if is_login_page:
            if job.requires_verification or job.auth_type_meaning in {"sms_verification_required", "email_verification_required"}:
                return "auth_required", "request_verification_and_resume", "authentication step is still required"
            return "login_required", "recheck_credentials_and_login_flow", "page is still on login form after browser actions"
        return None

    def detect_output_state(self, output_dir: Path) -> tuple[bool, bool]:
        allowed = {".xlsx", ".xlsm", ".xls", ".csv"}
        no_data_downloaded = False
        for path in output_dir.rglob("*"):
            if not path.is_file():
                continue
            if path.name.endswith(".no_data.html"):
                no_data_downloaded = True
                continue
            if path.name.endswith("_analysis.json"):
                return True, no_data_downloaded
            if path.suffix.lower() in allowed:
                try:
                    payload = path.read_bytes()[:4096]
                    lowered = self.decode_payload_text(payload).lower()
                    if path.suffix.lower() == ".xls" and "<script" in lowered and "alert(" in lowered:
                        no_data_downloaded = True
                        continue
                    if path.suffix.lower() == ".csv" and "<script" in lowered and "downloadfrm" in lowered:
                        no_data_downloaded = True
                        continue
                    return True, no_data_downloaded
                except Exception:
                    continue
        return False, no_data_downloaded

    def run_actions(self, page, output_dir: Path, job: Job) -> list[dict[str, Any]]:
        actions = job.playbook.browser_actions if job.playbook else []
        credentials = self.channel_credentials.get(job.vendor_name)
        template_context = self.build_template_context(job)
        executed: list[dict[str, Any]] = []
        for action in actions:
            action = self.render_templates(action, template_context)
            if action.get("type") == "goto" and not action.get("url"):
                action = {**action, "url": job.login_url}
            executed.append(self.run_action_with_retry(page, output_dir, action, credentials))
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
        reuse_session_state = bool(job.playbook.metadata.get("reuse_session_state", True)) if job.playbook else True

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
            category = "planned"
            next_action = "run_without_dry_run"
            data_ready = False
        else:
            try:
                from playwright.sync_api import sync_playwright  # type: ignore
            except Exception:
                status = "missing_dependency"
                detail = "playwright not installed"
                category = "environment_blocked"
                next_action = "install_playwright"
                data_ready = False
            else:
                browser = None
                context = None
                page = None
                executed_actions: list[dict[str, Any]] = []
                try:
                    with sync_playwright() as playwright:
                        browser = playwright.chromium.launch(headless=True)
                        context = browser.new_context(
                            accept_downloads=True,
                            storage_state=str(session_state_path) if reuse_session_state and session_state_path.exists() else None
                        )
                        page = context.new_page()
                        executed_actions = self.run_actions(page, output_dir, job)
                        if not executed_actions:
                            page.goto(job.login_url, wait_until="domcontentloaded", timeout=30000)
                            executed_actions = [{"type": "goto", "url": job.login_url}]
                        (output_dir / "last_url.txt").write_text(page.url + "\n", encoding="utf-8")
                        summary = self.dump_page_summary(page, output_dir)
                        (output_dir / "browser_actions_executed.json").write_text(
                            json.dumps(executed_actions, ensure_ascii=False, indent=2) + "\n",
                            encoding="utf-8",
                        )
                        if reuse_session_state:
                            context.storage_state(path=str(session_state_path))
                        context.close()
                        browser.close()
                    data_ready, no_data_downloaded = self.detect_output_state(output_dir)
                    page_state = self.classify_post_action_page_state(job, summary) if not data_ready else None
                    if data_ready:
                        status = "executed"
                        detail = "browser collector executed"
                        category = "collected"
                        next_action = "analyze_and_merge"
                    elif no_data_downloaded:
                        status = "executed"
                        detail = "browser collector executed (no data for selected date range)"
                        category = "no_data"
                        next_action = "choose_alternate_date_range_or_accept_zero"
                    elif page_state:
                        category, next_action, detail = page_state
                        status = "blocked" if category in {"auth_required", "captcha_required"} else "failed"
                    else:
                        status = "scaffolded"
                        detail = "playwright session initialized"
                        category = "session_ready"
                        next_action = "reuse_session_for_download"
                except Exception as exc:
                    try:
                        current_action = None
                        if job.playbook and len(executed_actions) < len(job.playbook.browser_actions):
                            current_action = job.playbook.browser_actions[len(executed_actions)]
                        error_payload = self.capture_failure_diagnostics(
                            page=page,
                            output_dir=output_dir,
                            exc=exc,
                            current_action=current_action,
                            executed=executed_actions,
                        )
                    except Exception:
                        error_payload = {"error": type(exc).__name__, "message": str(exc)}
                    try:
                        if context is not None:
                            context.close()
                    except Exception:
                        pass
                    try:
                        if browser is not None:
                            browser.close()
                    except Exception:
                        pass
                    status = "failed"
                    detail = f"browser collector failed: {type(exc).__name__}"
                    category, next_action = self.classify_browser_failure(job, error_payload)
                    data_ready = False

        return JobResult(
            vendor_name=job.vendor_name,
            strategy=job.strategy,
            status=status,
            output_dir=str(output_dir),
            detail=detail,
            category=category,
            next_action=next_action,
            data_ready=data_ready,
            metadata={
                "run_mode": job.run_mode,
                "session_state_path": str(session_state_path),
                "reuse_session_state": reuse_session_state,
                "has_video": job.has_video,
            },
        )
