from __future__ import annotations

import json
import urllib.error
import urllib.parse
import urllib.request
from typing import Any


def request_json(
    method: str,
    url: str,
    headers: dict[str, str] | None = None,
    params: dict[str, Any] | None = None,
    body: dict[str, Any] | None = None,
    timeout: int = 30,
) -> dict[str, Any]:
    if params:
        encoded = urllib.parse.urlencode(
            {key: value for key, value in params.items() if value is not None},
            doseq=True,
        )
        url = f"{url}?{encoded}" if encoded else url

    payload = None
    request_headers = dict(headers or {})
    if body is not None:
        payload = json.dumps(body).encode("utf-8")
        request_headers.setdefault("Content-Type", "application/json")

    request = urllib.request.Request(url, data=payload, method=method.upper())
    for key, value in request_headers.items():
        request.add_header(key, value)

    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            raw = response.read()
            if not raw:
                return {"status": response.status, "headers": dict(response.headers), "body": None}
            return {
                "status": response.status,
                "headers": dict(response.headers),
                "body": json.loads(raw.decode("utf-8")),
            }
    except urllib.error.HTTPError as exc:
        raw = exc.read().decode("utf-8", errors="replace")
        try:
            body_json = json.loads(raw)
        except Exception:
            body_json = raw
        return {"status": exc.code, "headers": dict(exc.headers), "body": body_json, "error": True}
