from __future__ import annotations

import json
from pathlib import Path
from typing import Any


class SecretStore:
    REQUIRED_FIELDS = {
        "smartstore": ["client_id", "client_secret"],
        "cafe24": ["mall_id", "client_id", "client_secret"],
        "coupang": ["access_key", "secret_key", "vendor_id"],
        "elevenst": ["api_key"],
        "esm": ["api_key", "seller_id"],
        "smtp": ["host", "port", "username", "password", "from_addr"],
    }

    def __init__(self, path: Path) -> None:
        self.path = path
        self._data: dict[str, Any] = {}
        if path.exists():
            with path.open("r", encoding="utf-8") as handle:
                self._data = json.load(handle)

    def has(self, key: str | None) -> bool:
        if not key:
            return False
        if key not in self._data:
            return False
        value = self._data[key]
        if isinstance(value, dict):
            required = self.REQUIRED_FIELDS.get(key, [])
            if required:
                return all(value.get(field) not in ("", None, [], {}) for field in required)
            return any(v not in ("", None, [], {}) for v in value.values())
        return bool(value)

    def get(self, key: str | None) -> dict[str, Any]:
        if not key:
            return {}
        value = self._data.get(key, {})
        if isinstance(value, dict):
            return value
        return {}
