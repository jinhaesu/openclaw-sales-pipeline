from __future__ import annotations

import json
from pathlib import Path
from typing import Any


class SecretStore:
    def __init__(self, path: Path) -> None:
        self.path = path
        self._data: dict[str, Any] = {}
        if path.exists():
            with path.open("r", encoding="utf-8") as handle:
                self._data = json.load(handle)

    def has(self, key: str | None) -> bool:
        if not key:
            return False
        return key in self._data and bool(self._data[key])

    def get(self, key: str | None) -> dict[str, Any]:
        if not key:
            return {}
        value = self._data.get(key, {})
        if isinstance(value, dict):
            return value
        return {}
