from __future__ import annotations

import json
from pathlib import Path
from typing import Any


class ChannelCredentialStore:
    def __init__(self, path: Path) -> None:
        self.path = path
        self._data: dict[str, dict[str, Any]] = {}
        if path.exists():
            with path.open("r", encoding="utf-8") as handle:
                raw = json.load(handle)
            if isinstance(raw, dict):
                self._data = {str(k): v for k, v in raw.items() if isinstance(v, dict)}

    def get(self, vendor_name: str) -> dict[str, Any]:
        value = self._data.get(vendor_name, {})
        if isinstance(value, dict):
            return value
        return {}
