from __future__ import annotations

from ..models import Job, RuntimeConfig
from ..secrets import SecretStore
from .api import (
    ApiCollector,
    Cafe24ApiCollector,
    CoupangApiCollector,
    ElevenstApiCollector,
    EsmApiCollector,
    SmartstoreApiCollector,
)
from .base import BaseCollector
from .browser import BrowserCollector


def get_collector(job: Job, cfg: RuntimeConfig, secrets: SecretStore) -> BaseCollector:
    if job.run_mode == "browser":
        return BrowserCollector(cfg, secrets)

    provider = job.playbook.api_provider if job.playbook else None
    if provider == "naver_commerce":
        return SmartstoreApiCollector(cfg, secrets)
    if provider == "cafe24_admin":
        return Cafe24ApiCollector(cfg, secrets)
    if provider == "coupang_open_api":
        return CoupangApiCollector(cfg, secrets)
    if provider == "elevenst_open_api":
        return ElevenstApiCollector(cfg, secrets)
    if provider == "esm_trading_api":
        return EsmApiCollector(cfg, secrets)
    return ApiCollector(cfg, secrets)
