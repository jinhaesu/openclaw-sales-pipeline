from __future__ import annotations

from abc import ABC, abstractmethod
from pathlib import Path

from ..models import Job, JobResult, RuntimeConfig
from ..secrets import SecretStore


class BaseCollector(ABC):
    def __init__(self, cfg: RuntimeConfig, secrets: SecretStore) -> None:
        self.cfg = cfg
        self.secrets = secrets

    @abstractmethod
    def collect(self, job: Job, dry_run: bool) -> JobResult:
        raise NotImplementedError

    def ensure_output_dir(self, job: Job) -> Path:
        path = Path(job.output_dir)
        path.mkdir(parents=True, exist_ok=True)
        return path
