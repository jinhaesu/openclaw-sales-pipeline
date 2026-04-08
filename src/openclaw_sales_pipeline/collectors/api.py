from __future__ import annotations

import json
from pathlib import Path

from ..models import Job, JobResult
from .base import BaseCollector


class ApiCollector(BaseCollector):
    def collect(self, job: Job, dry_run: bool) -> JobResult:
        output_dir = self.ensure_output_dir(job)
        playbook = job.playbook
        credential_key = playbook.credential_key if playbook else None
        has_credentials = self.secrets.has(credential_key)

        payload = {
            "vendor_name": job.vendor_name,
            "strategy": job.strategy,
            "provider": playbook.api_provider if playbook else None,
            "credential_key": credential_key,
            "has_credentials": has_credentials,
            "preferred_dataset": playbook.preferred_dataset if playbook else [],
            "business_date": job.business_date,
            "notes": job.notes,
        }
        (output_dir / "api_request_plan.json").write_text(
            json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )

        if dry_run:
            detail = "api collector planned"
            status = "planned"
        elif not has_credentials:
            detail = "missing api credentials"
            status = "missing_credentials"
        else:
            detail = "api collector scaffold ready"
            status = "scaffolded"

        return JobResult(
            vendor_name=job.vendor_name,
            strategy=job.strategy,
            status=status,
            output_dir=str(output_dir),
            detail=detail,
            metadata={
                "run_mode": job.run_mode,
                "provider": playbook.api_provider if playbook else None,
                "has_credentials": has_credentials,
            },
        )


class SmartstoreApiCollector(ApiCollector):
    pass


class Cafe24ApiCollector(ApiCollector):
    pass


class CoupangApiCollector(ApiCollector):
    pass


class ElevenstApiCollector(ApiCollector):
    pass


class EsmApiCollector(ApiCollector):
    pass
