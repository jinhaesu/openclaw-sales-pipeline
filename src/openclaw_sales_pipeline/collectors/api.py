from __future__ import annotations

import json
from dataclasses import asdict
from pathlib import Path

from ..api_clients import build_api_client
from ..models import ApiRequestSpec, Job, JobResult
from .base import BaseCollector


class ApiCollector(BaseCollector):
    def build_request_specs(self, job: Job) -> list[ApiRequestSpec]:
        playbook = job.playbook
        provider = playbook.api_provider if playbook else "unknown"
        credential_key = playbook.credential_key if playbook else None
        datasets = playbook.preferred_dataset if playbook and playbook.preferred_dataset else ["sales_summary"]
        return [
            ApiRequestSpec(
                provider=provider,
                credential_key=credential_key,
                dataset=dataset,
                method="GET",
                url=self.default_url(provider, dataset),
                params={"business_date": job.business_date},
            )
            for dataset in datasets
        ]

    def default_url(self, provider: str | None, dataset: str) -> str:
        provider = provider or "unknown"
        if provider == "naver_commerce":
            return f"https://apicenter.commerce.naver.com/external/{dataset}"
        if provider == "cafe24_admin":
            return f"https://{{mall_id}}.cafe24api.com/api/v2/admin/{dataset}"
        if provider == "coupang_open_api":
            return f"https://api-gateway.coupang.com/v2/providers/openapi/apis/api/v4/{dataset}"
        if provider == "elevenst_open_api":
            return f"https://openapi.11st.co.kr/openapi/v1/{dataset}"
        if provider == "esm_trading_api":
            return f"https://sa2.esmplus.com/{dataset}"
        return f"https://example.invalid/{dataset}"

    def collect(self, job: Job, dry_run: bool) -> JobResult:
        output_dir = self.ensure_output_dir(job)
        playbook = job.playbook
        credential_key = playbook.credential_key if playbook else None
        has_credentials = self.secrets.has(credential_key)
        request_specs = self.build_request_specs(job)

        payload = {
            "vendor_name": job.vendor_name,
            "strategy": job.strategy,
            "provider": playbook.api_provider if playbook else None,
            "credential_key": credential_key,
            "has_credentials": has_credentials,
            "preferred_dataset": playbook.preferred_dataset if playbook else [],
            "business_date": job.business_date,
            "notes": job.notes,
            "request_specs": [asdict(spec) for spec in request_specs],
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
            client = build_api_client(playbook.api_provider if playbook else None, self.secrets.get(credential_key))
            if client is None:
                detail = "no api client registered"
                status = "unsupported_provider"
            else:
                dataset_results = {}
                try:
                    for spec in request_specs:
                        dataset_results[spec.dataset] = client.fetch_dataset(spec.dataset, job.business_date)
                        (Path(output_dir) / f"api_{spec.dataset}.json").write_text(
                            json.dumps(dataset_results[spec.dataset], ensure_ascii=False, indent=2) + "\n",
                            encoding="utf-8",
                        )
                    (Path(output_dir) / "api_results_summary.json").write_text(
                        json.dumps(dataset_results, ensure_ascii=False, indent=2) + "\n",
                        encoding="utf-8",
                    )
                    detail = "api collector executed"
                    status = "executed"
                except Exception as exc:
                    error_payload = {"error": type(exc).__name__, "message": str(exc)}
                    (Path(output_dir) / "api_error.json").write_text(
                        json.dumps(error_payload, ensure_ascii=False, indent=2) + "\n",
                        encoding="utf-8",
                    )
                    detail = f"api collector failed: {type(exc).__name__}"
                    status = "failed"

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
                "request_count": len(request_specs),
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
