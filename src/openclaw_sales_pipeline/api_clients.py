from __future__ import annotations

import base64
import hashlib
import hmac
import time
from abc import ABC, abstractmethod
from datetime import datetime, timedelta, timezone
from typing import Any

import bcrypt

from .http import request_json


KST = timezone(timedelta(hours=9))


def business_date_window(business_date: str) -> tuple[str, str]:
    start = datetime.strptime(business_date, "%Y-%m-%d").replace(tzinfo=KST)
    end = start + timedelta(days=1) - timedelta(seconds=1)
    return start.isoformat(timespec="seconds"), end.isoformat(timespec="seconds")


class BaseApiClient(ABC):
    def __init__(self, credentials: dict[str, Any]) -> None:
        self.credentials = credentials

    @abstractmethod
    def fetch_dataset(self, dataset: str, business_date: str) -> dict[str, Any]:
        raise NotImplementedError


class NaverCommerceClient(BaseApiClient):
    token_url = "https://api.commerce.naver.com/external/v1/oauth2/token"
    api_root = "https://api.commerce.naver.com/external"

    def issue_token(self) -> str:
        client_id = self.credentials["client_id"]
        client_secret = self.credentials["client_secret"]
        timestamp = str(int(time.time() * 1000))
        password = f"{client_id}_{timestamp}"
        hashed = bcrypt.hashpw(password.encode("utf-8"), client_secret.encode("utf-8"))
        signature = base64.b64encode(hashed).decode("utf-8")
        response = request_json(
            "POST",
            self.token_url,
            params={
                "client_id": client_id,
                "timestamp": timestamp,
                "grant_type": "client_credentials",
                "client_secret_sign": signature,
                "type": self.credentials.get("type", "SELF"),
                "account_id": self.credentials.get("account_id"),
            },
        )
        body = response.get("body") or {}
        return body.get("access_token", "")

    def fetch_dataset(self, dataset: str, business_date: str) -> dict[str, Any]:
        token = self.issue_token()
        if not token:
            return {"status": "auth_failed", "dataset": dataset}

        start_dt, end_dt = business_date_window(business_date)
        headers = {"Authorization": f"Bearer {token}"}

        if dataset == "orders":
            return request_json(
                "GET",
                f"{self.api_root}/v1/pay-order/seller/product-orders/last-changed-statuses",
                headers=headers,
                params={"lastChangedFrom": start_dt, "lastChangedTo": end_dt},
            )
        if dataset in {"settlements", "sales_summary"}:
            return request_json(
                "GET",
                f"{self.api_root}/v1/pay-settle/settle/daily",
                headers=headers,
                params={"fromDate": business_date, "toDate": business_date},
            )
        return {"status": "unsupported_dataset", "dataset": dataset}


class Cafe24AdminClient(BaseApiClient):
    def access_token(self) -> str:
        token = self.credentials.get("access_token")
        if token:
            return token
        refresh_token = self.credentials.get("refresh_token")
        if not refresh_token:
            return ""
        mall_id = self.credentials["mall_id"]
        response = request_json(
            "POST",
            f"https://{mall_id}.cafe24api.com/api/v2/oauth/token",
            headers={
                "Authorization": "Basic "
                + base64.b64encode(
                    f"{self.credentials['client_id']}:{self.credentials['client_secret']}".encode("utf-8")
                ).decode("utf-8")
            },
            params={"grant_type": "refresh_token", "refresh_token": refresh_token},
        )
        body = response.get("body") or {}
        return body.get("access_token", "")

    def fetch_dataset(self, dataset: str, business_date: str) -> dict[str, Any]:
        token = self.access_token()
        if not token:
            return {"status": "auth_failed", "dataset": dataset}
        mall_id = self.credentials["mall_id"]
        headers = {"Authorization": f"Bearer {token}"}
        if dataset == "orders":
            return request_json(
                "GET",
                f"https://{mall_id}.cafe24api.com/api/v2/admin/orders",
                headers=headers,
                params={"start_date": business_date, "end_date": business_date},
            )
        if dataset == "sales_statistics":
            return request_json(
                "GET",
                f"https://{mall_id}.cafe24api.com/api/v2/admin/dashboard",
                headers=headers,
                params={"shop_no": self.credentials.get("shop_no", 1)},
            )
        return {"status": "unsupported_dataset", "dataset": dataset}


class CoupangOpenApiClient(BaseApiClient):
    host = "https://api-gateway.coupang.com"

    def auth_header(self, method: str, path: str, query: str) -> str:
        access_key = self.credentials["access_key"]
        secret_key = self.credentials["secret_key"]
        signed_date = time.strftime("%y%m%dT%H%M%SZ", time.gmtime())
        message = f"{signed_date}{method.upper()}{path}{query}"
        signature = hmac.new(secret_key.encode("utf-8"), message.encode("utf-8"), hashlib.sha256).hexdigest()
        return (
            f"CEA algorithm=HmacSHA256, access-key={access_key}, "
            f"signed-date={signed_date}, signature={signature}"
        )

    def signed_get(self, path: str, params: dict[str, Any]) -> dict[str, Any]:
        from urllib.parse import urlencode

        query = urlencode({key: value for key, value in params.items() if value is not None})
        headers = {
            "Authorization": self.auth_header("GET", path, query),
            "Content-Type": "application/json;charset=UTF-8",
            "X-Requested-By": self.credentials["vendor_id"],
            "X-EXTENDED-TIMEOUT": "90000",
        }
        return request_json("GET", f"{self.host}{path}", headers=headers, params=params, timeout=90)

    def fetch_dataset(self, dataset: str, business_date: str) -> dict[str, Any]:
        vendor_id = self.credentials["vendor_id"]
        if dataset == "orders":
            path = f"/v2/providers/openapi/apis/api/v5/vendors/{vendor_id}/ordersheets"
            return self.signed_get(
                path,
                {
                    "createdAtFrom": f"{business_date}+09:00",
                    "createdAtTo": f"{business_date}+09:00",
                    "status": "FINAL_DELIVERY",
                    "maxPerPage": 50,
                },
            )
        if dataset == "vendor_sales":
            path = "/v2/providers/openapi/apis/api/v1/revenue-history"
            return self.signed_get(
                path,
                {
                    "vendorId": vendor_id,
                    "recognitionDateFrom": business_date,
                    "recognitionDateTo": business_date,
                    "token": "",
                    "maxPerPage": 50,
                },
            )
        return {"status": "unsupported_dataset", "dataset": dataset}


def build_api_client(provider: str | None, credentials: dict[str, Any]) -> BaseApiClient | None:
    if provider == "naver_commerce":
        return NaverCommerceClient(credentials)
    if provider == "cafe24_admin":
        return Cafe24AdminClient(credentials)
    if provider == "coupang_open_api":
        return CoupangOpenApiClient(credentials)
    return None
