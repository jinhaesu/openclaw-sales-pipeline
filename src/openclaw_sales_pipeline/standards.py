from __future__ import annotations

from copy import deepcopy
from typing import Any

from .operations import build_channel_operating_model


STANDARDS_VERSION = "2026-04-14"
CHANNEL_OUTPUT_CONTRACT_ID = "channel_sales_output_v1"
EXCEL_POSTPROCESS_RULESET_ID = "excel_download_postprocess_v1"
PRODUCT_ANALYSIS_MASTER_SCHEMA_ID = "product_sales_master_schema_v1"


STANDARD_OUTPUT_SECTIONS = [
    "metadata",
    "channel_summary",
    "product_summary",
    "normalized_records",
    "applied_postprocess_rules",
    "quality",
]


DEFAULT_POSTPROCESS_RULES: dict[str, Any] = {
    "drop_empty_rows": True,
    "drop_footer_keywords": [
        "합계",
        "총계",
        "소계",
        "누계",
        "총 매출",
        "총판매",
        "배송비",
    ],
    "exclude_status_keywords": [],
    "include_status_keywords": [],
    "exclude_product_keywords": [],
    "normalize_whitespace": True,
    "strip_bracket_suffixes": False,
    "product_name_aliases": {},
    "amount_priority_columns": [
        "net_sales",
        "sales",
        "gross_sales",
        "supply_amount",
        "delivery_amount",
    ],
    "qty_priority_columns": [
        "qty",
        "quantity",
        "sales_qty",
        "order_qty",
    ],
    "date_priority_columns": [
        "business_date",
        "payment_date",
        "delivery_date",
        "receipt_date",
        "order_date",
    ],
    "refund_status_keywords": [
        "취소",
        "반품",
        "환불",
    ],
    "store_rollup_keys": [
        "vendor_name",
        "business_date",
        "normalized_product_name",
    ],
}


NORMALIZED_RECORD_FIELDS = [
    {"name": "vendor_name", "type": "string", "required": True, "description": "채널명"},
    {"name": "channel_group", "type": "string", "required": False, "description": "채널 그룹"},
    {"name": "manager", "type": "string", "required": False, "description": "담당자"},
    {"name": "business_date", "type": "string", "required": False, "description": "집계 일자 (YYYY-MM-DD)"},
    {"name": "business_month", "type": "string", "required": False, "description": "집계 월 (YYYY-MM)"},
    {"name": "status", "type": "string", "required": False, "description": "주문/정산 상태"},
    {"name": "sku", "type": "string", "required": False, "description": "SKU 또는 채널 품목 코드"},
    {"name": "product_name", "type": "string", "required": True, "description": "원본 품목명"},
    {
        "name": "normalized_product_name",
        "type": "string",
        "required": True,
        "description": "후처리 규칙이 적용된 품목 기준명",
    },
    {"name": "qty", "type": "number", "required": True, "description": "판매량"},
    {"name": "sales", "type": "number", "required": True, "description": "대표 매출 금액"},
    {"name": "gross_sales", "type": "number", "required": False, "description": "주문/총매출"},
    {"name": "net_sales", "type": "number", "required": False, "description": "순매출"},
    {"name": "supply_amount", "type": "number", "required": False, "description": "공급가 또는 입고금액"},
    {"name": "delivery_amount", "type": "number", "required": False, "description": "납품금액"},
    {"name": "refund_flag", "type": "boolean", "required": False, "description": "반품/취소 여부"},
    {"name": "revenue_basis", "type": "string", "required": False, "description": "채널 최종 매출 기준"},
    {"name": "date_basis", "type": "string", "required": False, "description": "집계 기준일 정의"},
    {"name": "source_file", "type": "string", "required": False, "description": "원본 파일 경로"},
    {"name": "raw_file_name", "type": "string", "required": False, "description": "원본 파일명"},
    {"name": "raw", "type": "object", "required": False, "description": "원본 행 데이터"},
]


CHANNEL_SUMMARY_FIELDS = [
    {"name": "vendor_name", "type": "string"},
    {"name": "business_date", "type": "string"},
    {"name": "business_month", "type": "string"},
    {"name": "revenue_basis", "type": "string"},
    {"name": "date_basis", "type": "string"},
    {"name": "row_count", "type": "integer"},
    {"name": "product_count", "type": "integer"},
    {"name": "total_qty", "type": "number"},
    {"name": "total_sales", "type": "number"},
    {"name": "total_gross_sales", "type": "number"},
    {"name": "total_net_sales", "type": "number"},
    {"name": "total_supply_amount", "type": "number"},
    {"name": "total_delivery_amount", "type": "number"},
    {"name": "source_file", "type": "string"},
]


PRODUCT_SUMMARY_FIELDS = [
    {"name": "sku", "type": "string"},
    {"name": "product_name", "type": "string"},
    {"name": "normalized_product_name", "type": "string"},
    {"name": "qty", "type": "number"},
    {"name": "sales", "type": "number"},
    {"name": "gross_sales", "type": "number"},
    {"name": "net_sales", "type": "number"},
    {"name": "supply_amount", "type": "number"},
    {"name": "delivery_amount", "type": "number"},
]


REPORT_WORKBOOK_SHEETS = [
    "Overview",
    "SourceFiles",
    "DailyChannelSales",
    "MonthlyChannelSales",
    "ProductSales",
    "ProductQty",
    "ChannelProductSales",
    "DailyProductSales",
]


def merge_postprocess_rules(
    profile: dict[str, Any] | None = None,
    overrides: dict[str, Any] | None = None,
) -> dict[str, Any]:
    profile = profile or {}
    merged = deepcopy(DEFAULT_POSTPROCESS_RULES)
    candidates = []
    if isinstance(profile.get("postprocess_rules"), dict):
        candidates.append(profile["postprocess_rules"])
    if isinstance(overrides, dict):
        candidates.append(overrides)
    for candidate in candidates:
        for key, value in candidate.items():
            if isinstance(value, list):
                existing = list(merged.get(key, []))
                for item in value:
                    if item not in existing:
                        existing.append(item)
                merged[key] = existing
            elif isinstance(value, dict):
                nested = dict(merged.get(key, {}))
                nested.update(value)
                merged[key] = nested
            else:
                merged[key] = value
    return merged


def build_channel_output_contract() -> dict[str, Any]:
    return {
        "id": CHANNEL_OUTPUT_CONTRACT_ID,
        "version": STANDARDS_VERSION,
        "output_type": "channel_sales_analysis",
        "sections": list(STANDARD_OUTPUT_SECTIONS),
        "channel_summary_fields": list(CHANNEL_SUMMARY_FIELDS),
        "product_summary_fields": list(PRODUCT_SUMMARY_FIELDS),
        "normalized_record_fields": list(NORMALIZED_RECORD_FIELDS),
        "report_workbook_sheets": list(REPORT_WORKBOOK_SHEETS),
    }


def build_excel_postprocess_ruleset() -> dict[str, Any]:
    return {
        "id": EXCEL_POSTPROCESS_RULESET_ID,
        "version": STANDARDS_VERSION,
        "default_rules": deepcopy(DEFAULT_POSTPROCESS_RULES),
        "notes": [
            "채널별 다운로드 파일은 기본적으로 빈 행, 합계/총계 행, 불필요한 품목 문자열을 제거한다.",
            "채널별 플레이북은 postprocess_rules로 상태 포함/제외 규칙을 추가할 수 있다.",
            "품목 기준명은 normalized_product_name 필드로 저장해 품목별 집계를 안정화한다.",
            "취소/반품/환불 여부는 refund_flag 또는 status 기반 키워드 규칙으로 표준화한다.",
            "매출 컬럼은 net_sales > sales > gross_sales > supply_amount > delivery_amount 우선순위로 해석한다.",
            "점포/센터/거래처가 여러 개인 다운로드는 store_rollup_keys 기준으로 합산한다.",
        ],
    }


def build_product_analysis_master_schema() -> dict[str, Any]:
    return {
        "id": PRODUCT_ANALYSIS_MASTER_SCHEMA_ID,
        "version": STANDARDS_VERSION,
        "normalized_record_fields": list(NORMALIZED_RECORD_FIELDS),
        "channel_summary_fields": list(CHANNEL_SUMMARY_FIELDS),
        "product_summary_fields": list(PRODUCT_SUMMARY_FIELDS),
        "derived_metrics": [
            {"name": "total_sales", "formula": "sum(records.sales)"},
            {"name": "total_qty", "formula": "sum(records.qty)"},
            {"name": "total_gross_sales", "formula": "sum(records.gross_sales)"},
            {"name": "total_net_sales", "formula": "sum(records.net_sales)"},
            {"name": "total_supply_amount", "formula": "sum(records.supply_amount)"},
            {"name": "total_delivery_amount", "formula": "sum(records.delivery_amount)"},
            {"name": "product_count", "formula": "count_distinct(records.normalized_product_name)"},
            {"name": "channel_count", "formula": "count_distinct(records.vendor_name)"},
        ],
    }


def build_standards_bundle() -> dict[str, Any]:
    return {
        "version": STANDARDS_VERSION,
        "channel_output_contract": build_channel_output_contract(),
        "excel_postprocess_ruleset": build_excel_postprocess_ruleset(),
        "product_analysis_master_schema": build_product_analysis_master_schema(),
        "channel_operating_model": build_channel_operating_model(),
    }
