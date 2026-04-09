from __future__ import annotations

from copy import deepcopy
from typing import Any


STANDARDS_VERSION = "2026-04-09"
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
}


NORMALIZED_RECORD_FIELDS = [
    {"name": "vendor_name", "type": "string", "required": True, "description": "채널명"},
    {"name": "channel_group", "type": "string", "required": False, "description": "채널 그룹"},
    {"name": "manager", "type": "string", "required": False, "description": "담당자"},
    {"name": "business_date", "type": "string", "required": False, "description": "집계 일자 (YYYY-MM-DD)"},
    {"name": "business_month", "type": "string", "required": False, "description": "집계 월 (YYYY-MM)"},
    {"name": "status", "type": "string", "required": False, "description": "주문/정산 상태"},
    {"name": "product_name", "type": "string", "required": True, "description": "원본 품목명"},
    {
        "name": "normalized_product_name",
        "type": "string",
        "required": True,
        "description": "후처리 규칙이 적용된 품목 기준명",
    },
    {"name": "qty", "type": "number", "required": True, "description": "판매량"},
    {"name": "sales", "type": "number", "required": True, "description": "매출액"},
    {"name": "source_file", "type": "string", "required": False, "description": "원본 파일 경로"},
    {"name": "raw", "type": "object", "required": False, "description": "원본 행 데이터"},
]


CHANNEL_SUMMARY_FIELDS = [
    {"name": "vendor_name", "type": "string"},
    {"name": "business_date", "type": "string"},
    {"name": "business_month", "type": "string"},
    {"name": "row_count", "type": "integer"},
    {"name": "product_count", "type": "integer"},
    {"name": "total_qty", "type": "number"},
    {"name": "total_sales", "type": "number"},
    {"name": "source_file", "type": "string"},
]


PRODUCT_SUMMARY_FIELDS = [
    {"name": "product_name", "type": "string"},
    {"name": "normalized_product_name", "type": "string"},
    {"name": "qty", "type": "number"},
    {"name": "sales", "type": "number"},
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
    }
