from __future__ import annotations

import csv
import json
from collections import defaultdict
from datetime import date, datetime
from pathlib import Path
from typing import Any

from openpyxl import load_workbook


HEADER_CANDIDATES = {
    "product": ["상품명", "품목명", "상품", "품목", "제품명", "옵션명", "상품명(옵션포함)"],
    "qty": ["수량", "판매수량", "주문수량", "구매수량"],
    "sales": ["매출", "매출액", "판매금액", "결제금액", "주문금액", "실결제금액", "상품구매금액"],
    "date": ["일자", "날짜", "주문일", "판매일", "결제일", "정산일", "출고일", "등록일", "작성일"],
    "status": ["상태", "주문상태", "배송상태", "처리상태", "진행상태", "구분"],
}


def analyze_sales_file(
    path: Path,
    profile: dict[str, Any] | None = None,
    context: dict[str, Any] | None = None,
) -> dict[str, Any]:
    profile = profile or {}
    context = context or {}
    rows = load_rows(path)
    header_index, headers = detect_header(rows)
    records = rows_to_dicts(rows[header_index + 1 :], headers)
    normalized = normalize_records(records, profile=profile, context=context, source_path=path)
    summary = summarize_by_product(normalized)
    return {
        "file": str(path),
        "vendor_name": context.get("vendor_name"),
        "business_date": context.get("business_date"),
        "row_count": len(normalized),
        "product_count": len(summary),
        "totals": summarize_totals(normalized),
        "items": summary,
        "records": normalized,
    }


def load_rows(path: Path) -> list[list[Any]]:
    suffix = path.suffix.lower()
    if suffix in {".csv", ".txt"}:
        with path.open("r", encoding="utf-8-sig", newline="") as handle:
            return [row for row in csv.reader(handle)]
    if suffix in {".xlsx", ".xlsm"}:
        workbook = load_workbook(path, data_only=True, read_only=True)
        sheet = workbook[workbook.sheetnames[0]]
        return [list(row) for row in sheet.iter_rows(values_only=True)]
    raise ValueError(f"Unsupported file type: {path.suffix}")


def detect_header(rows: list[list[Any]]) -> tuple[int, list[str]]:
    for idx, row in enumerate(rows[:20]):
        text_row = [str(cell).strip() if cell is not None else "" for cell in row]
        if score_header(text_row) >= 2:
            return idx, text_row
    fallback = [str(cell).strip() if cell is not None else "" for cell in rows[0]]
    return 0, fallback


def score_header(row: list[str]) -> int:
    score = 0
    for candidates in HEADER_CANDIDATES.values():
        if any(cell in candidates for cell in row):
            score += 1
    return score


def rows_to_dicts(rows: list[list[Any]], headers: list[str]) -> list[dict[str, Any]]:
    result = []
    width = len(headers)
    for row in rows:
        padded = list(row[:width]) + [None] * max(0, width - len(row))
        result.append({headers[i] or f"col_{i}": padded[i] for i in range(width)})
    return result


def normalize_records(
    records: list[dict[str, Any]],
    profile: dict[str, Any] | None = None,
    context: dict[str, Any] | None = None,
    source_path: Path | None = None,
) -> list[dict[str, Any]]:
    profile = profile or {}
    context = context or {}
    header_candidates = build_header_candidates(profile)
    exclude_status_keywords = [str(item).strip().lower() for item in profile.get("exclude_status_keywords", [])]
    exclude_product_keywords = [str(item).strip().lower() for item in profile.get("exclude_product_keywords", [])]
    normalized = []
    for record in records:
        product = first_value(record, header_candidates["product"])
        qty = to_number(first_value(record, header_candidates["qty"]))
        sales = to_number(first_value(record, header_candidates["sales"]))
        status = normalize_text(first_value(record, header_candidates["status"]))
        business_date = to_iso_date(first_value(record, header_candidates["date"])) or context.get("business_date")
        if not product and qty == 0 and sales == 0:
            continue
        product_name = str(product).strip() if product is not None else "(unknown)"
        if exclude_status_keywords and any(keyword in status.lower() for keyword in exclude_status_keywords if keyword):
            continue
        if exclude_product_keywords and any(keyword in product_name.lower() for keyword in exclude_product_keywords if keyword):
            continue
        normalized.append(
            {
                "vendor_name": context.get("vendor_name", ""),
                "channel_group": context.get("channel_group", ""),
                "manager": context.get("manager", ""),
                "business_date": business_date or "",
                "business_month": business_date[:7] if business_date else "",
                "status": status,
                "product_name": product_name,
                "qty": qty,
                "sales": sales,
                "source_file": str(source_path) if source_path else "",
                "raw": record,
            }
        )
    return normalized


def first_value(record: dict[str, Any], candidates: list[str]) -> Any:
    for candidate in candidates:
        if candidate in record and record[candidate] not in (None, ""):
            return record[candidate]
    return None


def to_number(value: Any) -> float:
    if value in (None, ""):
        return 0.0
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip().replace(",", "")
    if not text:
        return 0.0
    try:
        return float(text)
    except ValueError:
        return 0.0


def build_header_candidates(profile: dict[str, Any]) -> dict[str, list[str]]:
    candidates = {key: list(values) for key, values in HEADER_CANDIDATES.items()}
    overrides = {
        "product": profile.get("product_header_candidates", []),
        "qty": profile.get("qty_header_candidates", []),
        "sales": profile.get("sales_header_candidates", []),
        "date": profile.get("date_header_candidates", []),
        "status": profile.get("status_header_candidates", []),
    }
    for key, items in overrides.items():
        for item in items:
            if item not in candidates[key]:
                candidates[key].insert(0, item)
    return candidates


def normalize_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def to_iso_date(value: Any) -> str:
    if value in (None, ""):
        return ""
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    text = normalize_text(value)
    if not text:
        return ""
    text = text.replace(".", "-").replace("/", "-")
    for fmt in ("%Y-%m-%d", "%Y-%m", "%Y%m%d", "%Y%m", "%m-%d-%Y", "%m-%d-%y"):
        try:
            parsed = datetime.strptime(text, fmt)
            if fmt in {"%Y-%m", "%Y%m"}:
                return parsed.strftime("%Y-%m-01")
            return parsed.date().isoformat()
        except ValueError:
            continue
    return ""


def summarize_by_product(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    bucket: dict[str, dict[str, float]] = defaultdict(lambda: {"qty": 0.0, "sales": 0.0})
    for record in records:
        key = record["product_name"]
        bucket[key]["qty"] += record["qty"]
        bucket[key]["sales"] += record["sales"]
    items = [
        {"product_name": key, "qty": round(value["qty"], 2), "sales": round(value["sales"], 2)}
        for key, value in bucket.items()
    ]
    items.sort(key=lambda item: (-item["sales"], item["product_name"]))
    return items


def summarize_totals(records: list[dict[str, Any]]) -> dict[str, float]:
    return {
        "qty": round(sum(record["qty"] for record in records), 2),
        "sales": round(sum(record["sales"] for record in records), 2),
    }


def write_analysis(output_path: Path, analysis: dict[str, Any]) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(analysis, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
