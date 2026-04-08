from __future__ import annotations

import csv
import json
from collections import defaultdict
from pathlib import Path
from typing import Any

from openpyxl import load_workbook


HEADER_CANDIDATES = {
    "product": ["상품명", "품목명", "상품", "품목", "제품명", "옵션명", "상품명(옵션포함)"],
    "qty": ["수량", "판매수량", "주문수량", "구매수량"],
    "sales": ["매출", "매출액", "판매금액", "결제금액", "주문금액", "실결제금액", "상품구매금액"],
}


def analyze_sales_file(path: Path) -> dict[str, Any]:
    rows = load_rows(path)
    header_index, headers = detect_header(rows)
    records = rows_to_dicts(rows[header_index + 1 :], headers)
    normalized = normalize_records(records)
    summary = summarize_by_product(normalized)
    return {
        "file": str(path),
        "row_count": len(normalized),
        "product_count": len(summary),
        "items": summary,
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


def normalize_records(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    normalized = []
    for record in records:
        product = first_value(record, HEADER_CANDIDATES["product"])
        qty = to_number(first_value(record, HEADER_CANDIDATES["qty"]))
        sales = to_number(first_value(record, HEADER_CANDIDATES["sales"]))
        if not product and qty == 0 and sales == 0:
            continue
        normalized.append(
            {
                "product_name": str(product).strip() if product is not None else "(unknown)",
                "qty": qty,
                "sales": sales,
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


def write_analysis(output_path: Path, analysis: dict[str, Any]) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(analysis, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
