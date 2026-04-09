from __future__ import annotations

import json
import mimetypes
import smtplib
from collections import defaultdict
from datetime import datetime
from email.message import EmailMessage
from pathlib import Path
from typing import Any

from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill

from .excel_analysis import analyze_sales_file
from .models import ChannelRecord, Playbook
from .secrets import SecretStore
from .standards import (
    CHANNEL_OUTPUT_CONTRACT_ID,
    EXCEL_POSTPROCESS_RULESET_ID,
    PRODUCT_ANALYSIS_MASTER_SCHEMA_ID,
    build_standards_bundle,
)


SUPPORTED_SOURCE_SUFFIXES = {".xlsx", ".xlsm", ".csv", ".txt"}


def safe_vendor_name(value: str) -> str:
    return value.replace(" ", "_").replace("/", "_").replace("(", "").replace(")", "")


def build_report_bundle(
    channels: list[ChannelRecord],
    playbooks: dict[str, Playbook],
    secrets: SecretStore,
    input_root: Path | None,
    output_dir: Path,
    date_from: str | None = None,
    date_to: str | None = None,
    channel_filters: list[str] | None = None,
    explicit_files: list[str] | None = None,
    manifest_path: str | None = None,
    label: str | None = None,
    email_to: list[str] | None = None,
    email_cc: list[str] | None = None,
    email_subject: str | None = None,
    send_email: bool = False,
    smtp_profile: str = "smtp",
) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)
    sources = collect_report_sources(
        channels=channels,
        playbooks=playbooks,
        input_root=input_root,
        date_from=date_from,
        date_to=date_to,
        channel_filters=channel_filters or [],
        explicit_files=explicit_files or [],
        manifest_path=manifest_path,
    )
    analyses = analyze_sources(sources, channels, playbooks)
    records = [record for analysis in analyses for record in analysis["records"]]
    aggregates = aggregate_records(records)

    report_label = label or infer_label(analyses, date_from=date_from, date_to=date_to)
    workbook_path = output_dir / f"sales_report_{report_label}.xlsx"
    summary_path = output_dir / f"sales_report_{report_label}.md"
    summary_json_path = output_dir / f"sales_report_{report_label}.json"
    manifest_output_path = output_dir / f"sales_report_{report_label}_manifest.json"

    report = {
        "label": report_label,
        "standards": build_standards_bundle(),
        "source_count": len(sources),
        "analysis_count": len(analyses),
        "record_count": len(records),
        "sources": sources,
        "analyses": analyses,
        "aggregates": aggregates,
        "summary": build_summary(aggregates, analyses),
    }

    export_report_workbook(workbook_path, report)
    summary_markdown = build_summary_markdown(report)
    summary_path.write_text(summary_markdown, encoding="utf-8")
    summary_json_path.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    manifest_output_path.write_text(json.dumps({"sources": sources}, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    draft_path = create_email_draft(
        output_dir=output_dir,
        subject=email_subject or f"[OpenClaw] 매출 리포트 {report_label}",
        to_addrs=email_to or [],
        cc_addrs=email_cc or [],
        summary_markdown=summary_markdown,
        attachments=[workbook_path, summary_path],
        from_addr=secrets.get(smtp_profile).get("from_addr", ""),
    )

    smtp_status = validate_smtp_profile(secrets, smtp_profile)
    sent = False
    send_error = ""
    if send_email:
        sent, send_error = send_email_bundle(
            secrets=secrets,
            smtp_profile=smtp_profile,
            subject=email_subject or f"[OpenClaw] 매출 리포트 {report_label}",
            to_addrs=email_to or [],
            cc_addrs=email_cc or [],
            summary_markdown=summary_markdown,
            attachments=[workbook_path, summary_path],
        )

    return {
        "label": report_label,
        "source_count": len(sources),
        "analysis_count": len(analyses),
        "record_count": len(records),
        "outputs": {
            "workbook": str(workbook_path.resolve()),
            "summary_markdown": str(summary_path.resolve()),
            "summary_json": str(summary_json_path.resolve()),
            "manifest": str(manifest_output_path.resolve()),
            "email_draft": str(draft_path.resolve()),
        },
        "standards": {
            "channel_output_contract_id": CHANNEL_OUTPUT_CONTRACT_ID,
            "excel_postprocess_ruleset_id": EXCEL_POSTPROCESS_RULESET_ID,
            "product_analysis_master_schema_id": PRODUCT_ANALYSIS_MASTER_SCHEMA_ID,
        },
        "smtp_status": smtp_status,
        "sent_email": sent,
        "send_error": send_error,
        "summary": report["summary"],
    }


def collect_report_sources(
    channels: list[ChannelRecord],
    playbooks: dict[str, Playbook],
    input_root: Path | None,
    date_from: str | None,
    date_to: str | None,
    channel_filters: list[str],
    explicit_files: list[str],
    manifest_path: str | None,
) -> list[dict[str, Any]]:
    sources: list[dict[str, Any]] = []
    vendor_lookup = {safe_vendor_name(channel.vendor_name): channel.vendor_name for channel in channels}
    channel_set = set(channel_filters)

    if manifest_path:
        manifest_raw = json.loads(Path(manifest_path).expanduser().read_text(encoding="utf-8"))
        for item in manifest_raw.get("sources", []):
            vendor_name = str(item.get("vendor_name", ""))
            if channel_set and vendor_name not in channel_set:
                continue
            sources.append(
                {
                    "vendor_name": vendor_name,
                    "business_date": str(item.get("business_date", "")),
                    "path": str(Path(item["path"]).expanduser()),
                    "source_type": item.get("source_type", "raw_file"),
                }
            )

    for file_path in explicit_files:
        candidate = Path(file_path).expanduser().resolve()
        inferred = infer_context_from_path(candidate, vendor_lookup)
        vendor_name = inferred.get("vendor_name", "")
        if channel_set and vendor_name and vendor_name not in channel_set:
            continue
        sources.append(
            {
                "vendor_name": vendor_name,
                "business_date": inferred.get("business_date", ""),
                "path": str(candidate),
                "source_type": inferred.get("source_type", "raw_file"),
            }
        )

    if input_root and input_root.exists():
        for date_dir in sorted(path for path in input_root.iterdir() if path.is_dir()):
            business_date = date_dir.name
            if not looks_like_date(business_date):
                continue
            if date_from and business_date < date_from:
                continue
            if date_to and business_date > date_to:
                continue
            for vendor_dir in sorted(path for path in date_dir.iterdir() if path.is_dir()):
                vendor_name = vendor_lookup.get(vendor_dir.name, vendor_dir.name.replace("_", " "))
                if channel_set and vendor_name not in channel_set:
                    continue
                analysis_by_stem: dict[str, Path] = {}
                raw_files: list[Path] = []
                for file_path in sorted(vendor_dir.rglob("*")):
                    if not file_path.is_file():
                        continue
                    suffix = file_path.suffix.lower()
                    if file_path.name.endswith("_analysis.json") or file_path.name == "file_analysis.json":
                        stem = file_path.name.removesuffix("_analysis.json") if file_path.name.endswith("_analysis.json") else file_path.stem
                        analysis_by_stem[stem] = file_path
                    elif suffix in SUPPORTED_SOURCE_SUFFIXES:
                        raw_files.append(file_path)

                for file_path in sorted(analysis_by_stem.values()):
                    sources.append(
                        {
                            "vendor_name": vendor_name,
                            "business_date": business_date,
                            "path": str(file_path.resolve()),
                            "source_type": "analysis_json",
                        }
                    )

                for file_path in sorted(raw_files):
                    if file_path.stem in analysis_by_stem:
                        continue
                    sources.append(
                        {
                            "vendor_name": vendor_name,
                            "business_date": business_date,
                            "path": str(file_path.resolve()),
                            "source_type": "raw_file",
                        }
                    )

    unique: dict[tuple[str, str, str], dict[str, Any]] = {}
    for item in sources:
        key = (item["vendor_name"], item["business_date"], item["path"])
        unique[key] = item
    return sorted(unique.values(), key=lambda item: (item["business_date"], item["vendor_name"], item["path"]))


def infer_context_from_path(path: Path, vendor_lookup: dict[str, str]) -> dict[str, str]:
    parts = list(path.parts)
    for index, part in enumerate(parts[:-1]):
        if looks_like_date(part):
            vendor_part = parts[index + 1] if index + 1 < len(parts) else ""
            return {
                "business_date": part,
                "vendor_name": vendor_lookup.get(vendor_part, vendor_part.replace("_", " ")),
                "source_type": "analysis_json" if path.name.endswith("_analysis.json") else "raw_file",
            }
    return {"business_date": "", "vendor_name": "", "source_type": "raw_file"}


def analyze_sources(
    sources: list[dict[str, Any]],
    channels: list[ChannelRecord],
    playbooks: dict[str, Playbook],
) -> list[dict[str, Any]]:
    channel_lookup = {channel.vendor_name: channel for channel in channels}
    analyses: list[dict[str, Any]] = []
    for source in sources:
        path = Path(source["path"]).expanduser()
        vendor_name = source.get("vendor_name", "")
        channel = channel_lookup.get(vendor_name)
        playbook = playbooks.get(vendor_name)
        context = {
            "vendor_name": vendor_name,
            "business_date": source.get("business_date", ""),
            "channel_group": channel.channel_group if channel else "",
            "manager": channel.manager if channel else "",
        }
        if source.get("source_type") == "analysis_json":
            raw = json.loads(path.read_text(encoding="utf-8"))
            records = []
            for item in raw.get("records", []):
                record = dict(item)
                if not record.get("vendor_name"):
                    record["vendor_name"] = vendor_name
                if not record.get("business_date"):
                    record["business_date"] = source.get("business_date", "")
                if not record.get("business_month") and record.get("business_date"):
                    record["business_month"] = str(record["business_date"])[:7]
                if not record.get("normalized_product_name"):
                    record["normalized_product_name"] = record.get("product_name", "(unknown)")
                records.append(record)
            if not records and raw.get("items"):
                for item in raw["items"]:
                    records.append(
                        {
                            "vendor_name": vendor_name,
                            "channel_group": context["channel_group"],
                            "manager": context["manager"],
                            "business_date": source.get("business_date", ""),
                            "business_month": source.get("business_date", "")[:7] if source.get("business_date") else "",
                            "status": "",
                            "product_name": item.get("product_name", "(unknown)"),
                            "normalized_product_name": item.get("normalized_product_name", item.get("product_name", "(unknown)")),
                            "qty": float(item.get("qty", 0) or 0),
                            "sales": float(item.get("sales", 0) or 0),
                            "source_file": str(path),
                        }
                    )
            analysis = {
                "output_type": raw.get("output_type", "channel_sales_analysis"),
                "format_version": raw.get("format_version", "2026-04-09"),
                "channel_output_contract_id": raw.get("channel_output_contract_id", CHANNEL_OUTPUT_CONTRACT_ID),
                "product_analysis_master_schema_id": raw.get(
                    "product_analysis_master_schema_id",
                    PRODUCT_ANALYSIS_MASTER_SCHEMA_ID,
                ),
                "excel_postprocess_ruleset_id": raw.get(
                    "excel_postprocess_ruleset_id",
                    EXCEL_POSTPROCESS_RULESET_ID,
                ),
                "metadata": raw.get("metadata", {}),
                "channel_summary": raw.get("channel_summary", {}),
                "applied_postprocess_rules": raw.get("applied_postprocess_rules", {}),
                "quality": raw.get("quality", {}),
                "vendor_name": vendor_name,
                "business_date": source.get("business_date", ""),
                "file": str(path),
                "row_count": len(records),
                "product_count": len({item.get("product_name", "") for item in records}),
                "totals": {
                    "qty": round(sum(float(item.get("qty", 0) or 0) for item in records), 2),
                    "sales": round(sum(float(item.get("sales", 0) or 0) for item in records), 2),
                },
                "records": records,
            }
        else:
            analysis = analyze_sales_file(
                path=path,
                profile=build_analysis_profile(playbook),
                context=context,
            )
        analyses.append(analysis)
    return analyses


def build_analysis_profile(playbook: Playbook | None) -> dict[str, Any]:
    if not playbook:
        return {}
    profile = dict(playbook.analysis_profile)
    if playbook.postprocess_rules:
        profile["postprocess_rules"] = dict(playbook.postprocess_rules)
    return profile


def aggregate_records(records: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    daily_channel: dict[tuple[str, str], dict[str, float]] = defaultdict(lambda: {"qty": 0.0, "sales": 0.0})
    monthly_channel: dict[tuple[str, str], dict[str, float]] = defaultdict(lambda: {"qty": 0.0, "sales": 0.0})
    product_totals: dict[str, dict[str, Any]] = defaultdict(
        lambda: {"qty": 0.0, "sales": 0.0, "channels": set(), "display_name": ""}
    )
    channel_product: dict[tuple[str, str], dict[str, float]] = defaultdict(lambda: {"qty": 0.0, "sales": 0.0})
    daily_product: dict[tuple[str, str], dict[str, float]] = defaultdict(lambda: {"qty": 0.0, "sales": 0.0})
    product_names: dict[str, str] = {}

    for record in records:
        vendor_name = str(record.get("vendor_name", ""))
        business_date = str(record.get("business_date", ""))
        business_month = str(record.get("business_month", ""))
        product_key = str(record.get("normalized_product_name") or record.get("product_name", "(unknown)"))
        product_name = str(record.get("product_name", product_key))
        qty = float(record.get("qty", 0) or 0)
        sales = float(record.get("sales", 0) or 0)

        if business_date:
            daily_channel[(business_date, vendor_name)]["qty"] += qty
            daily_channel[(business_date, vendor_name)]["sales"] += sales
            daily_product[(business_date, product_key)]["qty"] += qty
            daily_product[(business_date, product_key)]["sales"] += sales
        if business_month:
            monthly_channel[(business_month, vendor_name)]["qty"] += qty
            monthly_channel[(business_month, vendor_name)]["sales"] += sales
        product_totals[product_key]["qty"] += qty
        product_totals[product_key]["sales"] += sales
        product_totals[product_key]["channels"].add(vendor_name)
        if not product_totals[product_key]["display_name"]:
            product_totals[product_key]["display_name"] = product_name
        product_names[product_key] = product_totals[product_key]["display_name"] or product_name
        channel_product[(vendor_name, product_key)]["qty"] += qty
        channel_product[(vendor_name, product_key)]["sales"] += sales

    return {
        "daily_channel_sales": sort_rows(
            [
                {"business_date": key[0], "vendor_name": key[1], "qty": round(value["qty"], 2), "sales": round(value["sales"], 2)}
                for key, value in daily_channel.items()
            ],
            ["business_date", "vendor_name"],
        ),
        "monthly_channel_sales": sort_rows(
            [
                {"business_month": key[0], "vendor_name": key[1], "qty": round(value["qty"], 2), "sales": round(value["sales"], 2)}
                for key, value in monthly_channel.items()
            ],
            ["business_month", "vendor_name"],
        ),
        "product_sales": sorted(
            [
                {
                    "product_name": value["display_name"] or key,
                    "normalized_product_name": key,
                    "sales": round(value["sales"], 2),
                    "qty": round(value["qty"], 2),
                    "channel_count": len(value["channels"]),
                }
                for key, value in product_totals.items()
            ],
            key=lambda item: (-item["sales"], item["product_name"]),
        ),
        "product_qty": sorted(
            [
                {
                    "product_name": value["display_name"] or key,
                    "normalized_product_name": key,
                    "qty": round(value["qty"], 2),
                    "sales": round(value["sales"], 2),
                    "channel_count": len(value["channels"]),
                }
                for key, value in product_totals.items()
            ],
            key=lambda item: (-item["qty"], item["product_name"]),
        ),
        "channel_product_sales": sorted(
            [
                {
                    "vendor_name": key[0],
                    "product_name": product_names.get(key[1], key[1]),
                    "normalized_product_name": key[1],
                    "qty": round(value["qty"], 2),
                    "sales": round(value["sales"], 2),
                }
                for key, value in channel_product.items()
            ],
            key=lambda item: (item["vendor_name"], -item["sales"], item["product_name"]),
        ),
        "daily_product_sales": sorted(
            [
                {
                    "business_date": key[0],
                    "product_name": product_names.get(key[1], key[1]),
                    "normalized_product_name": key[1],
                    "qty": round(value["qty"], 2),
                    "sales": round(value["sales"], 2),
                }
                for key, value in daily_product.items()
            ],
            key=lambda item: (item["business_date"], -item["sales"], item["product_name"]),
        ),
    }


def build_summary(aggregates: dict[str, list[dict[str, Any]]], analyses: list[dict[str, Any]]) -> dict[str, Any]:
    total_sales = round(sum(item["sales"] for item in aggregates["product_sales"]), 2)
    total_qty = round(sum(item["qty"] for item in aggregates["product_qty"]), 2)
    channels = sorted({item["vendor_name"] for item in aggregates["channel_product_sales"] if item["vendor_name"]})
    dates = sorted({item["business_date"] for item in aggregates["daily_channel_sales"] if item["business_date"]})
    return {
        "total_sales": total_sales,
        "total_qty": total_qty,
        "channel_count": len(channels),
        "date_count": len(dates),
        "top_channels": top_rows(aggregates["daily_channel_sales"], key="sales", label="vendor_name", limit=5),
        "top_products_by_sales": top_rows(aggregates["product_sales"], key="sales", label="product_name", limit=10),
        "top_products_by_qty": top_rows(aggregates["product_qty"], key="qty", label="product_name", limit=10),
        "source_files": len(analyses),
    }


def export_report_workbook(path: Path, report: dict[str, Any]) -> None:
    workbook = Workbook()
    workbook.remove(workbook.active)

    add_sheet(
        workbook,
        "Overview",
        [
            {"metric": "label", "value": report["label"]},
            {"metric": "source_count", "value": report["source_count"]},
            {"metric": "analysis_count", "value": report["analysis_count"]},
            {"metric": "record_count", "value": report["record_count"]},
            {"metric": "total_sales", "value": report["summary"]["total_sales"]},
            {"metric": "total_qty", "value": report["summary"]["total_qty"]},
            {"metric": "channel_count", "value": report["summary"]["channel_count"]},
            {"metric": "date_count", "value": report["summary"]["date_count"]},
        ],
    )
    add_sheet(
        workbook,
        "Standards",
        [
            {"metric": "channel_output_contract_id", "value": CHANNEL_OUTPUT_CONTRACT_ID},
            {"metric": "excel_postprocess_ruleset_id", "value": EXCEL_POSTPROCESS_RULESET_ID},
            {"metric": "product_analysis_master_schema_id", "value": PRODUCT_ANALYSIS_MASTER_SCHEMA_ID},
            {"metric": "group_by", "value": "normalized_product_name"},
        ],
    )
    add_sheet(
        workbook,
        "SourceFiles",
        [
            {
                "business_date": item.get("business_date", ""),
                "vendor_name": item.get("vendor_name", ""),
                "path": item.get("file", item.get("path", "")),
                "row_count": item.get("row_count", 0),
                "product_count": item.get("product_count", 0),
                "sales": item.get("totals", {}).get("sales", 0),
                "qty": item.get("totals", {}).get("qty", 0),
            }
            for item in report["analyses"]
        ],
    )
    add_sheet(workbook, "DailyChannelSales", report["aggregates"]["daily_channel_sales"])
    add_sheet(workbook, "MonthlyChannelSales", report["aggregates"]["monthly_channel_sales"])
    add_sheet(workbook, "ProductSales", report["aggregates"]["product_sales"])
    add_sheet(workbook, "ProductQty", report["aggregates"]["product_qty"])
    add_sheet(workbook, "ChannelProductSales", report["aggregates"]["channel_product_sales"])
    add_sheet(workbook, "DailyProductSales", report["aggregates"]["daily_product_sales"])
    path.parent.mkdir(parents=True, exist_ok=True)
    workbook.save(path)


def add_sheet(workbook: Workbook, title: str, rows: list[dict[str, Any]]) -> None:
    sheet = workbook.create_sheet(title)
    if not rows:
        sheet["A1"] = "no data"
        return
    headers = list(rows[0].keys())
    for column, header in enumerate(headers, start=1):
        cell = sheet.cell(row=1, column=column, value=header)
        cell.font = Font(bold=True, color="FFFFFF")
        cell.fill = PatternFill(fill_type="solid", fgColor="1F4E78")
    for row_index, row in enumerate(rows, start=2):
        for column_index, header in enumerate(headers, start=1):
            value = row.get(header, "")
            cell = sheet.cell(row=row_index, column=column_index, value=value)
            if header in {"sales", "qty", "total_sales", "total_qty", "value"} and isinstance(value, (int, float)):
                cell.number_format = "#,##0.00"
    sheet.freeze_panes = "A2"
    sheet.auto_filter.ref = sheet.dimensions
    for column_cells in sheet.columns:
        max_length = max(len(str(cell.value or "")) for cell in column_cells)
        sheet.column_dimensions[column_cells[0].column_letter].width = min(max(12, max_length + 2), 48)


def build_summary_markdown(report: dict[str, Any]) -> str:
    summary = report["summary"]
    lines = [
        f"# 매출 리포트 {report['label']}",
        "",
        f"- 소스 파일 수: {report['source_count']}",
        f"- 분석 건수: {report['analysis_count']}",
        f"- 레코드 수: {report['record_count']}",
        f"- 총 매출액: {summary['total_sales']:.2f}",
        f"- 총 판매량: {summary['total_qty']:.2f}",
        f"- 채널 수: {summary['channel_count']}",
        f"- 집계 일수: {summary['date_count']}",
        "",
        "## 상위 채널",
    ]
    for item in summary["top_channels"]:
        lines.append(f"- {item['label']}: {item['value']:.2f}")
    lines.extend(["", "## 상위 품목(매출액)"])
    for item in summary["top_products_by_sales"]:
        lines.append(f"- {item['label']}: {item['value']:.2f}")
    lines.extend(["", "## 상위 품목(판매량)"])
    for item in summary["top_products_by_qty"]:
        lines.append(f"- {item['label']}: {item['value']:.2f}")
    lines.extend(
        [
            "",
            "## 첨부",
            "- 엑셀 리포트: 일별/월별 채널 매출, 품목별 매출, 품목별 판매량, 채널별 품목 매출 포함",
            "- 요약 문서: 상위 채널/품목, 전체 합계 포함",
            "- 품목 집계 기준: normalized_product_name 마스터 스키마 적용",
        ]
    )
    return "\n".join(lines) + "\n"


def create_email_draft(
    output_dir: Path,
    subject: str,
    to_addrs: list[str],
    cc_addrs: list[str],
    summary_markdown: str,
    attachments: list[Path],
    from_addr: str,
) -> Path:
    message = EmailMessage()
    message["Subject"] = subject
    if from_addr:
        message["From"] = from_addr
    if to_addrs:
        message["To"] = ", ".join(to_addrs)
    if cc_addrs:
        message["Cc"] = ", ".join(cc_addrs)
    message.set_content(summary_markdown)
    for attachment in attachments:
        mime_type, _ = mimetypes.guess_type(str(attachment))
        maintype, subtype = (mime_type or "application/octet-stream").split("/", 1)
        message.add_attachment(attachment.read_bytes(), maintype=maintype, subtype=subtype, filename=attachment.name)
    draft_path = output_dir / "report_email_draft.eml"
    draft_path.write_bytes(message.as_bytes())
    return draft_path


def send_email_bundle(
    secrets: SecretStore,
    smtp_profile: str,
    subject: str,
    to_addrs: list[str],
    cc_addrs: list[str],
    summary_markdown: str,
    attachments: list[Path],
) -> tuple[bool, str]:
    smtp_config = secrets.get(smtp_profile)
    required = ["host", "port", "username", "password", "from_addr"]
    if not all(smtp_config.get(key) for key in required):
        missing = [key for key in required if not smtp_config.get(key)]
        return False, f"missing_smtp_fields:{','.join(missing)}"
    if not to_addrs and not cc_addrs:
        return False, "missing_recipients"

    message = EmailMessage()
    message["Subject"] = subject
    message["From"] = smtp_config["from_addr"]
    if to_addrs:
        message["To"] = ", ".join(to_addrs)
    if cc_addrs:
        message["Cc"] = ", ".join(cc_addrs)
    message.set_content(summary_markdown)
    for attachment in attachments:
        mime_type, _ = mimetypes.guess_type(str(attachment))
        maintype, subtype = (mime_type or "application/octet-stream").split("/", 1)
        message.add_attachment(attachment.read_bytes(), maintype=maintype, subtype=subtype, filename=attachment.name)

    host = str(smtp_config["host"])
    port = int(smtp_config["port"])
    use_ssl = bool(smtp_config.get("use_ssl", False))
    use_tls = bool(smtp_config.get("use_tls", True))
    server_cls = smtplib.SMTP_SSL if use_ssl else smtplib.SMTP
    with server_cls(host, port, timeout=30) as server:
        if use_tls and not use_ssl:
            server.starttls()
        server.login(str(smtp_config["username"]), str(smtp_config["password"]))
        server.send_message(message)
    return True, ""


def validate_smtp_profile(secrets: SecretStore, smtp_profile: str) -> dict[str, Any]:
    smtp_config = secrets.get(smtp_profile)
    required = ["host", "port", "username", "password", "from_addr"]
    missing = [key for key in required if not smtp_config.get(key)]
    return {
        "profile": smtp_profile,
        "configured": bool(smtp_config),
        "ready": not missing,
        "missing_fields": missing,
    }


def top_rows(rows: list[dict[str, Any]], key: str, label: str, limit: int) -> list[dict[str, Any]]:
    return [{"label": item[label], "value": item[key]} for item in rows[:limit]]


def sort_rows(rows: list[dict[str, Any]], keys: list[str]) -> list[dict[str, Any]]:
    return sorted(rows, key=lambda item: tuple(item.get(key, "") for key in keys))


def looks_like_date(value: str) -> bool:
    try:
        datetime.strptime(value, "%Y-%m-%d")
        return True
    except ValueError:
        return False


def infer_label(analyses: list[dict[str, Any]], date_from: str | None, date_to: str | None) -> str:
    if date_from and date_to and date_from != date_to:
        return f"{date_from}_to_{date_to}"
    if date_from:
        return date_from
    dates = sorted({item.get("business_date", "") for item in analyses if item.get("business_date")})
    if len(dates) == 1:
        return dates[0]
    if dates:
        return f"{dates[0]}_to_{dates[-1]}"
    return datetime.now().strftime("%Y-%m-%d")
