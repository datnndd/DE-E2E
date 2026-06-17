from __future__ import annotations

import argparse
import csv
import re
import sys
from datetime import datetime
from pathlib import Path

VALID_MODES = {"post", "like"}
FALSE_VALUES = {"0", "false", "no", "n"}
REQUIRED_COLUMNS = {"niche", "link", "limit", "start_date"}
OUTPUT_COLUMNS = [
    "enabled",
    "niche",
    "account_id",
    "account_type",
    "link",
    "mode",
    "limit",
    "start_time",
    "end_time",
]


def slugify(value: str) -> str:
    slug = "".join(char.lower() if char.isalnum() else "_" for char in value.strip())
    return "_".join(part for part in slug.split("_") if part)


def default_account_type(niche: str) -> str:
    slug = slugify(niche)
    if "food" in slug or "restaurant" in slug:
        return "food_creator"
    base = slug.removeprefix("douyin_").split("_")[0] or "creator"
    return f"{base}_creator"


def parse_date(value: str, field_name: str, row_number: int, errors: list[str]) -> None:
    if not value:
        return
    try:
        datetime.strptime(value, "%Y-%m-%d")
    except ValueError:
        errors.append(f"row {row_number}: {field_name} must use YYYY-MM-DD, got {value!r}")


def is_douyin_user_url(value: str) -> bool:
    return bool(re.match(r"^https?://(www\.)?douyin\.com/user/", value.strip()))


def normalize_rows(csv_path: Path) -> tuple[list[dict[str, str]], list[str], list[str]]:
    errors: list[str] = []
    warnings: list[str] = []
    normalized: list[dict[str, str]] = []

    with csv_path.open("r", encoding="utf-8-sig", newline="") as file:
        reader = csv.DictReader(file)
        fieldnames = set(reader.fieldnames or [])
        missing = REQUIRED_COLUMNS - fieldnames
        if missing:
            errors.append(f"missing required columns: {', '.join(sorted(missing))}")
            return normalized, errors, warnings

        seen_links: set[str] = set()
        seen_account_ids: set[str] = set()
        enabled_index = 0
        for row_index, row in enumerate(reader, start=1):
            enabled = str(row.get("enabled") or "true").strip().lower()
            if enabled in FALSE_VALUES:
                continue

            enabled_index += 1
            row_number = row_index + 1
            niche = (row.get("niche") or "").strip()
            link = (row.get("link") or "").strip()
            if not niche:
                errors.append(f"row {row_number}: niche is required")
                continue
            if not link:
                errors.append(f"row {row_number}: link is required")
                continue
            if not is_douyin_user_url(link):
                errors.append(f"row {row_number}: link must be a Douyin user URL")
                continue
            if link in seen_links:
                warnings.append(f"row {row_number}: duplicate link ignored in uniqueness warning: {link}")
            seen_links.add(link)

            account_type = (row.get("account_type") or row.get("type") or default_account_type(niche)).strip()
            account_id = (row.get("account_id") or row.get("id") or f"{account_type}_{enabled_index:03d}").strip()
            if account_id in seen_account_ids:
                errors.append(f"row {row_number}: duplicate account_id {account_id!r}")
                continue
            seen_account_ids.add(account_id)

            mode = (row.get("mode") or "post").strip() or "post"
            if mode not in VALID_MODES:
                errors.append(f"row {row_number}: mode must be one of {sorted(VALID_MODES)}, got {mode!r}")
                continue

            limit = (row.get("limit") or "20").strip() or "20"
            try:
                parsed_limit = int(limit)
                if parsed_limit < 0:
                    errors.append(f"row {row_number}: limit must be >= 0")
                    continue
            except ValueError:
                errors.append(f"row {row_number}: limit must be integer, got {limit!r}")
                continue

            start_time = (row.get("start_time") or row.get("start_date") or "").strip()
            end_time = (row.get("end_time") or row.get("end_date") or "").strip()
            parse_date(start_time, "start_date", row_number, errors)
            parse_date(end_time, "end_date", row_number, errors)

            normalized.append(
                {
                    "enabled": "true",
                    "niche": niche,
                    "account_id": account_id,
                    "account_type": account_type,
                    "link": link,
                    "mode": mode,
                    "limit": str(parsed_limit),
                    "start_time": start_time,
                    "end_time": end_time,
                }
            )

    if not normalized and not errors:
        errors.append("no enabled rows found")
    return normalized, errors, warnings


def write_normalized_csv(path: Path, rows: list[dict[str, str]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=OUTPUT_COLUMNS)
        writer.writeheader()
        writer.writerows(rows)


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate and normalize Douyin seed CSV files.")
    parser.add_argument("csv_path", type=Path)
    parser.add_argument("--write-normalized", type=Path, default=None)
    args = parser.parse_args()

    rows, errors, warnings = normalize_rows(args.csv_path)
    for warning in warnings:
        print(f"WARNING: {warning}", file=sys.stderr)
    if errors:
        for error in errors:
            print(f"ERROR: {error}", file=sys.stderr)
        return 1

    print(f"OK: {args.csv_path} has {len(rows)} enabled valid rows")
    print("Generated account mapping:")
    for row in rows:
        print(f"- {row['account_id']} | {row['account_type']} | {row['niche']} | {row['link']}")

    if args.write_normalized:
        write_normalized_csv(args.write_normalized, rows)
        print(f"Normalized CSV written: {args.write_normalized}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
