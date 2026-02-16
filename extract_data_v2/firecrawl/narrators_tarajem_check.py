#!/usr/bin/env python3
import argparse
import os
import json
from pathlib import Path
from typing import Dict, Iterable, Set

BASE_DIR = Path(__file__).resolve().parent


def sort_ids(ids: Iterable[str]) -> list[str]:
    def key(value: str):
        return (0, int(value)) if value.isdigit() else (1, value)

    return sorted(ids, key=key)


def normalize_name(name: str) -> str:
    return str(name or "").strip().rstrip("،:,")


def load_book_narrator_data(book_jsonl: Path) -> tuple[Set[str], Dict[str, Set[str]]]:
    ids: Set[str] = set()
    names_by_id: Dict[str, Set[str]] = {}
    with book_jsonl.open("r", encoding="utf-8") as f:
        for line_no, line in enumerate(f, start=1):
            line = line.strip()
            if not line:
                continue
            try:
                record = json.loads(line)
            except json.JSONDecodeError as exc:
                raise ValueError(f"Invalid JSON in {book_jsonl} at line {line_no}: {exc}") from exc

            for hadith in record.get("hadith_blocks", []):
                for narrator in hadith.get("narrators", []):
                    narrator_id = str(narrator.get("id", "")).strip()
                    if narrator_id:
                        ids.add(narrator_id)
                        raw_name = normalize_name(narrator.get("name", ""))
                        if raw_name:
                            names_by_id.setdefault(narrator_id, set()).add(raw_name)
    return ids, names_by_id


def load_narrator_hadith_names_map(path: Path) -> Dict[str, Set[str]]:
    if not path.exists():
        return {}

    with path.open("r", encoding="utf-8") as f:
        payload = json.load(f)

    if not isinstance(payload, dict):
        return {}

    names_map: Dict[str, Set[str]] = {}
    for key, value in payload.items():
        narrator_id = str(key).strip()
        if not narrator_id:
            continue
        if isinstance(value, list):
            cleaned = {normalize_name(v) for v in value if normalize_name(v)}
            names_map[narrator_id] = cleaned
        else:
            single = normalize_name(value)
            names_map[narrator_id] = {single} if single else set()
    return names_map


def save_narrator_hadith_names_map(path: Path, names_map: Dict[str, Set[str]]) -> None:
    output = {
        narrator_id: sorted(values)
        for narrator_id, values in sorted(names_map.items(), key=lambda item: (0, int(item[0])) if item[0].isdigit() else (1, item[0]))
    }
    with path.open("w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)


def load_shamela_narrators_ids(path: Path, success_only: bool = True) -> Set[str]:
    ids: Set[str] = set()
    if not path.exists():
        return ids

    with path.open("r", encoding="utf-8") as f:
        for line_no, line in enumerate(f, start=1):
            line = line.strip()
            if not line:
                continue
            try:
                record = json.loads(line)
            except json.JSONDecodeError as exc:
                raise ValueError(f"Invalid JSON in {path} at line {line_no}: {exc}") from exc

            status = str(record.get("status", "")).strip().lower()
            if success_only and status != "success":
                continue

            narrator_id = str(record.get("narrator_id") or record.get("id") or "").strip()
            if narrator_id:
                ids.add(narrator_id)
    return ids


def resolve_api_keys(cli_keys: list[str]) -> list[str]:
    keys = [k.strip() for k in cli_keys if k and k.strip()]
    if keys:
        return keys

    env_value = os.getenv("FIRECRAWL_API_KEYS", "").strip()
    if not env_value:
        return []
    return [k.strip() for k in env_value.split(",") if k.strip()]


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Check if narrator IDs in a Shamela book file exist in narrator_hadith_names and shamela_narrators."
    )
    parser.add_argument(
        "--book-jsonl",
        type=Path,
        default=BASE_DIR / "shamela_book_1681.jsonl",
        help="Path to shamela_book_*.jsonl",
    )
    parser.add_argument(
        "--narrator-hadith-names",
        type=Path,
        default=BASE_DIR / "narrator_hadith_names.json",
        help="Path to narrator_hadith_names.json",
    )
    parser.add_argument(
        "--shamela-narrators-jsonl",
        type=Path,
        default=BASE_DIR / "shamela_narrators.jsonl",
        help="Path to shamela_narrators.jsonl",
    )
    parser.add_argument(
        "--output-json",
        type=Path,
        default=None,
        help="Optional path to write full report as JSON",
    )
    parser.add_argument(
        "--show-limit",
        type=int,
        default=30,
        help="How many missing IDs to print in terminal per category",
    )
    parser.add_argument(
        "--append-missing-to-shamela",
        action="store_true",
        help="Append missing narrator IDs into shamela_narrators.jsonl as pending records.",
    )
    parser.add_argument(
        "--append-status",
        default="pending",
        help="Status value used for appended records (default: pending).",
    )
    parser.add_argument(
        "--scrape-missing",
        action="store_true",
        help="Use shamela_narrator_scraper.scrape_narrators to scrape missing IDs.",
    )
    parser.add_argument(
        "--api-key",
        action="append",
        default=[],
        help="Firecrawl API key. Repeat for multiple keys; falls back to FIRECRAWL_API_KEYS env (comma-separated).",
    )
    parser.add_argument(
        "--max-workers",
        type=int,
        default=2,
        help="Concurrent workers for scraping missing narrators.",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=3.0,
        help="Delay in seconds between scrape batches.",
    )
    parser.add_argument(
        "--expand-narrator-hadith-names",
        action="store_true",
        help="Merge missing name variants from the book into narrator_hadith_names.json.",
    )

    args = parser.parse_args()

    book_ids, book_names_map = load_book_narrator_data(args.book_jsonl)
    hadith_names_map = load_narrator_hadith_names_map(args.narrator_hadith_names)
    hadith_name_ids = set(hadith_names_map.keys())
    shamela_narrator_ids = load_shamela_narrators_ids(args.shamela_narrators_jsonl, success_only=True)

    missing_in_hadith_names = sort_ids(book_ids - hadith_name_ids)
    missing_in_shamela_narrators = sort_ids(book_ids - shamela_narrator_ids)
    present_in_both = sort_ids(book_ids & hadith_name_ids & shamela_narrator_ids)
    missing_in_either = sort_ids(
        {
            narrator_id
            for narrator_id in book_ids
            if narrator_id not in hadith_name_ids or narrator_id not in shamela_narrator_ids
        }
    )
    missing_name_variants: Dict[str, list[str]] = {}
    for narrator_id, book_names in book_names_map.items():
        existing_names = hadith_names_map.get(narrator_id, set())
        new_names = sorted(book_names - existing_names)
        if new_names:
            missing_name_variants[narrator_id] = new_names

    report = {
        "book_file": str(args.book_jsonl),
        "narrator_hadith_names_file": str(args.narrator_hadith_names),
        "shamela_narrators_file": str(args.shamela_narrators_jsonl),
        "totals": {
            "book_unique_narrator_ids": len(book_ids),
            "narrator_hadith_names_ids": len(hadith_name_ids),
            "shamela_narrators_ids": len(shamela_narrator_ids),
            "present_in_both_targets": len(present_in_both),
            "missing_in_either_target": len(missing_in_either),
            "missing_in_narrator_hadith_names": len(missing_in_hadith_names),
            "missing_in_shamela_narrators": len(missing_in_shamela_narrators),
            "ids_with_missing_name_variants": len(missing_name_variants),
            "total_missing_name_variants": sum(len(v) for v in missing_name_variants.values()),
        },
        "missing_in_narrator_hadith_names": missing_in_hadith_names,
        "missing_in_shamela_narrators": missing_in_shamela_narrators,
        "missing_in_either_target": missing_in_either,
        "present_in_both_targets": present_in_both,
        "missing_name_variants_by_id": missing_name_variants,
    }

    print("=== Narrator ID Coverage Check ===")
    print(f"Book narrator IDs (unique): {report['totals']['book_unique_narrator_ids']}")
    print(f"Found in narrator_hadith_names: {report['totals']['book_unique_narrator_ids'] - report['totals']['missing_in_narrator_hadith_names']}")
    print(f"Found in shamela_narrators (status=success): {report['totals']['book_unique_narrator_ids'] - report['totals']['missing_in_shamela_narrators']}")
    print(f"Found in BOTH targets: {report['totals']['present_in_both_targets']}")
    print(f"Missing in EITHER target: {report['totals']['missing_in_either_target']}")
    print(f"IDs with missing name variants: {report['totals']['ids_with_missing_name_variants']}")
    print(f"Total missing name variants: {report['totals']['total_missing_name_variants']}")

    limit = max(args.show_limit, 0)

    def preview(label: str, values: list[str]) -> None:
        if not values:
            print(f"\n{label}: 0")
            return
        print(f"\n{label}: {len(values)}")
        shown = values[:limit] if limit else values
        print(", ".join(shown))
        if limit and len(values) > limit:
            print(f"... and {len(values) - limit} more")

    preview("Missing in narrator_hadith_names", missing_in_hadith_names)
    preview("Missing in shamela_narrators", missing_in_shamela_narrators)
    preview("IDs with missing name variants", sort_ids(missing_name_variants.keys()))

    if args.expand_narrator_hadith_names:
        if missing_name_variants:
            for narrator_id, new_names in missing_name_variants.items():
                hadith_names_map.setdefault(narrator_id, set()).update(new_names)
            save_narrator_hadith_names_map(args.narrator_hadith_names, hadith_names_map)
            print(
                f"\nExpanded narrator_hadith_names.json with "
                f"{report['totals']['total_missing_name_variants']} new name variants "
                f"across {report['totals']['ids_with_missing_name_variants']} IDs."
            )
        else:
            print("\nNo missing name variants to merge into narrator_hadith_names.json.")

    if args.append_missing_to_shamela:
        if missing_in_shamela_narrators:
            with args.shamela_narrators_jsonl.open("a", encoding="utf-8") as f:
                for narrator_id in missing_in_shamela_narrators:
                    output_record = {
                        "status": args.append_status,
                        "narrator_id": narrator_id,
                        "url": f"https://shamela.ws/narrator/{narrator_id}",
                        "message": (
                            "Missing from shamela_narrators during narrators_info_check "
                            f"for book file: {args.book_jsonl.name}"
                        ),
                        "source": "narrators_info_check.py",
                    }
                    f.write(json.dumps(output_record, ensure_ascii=False) + "\n")
            print(
                f"\nAppended {len(missing_in_shamela_narrators)} missing narrator IDs to: "
                f"{args.shamela_narrators_jsonl}"
            )
        else:
            print("\nNo missing narrator IDs in shamela_narrators to append.")

    if args.scrape_missing:
        if not missing_in_shamela_narrators:
            print("\nNo missing narrator IDs in shamela_narrators to scrape.")
        else:
            api_keys = resolve_api_keys(args.api_key)
            if not api_keys:
                raise ValueError(
                    "No API keys provided. Use --api-key (repeatable) or set FIRECRAWL_API_KEYS."
                )

            # Local import to keep pure check mode fast and dependency-light.
            from shamela_narrator_scraper import scrape_narrators

            print(
                f"\nStarting scrape for {len(missing_in_shamela_narrators)} missing narrator IDs..."
            )
            scrape_narrators(
                narrator_ids=set(missing_in_shamela_narrators),
                api_keys=api_keys,
                jsonl_output=args.shamela_narrators_jsonl,
                delay=args.delay,
                max_workers=args.max_workers,
            )

    if args.output_json:
        args.output_json.parent.mkdir(parents=True, exist_ok=True)
        with args.output_json.open("w", encoding="utf-8") as f:
            json.dump(report, f, ensure_ascii=False, indent=2)
        print(f"\nFull JSON report written to: {args.output_json}")


if __name__ == "__main__":
    main()
