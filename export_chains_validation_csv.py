"""
Export narrator chains to CSV for human validation.

Creates a wide-format CSV with one row per hadith, showing all extracted
chains in a format optimized for manual review and validation.
"""

import csv
import json
from typing import List, Dict, Any
from parsing import extract_chains_from_result


def format_chain(chain: List[str]) -> str:
    """Format a chain with arrow notation for readability."""
    return " → ".join(chain)


def export_validation_csv(
    input_path: str,
    output_path: str,
    source_label: str = "Hadith Collection"
) -> int:
    """
    Export chains to CSV for human validation.

    Args:
        input_path: Path to input JSON file (results format)
        output_path: Path to output CSV file
        source_label: Human-readable label for logging

    Returns:
        Number of hadiths processed
    """
    # Load data
    with open(input_path, "r", encoding="utf-8") as f:
        hadiths: List[Dict[str, Any]] = json.load(f)

    # Prepare CSV
    with open(output_path, "w", encoding="utf-8", newline="") as f:
        fieldnames = [
            "hadith_index",
            "hadith_text",
            "n_chains",
            "all_chains_combined",
            "validation_status",
            "notes"
        ]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        hadith_count = 0
        for hadith in hadiths:
            hadith_index = hadith.get("hadith_index", "")
            hadith_text = hadith.get("hadith_text", "")

            # Extract chains
            chains = extract_chains_from_result(hadith)
            n_chains = len(chains)

            # Format chains as JSON object
            if chains:
                chains_dict = {
                    f"chain_{i+1}": format_chain(chain)
                    for i, chain in enumerate(chains)
                }
                all_chains_json = json.dumps(chains_dict, ensure_ascii=False)
            else:
                all_chains_json = json.dumps({"error": "No chain extracted"}, ensure_ascii=False)

            # Prepare row data
            row = {
                "hadith_index": hadith_index,
                "hadith_text": hadith_text,
                "n_chains": n_chains,
                "all_chains_combined": all_chains_json,
                "validation_status": "",
                "notes": ""
            }

            writer.writerow(row)
            hadith_count += 1

    print(f"✓ Exported {hadith_count} hadiths to {output_path} ({source_label})")
    return hadith_count


def main() -> None:
    """Export Bukhari chains for validation."""
    export_validation_csv(
        input_path="data/Sahih Bukhari Without_Tashkel_results.json",
        output_path="data/bukhari_chains_validation.csv",
        source_label="Sahih Bukhari",
    )


if __name__ == "__main__":
    main()
