import csv
import json
from collections import Counter
from typing import Any, Dict, Iterable, List


def export_narrator_occurrences(input_path: str, output_path: str, source_label: str) -> int:
    """
    Export narrator occurrences to CSV.

    Each row represents a narrator occurrence within a hadith.

    Args:
        input_path: Path to the input JSON file.
        output_path: Path to the output CSV file.
        source_label: Human-readable label for the source/book (for logging only).

    Returns:
        Number of rows written (excluding header).
    """
    with open(input_path, "r", encoding="utf-8") as f:
        hadiths: List[Dict[str, Any]] = json.load(f)

    row_count = 0
    with open(output_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["narrator_name", "hadith_index", "hadith_text", "role", "n_occurrence"],
            extrasaction="ignore",
        )
        writer.writeheader()

        name_counts: Counter[str] = Counter()
        for hadith in hadiths:
            for narrator in hadith.get("narrators", []):
                name = narrator.get("name", "")
                if name:
                    name_counts[name] += 1

        seen_names = set()
        for hadith in hadiths:
            hadith_index = hadith.get("hadith_index", "")
            hadith_text = hadith.get("hadith_text", "")
            narrators: Iterable[Dict[str, Any]] = hadith.get("narrators", [])

            for narrator in narrators:
                name = narrator.get("name", "")
                if not name or name in seen_names:
                    continue
                role = narrator.get("attributes", {}).get("role", "")
                writer.writerow(
                    {
                        "narrator_name": name,
                        "hadith_index": hadith_index,
                        "hadith_text": hadith_text,
                        "role": role,
                        "n_occurrence": name_counts[name],
                    }
                )
                seen_names.add(name)
                row_count += 1

    print(f"✓ Wrote {row_count} rows to {output_path} ({source_label})")
    return row_count


def main() -> None:
    export_narrator_occurrences(
        input_path="data/Sahih Bukhari Without_Tashkel_results.json",
        output_path="data/bukhari_narrators.csv",
        source_label="Sahih Bukhari",
    )
    export_narrator_occurrences(
        input_path="data/Sahih Muslime Without_Tashkel_results.json",
        output_path="data/muslim_narrators.csv",
        source_label="Sahih Muslim",
    )


if __name__ == "__main__":
    main()
