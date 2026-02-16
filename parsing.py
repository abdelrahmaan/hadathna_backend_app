"""
Parsing utilities for hadith data ingestion.

This module provides functions for:
- Format detection (chains vs result format)
- Source extraction from filename
- Chain extraction from result format
- Edge building with normalization
"""

import os
import json
import logging
from typing import Dict, List, Any, Tuple, Optional

from normalization import normalize_ar


logger = logging.getLogger(__name__)


def detect_format(data: List[Dict[str, Any]]) -> str:
    """
    Auto-detect the format of input data.

    Args:
        data: List of hadith records

    Returns:
        Format string: "chains" or "result"

    Raises:
        ValueError: If data is empty or format cannot be determined

    Examples:
        >>> detect_format([{"hadith_index": 1, "chains": [...]}])
        'chains'
        >>> detect_format([{"hadith_index": 1, "narrators": [...]}])
        'result'
    """
    if not data:
        raise ValueError("Empty data - cannot detect format")

    first = data[0]

    if "chains" in first:
        logger.info("Detected format: chains (narrator_chains.json style)")
        return "chains"
    elif "narrators" in first:
        logger.info("Detected format: result (results.json style)")
        return "result"
    else:
        raise ValueError(
            f"Unknown format - first record has keys: {list(first.keys())}. "
            f"Expected 'chains' or 'narrators' key."
        )


def extract_source_from_filename(filepath: str) -> Optional[str]:
    """
    Extract source collection name from filename.

    Supports patterns:
    - {source}_hadiths.json -> source
    - {source}_chains.json -> source
    - {source}.json -> source
    - narrator_chains.json -> None (can't extract)
    - results.json -> None (can't extract)

    Args:
        filepath: Path to input file

    Returns:
        Source name if extractable, None otherwise

    Examples:
        >>> extract_source_from_filename("bukhari_hadiths.json")
        'bukhari'
        >>> extract_source_from_filename("/path/to/muslim_chains.json")
        'muslim'
        >>> extract_source_from_filename("results.json")
        None
    """
    filename = os.path.basename(filepath)
    name = os.path.splitext(filename)[0]  # Remove extension

    # Skip generic names
    generic_names = {"results", "result", "narrator_chains", "narrator_graph", "data"}
    if name.lower() in generic_names:
        return None

    # Try to extract from pattern: {source}_*
    if '_' in name:
        source = name.split('_')[0]
        if source.lower() not in generic_names:
            logger.info(f"Extracted source '{source}' from filename")
            return source

    # Use the whole name as source if it's not generic
    logger.info(f"Using filename as source: '{name}'")
    return name


def extract_chains_from_result(hadith: Dict[str, Any]) -> List[List[str]]:
    """
    Extract narrator chains from a single hadith in result format.

    Chains are split whenever a narrator has role == "lead".
    Each chain is reversed to go from lead (companion) to sheikh (final recorder).

    Args:
        hadith: Dictionary with 'narrators' key containing list of
                {name: str, attributes: {role: str}} objects

    Returns:
        List of chains, where each chain is [lead, teacher1, ..., sheikh]
    """
    narrators = hadith.get("narrators", [])
    if not narrators:
        return []

    chains = []

    # Find positions of "lead" narrators
    lead_indices = [
        i for i, narrator in enumerate(narrators)
        if narrator.get("attributes", {}).get("role") == "lead"
    ]

    if not lead_indices:
        # No lead narrators - return entire list reversed
        chain = [n.get("name") for n in narrators if n.get("name")]
        if chain:
            chains.append(chain[::-1])  # Reverse: lead -> ... -> sheikh
        return chains

    # Build chain for each lead narrator
    start_index = 0
    for lead_index in lead_indices:
        chain = [
            narrators[i].get("name")
            for i in range(start_index, lead_index + 1)
            if narrators[i].get("name")
        ]
        if chain:
            chains.append(chain[::-1])  # Reverse: lead -> ... -> sheikh

        start_index = lead_index + 1

    return chains


def build_ingestion_data(
    data: List[Dict[str, Any]],
    source: str,
    data_format: str,
    results_data: Optional[List[Dict[str, Any]]] = None
) -> Tuple[
    List[Dict[str, str]],     # narrators
    List[Dict[str, Any]],     # hadiths
    List[Dict[str, Any]],     # edges
    List[Dict[str, Any]],     # chains (HAS_CHAIN)
    List[Dict[str, Any]]      # errors
]:
    """
    Build ingestion data from parsed input.

    Args:
        data: List of hadith records
        source: Source collection name (e.g., "bukhari")
        data_format: "chains" or "result"
        results_data: Optional results.json data to get hadith text
                      (needed when using chains format)

    Returns:
        Tuple of (narrators, hadiths, edges, chain_starts, errors)
    """
    narrators_dict: Dict[str, str] = {}  # norm -> original name
    hadiths: List[Dict[str, Any]] = []
    edges: List[Dict[str, Any]] = []
    chain_starts: List[Dict[str, Any]] = []
    errors: List[Dict[str, Any]] = []

    # Build hadith text lookup if results_data provided
    hadith_texts: Dict[int, str] = {}
    if results_data:
        for h in results_data:
            idx = h.get("hadith_index")
            text = h.get("hadith_text", "")
            if idx is not None:
                hadith_texts[idx] = text

    for record in data:
        hadith_index = record.get("hadith_index")

        if hadith_index is None:
            errors.append({
                "record": record,
                "error": "Missing hadith_index"
            })
            continue

        try:
            # Extract chains based on format
            if data_format == "chains":
                chains_data = record.get("chains", [])
                chains = [
                    c.get("narrators", [])
                    for c in chains_data
                ]
                hadith_text = hadith_texts.get(hadith_index, "")
            else:  # result format
                chains = extract_chains_from_result(record)
                hadith_text = record.get("hadith_text", "")

            if not chains:
                errors.append({
                    "hadith_index": hadith_index,
                    "error": "No chains extracted"
                })
                continue

            # Create hadith node
            hadiths.append({
                "source": source,
                "hadith_index": hadith_index,
                "text": hadith_text
            })

            # Process each chain
            for chain_id, chain in enumerate(chains, 1):
                if not chain or len(chain) < 1:
                    continue

                # Add narrators
                for name in chain:
                    if name:
                        norm = normalize_ar(name)
                        if norm not in narrators_dict:
                            narrators_dict[norm] = name

                # Create edges: lead -> teacher1 -> ... -> sheikh
                for i in range(len(chain) - 1):
                    from_name = chain[i]
                    to_name = chain[i + 1]

                    if from_name and to_name:
                        edges.append({
                            "source": source,
                            "from_norm": normalize_ar(from_name),
                            "to_norm": normalize_ar(to_name),
                            "hadith_index": hadith_index,
                            "chain_id": chain_id,
                            "pos": i + 1
                        })

                # Create HAS_CHAIN relationship (Hadith -> lead narrator)
                if chain[0]:
                    chain_starts.append({
                        "source": source,
                        "hadith_index": hadith_index,
                        "chain_id": chain_id,
                        "start_norm": normalize_ar(chain[0])
                    })

        except Exception as e:
            errors.append({
                "hadith_index": hadith_index,
                "error": str(e)
            })

    # Convert narrators dict to list
    narrators = [
        {"source": source, "norm": norm, "name": name}
        for norm, name in narrators_dict.items()
    ]

    logger.info(
        f"Built ingestion data: {len(narrators)} narrators, "
        f"{len(hadiths)} hadiths, {len(edges)} edges, "
        f"{len(chain_starts)} chain starts, {len(errors)} errors"
    )

    return narrators, hadiths, edges, chain_starts, errors


def load_json_file(filepath: str) -> List[Dict[str, Any]]:
    """
    Load JSON array file.

    Args:
        filepath: Path to JSON file

    Returns:
        List of records from JSON file

    Raises:
        FileNotFoundError: If file doesn't exist
        json.JSONDecodeError: If file is not valid JSON
        ValueError: If file doesn't contain a JSON array
    """
    logger.info(f"Loading {filepath}...")

    with open(filepath, 'r', encoding='utf-8') as f:
        data = json.load(f)

    if not isinstance(data, list):
        raise ValueError(
            f"Expected JSON array, got {type(data).__name__}. "
            f"File must contain a JSON array of hadith records."
        )

    logger.info(f"Loaded {len(data)} records from {filepath}")
    return data


# ========== Chain Schema Functions ==========

def build_chains_data(
    data: List[Dict[str, Any]],
    source: str,
    data_format: str,
    results_data: Optional[List[Dict[str, Any]]] = None
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Build Chain nodes, HAS_CHAIN relationships, and POSITION relationships.

    Args:
        data: List of hadith records
        source: Source collection name (e.g., "bukhari")
        data_format: "chains" or "result"
        results_data: Optional results.json data for hadith text lookup

    Returns:
        Tuple of (chain_nodes, has_chain_rels, position_rels)
    """
    chain_nodes: List[Dict[str, Any]] = []
    has_chain_rels: List[Dict[str, Any]] = []
    position_rels: List[Dict[str, Any]] = []

    # Build hadith text lookup if results_data provided
    hadith_texts: Dict[int, str] = {}
    if results_data:
        for h in results_data:
            idx = h.get("hadith_index")
            if idx is not None:
                hadith_texts[idx] = h.get("hadith_text", "")

    for record in data:
        hadith_index = record.get("hadith_index")
        if hadith_index is None:
            continue

        # Extract chains based on format
        if data_format == "chains":
            chains_data = record.get("chains", [])
            chains = [c.get("narrators", []) for c in chains_data]
        else:  # result format
            chains = extract_chains_from_result(record)

        if not chains:
            continue

        # Process each chain
        for chain_id, chain in enumerate(chains, 1):
            if not chain or len(chain) < 1:
                continue

            # Chain node
            chain_nodes.append({
                "source": source,
                "hadith_index": hadith_index,
                "chain_id": chain_id,
                "length": len(chain)
            })

            # HAS_CHAIN relationship (Hadith -> Chain)
            has_chain_rels.append({
                "source": source,
                "hadith_index": hadith_index,
                "chain_id": chain_id
            })

            # POSITION relationships (Chain -> Narrator)
            for pos, name in enumerate(chain):
                if name:
                    position_rels.append({
                        "source": source,
                        "hadith_index": hadith_index,
                        "chain_id": chain_id,
                        "pos": pos,
                        "narrator_norm": normalize_ar(name)
                    })

    logger.info(
        f"Built chain data: {len(chain_nodes)} chains, "
        f"{len(has_chain_rels)} HAS_CHAIN, {len(position_rels)} POSITION"
    )

    return chain_nodes, has_chain_rels, position_rels


def build_transmitted_to_data(
    edges: List[Dict[str, Any]],
    source: str
) -> List[Dict[str, Any]]:
    """
    Aggregate edges into TRANSMITTED_TO relationships.

    Groups edges by (from_norm, to_norm) and aggregates hadith indices and counts.

    Args:
        edges: List of edge dicts with keys: from_norm, to_norm, hadith_index
        source: Source collection name

    Returns:
        List of aggregated transmission dicts
    """
    transmission_map: Dict[Tuple[str, str], Dict[str, Any]] = {}

    for edge in edges:
        key = (edge["from_norm"], edge["to_norm"])
        if key not in transmission_map:
            transmission_map[key] = {
                "source": source,
                "from_norm": edge["from_norm"],
                "to_norm": edge["to_norm"],
                "hadith_indices": [],
                "count": 0
            }
        # Only add hadith_index if not already present (avoid duplicates from same chain)
        if edge["hadith_index"] not in transmission_map[key]["hadith_indices"]:
            transmission_map[key]["hadith_indices"].append(edge["hadith_index"])
            transmission_map[key]["count"] += 1

    transmissions = list(transmission_map.values())
    logger.info(f"Built {len(transmissions)} TRANSMITTED_TO relationships")

    return transmissions


if __name__ == "__main__":
    # Test format detection
    logging.basicConfig(level=logging.INFO)

    print("Testing format detection...")

    # Test chains format
    chains_data = [{"hadith_index": 1, "chains": [{"narrators": ["a", "b"]}]}]
    assert detect_format(chains_data) == "chains"
    print("✓ Chains format detected")

    # Test result format
    result_data = [{"hadith_index": 1, "narrators": [{"name": "a"}]}]
    assert detect_format(result_data) == "result"
    print("✓ Result format detected")

    # Test source extraction
    assert extract_source_from_filename("bukhari_hadiths.json") == "bukhari"
    assert extract_source_from_filename("muslim.json") == "muslim"
    assert extract_source_from_filename("results.json") is None
    print("✓ Source extraction working")

    # Test chain extraction
    test_hadith = {
        "narrators": [
            {"name": "A", "attributes": {"role": "narrator"}},
            {"name": "B", "attributes": {"role": "narrator"}},
            {"name": "C", "attributes": {"role": "lead"}},
        ]
    }
    chains = extract_chains_from_result(test_hadith)
    assert chains == [["C", "B", "A"]]
    print("✓ Chain extraction working")

    print("\nAll tests passed!")
