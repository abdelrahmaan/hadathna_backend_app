#!/usr/bin/env python3
"""
Hadith Narrator Graph Ingestion CLI

This script ingests hadith narrator chains into Neo4j from JSON array files.

Supported formats:
  - chains: narrator_chains.json format with pre-extracted chains
  - result: results.json format with narrators and roles

Usage:
    python ingest.py --source bukhari --input results.json --format result
    python ingest.py --source bukhari --input narrator_chains.json --format chains
    python ingest.py --input bukhari_hadiths.json --format auto
    python ingest.py --source bukhari --input results.json --clear --batch-size 200
"""

import argparse
import json
import logging
import os
import sys
from datetime import datetime
from typing import Optional

from neo4j_client import Neo4jClient
from parsing import (
    load_json_file,
    detect_format,
    extract_source_from_filename,
    build_ingestion_data,
    build_chains_data,
    build_transmitted_to_data
)


def setup_logging(verbose: bool = False) -> None:
    """Configure logging."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%H:%M:%S'
    )


def parse_args() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Ingest hadith narrator chains into Neo4j",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    # Ingest from results.json (result format)
    python ingest.py --source bukhari --input results.json --format result

    # Ingest from narrator_chains.json (chains format)
    python ingest.py --source bukhari --input narrator_chains.json --format chains

    # Auto-detect format
    python ingest.py --source bukhari --input results.json --format auto

    # Clear database before import
    python ingest.py --source bukhari --input results.json --clear

    # With custom batch size
    python ingest.py --source bukhari --input results.json --batch-size 200
        """
    )

    parser.add_argument(
        "--input", "-i",
        required=True,
        help="Input JSON file path (required)"
    )

    parser.add_argument(
        "--source", "-s",
        help="Source collection name (e.g., bukhari, muslim). "
             "If not provided, will try to extract from filename."
    )

    parser.add_argument(
        "--format", "-f",
        choices=["chains", "result", "auto"],
        default="auto",
        help="Input format: chains (narrator_chains.json), result (results.json), "
             "or auto to detect (default: auto)"
    )

    parser.add_argument(
        "--results-file", "-r",
        help="Path to results.json for hadith text lookup when using chains format"
    )

    parser.add_argument(
        "--direction",
        choices=["up"],
        default="up",
        help="Chain direction: up means lead->sheikh (default: up)"
    )

    parser.add_argument(
        "--batch-size", "-b",
        type=int,
        default=100,
        help="Batch size for Neo4j operations (default: 100)"
    )

    parser.add_argument(
        "--clear",
        action="store_true",
        help="Clear database before importing"
    )

    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable verbose logging"
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Parse and validate data without importing to Neo4j"
    )

    return parser.parse_args()


def save_error_report(
    errors: list,
    source: str,
    input_file: str
) -> Optional[str]:
    """Save error report to JSON file."""
    if not errors:
        return None

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    error_file = f"errors_{source}_{timestamp}.json"

    report = {
        "timestamp": datetime.now().isoformat(),
        "source": source,
        "input_file": input_file,
        "total_errors": len(errors),
        "errors": errors
    }

    with open(error_file, 'w', encoding='utf-8') as f:
        json.dump(report, f, ensure_ascii=False, indent=2)

    return error_file


def main():
    """Main entry point."""
    args = parse_args()
    setup_logging(args.verbose)
    logger = logging.getLogger(__name__)

    print("=" * 60)
    print("Hadith Narrator Graph Ingestion")
    print("=" * 60)

    # Validate input file
    if not os.path.exists(args.input):
        logger.error(f"Input file not found: {args.input}")
        sys.exit(1)

    # Determine source
    source = args.source or extract_source_from_filename(args.input)
    if not source:
        logger.error(
            "Could not determine source from filename. "
            "Please provide --source argument."
        )
        sys.exit(1)

    print(f"Source: {source}")
    print(f"Input file: {args.input}")
    print(f"Format: {args.format}")
    print(f"Batch size: {args.batch_size}")
    print(f"Clear database: {args.clear}")
    print(f"Dry run: {args.dry_run}")
    print()

    # Load input data
    try:
        data = load_json_file(args.input)
    except Exception as e:
        logger.error(f"Failed to load input file: {e}")
        sys.exit(1)

    # Detect or validate format
    if args.format == "auto":
        try:
            data_format = detect_format(data)
        except ValueError as e:
            logger.error(f"Format detection failed: {e}")
            sys.exit(1)
    else:
        data_format = args.format

    print(f"Detected/using format: {data_format}")

    # Load results.json for hadith text if using chains format
    results_data = None
    if data_format == "chains":
        results_file = args.results_file
        if not results_file:
            # Try to find results.json in same directory
            input_dir = os.path.dirname(args.input) or "."
            candidate = os.path.join(input_dir, "results.json")
            if os.path.exists(candidate):
                results_file = candidate

        if results_file and os.path.exists(results_file):
            print(f"Loading hadith texts from: {results_file}")
            results_data = load_json_file(results_file)
        else:
            logger.warning(
                "No results.json found - hadith text will be empty. "
                "Use --results-file to specify."
            )

    # Build ingestion data
    print("\nParsing and building ingestion data...")
    narrators, hadiths, edges, _, errors = build_ingestion_data(
        data=data,
        source=source,
        data_format=data_format,
        results_data=results_data
    )

    # Build chain data
    chain_nodes, has_chain_rels, position_rels = build_chains_data(
        data=data,
        source=source,
        data_format=data_format,
        results_data=results_data
    )
    transmissions = build_transmitted_to_data(edges, source)

    print(f"\nParsing complete:")
    print(f"  Narrators: {len(narrators)}")
    print(f"  Hadiths: {len(hadiths)}")
    print(f"  Chain nodes: {len(chain_nodes)}")
    print(f"  POSITION relationships: {len(position_rels)}")
    print(f"  TRANSMITTED_TO relationships: {len(transmissions)}")
    print(f"  HAS_CHAIN relationships: {len(has_chain_rels)}")

    print(f"  Errors: {len(errors)}")

    # Save error report if there are errors
    if errors:
        error_file = save_error_report(errors, source, args.input)
        logger.warning(f"Errors saved to: {error_file}")

    # Dry run stops here
    if args.dry_run:
        print("\nDry run complete - no data imported.")
        return

    # Connect to Neo4j and import
    print("\nConnecting to Neo4j...")
    try:
        with Neo4jClient() as client:
            # Clear database if requested
            if args.clear:
                confirm = input(
                    "\n⚠️  This will delete ALL data in the database. "
                    "Continue? (yes/no): "
                )
                if confirm.lower() != "yes":
                    print("Aborted.")
                    return
                client.clear_database()

            # Create constraints
            print("\nCreating constraints and indexes...")
            client.create_constraints()

            # Import data
            batch_size = args.batch_size

            print(f"\nImporting {len(narrators)} narrators...")
            client.batch_create_narrators(narrators, batch_size=batch_size)

            print(f"Importing {len(hadiths)} hadiths...")
            client.batch_create_hadiths(hadiths, batch_size=batch_size)

            # Import Chain nodes + POSITION + TRANSMITTED_TO
            print(f"Importing {len(chain_nodes)} Chain nodes...")
            client.batch_create_chains(chain_nodes, batch_size=batch_size)

            print(f"Importing {len(has_chain_rels)} HAS_CHAIN relationships...")
            client.batch_create_has_chain_to_chain(
                has_chain_rels, batch_size=batch_size * 5
            )

            print(f"Importing {len(position_rels)} POSITION relationships...")
            client.batch_create_position_relationships(
                position_rels, batch_size=batch_size * 5
            )

            print(f"Importing {len(transmissions)} TRANSMITTED_TO relationships...")
            client.batch_create_transmitted_to(
                transmissions, batch_size=batch_size * 5
            )

            # Print final stats
            print("\n" + "=" * 60)
            print("Import Complete!")
            print("=" * 60)
            stats = client.get_stats()
            print(f"  Narrators: {stats['narrators']}")
            print(f"  Hadiths: {stats['hadiths']}")
            print(f"  Chains: {stats['chains']}")
            print(f"  POSITION edges: {stats['position_edges']}")
            print(f"  TRANSMITTED_TO edges: {stats['transmitted_to_edges']}")
            print(f"  HAS_CHAIN edges: {stats['has_chain_edges']}")

    except Exception as e:
        logger.error(f"Import failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
