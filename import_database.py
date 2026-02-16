#!/usr/bin/env python3
"""
Import Neo4j hadith graph from JSON export.

Usage:
    python import_database.py hadith_graph_export.json
    python import_database.py hadith_graph_export.json --clear
"""

import os
import sys
import json
import argparse
from typing import Dict, List, Any

try:
    from neo4j import GraphDatabase
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    print("Please install: pip install neo4j python-dotenv")
    sys.exit(1)


class DatabaseImporter:
    """Import Neo4j database from JSON export."""

    def __init__(self):
        self.uri = os.getenv("NEO4J_URI", "bolt://localhost:7687")
        self.user = os.getenv("NEO4J_USER", "neo4j")
        self.password = os.getenv("NEO4J_PASSWORD", "password")
        self.driver = None

    def connect(self):
        self.driver = GraphDatabase.driver(self.uri, auth=(self.user, self.password))
        self.driver.verify_connectivity()

    def close(self):
        if self.driver:
            self.driver.close()

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, *args):
        self.close()

    def clear_database(self):
        """Delete all nodes and relationships."""
        with self.driver.session() as session:
            session.run("MATCH (n) DETACH DELETE n")
        print("Database cleared")

    def create_constraints(self):
        """Create schema constraints."""
        with self.driver.session() as session:
            constraints = [
                "CREATE CONSTRAINT narrator_unique IF NOT EXISTS FOR (n:Narrator) REQUIRE (n.source, n.norm) IS UNIQUE",
                "CREATE CONSTRAINT hadith_unique IF NOT EXISTS FOR (h:Hadith) REQUIRE (h.source, h.hadith_index) IS UNIQUE",
                "CREATE CONSTRAINT chain_unique IF NOT EXISTS FOR (c:Chain) REQUIRE (c.source, c.hadith_index, c.chain_id) IS UNIQUE"
            ]
            for constraint in constraints:
                try:
                    session.run(constraint)
                except:
                    pass
        print("Constraints created")

    def import_from_json(self, filepath: str, batch_size: int = 500):
        """Import database from JSON file."""
        print(f"Loading {filepath}...")
        
        with open(filepath, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        print(f"Schema version: {data['metadata'].get('schema_version', 'unknown')}")
        print(f"Exported: {data['metadata'].get('exported', 'unknown')}")
        print()

        with self.driver.session() as session:
            # Import Narrators
            narrators = data.get("narrators", [])
            print(f"Importing {len(narrators)} narrators...")
            for i in range(0, len(narrators), batch_size):
                batch = narrators[i:i + batch_size]
                session.run("""
                    UNWIND $batch AS n
                    MERGE (narrator:Narrator {source: n.source, norm: n.norm})
                    SET narrator.name = n.name
                """, batch=batch)

            # Import Hadiths
            hadiths = data.get("hadiths", [])
            print(f"Importing {len(hadiths)} hadiths...")
            for i in range(0, len(hadiths), batch_size):
                batch = hadiths[i:i + batch_size]
                session.run("""
                    UNWIND $batch AS h
                    MERGE (hadith:Hadith {source: h.source, hadith_index: h.hadith_index})
                    SET hadith.text = h.text
                """, batch=batch)

            # Import Chains
            chains = data.get("chains", [])
            print(f"Importing {len(chains)} chains...")
            for i in range(0, len(chains), batch_size):
                batch = chains[i:i + batch_size]
                session.run("""
                    UNWIND $batch AS c
                    MERGE (chain:Chain {source: c.source, hadith_index: c.hadith_index, chain_id: c.chain_id})
                    SET chain.length = c.length
                """, batch=batch)

            # Import HAS_CHAIN relationships
            has_chain = data.get("relationships", {}).get("has_chain", [])
            print(f"Importing {len(has_chain)} HAS_CHAIN relationships...")
            for i in range(0, len(has_chain), batch_size):
                batch = has_chain[i:i + batch_size]
                session.run("""
                    UNWIND $batch AS r
                    MATCH (h:Hadith {source: r.source, hadith_index: r.hadith_index})
                    MATCH (c:Chain {source: r.source, hadith_index: r.hadith_index, chain_id: r.chain_id})
                    MERGE (h)-[:HAS_CHAIN]->(c)
                """, batch=batch)

            # Import POSITION relationships
            positions = data.get("relationships", {}).get("position", [])
            print(f"Importing {len(positions)} POSITION relationships...")
            for i in range(0, len(positions), batch_size):
                batch = positions[i:i + batch_size]
                session.run("""
                    UNWIND $batch AS p
                    MATCH (c:Chain {source: p.source, hadith_index: p.hadith_index, chain_id: p.chain_id})
                    MATCH (n:Narrator {source: p.source, norm: p.narrator_norm})
                    MERGE (c)-[:POSITION {pos: p.pos}]->(n)
                """, batch=batch)

            # Import TRANSMITTED_TO relationships
            transmitted = data.get("relationships", {}).get("transmitted_to", [])
            print(f"Importing {len(transmitted)} TRANSMITTED_TO relationships...")
            for i in range(0, len(transmitted), batch_size):
                batch = transmitted[i:i + batch_size]
                session.run("""
                    UNWIND $batch AS t
                    MATCH (n1:Narrator {source: t.source, norm: t.from_norm})
                    MATCH (n2:Narrator {source: t.source, norm: t.to_norm})
                    MERGE (n1)-[r:TRANSMITTED_TO]->(n2)
                    SET r.count = t.count, r.hadith_indices = t.hadith_indices
                """, batch=batch)

        print("\n✅ Import complete!")


def main():
    parser = argparse.ArgumentParser(description="Import Neo4j hadith graph from JSON")
    parser.add_argument("input", help="Input JSON file")
    parser.add_argument("--clear", action="store_true", help="Clear database before import")
    parser.add_argument("--batch-size", type=int, default=500, help="Batch size for imports")
    
    args = parser.parse_args()
    
    if not os.path.exists(args.input):
        print(f"Error: File not found: {args.input}")
        sys.exit(1)
    
    try:
        with DatabaseImporter() as importer:
            print("Connected to Neo4j\n")
            
            if args.clear:
                confirm = input("⚠️  Clear database? (yes/no): ")
                if confirm.lower() == "yes":
                    importer.clear_database()
                else:
                    print("Aborted")
                    return
            
            importer.create_constraints()
            importer.import_from_json(args.input, args.batch_size)
            
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
