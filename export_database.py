#!/usr/bin/env python3
"""
Export Neo4j hadith graph database to importable formats.

Exports:
1. Cypher statements (portable, works with any Neo4j)
2. JSON format (for programmatic use)
"""

import os
import sys
import json
from datetime import datetime
from typing import Dict, List, Any

try:
    from neo4j import GraphDatabase
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    print("Please install: pip install neo4j python-dotenv")
    sys.exit(1)


class DatabaseExporter:
    """Export Neo4j database to various formats."""

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

    def export_to_cypher(self, output_file: str = "hadith_graph_export.cypher") -> str:
        """Export entire database to Cypher statements."""
        print(f"Exporting to Cypher: {output_file}")
        
        with open(output_file, 'w', encoding='utf-8') as f:
            # Header
            f.write("// Hadith Graph Database Export\n")
            f.write(f"// Exported: {datetime.now().isoformat()}\n")
            f.write("// Schema: V2 (Chain nodes + POSITION + TRANSMITTED_TO)\n")
            f.write("//\n")
            f.write("// To import: Run this file in Neo4j Browser or cypher-shell\n")
            f.write("// Note: Clear database first with: MATCH (n) DETACH DELETE n\n\n")

            # Constraints
            f.write("// === CONSTRAINTS ===\n")
            f.write("CREATE CONSTRAINT narrator_unique IF NOT EXISTS FOR (n:Narrator) REQUIRE (n.source, n.norm) IS UNIQUE;\n")
            f.write("CREATE CONSTRAINT hadith_unique IF NOT EXISTS FOR (h:Hadith) REQUIRE (h.source, h.hadith_index) IS UNIQUE;\n")
            f.write("CREATE CONSTRAINT chain_unique IF NOT EXISTS FOR (c:Chain) REQUIRE (c.source, c.hadith_index, c.chain_id) IS UNIQUE;\n\n")

            with self.driver.session() as session:
                # Export Narrators
                f.write("// === NARRATORS ===\n")
                result = session.run("MATCH (n:Narrator) RETURN n.source AS source, n.norm AS norm, n.name AS name")
                count = 0
                for record in result:
                    name_escaped = record["name"].replace("'", "\\'").replace('"', '\\"')
                    norm_escaped = record["norm"].replace("'", "\\'").replace('"', '\\"')
                    f.write(f"MERGE (n:Narrator {{source: '{record['source']}', norm: '{norm_escaped}'}}) SET n.name = '{name_escaped}';\n")
                    count += 1
                print(f"  Exported {count} narrators")

                # Export Hadiths
                f.write("\n// === HADITHS ===\n")
                result = session.run("MATCH (h:Hadith) RETURN h.source AS source, h.hadith_index AS idx, h.text AS text")
                count = 0
                for record in result:
                    text_escaped = record["text"].replace("'", "\\'").replace('"', '\\"').replace("\n", "\\n") if record["text"] else ""
                    f.write(f"MERGE (h:Hadith {{source: '{record['source']}', hadith_index: {record['idx']}}}) SET h.text = '{text_escaped}';\n")
                    count += 1
                print(f"  Exported {count} hadiths")

                # Export Chains
                f.write("\n// === CHAINS ===\n")
                result = session.run("MATCH (c:Chain) RETURN c.source AS source, c.hadith_index AS idx, c.chain_id AS cid, c.length AS length")
                count = 0
                for record in result:
                    f.write(f"MERGE (c:Chain {{source: '{record['source']}', hadith_index: {record['idx']}, chain_id: {record['cid']}}}) SET c.length = {record['length']};\n")
                    count += 1
                print(f"  Exported {count} chains")

                # Export HAS_CHAIN relationships
                f.write("\n// === HAS_CHAIN RELATIONSHIPS ===\n")
                result = session.run("""
                    MATCH (h:Hadith)-[:HAS_CHAIN]->(c:Chain)
                    RETURN h.source AS source, h.hadith_index AS idx, c.chain_id AS cid
                """)
                count = 0
                for record in result:
                    f.write(f"MATCH (h:Hadith {{source: '{record['source']}', hadith_index: {record['idx']}}}) ")
                    f.write(f"MATCH (c:Chain {{source: '{record['source']}', hadith_index: {record['idx']}, chain_id: {record['cid']}}}) ")
                    f.write("MERGE (h)-[:HAS_CHAIN]->(c);\n")
                    count += 1
                print(f"  Exported {count} HAS_CHAIN relationships")

                # Export POSITION relationships
                f.write("\n// === POSITION RELATIONSHIPS ===\n")
                result = session.run("""
                    MATCH (c:Chain)-[p:POSITION]->(n:Narrator)
                    RETURN c.source AS source, c.hadith_index AS idx, c.chain_id AS cid, p.pos AS pos, n.norm AS norm
                """)
                count = 0
                for record in result:
                    norm_escaped = record["norm"].replace("'", "\\'")
                    f.write(f"MATCH (c:Chain {{source: '{record['source']}', hadith_index: {record['idx']}, chain_id: {record['cid']}}}) ")
                    f.write(f"MATCH (n:Narrator {{source: '{record['source']}', norm: '{norm_escaped}'}}) ")
                    f.write(f"MERGE (c)-[:POSITION {{pos: {record['pos']}}}]->(n);\n")
                    count += 1
                print(f"  Exported {count} POSITION relationships")

                # Export TRANSMITTED_TO relationships
                f.write("\n// === TRANSMITTED_TO RELATIONSHIPS ===\n")
                result = session.run("""
                    MATCH (n1:Narrator)-[t:TRANSMITTED_TO]->(n2:Narrator)
                    RETURN n1.source AS source, n1.norm AS from_norm, n2.norm AS to_norm, 
                           t.count AS count, t.hadith_indices AS indices
                """)
                count = 0
                for record in result:
                    from_escaped = record["from_norm"].replace("'", "\\'")
                    to_escaped = record["to_norm"].replace("'", "\\'")
                    indices = record["indices"] if record["indices"] else []
                    f.write(f"MATCH (n1:Narrator {{source: '{record['source']}', norm: '{from_escaped}'}}) ")
                    f.write(f"MATCH (n2:Narrator {{source: '{record['source']}', norm: '{to_escaped}'}}) ")
                    f.write(f"MERGE (n1)-[t:TRANSMITTED_TO]->(n2) SET t.count = {record['count']}, t.hadith_indices = {indices};\n")
                    count += 1
                print(f"  Exported {count} TRANSMITTED_TO relationships")

        print(f"\n✅ Export complete: {output_file}")
        return output_file

    def export_to_json(self, output_file: str = "hadith_graph_export.json") -> str:
        """Export entire database to JSON format."""
        print(f"Exporting to JSON: {output_file}")
        
        data = {
            "metadata": {
                "exported": datetime.now().isoformat(),
                "schema_version": "v2",
                "description": "Hadith Narrator Knowledge Graph"
            },
            "narrators": [],
            "hadiths": [],
            "chains": [],
            "relationships": {
                "has_chain": [],
                "position": [],
                "transmitted_to": []
            }
        }

        with self.driver.session() as session:
            # Export Narrators
            result = session.run("MATCH (n:Narrator) RETURN n.source AS source, n.norm AS norm, n.name AS name")
            for record in result:
                data["narrators"].append({
                    "source": record["source"],
                    "norm": record["norm"],
                    "name": record["name"]
                })
            print(f"  Exported {len(data['narrators'])} narrators")

            # Export Hadiths
            result = session.run("MATCH (h:Hadith) RETURN h.source AS source, h.hadith_index AS idx, h.text AS text")
            for record in result:
                data["hadiths"].append({
                    "source": record["source"],
                    "hadith_index": record["idx"],
                    "text": record["text"]
                })
            print(f"  Exported {len(data['hadiths'])} hadiths")

            # Export Chains
            result = session.run("MATCH (c:Chain) RETURN c.source AS source, c.hadith_index AS idx, c.chain_id AS cid, c.length AS length")
            for record in result:
                data["chains"].append({
                    "source": record["source"],
                    "hadith_index": record["idx"],
                    "chain_id": record["cid"],
                    "length": record["length"]
                })
            print(f"  Exported {len(data['chains'])} chains")

            # Export HAS_CHAIN
            result = session.run("""
                MATCH (h:Hadith)-[:HAS_CHAIN]->(c:Chain)
                RETURN h.source AS source, h.hadith_index AS idx, c.chain_id AS cid
            """)
            for record in result:
                data["relationships"]["has_chain"].append({
                    "source": record["source"],
                    "hadith_index": record["idx"],
                    "chain_id": record["cid"]
                })
            print(f"  Exported {len(data['relationships']['has_chain'])} HAS_CHAIN relationships")

            # Export POSITION
            result = session.run("""
                MATCH (c:Chain)-[p:POSITION]->(n:Narrator)
                RETURN c.source AS source, c.hadith_index AS idx, c.chain_id AS cid, p.pos AS pos, n.norm AS norm
            """)
            for record in result:
                data["relationships"]["position"].append({
                    "source": record["source"],
                    "hadith_index": record["idx"],
                    "chain_id": record["cid"],
                    "pos": record["pos"],
                    "narrator_norm": record["norm"]
                })
            print(f"  Exported {len(data['relationships']['position'])} POSITION relationships")

            # Export TRANSMITTED_TO
            result = session.run("""
                MATCH (n1:Narrator)-[t:TRANSMITTED_TO]->(n2:Narrator)
                RETURN n1.source AS source, n1.norm AS from_norm, n2.norm AS to_norm, 
                       t.count AS count, t.hadith_indices AS indices
            """)
            for record in result:
                data["relationships"]["transmitted_to"].append({
                    "source": record["source"],
                    "from_norm": record["from_norm"],
                    "to_norm": record["to_norm"],
                    "count": record["count"],
                    "hadith_indices": record["indices"] or []
                })
            print(f"  Exported {len(data['relationships']['transmitted_to'])} TRANSMITTED_TO relationships")

        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

        print(f"\n✅ Export complete: {output_file}")
        return output_file


def main():
    import argparse
    
    parser = argparse.ArgumentParser(description="Export Neo4j hadith graph database")
    parser.add_argument(
        "--format", "-f",
        choices=["cypher", "json", "both"],
        default="both",
        help="Export format (default: both)"
    )
    parser.add_argument(
        "--output", "-o",
        help="Output filename (without extension)"
    )
    
    args = parser.parse_args()
    
    base_name = args.output or "hadith_graph_export"
    
    try:
        with DatabaseExporter() as exporter:
            print("Connected to Neo4j\n")
            
            if args.format in ["cypher", "both"]:
                exporter.export_to_cypher(f"{base_name}.cypher")
                print()
            
            if args.format in ["json", "both"]:
                exporter.export_to_json(f"{base_name}.json")
            
            print("\n" + "=" * 50)
            print("Export files ready for sharing!")
            print("=" * 50)
            print("\nTo import Cypher file:")
            print("  1. Open Neo4j Browser")
            print("  2. Clear database: MATCH (n) DETACH DELETE n")
            print("  3. Copy/paste or run the .cypher file")
            print("\nTo import JSON file:")
            print("  python import_database.py hadith_graph_export.json")
            
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
