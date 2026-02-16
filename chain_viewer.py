#!/usr/bin/env python3
"""
Hadith Chain Viewer - Simple visualization for hadith narrator chains.

This tool displays chains in a clear, readable format instead of
complex graph visualizations.
"""

import os
import sys
from typing import List, Dict, Any, Optional

try:
    from neo4j import GraphDatabase
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    print("Please install: pip install neo4j python-dotenv")
    sys.exit(1)


class ChainViewer:
    """Simple hadith chain viewer."""

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

    def get_hadith_chains(self, hadith_index: int, source: str = "bukhari") -> Dict[str, Any]:
        """Get all chains for a specific hadith."""
        with self.driver.session() as session:
            # Get hadith text
            result = session.run("""
                MATCH (h:Hadith {source: $source, hadith_index: $idx})
                RETURN h.text AS text
            """, source=source, idx=hadith_index)
            record = result.single()
            hadith_text = record["text"] if record else ""

            # Get chains with narrators
            result = session.run("""
                MATCH (h:Hadith {source: $source, hadith_index: $idx})-[:HAS_CHAIN]->(c:Chain)
                MATCH (c)-[p:POSITION]->(n:Narrator)
                WITH c.chain_id AS chain_id, c.length AS chain_length,
                     p.pos AS pos, n.name AS narrator
                ORDER BY chain_id, pos
                WITH chain_id, chain_length, collect({pos: pos, name: narrator}) AS narrators
                RETURN chain_id, chain_length, narrators
                ORDER BY chain_id
            """, source=source, idx=hadith_index)

            chains = []
            for record in result:
                narrators = sorted(record["narrators"], key=lambda x: x["pos"])
                chains.append({
                    "chain_id": record["chain_id"],
                    "length": record["chain_length"],
                    "narrators": [n["name"] for n in narrators]
                })

            return {
                "hadith_index": hadith_index,
                "source": source,
                "text": hadith_text,
                "chains": chains
            }

    def display_hadith(self, hadith_index: int, source: str = "bukhari",
                       show_text: bool = True, max_text_length: int = 200):
        """Display hadith chains in a clear format."""
        data = self.get_hadith_chains(hadith_index, source)

        print()
        print("═" * 70)
        print(f"  📜 الحديث رقم {hadith_index} - {source}")
        print("═" * 70)

        if show_text and data["text"]:
            text = data["text"]
            if len(text) > max_text_length:
                text = text[:max_text_length] + "..."
            print(f"\n📝 النص:\n{text}")

        print(f"\n📊 عدد السلاسل: {len(data['chains'])}")
        print("─" * 70)

        for chain in data["chains"]:
            chain_id = chain["chain_id"]
            narrators = chain["narrators"]
            length = chain["length"]

            # Determine chain type
            if narrators:
                first = narrators[0]
                last = narrators[-1] if len(narrators) > 1 else ""
            else:
                first = last = "?"

            print(f"\n🔗 السلسلة {chain_id} ({length} رواة)")
            print(f"   الصحابي: {first}")
            if last:
                print(f"   الراوي الأخير: {last}")

            # Display chain as arrow sequence
            print("\n   " + self._format_chain(narrators))

        print("\n" + "═" * 70)

    def _format_chain(self, narrators: List[str], max_width: int = 65) -> str:
        """Format chain as arrow sequence with wrapping."""
        if not narrators:
            return "(فارغة)"

        arrow = " ← "
        lines = []
        current_line = ""

        for i, narrator in enumerate(narrators):
            if i == 0:
                addition = narrator
            else:
                addition = arrow + narrator

            if len(current_line) + len(addition) > max_width and current_line:
                lines.append(current_line)
                current_line = "   " + narrator  # Indent continuation
            else:
                current_line += addition

        if current_line:
            lines.append(current_line)

        return "\n   ".join(lines)

    def display_chain_tree(self, hadith_index: int, source: str = "bukhari"):
        """Display chains as a tree structure."""
        data = self.get_hadith_chains(hadith_index, source)

        print()
        print(f"🌳 الحديث {hadith_index}")
        print("│")

        for i, chain in enumerate(data["chains"]):
            is_last = (i == len(data["chains"]) - 1)
            prefix = "└──" if is_last else "├──"
            continuation = "   " if is_last else "│  "

            print(f"{prefix} السلسلة {chain['chain_id']} ({chain['length']} رواة)")

            narrators = chain["narrators"]
            for j, narrator in enumerate(narrators):
                is_last_narrator = (j == len(narrators) - 1)
                n_prefix = "└─" if is_last_narrator else "├─"
                pos_label = "صحابي" if j == 0 else f"[{j}]"
                print(f"{continuation}   {n_prefix} {pos_label}: {narrator}")

        print()

    def compare_chains(self, hadith_index: int, source: str = "bukhari"):
        """Compare chains side by side."""
        data = self.get_hadith_chains(hadith_index, source)

        if len(data["chains"]) < 2:
            print("الحديث فيه سلسلة واحدة فقط")
            return

        # Find max length
        max_len = max(c["length"] for c in data["chains"])

        print()
        print(f"📊 مقارنة سلاسل الحديث {hadith_index}")
        print("─" * 70)

        # Header
        header = "الموقع │"
        for chain in data["chains"]:
            header += f" السلسلة {chain['chain_id']} │"
        print(header)
        print("─" * 70)

        # Rows
        for pos in range(max_len):
            row = f"  {pos:2d}   │"
            for chain in data["chains"]:
                if pos < len(chain["narrators"]):
                    name = chain["narrators"][pos]
                    # Truncate long names
                    if len(name) > 20:
                        name = name[:17] + "..."
                    row += f" {name:20s} │"
                else:
                    row += f" {'─' * 20} │"
            print(row)

        print("─" * 70)

        # Find common narrators
        if len(data["chains"]) >= 2:
            sets = [set(c["narrators"]) for c in data["chains"]]
            common = sets[0].intersection(*sets[1:])
            if common:
                print(f"\n🔄 رواة مشتركون: {', '.join(common)}")
        print()

    def search_narrator(self, name: str, source: str = "bukhari", limit: int = 10):
        """Search for a narrator and show their hadiths."""
        with self.driver.session() as session:
            result = session.run("""
                MATCH (n:Narrator {source: $source})
                WHERE n.name CONTAINS $name
                OPTIONAL MATCH (c:Chain)-[:POSITION]->(n)
                WITH n, collect(DISTINCT c.hadith_index) AS hadith_indices
                RETURN n.name AS name,
                       size(hadith_indices) AS hadith_count,
                       hadith_indices[0..10] AS sample_hadiths
                ORDER BY hadith_count DESC
                LIMIT $limit
            """, source=source, name=name, limit=limit)

            print()
            print(f"🔍 نتائج البحث عن: {name}")
            print("─" * 50)

            for record in result:
                print(f"\n👤 {record['name']}")
                print(f"   عدد الأحاديث: {record['hadith_count']}")
                if record['sample_hadiths']:
                    print(f"   أمثلة: {record['sample_hadiths']}")


def main():
    """Interactive chain viewer."""
    print("\n" + "=" * 50)
    print("   🔍 عارض سلاسل الحديث")
    print("=" * 50)

    try:
        with ChainViewer() as viewer:
            print("✅ متصل بـ Neo4j")

            while True:
                print("\n📋 الأوامر:")
                print("  1. عرض حديث (رقم)")
                print("  2. عرض شجري (رقم)")
                print("  3. مقارنة السلاسل (رقم)")
                print("  4. بحث عن راوي (اسم)")
                print("  0. خروج")

                choice = input("\nاختيارك: ").strip()

                if choice == "0":
                    break
                elif choice == "1":
                    idx = input("رقم الحديث: ").strip()
                    if idx.isdigit():
                        viewer.display_hadith(int(idx))
                elif choice == "2":
                    idx = input("رقم الحديث: ").strip()
                    if idx.isdigit():
                        viewer.display_chain_tree(int(idx))
                elif choice == "3":
                    idx = input("رقم الحديث: ").strip()
                    if idx.isdigit():
                        viewer.compare_chains(int(idx))
                elif choice == "4":
                    name = input("اسم الراوي: ").strip()
                    if name:
                        viewer.search_narrator(name)
                else:
                    print("❌ اختيار غير صحيح")

    except Exception as e:
        print(f"❌ خطأ: {e}")
        sys.exit(1)


if __name__ == "__main__":
    # Quick demo mode
    if len(sys.argv) > 1:
        hadith_idx = int(sys.argv[1])
        with ChainViewer() as viewer:
            viewer.display_hadith(hadith_idx)
            viewer.display_chain_tree(hadith_idx)
            if len(sys.argv) > 2 and sys.argv[2] == "--compare":
                viewer.compare_chains(hadith_idx)
    else:
        main()
