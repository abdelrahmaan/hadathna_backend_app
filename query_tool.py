#!/usr/bin/env python3
"""
Simple CLI tool for querying the Hadith Narrator Graph.

Usage:
    python query_tool.py
"""

from neo4j import GraphDatabase
import sys


class HadithQuery:
    def __init__(self):
        self.driver = GraphDatabase.driver(
            "bolt://localhost:7687",
            auth=("neo4j", "password")
        )

    def close(self):
        self.driver.close()

    def execute(self, query):
        with self.driver.session() as session:
            return session.run(query)

    def top_lead_narrators(self, limit=10):
        """Find most cited lead narrators."""
        query = f"""
        MATCH ()-[r:NARRATED_TO]->(lead:LeadNarrator)
        WITH lead, count(r) as citations
        ORDER BY citations DESC
        LIMIT {limit}
        RETURN lead.name as name, citations
        """
        result = self.execute(query)
        print(f"\n📊 Top {limit} Most Cited Lead Narrators (الراوي الأعظم)")
        print("=" * 60)
        for i, record in enumerate(result, 1):
            print(f"{i:2}. {record['name']:40} {record['citations']:4} citations")

    def narrator_info(self, name):
        """Get info about a specific narrator."""
        query = """
        MATCH (n) WHERE n.name = $name AND (n:Narrator OR n:LeadNarrator)
        OPTIONAL MATCH (n)-[r_out:NARRATED_TO]->()
        OPTIONAL MATCH ()-[r_in:NARRATED_TO]->(n)
        RETURN
            n.name as name,
            labels(n) as labels,
            count(DISTINCT r_out) as narrations_from,
            count(DISTINCT r_in) as narrations_to,
            count(DISTINCT r_out.hadith_index) as hadiths
        """
        with self.driver.session() as session:
            result = session.run(query, {"name": name})
            record = result.single()

            if record:
                print(f"\n🔍 Narrator: {record['name']}")
                print("=" * 60)
                print(f"Type: {', '.join(record['labels'])}")
                print(f"Narrations from this narrator: {record['narrations_from']}")
                print(f"Narrations to this narrator: {record['narrations_to']}")
                print(f"Hadiths involved: {record['hadiths']}")
            else:
                print(f"\n❌ Narrator '{name}' not found")

    def find_chain(self, hadith_index):
        """Get chain for a specific hadith."""
        query = """
        MATCH path = (start)-[r:NARRATED_TO*]->(end:LeadNarrator)
        WHERE ALL(rel IN r WHERE rel.hadith_index = $hadith_index)
          AND NOT ()-[:NARRATED_TO]->(start)
        WITH path, r[0].chain_number as chain_num
        ORDER BY chain_num
        RETURN
          chain_num,
          [n in nodes(path) | {
            name: n.name,
            type: CASE WHEN n:LeadNarrator THEN 'LEAD' ELSE 'REG' END
          }] as chain
        """
        with self.driver.session() as session:
            result = session.run(query, {"hadith_index": hadith_index})
            records = list(result)

            if records:
                print(f"\n📖 Hadith #{hadith_index} - Narrator Chains")
                print("=" * 60)
                for record in records:
                    print(f"\nChain #{record['chain_num']}:")
                    for i, node in enumerate(record['chain'], 1):
                        marker = "🔵" if node['type'] == 'LEAD' else "⚪"
                        print(f"  {marker} {i}. {node['name']}")
            else:
                print(f"\n❌ No chains found for hadith #{hadith_index}")

    def stats(self):
        """Show database statistics."""
        with self.driver.session() as session:
            # Lead narrators
            result = session.run("MATCH (n:LeadNarrator) RETURN count(n) as count")
            lead_count = result.single()['count']

            # Regular narrators
            result = session.run("MATCH (n:Narrator) RETURN count(n) as count")
            narrator_count = result.single()['count']

            # Relationships
            result = session.run("MATCH ()-[r:NARRATED_TO]->() RETURN count(r) as count")
            rel_count = result.single()['count']

            # Hadiths
            result = session.run(
                "MATCH ()-[r:NARRATED_TO]->() "
                "RETURN count(DISTINCT r.hadith_index) as count"
            )
            hadith_count = result.single()['count']

            print("\n📈 Database Statistics")
            print("=" * 60)
            print(f"Lead Narrators (الراوي الأعظم): {lead_count:,}")
            print(f"Regular Narrators: {narrator_count:,}")
            print(f"Total Narrators: {lead_count + narrator_count:,}")
            print(f"Total Relationships: {rel_count:,}")
            print(f"Total Hadiths: {hadith_count:,}")


def show_menu():
    """Display interactive menu."""
    print("\n" + "=" * 60)
    print("HADITH NARRATOR GRAPH - Query Tool")
    print("=" * 60)
    print("\n1. Show database statistics")
    print("2. Top lead narrators")
    print("3. Search narrator by name")
    print("4. Show chain for hadith")
    print("5. Exit")
    print("\n" + "=" * 60)


def main():
    """Main interactive loop."""
    query = HadithQuery()

    try:
        while True:
            show_menu()
            choice = input("\nEnter choice (1-5): ").strip()

            if choice == '1':
                query.stats()

            elif choice == '2':
                limit = input("How many? (default: 10): ").strip()
                limit = int(limit) if limit else 10
                query.top_lead_narrators(limit)

            elif choice == '3':
                name = input("Enter narrator name: ").strip()
                if name:
                    query.narrator_info(name)

            elif choice == '4':
                hadith_num = input("Enter hadith number: ").strip()
                if hadith_num.isdigit():
                    query.find_chain(int(hadith_num))

            elif choice == '5':
                print("\n👋 Goodbye!")
                break

            else:
                print("\n❌ Invalid choice. Please try again.")

            input("\nPress Enter to continue...")

    except KeyboardInterrupt:
        print("\n\n👋 Goodbye!")

    finally:
        query.close()


if __name__ == "__main__":
    main()
