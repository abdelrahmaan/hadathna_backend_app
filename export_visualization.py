#!/usr/bin/env python3
"""
Export hadith chains to beautiful interactive HTML visualizations.

Uses pyvis for clean, interactive graph rendering.
Install: pip install pyvis
"""

import os
import sys
from typing import Dict, List, Any

try:
    from pyvis.network import Network
    from neo4j import GraphDatabase
    from dotenv import load_dotenv
    load_dotenv()
except ImportError as e:
    print(f"Missing dependency: {e}")
    print("Install with: pip install pyvis neo4j python-dotenv")
    sys.exit(1)


class HadithGraphExporter:
    """Export hadith chains to interactive HTML visualizations."""

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

    def export_hadith_chain(
        self,
        hadith_index: int,
        source: str = "bukhari",
        output_file: str = None,
        show_transmitted_to: bool = False
    ) -> str:
        """
        Export a single hadith's chains to an interactive HTML file.

        Args:
            hadith_index: The hadith number
            source: Source collection (e.g., 'bukhari')
            output_file: Output HTML filename (auto-generated if None)
            show_transmitted_to: Whether to show TRANSMITTED_TO relationships

        Returns:
            Path to the generated HTML file
        """
        if output_file is None:
            output_file = f"hadith_{hadith_index}_chain.html"

        # Create network with settings for Arabic text
        net = Network(
            height="700px",
            width="100%",
            bgcolor="#ffffff",
            font_color="#333333",
            directed=True,
            notebook=False
        )

        # Physics settings for better layout
        net.set_options("""
        {
            "nodes": {
                "font": {
                    "size": 14,
                    "face": "Arial"
                }
            },
            "edges": {
                "arrows": {
                    "to": {
                        "enabled": true,
                        "scaleFactor": 0.5
                    }
                },
                "smooth": {
                    "type": "cubicBezier",
                    "forceDirection": "vertical"
                }
            },
            "physics": {
                "hierarchicalRepulsion": {
                    "centralGravity": 0.0,
                    "springLength": 150,
                    "springConstant": 0.01,
                    "nodeDistance": 200
                },
                "solver": "hierarchicalRepulsion"
            },
            "layout": {
                "hierarchical": {
                    "enabled": true,
                    "direction": "UD",
                    "sortMethod": "directed",
                    "levelSeparation": 150,
                    "nodeSpacing": 200
                }
            }
        }
        """)

        with self.driver.session() as session:
            # Get hadith info
            result = session.run("""
                MATCH (h:Hadith {source: $source, hadith_index: $idx})
                RETURN h.text AS text
            """, source=source, idx=hadith_index)
            record = result.single()
            hadith_text = record["text"][:100] + "..." if record and record["text"] else ""

            # Add hadith node
            net.add_node(
                f"hadith_{hadith_index}",
                label=f"الحديث {hadith_index}",
                title=hadith_text,
                color="#4ecdc4",
                size=30,
                shape="box",
                level=0
            )

            # Get chains and positions
            result = session.run("""
                MATCH (h:Hadith {source: $source, hadith_index: $idx})-[:HAS_CHAIN]->(c:Chain)
                MATCH (c)-[p:POSITION]->(n:Narrator)
                RETURN c.chain_id AS chain_id, c.length AS chain_length,
                       p.pos AS pos, n.name AS narrator, n.norm AS norm
                ORDER BY chain_id, pos
            """, source=source, idx=hadith_index)

            chains_data: Dict[int, List[Dict]] = {}
            for record in result:
                chain_id = record["chain_id"]
                if chain_id not in chains_data:
                    chains_data[chain_id] = []
                chains_data[chain_id].append({
                    "pos": record["pos"],
                    "name": record["narrator"],
                    "norm": record["norm"],
                    "length": record["chain_length"]
                })

            # Color palette for chains
            chain_colors = ["#ff6b6b", "#4ecdc4", "#45b7d1", "#96ceb4", "#ffeaa7", "#dfe6e9"]

            # Add chain nodes and narrator nodes
            narrator_nodes = set()

            for chain_id, narrators in chains_data.items():
                chain_color = chain_colors[(chain_id - 1) % len(chain_colors)]

                # Add chain node
                chain_node_id = f"chain_{hadith_index}_{chain_id}"
                net.add_node(
                    chain_node_id,
                    label=f"السلسلة {chain_id}",
                    title=f"عدد الرواة: {len(narrators)}",
                    color=chain_color,
                    size=20,
                    shape="diamond",
                    level=1
                )

                # Connect hadith to chain
                net.add_edge(
                    f"hadith_{hadith_index}",
                    chain_node_id,
                    color="#999999",
                    width=2
                )

                # Sort narrators by position
                narrators = sorted(narrators, key=lambda x: x["pos"])

                # Add narrator nodes
                prev_node_id = chain_node_id
                for i, narrator in enumerate(narrators):
                    node_id = f"narrator_{narrator['norm']}_{chain_id}"

                    # Determine role
                    if i == 0:
                        role = "صحابي"
                        node_color = "#f39c12"  # Gold for companion
                    elif i == len(narrators) - 1:
                        role = "الراوي الأخير"
                        node_color = "#9b59b6"  # Purple for final narrator
                    else:
                        role = f"راوي [{i}]"
                        node_color = "#3498db"  # Blue for middle narrators

                    # Add node if not exists
                    if node_id not in narrator_nodes:
                        net.add_node(
                            node_id,
                            label=narrator["name"],
                            title=f"{role}\nالموقع: {narrator['pos']}",
                            color=node_color,
                            size=15,
                            level=i + 2
                        )
                        narrator_nodes.add(node_id)

                    # Add edge from previous node
                    net.add_edge(
                        prev_node_id,
                        node_id,
                        color=chain_color,
                        width=2,
                        title=f"الموقع {narrator['pos']}"
                    )

                    prev_node_id = node_id

        # Generate HTML
        net.save_graph(output_file)

        # Add RTL support to the HTML file
        self._add_rtl_support(output_file)

        print(f"✅ تم التصدير إلى: {output_file}")
        return output_file

    def _add_rtl_support(self, filepath: str):
        """Add RTL CSS to the generated HTML."""
        with open(filepath, 'r', encoding='utf-8') as f:
            content = f.read()

        rtl_css = """
        <style>
            body { direction: rtl; font-family: 'Arial', sans-serif; }
            .card { direction: rtl; }
            h1, h2, h3, p { direction: rtl; text-align: right; }
        </style>
        """

        content = content.replace('</head>', f'{rtl_css}</head>')

        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(content)

    def export_narrator_network(
        self,
        narrator_name: str,
        source: str = "bukhari",
        depth: int = 2,
        output_file: str = None
    ) -> str:
        """
        Export a narrator's network to an interactive HTML file.

        Args:
            narrator_name: Part of the narrator's name to search
            source: Source collection
            depth: How many hops to include (1-3)
            output_file: Output HTML filename

        Returns:
            Path to the generated HTML file
        """
        if output_file is None:
            safe_name = narrator_name.replace(" ", "_")[:20]
            output_file = f"narrator_{safe_name}_network.html"

        net = Network(
            height="700px",
            width="100%",
            bgcolor="#ffffff",
            font_color="#333333",
            directed=True
        )

        net.set_options("""
        {
            "nodes": {
                "font": {"size": 12}
            },
            "physics": {
                "forceAtlas2Based": {
                    "gravitationalConstant": -50,
                    "centralGravity": 0.01,
                    "springLength": 200
                },
                "solver": "forceAtlas2Based"
            }
        }
        """)

        with self.driver.session() as session:
            # Find the narrator
            result = session.run("""
                MATCH (n:Narrator {source: $source})
                WHERE n.name CONTAINS $name
                RETURN n.name AS name, n.norm AS norm
                LIMIT 1
            """, source=source, name=narrator_name)

            record = result.single()
            if not record:
                print(f"❌ لم يتم العثور على راوي باسم: {narrator_name}")
                return None

            center_name = record["name"]
            center_norm = record["norm"]

            # Get network
            result = session.run(f"""
                MATCH (center:Narrator {{source: $source, norm: $norm}})
                MATCH path = (center)-[:TRANSMITTED_TO*1..{depth}]-(other:Narrator)
                WITH center, other,
                     [r in relationships(path) | r] AS rels
                UNWIND rels AS rel
                RETURN DISTINCT
                    startNode(rel).name AS from_name,
                    startNode(rel).norm AS from_norm,
                    endNode(rel).name AS to_name,
                    endNode(rel).norm AS to_norm,
                    rel.count AS count
            """, source=source, norm=center_norm)

            nodes_added = set()

            for record in result:
                from_name = record["from_name"]
                from_norm = record["from_norm"]
                to_name = record["to_name"]
                to_norm = record["to_norm"]
                count = record["count"] or 1

                # Add nodes
                if from_norm not in nodes_added:
                    color = "#ff6b6b" if from_norm == center_norm else "#4ecdc4"
                    size = 25 if from_norm == center_norm else 15
                    net.add_node(from_norm, label=from_name, color=color, size=size)
                    nodes_added.add(from_norm)

                if to_norm not in nodes_added:
                    color = "#ff6b6b" if to_norm == center_norm else "#4ecdc4"
                    size = 25 if to_norm == center_norm else 15
                    net.add_node(to_norm, label=to_name, color=color, size=size)
                    nodes_added.add(to_norm)

                # Add edge
                net.add_edge(
                    from_norm,
                    to_norm,
                    value=count,
                    title=f"{count} أحاديث مشتركة"
                )

        net.save_graph(output_file)
        self._add_rtl_support(output_file)

        print(f"✅ تم تصدير شبكة {center_name} إلى: {output_file}")
        return output_file


def main():
    """Demo export."""
    print("\n" + "=" * 50)
    print("   📊 مُصدِّر الرسوم البيانية")
    print("=" * 50)

    try:
        with HadithGraphExporter() as exporter:
            print("✅ متصل بـ Neo4j\n")

            while True:
                print("📋 الأوامر:")
                print("  1. تصدير سلسلة حديث")
                print("  2. تصدير شبكة راوي")
                print("  0. خروج")

                choice = input("\nاختيارك: ").strip()

                if choice == "0":
                    break
                elif choice == "1":
                    idx = input("رقم الحديث: ").strip()
                    if idx.isdigit():
                        output = exporter.export_hadith_chain(int(idx))
                        print(f"\n🌐 افتح الملف في المتصفح: {output}\n")
                elif choice == "2":
                    name = input("اسم الراوي: ").strip()
                    if name:
                        output = exporter.export_narrator_network(name)
                        if output:
                            print(f"\n🌐 افتح الملف في المتصفح: {output}\n")
                else:
                    print("❌ اختيار غير صحيح")

    except Exception as e:
        print(f"❌ خطأ: {e}")
        sys.exit(1)


if __name__ == "__main__":
    if len(sys.argv) > 1:
        # Quick export mode
        hadith_idx = int(sys.argv[1])
        with HadithGraphExporter() as exporter:
            exporter.export_hadith_chain(hadith_idx)
    else:
        main()
