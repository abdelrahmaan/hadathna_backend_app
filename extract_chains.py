import json
from typing import List, Dict, Any


def extract_narrator_chains(hadith: Dict[str, Any]) -> List[List[str]]:
    """
    Extract narrator chains from a single hadith.

    Args:
        hadith: Dictionary containing hadith data

    Returns:
        List of chains, where each chain is a list of narrator names
    """
    narrators = hadith.get("narrators", [])
    chains = []

    # Find positions of "lead" narrators
    lead_indices = [
        i for i, narrator in enumerate(narrators)
        if narrator.get("attributes", {}).get("role") == "lead"
    ]

    if not lead_indices:
        # If there are no lead narrators, return the entire chain reversed
        # (from الراوي الأعظم/الصحابي to the final narrator)
        chain = [n.get("name") for n in narrators]
        if chain:
            chains.append(chain[::-1])  # Reverse the chain
        return chains

    # Build a chain for each lead narrator
    # Chains should go from الراوي الأعظم/الصحابي down to the final narrator
    start_index = 0
    for lead_index in lead_indices:
        chain = [
            narrators[i].get("name")
            for i in range(start_index, lead_index + 1)
        ]
        if chain:
            chains.append(chain[::-1])  # Reverse to start from الصحابي

        # The next chain starts after the current lead narrator
        start_index = lead_index + 1

    return chains


def process_all_hadiths(input_file: str, output_file: str = None):
    """
    Process all hadiths and extract narrator chains.

    Args:
        input_file: Path to input JSON file
        output_file: Path to output JSON file (optional)
    """
    # Read the file
    with open(input_file, 'r', encoding='utf-8') as f:
        hadiths = json.load(f)

    results = []

    for hadith in hadiths:
        hadith_index = hadith.get("hadith_index")
        chains = extract_narrator_chains(hadith)

        result = {
            "hadith_index": hadith_index,
            "chains": [
                {
                    "chain_number": i + 1,
                    "narrators": chain,
                    "chain_formatted": " -> ".join(chain)
                }
                for i, chain in enumerate(chains)
            ]
        }
        results.append(result)

        # Print result for display
        print(f"\n=== Hadith {hadith_index} ===")
        for i, chain_data in enumerate(result["chains"], 1):
            print(f"{i}. {chain_data['chain_formatted']}")

    # Save results if output file is specified
    if output_file:
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(results, f, ensure_ascii=False, indent=2)
        print(f"\n✓ Results saved to: {output_file}")

    return results


def create_knowledge_graph_data(input_file: str, output_file: str = None):
    """
    Create data ready for building a knowledge graph.

    Args:
        input_file: Path to input JSON file
        output_file: Path to output JSON file for graph data
    """
    with open(input_file, 'r', encoding='utf-8') as f:
        hadiths = json.load(f)

    nodes_dict = {}  # Narrator name -> role (to track if they're ever a lead)
    edges = []       # Relationships between narrators

    for hadith in hadiths:
        hadith_index = hadith.get("hadith_index")
        narrators_data = hadith.get("narrators", [])
        chains = extract_narrator_chains(hadith)

        for chain_num, chain in enumerate(chains, 1):
            # Add narrators as nodes and identify lead narrators
            for i, narrator in enumerate(chain):
                is_last = (i == len(chain) - 1)

                # If this is the last narrator in the chain, they are a lead narrator
                if narrator not in nodes_dict:
                    nodes_dict[narrator] = "lead" if is_last else "narrator"
                elif is_last:
                    # If we've seen this narrator before but now they're a lead, upgrade them
                    nodes_dict[narrator] = "lead"

            # Add relationships as edges
            for i in range(len(chain) - 1):
                edge = {
                    "from": chain[i],
                    "to": chain[i + 1],
                    "hadith_index": hadith_index,
                    "chain_number": chain_num,
                    "position": i + 1
                }
                edges.append(edge)

    # Convert nodes_dict to list with role information
    nodes = [
        {
            "id": name,
            "label": name,
            "role": role
        }
        for name, role in sorted(nodes_dict.items())
    ]

    # Count lead narrators vs regular narrators
    lead_count = sum(1 for n in nodes if n["role"] == "lead")
    regular_count = len(nodes) - lead_count

    graph_data = {
        "nodes": nodes,
        "edges": edges,
        "stats": {
            "total_narrators": len(nodes),
            "lead_narrators": lead_count,
            "regular_narrators": regular_count,
            "total_connections": len(edges)
        }
    }

    if output_file:
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(graph_data, f, ensure_ascii=False, indent=2)
        print(f"\n✓ Knowledge Graph data saved to: {output_file}")
        print(f"  - Total narrators: {graph_data['stats']['total_narrators']}")
        print(f"  - Lead narrators (الراوي الأعظم): {graph_data['stats']['lead_narrators']}")
        print(f"  - Regular narrators: {graph_data['stats']['regular_narrators']}")
        print(f"  - Total connections: {graph_data['stats']['total_connections']}")

    return graph_data


if __name__ == "__main__":
    # Example usage
    file_name = "Sahih Muslime Without_Tashkel_results"
    input_file = f"data/{file_name}.json"

    # Extract chains
    print("Extracting narrator chains...")
    chains_output = process_all_hadiths(
        input_file,
        output_file=f"data/{file_name}_narrator_chains.json"
    )

    # Create Knowledge Graph data
    print("\n" + "="*50)
    print("Creating Knowledge Graph data...")
    graph_data = create_knowledge_graph_data(
        input_file,
        output_file=f"data/{file_name}_narrator_graph.json"
    )
