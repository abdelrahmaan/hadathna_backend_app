"""
FastAPI backend for Hadith Narrator Graph.

Provides search and detail endpoints backed by Neo4j (V2 schema).
"""

import os
import sys
from typing import List, Dict, Any

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware

try:
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover - handled by requirements
    load_dotenv = None


ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

from neo4j_client import Neo4jClient  # noqa: E402


if load_dotenv:
    load_dotenv(os.path.join(ROOT_DIR, ".env"))


app = FastAPI(title="Hadith Narrator Graph API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


neo4j_client = Neo4jClient()


@app.on_event("startup")
def startup() -> None:
    neo4j_client.connect()


@app.on_event("shutdown")
def shutdown() -> None:
    neo4j_client.close()


@app.get("/api/hadith")
def search_hadith(query: str = "", source: str = "all") -> List[Dict[str, Any]]:
    cypher = """
        MATCH (h:Hadith)
        WHERE ($source = 'all' OR h.source = $source)
          AND ($query = '' OR coalesce(h.text, '') CONTAINS $query)
        OPTIONAL MATCH (h)-[:HAS_CHAIN]->(c:Chain)
        RETURN h.hadith_index AS hadith_index,
               h.source AS source,
               substring(coalesce(h.text, ''), 0, 200) AS hadith_text,
               count(DISTINCT c) AS chain_count
        ORDER BY h.hadith_index
    """

    with neo4j_client.session() as session:
        result = session.run(cypher, source=source, query=query)
        return [
            {
                "hadith_index": record["hadith_index"],
                "source": record["source"],
                "hadith_text": record["hadith_text"],
                "chain_count": record["chain_count"],
            }
            for record in result
        ]


@app.get("/api/hadith/{source}/{hadith_index}")
def get_hadith_detail(source: str, hadith_index: int) -> Dict[str, Any]:
    hadith_cypher = """
        MATCH (h:Hadith {source: $source, hadith_index: $hadith_index})
        RETURN h.hadith_index AS hadith_index,
               h.source AS source,
               h.text AS hadith_text
    """

    chains_cypher = """
        MATCH (h:Hadith {source: $source, hadith_index: $hadith_index})-[:HAS_CHAIN]->(c:Chain)
        MATCH (c)-[p:POSITION]->(n:Narrator)
        RETURN c.chain_id AS chain_id,
               p.pos AS pos,
               n.name AS name,
               coalesce(n.full_name, n.name) AS full_name
        ORDER BY c.chain_id, p.pos
    """

    with neo4j_client.session() as session:
        hadith_record = session.run(
            hadith_cypher,
            source=source,
            hadith_index=hadith_index,
        ).single()

        if not hadith_record:
            raise HTTPException(status_code=404, detail="Hadith not found")

        chain_rows = session.run(
            chains_cypher,
            source=source,
            hadith_index=hadith_index,
        )

        chains_map: Dict[int, List[Dict[str, Any]]] = {}
        for row in chain_rows:
            chain_id = row["chain_id"]
            narrator = {
                "name": row["name"],
                "full_name": row["full_name"],
                "position": row["pos"],
            }
            if chain_id not in chains_map:
                chains_map[chain_id] = []
            chains_map[chain_id].append(narrator)

        chains = [
            {
                "chain_id": chain_id,
                "narrators": narrators,
            }
            for chain_id, narrators in sorted(chains_map.items())
        ]

        return {
            "hadith_index": hadith_record["hadith_index"],
            "source": hadith_record["source"],
            "hadith_text": hadith_record["hadith_text"] or "",
            "chains": chains,
        }
