"""
Neo4j client utilities for hadith narrator graph ingestion.

This module provides connection management, constraint creation,
and batch operations for Neo4j database.
"""

import os
import time
import logging
from typing import Optional, Dict, List, Any
from contextlib import contextmanager

from neo4j import GraphDatabase
from neo4j.exceptions import ServiceUnavailable, AuthError, TransientError

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass  # python-dotenv not installed, use environment variables directly


logger = logging.getLogger(__name__)


class Neo4jClient:
    """Neo4j database client with connection management and batch operations."""

    def __init__(
        self,
        uri: Optional[str] = None,
        user: Optional[str] = None,
        password: Optional[str] = None,
        max_retries: int = 3,
        retry_delay: float = 1.0
    ):
        """
        Initialize Neo4j connection.

        Args:
            uri: Neo4j connection URI (defaults to NEO4J_URI env var)
            user: Neo4j username (defaults to NEO4J_USER env var)
            password: Neo4j password (defaults to NEO4J_PASSWORD env var)
            max_retries: Maximum number of connection retry attempts
            retry_delay: Delay between retries in seconds
        """
        self.uri = uri or os.getenv("NEO4J_URI", "bolt://localhost:7687")
        self.user = user or os.getenv("NEO4J_USER", "neo4j")
        self.password = password or os.getenv("NEO4J_PASSWORD", "password")
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.driver = None

    def connect(self) -> None:
        """Establish connection to Neo4j with retry logic."""
        last_error = None

        for attempt in range(self.max_retries):
            try:
                self.driver = GraphDatabase.driver(
                    self.uri,
                    auth=(self.user, self.password)
                )
                self.driver.verify_connectivity()
                logger.info(f"Connected to Neo4j at {self.uri}")
                return
            except AuthError as e:
                raise Exception(
                    f"Authentication failed. Check username and password. Error: {e}"
                )
            except ServiceUnavailable as e:
                last_error = e
                if attempt < self.max_retries - 1:
                    logger.warning(
                        f"Connection attempt {attempt + 1} failed. "
                        f"Retrying in {self.retry_delay}s..."
                    )
                    time.sleep(self.retry_delay * (attempt + 1))  # Exponential backoff

        raise Exception(
            f"Could not connect to Neo4j at {self.uri} after {self.max_retries} attempts. "
            f"Make sure Neo4j is running. Last error: {last_error}"
        )

    def close(self) -> None:
        """Close the Neo4j connection."""
        if self.driver:
            self.driver.close()
            self.driver = None
            logger.info("Neo4j connection closed")

    def __enter__(self):
        """Context manager entry."""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()

    @contextmanager
    def session(self):
        """Get a Neo4j session as a context manager."""
        if not self.driver:
            raise Exception("Not connected to Neo4j. Call connect() first.")

        session = self.driver.session()
        try:
            yield session
        finally:
            session.close()

    def clear_database(self) -> None:
        """
        Clear all nodes and relationships from the database.
        WARNING: This will delete all data!
        """
        with self.session() as session:
            session.run("MATCH (n) DETACH DELETE n")
        logger.info("Database cleared")

    def batch_create_narrators(
        self,
        narrators: List[Dict[str, str]],
        batch_size: int = 100
    ) -> int:
        """
        Batch create or merge narrator nodes.

        Args:
            narrators: List of dicts with keys: source, norm, name
            batch_size: Number of narrators per batch

        Returns:
            Number of narrators processed
        """
        total = len(narrators)
        processed = 0

        with self.session() as session:
            for i in range(0, total, batch_size):
                batch = narrators[i:i + batch_size]
                session.run("""
                    UNWIND $narrators AS n
                    MERGE (narrator:Narrator {source: n.source, norm: n.norm})
                    ON CREATE SET narrator.name = n.name
                """, narrators=batch)
                processed += len(batch)
                logger.debug(f"Processed {processed}/{total} narrators")

        return processed

    def batch_create_hadiths(
        self,
        hadiths: List[Dict[str, Any]],
        batch_size: int = 100
    ) -> int:
        """
        Batch create or merge hadith nodes.

        Args:
            hadiths: List of dicts with keys: source, hadith_index, text
            batch_size: Number of hadiths per batch

        Returns:
            Number of hadiths processed
        """
        total = len(hadiths)
        processed = 0

        with self.session() as session:
            for i in range(0, total, batch_size):
                batch = hadiths[i:i + batch_size]
                session.run("""
                    UNWIND $hadiths AS h
                    MERGE (hadith:Hadith {source: h.source, hadith_index: h.hadith_index})
                    ON CREATE SET hadith.text = h.text
                """, hadiths=batch)
                processed += len(batch)
                logger.debug(f"Processed {processed}/{total} hadiths")

        return processed

    def batch_create_narrated_from_edges(
        self,
        edges: List[Dict[str, Any]],
        batch_size: int = 500
    ) -> int:
        """
        Batch create NARRATED_FROM relationships between narrators.

        Args:
            edges: List of dicts with keys: source, from_norm, to_norm,
                   hadith_index, chain_id, pos
            batch_size: Number of edges per batch

        Returns:
            Number of edges processed
        """
        total = len(edges)
        processed = 0

        with self.session() as session:
            for i in range(0, total, batch_size):
                batch = edges[i:i + batch_size]
                session.run("""
                    UNWIND $edges AS e
                    MATCH (from:Narrator {source: e.source, norm: e.from_norm})
                    MATCH (to:Narrator {source: e.source, norm: e.to_norm})
                    MERGE (from)-[r:NARRATED_FROM {
                        source: e.source,
                        hadith_index: e.hadith_index,
                        chain_id: e.chain_id
                    }]->(to)
                    SET r.pos = e.pos
                """, edges=batch)
                processed += len(batch)
                logger.debug(f"Processed {processed}/{total} edges")

        return processed

    def batch_create_has_chain_relationships(
        self,
        chains: List[Dict[str, Any]],
        batch_size: int = 500
    ) -> int:
        """
        Batch create HAS_CHAIN relationships from Hadith to chain start narrator.

        Args:
            chains: List of dicts with keys: source, hadith_index, chain_id, start_norm
            batch_size: Number of relationships per batch

        Returns:
            Number of relationships processed
        """
        total = len(chains)
        processed = 0

        with self.session() as session:
            for i in range(0, total, batch_size):
                batch = chains[i:i + batch_size]
                session.run("""
                    UNWIND $chains AS c
                    MATCH (h:Hadith {source: c.source, hadith_index: c.hadith_index})
                    MATCH (n:Narrator {source: c.source, norm: c.start_norm})
                    MERGE (h)-[:HAS_CHAIN {chain_id: c.chain_id}]->(n)
                """, chains=batch)
                processed += len(batch)
                logger.debug(f"Processed {processed}/{total} HAS_CHAIN relationships")

        return processed

    def create_constraints(self) -> None:
        """Create uniqueness constraints and indexes for the hadith graph schema."""
        with self.session() as session:
            # Narrator constraint: (source, norm) must be unique
            try:
                session.run("""
                    CREATE CONSTRAINT narrator_unique IF NOT EXISTS
                    FOR (n:Narrator) REQUIRE (n.source, n.norm) IS UNIQUE
                """)
                logger.info("Created constraint: narrator_unique")
            except Exception as e:
                logger.debug(f"Constraint narrator_unique may already exist: {e}")

            # Hadith constraint: (source, hadith_index) must be unique
            try:
                session.run("""
                    CREATE CONSTRAINT hadith_unique IF NOT EXISTS
                    FOR (h:Hadith) REQUIRE (h.source, h.hadith_index) IS UNIQUE
                """)
                logger.info("Created constraint: hadith_unique")
            except Exception as e:
                logger.debug(f"Constraint hadith_unique may already exist: {e}")

            # Chain constraint: (source, hadith_index, chain_id) must be unique
            try:
                session.run("""
                    CREATE CONSTRAINT chain_unique IF NOT EXISTS
                    FOR (c:Chain) REQUIRE (c.source, c.hadith_index, c.chain_id) IS UNIQUE
                """)
                logger.info("Created constraint: chain_unique")
            except Exception as e:
                logger.debug(f"Constraint chain_unique may already exist: {e}")

            # Create indexes for better query performance
            try:
                session.run("""
                    CREATE INDEX narrator_name IF NOT EXISTS
                    FOR (n:Narrator) ON (n.name)
                """)
                session.run("""
                    CREATE INDEX narrator_source IF NOT EXISTS
                    FOR (n:Narrator) ON (n.source)
                """)
                session.run("""
                    CREATE INDEX hadith_source IF NOT EXISTS
                    FOR (h:Hadith) ON (h.source)
                """)
                session.run("""
                    CREATE INDEX chain_hadith IF NOT EXISTS
                    FOR (c:Chain) ON (c.hadith_index)
                """)
                logger.info("Created indexes")
            except Exception as e:
                logger.debug(f"Some indexes may already exist: {e}")

    def batch_create_chains(
        self,
        chains: List[Dict[str, Any]],
        batch_size: int = 500
    ) -> int:
        """
        Batch create Chain nodes .

        Args:
            chains: List of dicts with keys: source, hadith_index, chain_id, length
            batch_size: Number of chains per batch

        Returns:
            Number of chains processed
        """
        total = len(chains)
        processed = 0

        with self.session() as session:
            for i in range(0, total, batch_size):
                batch = chains[i:i + batch_size]
                session.run("""
                    UNWIND $chains AS c
                    MERGE (chain:Chain {source: c.source, hadith_index: c.hadith_index, chain_id: c.chain_id})
                    ON CREATE SET chain.length = c.length
                """, chains=batch)
                processed += len(batch)
                logger.debug(f"Processed {processed}/{total} Chain nodes")

        return processed

    def batch_create_has_chain_to_chain(
        self,
        relationships: List[Dict[str, Any]],
        batch_size: int = 500
    ) -> int:
        """
        Batch create HAS_CHAIN relationships from Hadith to Chain nodes .

        Args:
            relationships: List of dicts with keys: source, hadith_index, chain_id
            batch_size: Number of relationships per batch

        Returns:
            Number of relationships processed
        """
        total = len(relationships)
        processed = 0

        with self.session() as session:
            for i in range(0, total, batch_size):
                batch = relationships[i:i + batch_size]
                session.run("""
                    UNWIND $rels AS r
                    MATCH (h:Hadith {source: r.source, hadith_index: r.hadith_index})
                    MATCH (c:Chain {source: r.source, hadith_index: r.hadith_index, chain_id: r.chain_id})
                    MERGE (h)-[:HAS_CHAIN]->(c)
                """, rels=batch)
                processed += len(batch)
                logger.debug(f"Processed {processed}/{total} HAS_CHAIN relationships")

        return processed

    def batch_create_position_relationships(
        self,
        positions: List[Dict[str, Any]],
        batch_size: int = 500
    ) -> int:
        """
        Batch create POSITION relationships from Chain to Narrator .

        Args:
            positions: List of dicts with keys: source, hadith_index, chain_id, pos, narrator_norm
            batch_size: Number of positions per batch

        Returns:
            Number of positions processed
        """
        total = len(positions)
        processed = 0

        with self.session() as session:
            for i in range(0, total, batch_size):
                batch = positions[i:i + batch_size]
                session.run("""
                    UNWIND $positions AS p
                    MATCH (c:Chain {source: p.source, hadith_index: p.hadith_index, chain_id: p.chain_id})
                    MATCH (n:Narrator {source: p.source, norm: p.narrator_norm})
                    MERGE (c)-[:POSITION {pos: p.pos}]->(n)
                """, positions=batch)
                processed += len(batch)
                logger.debug(f"Processed {processed}/{total} POSITION relationships")

        return processed

    def batch_create_transmitted_to(
        self,
        transmissions: List[Dict[str, Any]],
        batch_size: int = 500
    ) -> int:
        """
        Batch create aggregate TRANSMITTED_TO relationships between narrators .

        Args:
            transmissions: List of dicts with keys: source, from_norm, to_norm, count, hadith_indices
            batch_size: Number of transmissions per batch

        Returns:
            Number of transmissions processed
        """
        total = len(transmissions)
        processed = 0

        with self.session() as session:
            for i in range(0, total, batch_size):
                batch = transmissions[i:i + batch_size]
                session.run("""
                    UNWIND $transmissions AS t
                    MATCH (from:Narrator {source: t.source, norm: t.from_norm})
                    MATCH (to:Narrator {source: t.source, norm: t.to_norm})
                    MERGE (from)-[r:TRANSMITTED_TO]->(to)
                    ON CREATE SET r.count = t.count, r.hadith_indices = t.hadith_indices, r.source = t.source
                    ON MATCH SET r.count = t.count, r.hadith_indices = t.hadith_indices
                """, transmissions=batch)
                processed += len(batch)
                logger.debug(f"Processed {processed}/{total} TRANSMITTED_TO relationships")

        return processed

    def get_stats(self) -> Dict[str, int]:
        """Get database statistics."""
        stats = {}

        with self.session() as session:
            # Count narrators
            result = session.run("MATCH (n:Narrator) RETURN count(n) as count")
            stats["narrators"] = result.single()["count"]

            # Count hadiths
            result = session.run("MATCH (h:Hadith) RETURN count(h) as count")
            stats["hadiths"] = result.single()["count"]

            # Count Chain nodes
            result = session.run("MATCH (c:Chain) RETURN count(c) as count")
            stats["chains"] = result.single()["count"]

            # Count POSITION relationships
            result = session.run(
                "MATCH ()-[r:POSITION]->() RETURN count(r) as count"
            )
            stats["position_edges"] = result.single()["count"]

            # Count TRANSMITTED_TO relationships
            result = session.run(
                "MATCH ()-[r:TRANSMITTED_TO]->() RETURN count(r) as count"
            )
            stats["transmitted_to_edges"] = result.single()["count"]

            # Count HAS_CHAIN relationships
            result = session.run(
                "MATCH ()-[r:HAS_CHAIN]->() RETURN count(r) as count"
            )
            stats["has_chain_edges"] = result.single()["count"]

        return stats


if __name__ == "__main__":
    # Test connection
    logging.basicConfig(level=logging.INFO)

    print("Testing Neo4j connection...")
    try:
        with Neo4jClient() as client:
            print(f"Connected to: {client.uri}")
            stats = client.get_stats()
            print(f"Current stats: {stats}")
    except Exception as e:
        print(f"Connection failed: {e}")
