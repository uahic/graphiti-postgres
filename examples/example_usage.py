"""
Example usage of PostgreSQL driver for Graphiti
"""

import asyncio
import sys
import uuid
from datetime import datetime
from pathlib import Path

# Add parent directory to path so we can import modules
sys.path.insert(0, str(Path(__file__).parent.parent))

from postgres_driver import PostgresDriver


async def example_basic_operations():
    """Example: Basic node and edge operations"""

    # Initialize driver (adjust credentials for your setup)
    driver = PostgresDriver(
        host='localhost',
        port=5433,
        user='postgres',
        password='postgres',  # Change to your password
        database='postgres',
        group_id='example_app'
    )

    try:
        # Wait for pool initialization
        await asyncio.sleep(0.5)

        # Health check
        is_healthy = await driver.health_check()
        print(f"Database connection healthy: {is_healthy}")

        # Build indices (run once during setup)
        await driver.build_indices_and_constraints(delete_existing=False)
        print("Indices and constraints created")

        # Create nodes
        person_id = str(uuid.uuid4())
        person = await driver.create_node(
            uuid=person_id,
            name="Alice",
            node_type="entity",
            properties={
                "type": "person",
                "age": 30,
                "occupation": "Engineer"
            },
            summary="Alice is a software engineer who loves Python",
            valid_at=datetime.now()
        )
        print(f"Created person node: {person['name']}")

        company_id = str(uuid.uuid4())
        company = await driver.create_node(
            uuid=company_id,
            name="TechCorp",
            node_type="entity",
            properties={
                "type": "company",
                "industry": "Technology",
                "size": "Large"
            },
            summary="TechCorp is a leading technology company",
            valid_at=datetime.now()
        )
        print(f"Created company node: {company['name']}")

        # Create edge (relationship)
        edge_id = str(uuid.uuid4())
        edge = await driver.create_edge(
            uuid=edge_id,
            source_uuid=person_id,
            target_uuid=company_id,
            relation_type="WORKS_AT",
            properties={
                "since": "2020",
                "position": "Senior Engineer"
            },
            fact="Alice works at TechCorp as a Senior Engineer"
        )
        print(f"Created edge: {person['name']} -> WORKS_AT -> {company['name']}")

        # Retrieve node
        retrieved_person = await driver.get_node(person_id)
        print(f"\nRetrieved node: {retrieved_person['name']}")
        print(f"Properties: {retrieved_person['properties']}")

        # Search nodes using fulltext search
        # Note: Requires pg_trgm extension and may need similarity threshold adjustment
        # The search uses trigram similarity on the summary field
        search_results = await driver.search_nodes(
            search_term="software engineer",  # More specific term for better matching
            node_type="entity",
            limit=5
        )
        print(f"\nSearch results for 'software engineer': {len(search_results)} nodes found")
        if len(search_results) > 0:
            for result in search_results:
                print(f"  - {result['name']} (similarity: {result.get('similarity', 'N/A')})")
        else:
            print("  (Note: pg_trgm similarity threshold may be too high. See README for details.)")

    finally:
        # Close connection
        await driver.close()
        print("\nConnection closed")


async def example_graphiti_integration():
    """Example: Using with Graphiti (pseudo-code)"""

    # This is how you'd integrate with actual Graphiti library
    # (assuming graphiti_core is installed)

    try:
        from graphiti_core import Graphiti
        from graphiti_core.llm_client import OpenAIClient
        from graphiti_core.embedder import OpenAIEmbedder

        # Initialize PostgreSQL driver (set your credentials)
        driver = PostgresDriver(
            host='localhost',
            port=5433,
            user='postgres',
            password='postgres',  # Change to your password
            database='postgres',
            group_id='my_app'
        )

        # Initialize Graphiti with PostgreSQL driver
        # Set your OpenAI API key in environment variable OPENAI_API_KEY
        # or pass it directly: api_key="sk-..."
        graphiti = Graphiti(
            driver=driver,
            llm_client=OpenAIClient(),  # Uses OPENAI_API_KEY env var
            embedder=OpenAIEmbedder()   # Uses OPENAI_API_KEY env var
        )

        # Add episodes (events/facts)
        await graphiti.add_episode(
            name="meeting_notes",
            episode_body="Alice met with the CEO to discuss the new product launch.",
            source_description="Meeting notes from 2024-01-15"
        )

        # Search the knowledge graph
        results = await graphiti.search(
            query="What did Alice discuss with the CEO?",
            num_results=5
        )

        print("Search results:", results)

        await driver.close()

    except ImportError:
        print("graphiti-core not installed. Install with: pip install graphiti-core")


async def example_multi_tenancy():
    """Example: Multi-tenancy support using group_id"""

    # Create driver for tenant 1 (adjust credentials for your setup)
    tenant1_driver = PostgresDriver(
        host='localhost',
        port=5433,
        user='postgres',
        password='postgres',  # Change to your password
        database='postgres',
        group_id='tenant_1'
    )

    # Create driver for tenant 2 (cloning)
    tenant2_driver = tenant1_driver.clone(group_id='tenant_2')

    try:
        await asyncio.sleep(0.5)  # Wait for pool init

        # Create node for tenant 1
        node1 = await tenant1_driver.create_node(
            uuid=str(uuid.uuid4()),
            name="Tenant 1 Data",
            node_type="entity",
            summary="This data belongs to tenant 1"
        )
        print(f"Created node for tenant 1: {node1['name']}")

        # Create node for tenant 2
        node2 = await tenant2_driver.create_node(
            uuid=str(uuid.uuid4()),
            name="Tenant 2 Data",
            node_type="entity",
            summary="This data belongs to tenant 2"
        )
        print(f"Created node for tenant 2: {node2['name']}")

        # Each tenant's data is isolated by group_id
        print("\nData isolation: Each tenant only sees their own data")

    finally:
        await tenant1_driver.close()
        # tenant2_driver shares the pool, so no need to close separately


async def example_graph_traversal():
    """
    Example: Using helper functions for graph traversal with deeper graphs

    Graph structure:

        Main Chain (10 nodes):
        Alice -> Bob -> Charlie -> Diana -> Eve -> Frank -> Grace -> Henry -> Ivy -> Jack
          |                        |                                   |
          v (mentors)              v (manages)                         v (teaches)
        Charlie                  Karen                               Mike
                                   |                                   |
                                   v (supervises)                      v (mentors)
                                 Leo                                 Nina
                                                                       |
                                                                       v (advises)
                                                                     Oscar

        Disconnected Component (unreachable from Alice):
        Patricia -> Quinn -> Rachel -> Sam -> Tina

    Additional cross-connections:
    - Diana -> Alice (collaborates_with)
    - Eve -> Bob (reports_to)
    """

    driver = PostgresDriver(
        host='localhost',
        port=5433,
        user='postgres',
        password='postgres',  # Change to your password
        database='postgres',
        group_id='traversal_demo'
    )

    try:
        await asyncio.sleep(0.5)

        # Create a complex knowledge graph with multiple components
        print("Creating a complex graph with 20 nodes...")

        # Main chain (10 people)
        main_chain = ["Alice", "Bob", "Charlie", "Diana", "Eve", "Frank", "Grace", "Henry", "Ivy", "Jack"]

        # Branch off Diana (will be reachable)
        diana_branch = ["Karen", "Leo"]

        # Branch off Henry (requires deeper traversal)
        henry_branch = ["Mike", "Nina", "Oscar"]

        # Disconnected component (unreachable from Alice)
        disconnected = ["Patricia", "Quinn", "Rachel", "Sam", "Tina"]

        all_people = main_chain + diana_branch + henry_branch + disconnected
        person_ids = {}

        # Create all nodes
        for person in all_people:
            person_id = str(uuid.uuid4())
            person_ids[person] = person_id
            await driver.create_node(
                uuid=person_id,
                name=person,
                node_type="entity",
                properties={"type": "person"}
            )

        # Create main chain of KNOWS relationships
        for i in range(len(main_chain) - 1):
            await driver.create_edge(
                uuid=str(uuid.uuid4()),
                source_uuid=person_ids[main_chain[i]],
                target_uuid=person_ids[main_chain[i + 1]],
                relation_type="KNOWS"
            )

        # Create Diana's branch (depth 4 from Alice)
        await driver.create_edge(
            uuid=str(uuid.uuid4()),
            source_uuid=person_ids["Diana"],
            target_uuid=person_ids["Karen"],
            relation_type="MANAGES"
        )
        await driver.create_edge(
            uuid=str(uuid.uuid4()),
            source_uuid=person_ids["Karen"],
            target_uuid=person_ids["Leo"],
            relation_type="SUPERVISES"
        )

        # Create Henry's branch (depth 8 from Alice, requires deep traversal)
        await driver.create_edge(
            uuid=str(uuid.uuid4()),
            source_uuid=person_ids["Henry"],
            target_uuid=person_ids["Mike"],
            relation_type="TEACHES"
        )
        await driver.create_edge(
            uuid=str(uuid.uuid4()),
            source_uuid=person_ids["Mike"],
            target_uuid=person_ids["Nina"],
            relation_type="MENTORS"
        )
        await driver.create_edge(
            uuid=str(uuid.uuid4()),
            source_uuid=person_ids["Nina"],
            target_uuid=person_ids["Oscar"],
            relation_type="ADVISES"
        )

        # Add cross-connections in main chain
        await driver.create_edge(
            uuid=str(uuid.uuid4()),
            source_uuid=person_ids["Alice"],
            target_uuid=person_ids["Charlie"],
            relation_type="MENTORS"
        )
        await driver.create_edge(
            uuid=str(uuid.uuid4()),
            source_uuid=person_ids["Diana"],
            target_uuid=person_ids["Alice"],
            relation_type="COLLABORATES_WITH"
        )
        await driver.create_edge(
            uuid=str(uuid.uuid4()),
            source_uuid=person_ids["Eve"],
            target_uuid=person_ids["Bob"],
            relation_type="REPORTS_TO"
        )

        # Create disconnected component (completely separate from Alice's network)
        for i in range(len(disconnected) - 1):
            await driver.create_edge(
                uuid=str(uuid.uuid4()),
                source_uuid=person_ids[disconnected[i]],
                target_uuid=person_ids[disconnected[i + 1]],
                relation_type="KNOWS"
            )

        total_edges = (len(main_chain) - 1) + 2 + 3 + 3 + (len(disconnected) - 1)
        print(f"Created graph with {len(all_people)} nodes and {total_edges} edges")
        print(f"  - Main chain: {len(main_chain)} nodes")
        print(f"  - Diana's branch: {len(diana_branch)} nodes")
        print(f"  - Henry's branch: {len(henry_branch)} nodes")
        print(f"  - Disconnected component: {len(disconnected)} nodes (unreachable from Alice)")
        print()

        # Use SQL functions for traversal
        async with driver.pool.acquire() as conn:
            # Get Alice's direct neighbors
            neighbors = await conn.fetch(
                "SELECT * FROM get_node_neighbors($1, 'both')",
                person_ids["Alice"]
            )
            print(f"Alice's direct neighbors: {len(neighbors)}")
            for neighbor in neighbors:
                print(f"  - {neighbor['relation_type']} ({neighbor['direction']})")

            # Shallow traversal (depth 2)
            traversal_shallow = await conn.fetch(
                "SELECT * FROM traverse_graph($1, $2)",
                person_ids["Alice"],
                2  # max depth
            )
            print(f"\nShallow traversal from Alice (depth 2): {len(traversal_shallow)} nodes reached")
            depth_counts_shallow = {}
            for node in traversal_shallow:
                depth = node['depth']
                depth_counts_shallow[depth] = depth_counts_shallow.get(depth, 0) + 1
            for depth in sorted(depth_counts_shallow.keys()):
                print(f"  - Depth {depth}: {depth_counts_shallow[depth]} nodes")
            print(f"  → Only reaches main chain + immediate branches")

            # Medium traversal (depth 5)
            traversal_medium = await conn.fetch(
                "SELECT * FROM traverse_graph($1, $2)",
                person_ids["Alice"],
                5  # max depth - reaches Diana's branch
            )
            print(f"\nMedium traversal from Alice (depth 5): {len(traversal_medium)} nodes reached")
            depth_counts_medium = {}
            for node in traversal_medium:
                depth = node['depth']
                depth_counts_medium[depth] = depth_counts_medium.get(depth, 0) + 1
            for depth in sorted(depth_counts_medium.keys()):
                print(f"  - Depth {depth}: {depth_counts_medium[depth]} nodes")
            print(f"  → Reaches Diana's branch (Karen, Leo) but not Henry's branch yet")

            # Deep traversal (depth 8)
            traversal_deep = await conn.fetch(
                "SELECT * FROM traverse_graph($1, $2)",
                person_ids["Alice"],
                8  # max depth - should reach most of the connected graph
            )
            print(f"\nDeep traversal from Alice (depth 8): {len(traversal_deep)} nodes reached")
            depth_counts_deep = {}
            for node in traversal_deep:
                depth = node['depth']
                depth_counts_deep[depth] = depth_counts_deep.get(depth, 0) + 1
            for depth in sorted(depth_counts_deep.keys()):
                print(f"  - Depth {depth}: {depth_counts_deep[depth]} nodes")
            print(f"  → Reaches most connected nodes including Henry's branch")

            # Very deep traversal (depth 12)
            traversal_very_deep = await conn.fetch(
                "SELECT * FROM traverse_graph($1, $2)",
                person_ids["Alice"],
                12  # max depth - reaches everything connected to Alice
            )
            print(f"\nVery deep traversal from Alice (depth 12): {len(traversal_very_deep)} nodes reached")
            depth_counts_very_deep = {}
            for node in traversal_very_deep:
                depth = node['depth']
                depth_counts_very_deep[depth] = depth_counts_very_deep.get(depth, 0) + 1
            for depth in sorted(depth_counts_very_deep.keys()):
                print(f"  - Depth {depth}: {depth_counts_very_deep[depth]} nodes")
            unreached = len(all_people) - len(traversal_very_deep)
            print(f"  → Reaches all connected nodes ({len(traversal_very_deep)}/{len(all_people)})")
            print(f"  → {unreached} nodes remain unreachable (disconnected component)")

            print("\n✓ Demonstrates how traversal depth affects reach in the graph")
            print("✓ Shows that disconnected nodes are never reached, regardless of depth")

    finally:
        await driver.close()


if __name__ == "__main__":
    print("=" * 60)
    print("PostgreSQL Driver for Graphiti - Examples")
    print("=" * 60)

    print("\n[1] Basic Operations")
    print("-" * 60)
    asyncio.run(example_basic_operations())

    print("\n\n[2] Multi-Tenancy")
    print("-" * 60)
    asyncio.run(example_multi_tenancy())

    print("\n\n[3] Graph Traversal")
    print("-" * 60)
    asyncio.run(example_graph_traversal())

    print("\n\n[4] Graphiti Integration")
    print("-" * 60)
    print("See example_graphiti_integration() for integration code")

    # asyncio.run(example_graphiti_integration())

    print("\n" + "=" * 60)
    print("Examples completed!")
    print("=" * 60)
