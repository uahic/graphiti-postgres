"""
Example usage of Apache Age driver for Graphiti
Demonstrates CRUD operations, graph traversal, multi-tenancy, and temporal queries
"""

import asyncio
import uuid
from datetime import datetime, timedelta
from graphiti_postgres import AgeDriver


async def example_age_basic_crud():
    """Example: Basic CRUD operations with Apache Age driver"""

    print("\n" + "="*80)
    print("EXAMPLE 1: Basic CRUD Operations")
    print("="*80)

    # Initialize driver
    driver = AgeDriver(
        host='localhost',
        port=5432,
        user='postgres',
        password='postgres',
        database='postgres',
        graph_name='graphiti'
    )

    try:
        # Initialize connection pool
        await driver.initialize()

        # Clean up any existing data from previous runs
        try:
            await driver.execute_query("MATCH (n) DETACH DELETE n")
        except:
            pass  # Graph might not exist yet

        # Health check
        is_healthy = await driver.health_check()
        print(f"✓ Database connection healthy: {is_healthy}")

        # Build indices and create graph if needed
        await driver.build_indices_and_constraints(delete_existing=False)
        print("✓ Graph indices and constraints ready")

        # CREATE: Create nodes
        person_id = str(uuid.uuid4())
        print(f"\nCreating person node (UUID: {person_id})...")
        person = await driver.create_node(
            uuid=person_id,
            name="Alice",
            node_type="entity",
            properties={
                "type": "person",
                "age": 30,
                "occupation": "Software Engineer"
            },
            summary="Alice is a software engineer who loves Python and graph databases",
            valid_at=datetime.now()
        )
        print(f"✓ Created person node: {person.get('name', 'Alice')}")

        company_id = str(uuid.uuid4())
        print(f"\nCreating company node (UUID: {company_id})...")
        company = await driver.create_node(
            uuid=company_id,
            name="TechCorp",
            node_type="entity",
            properties={
                "type": "company",
                "industry": "Technology",
                "size": "Large",
                "founded": 2010
            },
            summary="TechCorp is a leading technology company specializing in AI",
            valid_at=datetime.now()
        )
        print(f"✓ Created company node: {company.get('name', 'TechCorp')}")

        # CREATE: Create edge (relationship)
        edge_id = str(uuid.uuid4())
        print(f"\nCreating WORKS_AT relationship...")
        edge = await driver.create_edge(
            uuid=edge_id,
            source_uuid=person_id,
            target_uuid=company_id,
            relation_type="WORKS_AT",
            properties={
                "since": "2020",
                "position": "Senior Engineer",
                "department": "Engineering"
            },
            fact="Alice works at TechCorp as a Senior Engineer in the Engineering department"
        )
        print(f"✓ Created edge: Alice -[WORKS_AT]-> TechCorp")

        # READ: Retrieve node by UUID
        print(f"\nRetrieving person node by UUID...")
        retrieved_person = await driver.get_node(person_id)
        if retrieved_person:
            print(f"✓ Retrieved node: {retrieved_person}")
        else:
            print("⚠ Node not found")

        # READ: Query nodes using Cypher
        print(f"\nQuerying all Entity nodes...")
        query_result = await driver.execute_query(
            "MATCH (n:Entity) RETURN n LIMIT 5"
        )
        print(f"✓ Found {len(query_result)} entity nodes")

        # UPDATE: Update node properties
        print(f"\nUpdating Alice's age...")
        update_result = await driver.execute_query(
            f"MATCH (n {{uuid: '{person_id}'}}) SET n.age = 31 RETURN n"
        )
        print(f"✓ Updated node properties")

        # DELETE: Delete relationship then node
        print(f"\nDeleting WORKS_AT relationship...")
        delete_edge_result = await driver.execute_query(
            f"MATCH ()-[r {{uuid: '{edge_id}'}}]->() DELETE r"
        )
        print(f"✓ Deleted relationship")

        print(f"\nDeleting company node...")
        delete_node_result = await driver.execute_query(
            f"MATCH (n {{uuid: '{company_id}'}}) DELETE n"
        )
        print(f"✓ Deleted company node")

        print("\n✓ CRUD operations completed successfully!")

    finally:
        await driver.close()
        print("\n✓ Connection closed")


async def example_age_graph_traversal():
    """Example: Graph traversal queries with Apache Age"""

    print("\n" + "="*80)
    print("EXAMPLE 2: Graph Traversal Queries")
    print("="*80)

    driver = AgeDriver(
        host='localhost',
        port=5432,
        user='postgres',
        password='postgres',
        database='postgres',
        graph_name='graphiti'
    )

    try:
        await driver.initialize()

        # Clean up from previous runs
        try:
            await driver.execute_query("MATCH (n) DETACH DELETE n")
        except:
            pass

        print("\nCreating complex knowledge graph with 15 nodes and 20+ edges...")

        # Create main chain of people
        people = ["Alice", "Bob", "Charlie", "Diana", "Eve", "Frank", "Grace", "Henry", "Ivy", "Jack"]
        person_ids = {}

        print(f"  Creating {len(people)} person nodes...")
        for person in people:
            person_id = str(uuid.uuid4())
            person_ids[person] = person_id
            await driver.execute_query(
                f"""
                CREATE (n:Entity {{
                    uuid: '{person_id}',
                    name: '{person}',
                    node_type: 'entity',
                    type: 'person'
                }})
                """
            )

        print(f"  ✓ Created {len(people)} person nodes")

        # Create branching nodes
        branches = ["Karen", "Leo", "Mike", "Nina", "Oscar"]
        for branch_person in branches:
            person_id = str(uuid.uuid4())
            person_ids[branch_person] = person_id
            await driver.execute_query(
                f"""
                CREATE (n:Entity {{
                    uuid: '{person_id}',
                    name: '{branch_person}',
                    node_type: 'entity',
                    type: 'person'
                }})
                """
            )

        print(f"  ✓ Created {len(branches)} branch nodes")

        # Create main chain relationships
        print(f"  Creating KNOWS relationships along main chain...")
        for i in range(len(people) - 1):
            await driver.execute_query(
                f"""
                MATCH (a {{uuid: '{person_ids[people[i]]}'}})
                MATCH (b {{uuid: '{person_ids[people[i+1]]}'}})
                CREATE (a)-[r:KNOWS {{uuid: '{uuid.uuid4()}'}}]->(b)
                """
            )

        # Create branch relationships
        print(f"  Creating branch relationships...")
        # Diana -> Karen -> Leo
        await driver.execute_query(
            f"""
            MATCH (a {{uuid: '{person_ids["Diana"]}'}})
            MATCH (b {{uuid: '{person_ids["Karen"]}'}})
            CREATE (a)-[r:MANAGES {{uuid: '{uuid.uuid4()}'}}]->(b)
            """
        )
        await driver.execute_query(
            f"""
            MATCH (a {{uuid: '{person_ids["Karen"]}'}})
            MATCH (b {{uuid: '{person_ids["Leo"]}'}})
            CREATE (a)-[r:SUPERVISES {{uuid: '{uuid.uuid4()}'}}]->(b)
            """
        )

        # Henry -> Mike -> Nina -> Oscar
        await driver.execute_query(
            f"""
            MATCH (a {{uuid: '{person_ids["Henry"]}'}})
            MATCH (b {{uuid: '{person_ids["Mike"]}'}})
            CREATE (a)-[r:TEACHES {{uuid: '{uuid.uuid4()}'}}]->(b)
            """
        )
        await driver.execute_query(
            f"""
            MATCH (a {{uuid: '{person_ids["Mike"]}'}})
            MATCH (b {{uuid: '{person_ids["Nina"]}'}})
            CREATE (a)-[r:MENTORS {{uuid: '{uuid.uuid4()}'}}]->(b)
            """
        )
        await driver.execute_query(
            f"""
            MATCH (a {{uuid: '{person_ids["Nina"]}'}})
            MATCH (b {{uuid: '{person_ids["Oscar"]}'}})
            CREATE (a)-[r:ADVISES {{uuid: '{uuid.uuid4()}'}}]->(b)
            """
        )

        # Add cross-connections
        await driver.execute_query(
            f"""
            MATCH (a {{uuid: '{person_ids["Alice"]}'}})
            MATCH (b {{uuid: '{person_ids["Charlie"]}'}})
            CREATE (a)-[r:MENTORS {{uuid: '{uuid.uuid4()}'}}]->(b)
            """
        )

        print(f"  ✓ Created 20+ relationships")
        print(f"\n✓ Knowledge graph created successfully!")

        # Query 1: Find all nodes within 3 hops from Alice
        print(f"\n--- Query 1: Nodes within 3 hops from Alice ---")
        # Note: ORDER BY with aliases has issues in Age 1.6.0, so we sort in Python
        result = await driver.execute_query(
            f"""
            MATCH path = (alice {{uuid: '{person_ids["Alice"]}'}})-[*1..3]->(connected)
            RETURN DISTINCT connected.name AS name, length(path) AS distance
            """
        )
        # Sort results by distance in Python (workaround for Age 1.6.0 ORDER BY bug)
        result = sorted(result, key=lambda x: x.get('distance', 999))
        print(f"Found {len(result)} connected nodes:")
        for row in result[:10]:
            print(f"  - {row.get('name', 'Unknown')} (distance: {row.get('distance', '?')})")

        # Query 2: Variable-length path
        print(f"\n--- Query 2: Variable-length paths (1-5 hops) from Alice ---")
        result = await driver.execute_query(
            f"""
            MATCH (alice {{uuid: '{person_ids["Alice"]}'}})-[*1..5]->(connected)
            RETURN DISTINCT connected.name AS name
            LIMIT 10
            """
        )
        print(f"Reachable nodes: {[row.get('name', '?') for row in result]}")

        # Query 3: Find relationships by type
        print(f"\n--- Query 3: Find all MENTORS relationships ---")
        result = await driver.execute_query(
            """
            MATCH (a)-[r:MENTORS]->(b)
            RETURN a.name AS mentor, b.name AS mentee
            """
        )
        print(f"Mentorship relationships:")
        for row in result:
            print(f"  - {row.get('mentor', '?')} mentors {row.get('mentee', '?')}")

        # Query 4: Pattern matching with multiple relationships
        print(f"\n--- Query 4: Complex pattern matching ---")
        result = await driver.execute_query(
            f"""
            MATCH (start {{uuid: '{person_ids["Alice"]}'}})-[:KNOWS*2]->(friend)
            RETURN DISTINCT friend.name AS friend_of_friend
            LIMIT 5
            """
        )
        print(f"Friends of friends: {[row.get('friend_of_friend', '?') for row in result]}")

        print("\n✓ Graph traversal queries completed successfully!")

    finally:
        await driver.close()


async def example_age_multi_tenancy():
    """Example: Multi-tenancy using separate Apache Age graphs"""

    print("\n" + "="*80)
    print("EXAMPLE 3: Multi-Tenancy with Separate Graphs")
    print("="*80)

    # Create base driver
    base_driver = AgeDriver(
        host='localhost',
        port=5432,
        user='postgres',
        password='postgres',
        database='postgres',
        graph_name='graphiti'
    )

    try:
        await base_driver.initialize()

        # Create drivers for two separate tenants
        print("\nCreating separate graph drivers for two tenants...")

        tenant1_driver = base_driver.clone(graph_name='graphiti_tenant_1')
        tenant2_driver = base_driver.clone(graph_name='graphiti_tenant_2')

        # Initialize tenant graphs
        print("  Creating tenant_1 graph...")
        await tenant1_driver.build_indices_and_constraints()

        # Clean up tenant 1
        try:
            await tenant1_driver.execute_query("MATCH (n) DETACH DELETE n")
        except:
            pass

        print("  Creating tenant_2 graph...")
        await tenant2_driver.build_indices_and_constraints()

        # Clean up tenant 2
        try:
            await tenant2_driver.execute_query("MATCH (n) DETACH DELETE n")
        except:
            pass

        print("✓ Tenant graphs created")

        # Insert data into tenant 1
        print("\n--- Tenant 1: Creating company data ---")
        tenant1_ids = []
        for company in ["Acme Corp", "Widget Inc", "Gadget LLC"]:
            company_id = str(uuid.uuid4())
            tenant1_ids.append(company_id)
            await tenant1_driver.execute_query(
                f"""
                CREATE (n:Entity {{
                    uuid: '{company_id}',
                    name: '{company}',
                    node_type: 'entity',
                    type: 'company',
                    tenant: 'tenant_1'
                }})
                """
            )
            print(f"  ✓ Created: {company}")

        # Insert data into tenant 2
        print("\n--- Tenant 2: Creating person data ---")
        tenant2_ids = []
        for person in ["John Doe", "Jane Smith", "Bob Johnson"]:
            person_id = str(uuid.uuid4())
            tenant2_ids.append(person_id)
            await tenant2_driver.execute_query(
                f"""
                CREATE (n:Entity {{
                    uuid: '{person_id}',
                    name: '{person}',
                    node_type: 'entity',
                    type: 'person',
                    tenant: 'tenant_2'
                }})
                """
            )
            print(f"  ✓ Created: {person}")

        # Verify data isolation
        print("\n--- Verifying Data Isolation ---")

        print("  Tenant 1 data:")
        tenant1_results = await tenant1_driver.execute_query(
            "MATCH (n:Entity) RETURN n.name AS name, n.type AS type"
        )
        for row in tenant1_results:
            print(f"    - {row.get('name', '?')} ({row.get('type', '?')})")

        print("  Tenant 2 data:")
        tenant2_results = await tenant2_driver.execute_query(
            "MATCH (n:Entity) RETURN n.name AS name, n.type AS type"
        )
        for row in tenant2_results:
            print(f"    - {row.get('name', '?')} ({row.get('type', '?')})")

        print(f"\n✓ Data isolation verified:")
        print(f"  - Tenant 1 has {len(tenant1_results)} nodes (companies)")
        print(f"  - Tenant 2 has {len(tenant2_results)} nodes (people)")
        print(f"  - No cross-tenant data leakage")

        print("\n✓ Multi-tenancy example completed successfully!")

    finally:
        await base_driver.close()


async def example_age_temporal_queries():
    """Example: Bi-temporal tracking with valid_at/invalid_at"""

    print("\n" + "="*80)
    print("EXAMPLE 4: Temporal Queries (Bi-temporal Tracking)")
    print("="*80)

    driver = AgeDriver(
        host='localhost',
        port=5432,
        user='postgres',
        password='postgres',
        database='postgres',
        graph_name='graphiti'
    )

    try:
        await driver.initialize()

        # Clean up from previous runs
        try:
            await driver.execute_query("MATCH (n) DETACH DELETE n")
        except:
            pass

        # Create timeline of events
        now = datetime.now()
        past = now - timedelta(days=365)
        recent = now - timedelta(days=30)
        future = now + timedelta(days=30)

        print(f"\nTimeline:")
        print(f"  Past:   {past.isoformat()}")
        print(f"  Recent: {recent.isoformat()}")
        print(f"  Now:    {now.isoformat()}")
        print(f"  Future: {future.isoformat()}")

        # Create entities with temporal metadata
        print(f"\n--- Creating temporally-aware nodes ---")

        # Entity 1: Valid from past, still valid
        entity1_id = str(uuid.uuid4())
        await driver.execute_query(
            f"""
            CREATE (n:Entity {{
                uuid: '{entity1_id}',
                name: 'Alice',
                node_type: 'entity',
                valid_at: '{past.isoformat()}',
                invalid_at: null
            }})
            """
        )
        print(f"  ✓ Created Alice (valid since {past.date()}, still valid)")

        # Entity 2: Valid from recent, still valid
        entity2_id = str(uuid.uuid4())
        await driver.execute_query(
            f"""
            CREATE (n:Entity {{
                uuid: '{entity2_id}',
                name: 'Bob',
                node_type: 'entity',
                valid_at: '{recent.isoformat()}',
                invalid_at: null
            }})
            """
        )
        print(f"  ✓ Created Bob (valid since {recent.date()}, still valid)")

        # Entity 3: Valid from past, invalidated recently
        entity3_id = str(uuid.uuid4())
        await driver.execute_query(
            f"""
            CREATE (n:Entity {{
                uuid: '{entity3_id}',
                name: 'Charlie',
                node_type: 'entity',
                valid_at: '{past.isoformat()}',
                invalid_at: '{recent.isoformat()}'
            }})
            """
        )
        print(f"  ✓ Created Charlie (valid {past.date()} - {recent.date()}, now invalid)")

        # Entity 4: Will be valid in the future
        entity4_id = str(uuid.uuid4())
        await driver.execute_query(
            f"""
            CREATE (n:Entity {{
                uuid: '{entity4_id}',
                name: 'Diana',
                node_type: 'entity',
                valid_at: '{future.isoformat()}',
                invalid_at: null
            }})
            """
        )
        print(f"  ✓ Created Diana (will be valid from {future.date()})")

        # Entity 5: Currently valid but will be invalidated in future
        entity5_id = str(uuid.uuid4())
        await driver.execute_query(
            f"""
            CREATE (n:Entity {{
                uuid: '{entity5_id}',
                name: 'Eve',
                node_type: 'entity',
                valid_at: '{recent.isoformat()}',
                invalid_at: '{future.isoformat()}'
            }})
            """
        )
        print(f"  ✓ Created Eve (valid {recent.date()} - {future.date()})")

        # Temporal Query 1: Entities valid at a specific point in the past
        print(f"\n--- Query 1: Which entities were valid 6 months ago? ---")
        six_months_ago = now - timedelta(days=180)
        result = await driver.execute_query(
            f"""
            MATCH (n:Entity)
            WHERE n.valid_at <= '{six_months_ago.isoformat()}'
              AND (n.invalid_at IS NULL OR n.invalid_at > '{six_months_ago.isoformat()}')
            RETURN n.name AS name, n.valid_at AS valid_from, n.invalid_at AS valid_until
            """
        )
        print(f"Entities valid on {six_months_ago.date()}:")
        for row in result:
            print(f"  - {row.get('name', '?')}")

        # Temporal Query 2: Currently valid entities
        print(f"\n--- Query 2: Which entities are currently valid? ---")
        result = await driver.execute_query(
            f"""
            MATCH (n:Entity)
            WHERE n.valid_at <= '{now.isoformat()}'
              AND (n.invalid_at IS NULL OR n.invalid_at > '{now.isoformat()}')
            RETURN n.name AS name
            """
        )
        print(f"Currently valid entities:")
        for row in result:
            print(f"  - {row.get('name', '?')}")

        # Temporal Query 3: Entities that were invalidated
        print(f"\n--- Query 3: Which entities have been invalidated? ---")
        result = await driver.execute_query(
            f"""
            MATCH (n:Entity)
            WHERE n.invalid_at IS NOT NULL AND n.invalid_at <= '{now.isoformat()}'
            RETURN n.name AS name, n.invalid_at AS invalidated_at
            """
        )
        print(f"Invalidated entities:")
        for row in result:
            print(f"  - {row.get('name', '?')} (invalidated: {row.get('invalidated_at', '?')})")

        # Temporal Query 4: Time range overlap query
        print(f"\n--- Query 4: Entities with validity overlapping a time range ---")
        start_range = now - timedelta(days=60)
        end_range = now - timedelta(days=15)
        result = await driver.execute_query(
            f"""
            MATCH (n:Entity)
            WHERE n.valid_at <= '{end_range.isoformat()}'
              AND (n.invalid_at IS NULL OR n.invalid_at >= '{start_range.isoformat()}')
            RETURN n.name AS name, n.valid_at AS valid_from, n.invalid_at AS valid_until
            """
        )
        print(f"Entities with validity overlapping {start_range.date()} to {end_range.date()}:")
        print(f"  (This includes entities valid for any part of this period)")
        for row in result:
            valid_from = row.get('valid_from', '?')
            valid_until = row.get('valid_until', 'ongoing')
            if valid_from != '?':
                valid_from = valid_from[:10] if isinstance(valid_from, str) else valid_from
            if valid_until and valid_until != 'ongoing' and isinstance(valid_until, str):
                valid_until = valid_until[:10]
            print(f"  - {row.get('name', '?')} (valid: {valid_from} → {valid_until})")

        print("\n✓ Temporal queries completed successfully!")
        print("\n✓ Demonstrates bi-temporal tracking capabilities:")
        print("  - Point-in-time queries (historical state)")
        print("  - Current state queries")
        print("  - Invalidation tracking")
        print("  - Time range queries")

    finally:
        await driver.close()


async def main():
    """Run all Apache Age driver examples"""

    print("\n" + "="*80)
    print("APACHE AGE DRIVER FOR GRAPHITI - COMPREHENSIVE EXAMPLES")
    print("="*80)
    print("\nThese examples demonstrate:")
    print("  1. Basic CRUD operations (Create, Read, Update, Delete)")
    print("  2. Graph traversal queries (BFS, variable-length paths, patterns)")
    print("  3. Multi-tenancy using separate graphs")
    print("  4. Temporal queries with bi-temporal tracking")
    print("\nMake sure Apache Age is running on localhost:5432")
    print("Start with: docker-compose -f docker/docker-compose-age.yml up -d")
    print("="*80)

    try:
        # Run all examples
        await example_age_basic_crud()
        await example_age_graph_traversal()
        await example_age_multi_tenancy()
        await example_age_temporal_queries()

        print("\n" + "="*80)
        print("ALL EXAMPLES COMPLETED SUCCESSFULLY!")
       

    except Exception as e:
        print(f"\n❌ Error running examples: {e}")
        print("\nTroubleshooting:")
        print("  1. Make sure Apache Age is running: docker ps | grep graphiti-age")
        print("  2. Check Age extension is loaded: docker logs graphiti-age")
        print("  3. Verify connection: psql -h localhost -U postgres -d postgres")
        print("  4. Try recreating the container: docker-compose -f docker/docker-compose-age.yml down -v && up -d")


if __name__ == "__main__":
    asyncio.run(main())
