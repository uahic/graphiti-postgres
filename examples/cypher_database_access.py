"""
Complete example demonstrating how to use Cypher to access the PostgreSQL database.

This example shows:
1. Setting up the PostgreSQL driver
2. Creating data using Cypher queries
3. Querying data using Cypher queries
4. Using the driver's execute_query() method with Cypher
5. Direct SQL generation for advanced use cases
"""

import asyncio
import uuid
from datetime import datetime
from graphiti_postgres import PostgresDriver
from graphiti_postgres.cypher import CypherParser, SQLGenerator


async def example_basic_cypher_operations():
    """Example: Create and query nodes using Cypher through the driver"""
    print("\n" + "="*80)
    print("EXAMPLE 1: Basic Cypher Operations")
    print("="*80)

    # Initialize the driver
    driver = PostgresDriver(
        host='localhost',
        port=5432,
        user='postgres',
        password='postgres',
        database='postgres',
        group_id='cypher_demo'
    )

    try:
        # Wait for connection pool initialization
        await asyncio.sleep(0.5)

        # Setup database schema (run once)
        await driver.build_indices_and_constraints(delete_existing=False)
        print("✓ Database schema ready\n")

        # Clean up old test data
        async with driver.pool.acquire() as conn:
            await conn.execute("DELETE FROM graph_edges WHERE group_id = 'cypher_demo'")
            await conn.execute("DELETE FROM graph_nodes WHERE group_id = 'cypher_demo'")
        print("✓ Cleaned up old test data\n")

        # 1. CREATE nodes using Cypher
        print("1. Creating nodes with Cypher:")
        print("-" * 40)

        cypher_create_alice = """
        CREATE (p:Entity {
            name: 'Alice',
            age: 30,
            occupation: 'Software Engineer',
            city: 'San Francisco'
        })
        """
        print(f"Cypher: {cypher_create_alice.strip()}")

        # Execute through driver
        results = await driver.execute_query(cypher_create_alice)
        print(f"✓ Created node: {results[0]['name']}\n")

        cypher_create_bob = """
        CREATE (p:Entity {
            name: 'Bob',
            age: 25,
            occupation: 'Data Scientist',
            city: 'New York'
        })
        """
        results = await driver.execute_query(cypher_create_bob)
        print(f"✓ Created node: {results[0]['name']}\n")

        # 2. MATCH nodes using Cypher
        print("2. Querying nodes with Cypher:")
        print("-" * 40)

        cypher_match = "MATCH (n:Entity) RETURN n.name AS name, n.age AS age, n.city AS city"
        print(f"Cypher: {cypher_match}")

        results = await driver.execute_query(cypher_match)
        print(f"\nFound {len(results)} people:")
        for result in results:
            print(f"  - {result['name']}, age {result['age']}, lives in {result['city']}")

        # 3. MATCH with WHERE clause
        print("\n3. Filtering with WHERE clause:")
        print("-" * 40)

        cypher_where = """
        MATCH (n:Entity)
        WHERE n.age >= 30
        RETURN n.name AS name, n.age AS age
        """
        print(f"Cypher: {cypher_where.strip()}")

        results = await driver.execute_query(cypher_where)
        print(f"\nPeople aged 30 or older:")
        for result in results:
            print(f"  - {result['name']}, age {result['age']}")

        # 4. Using parameters
        print("\n4. Using parameterized queries:")
        print("-" * 40)

        cypher_params = """
        MATCH (n:Entity)
        WHERE n.city = $target_city
        RETURN n.name AS name, n.occupation AS occupation
        """
        print(f"Cypher: {cypher_params.strip()}")
        print(f"Parameters: {{'target_city': 'San Francisco'}}")

        results = await driver.execute_query(
            cypher_params,
            parameters={'target_city': 'San Francisco'}
        )
        print(f"\nPeople in San Francisco:")
        for result in results:
            print(f"  - {result['name']}, {result['occupation']}")

    finally:
        await driver.close()
        print("\n✓ Connection closed")


async def example_cypher_relationships():
    """Example: Create and query relationships using Cypher"""
    print("\n" + "="*80)
    print("EXAMPLE 2: Cypher Relationships")
    print("="*80)

    driver = PostgresDriver(
        host='localhost',
        port=5432,
        user='postgres',
        password='postgres',
        database='postgres',
        group_id='cypher_relationships'
    )

    try:
        await asyncio.sleep(0.5)
        await driver.build_indices_and_constraints(delete_existing=False)

        # Clean up
        async with driver.pool.acquire() as conn:
            await conn.execute("DELETE FROM graph_edges WHERE group_id = 'cypher_relationships'")
            await conn.execute("DELETE FROM graph_nodes WHERE group_id = 'cypher_relationships'")

        print("\n1. Creating nodes and relationships:")
        print("-" * 40)

        # Create nodes first using driver's create_node (to get UUIDs)
        alice_id = str(uuid.uuid4())
        bob_id = str(uuid.uuid4())
        techcorp_id = str(uuid.uuid4())

        await driver.create_node(
            uuid=alice_id,
            name="Alice",
            node_type="entity",
            properties={"type": "person", "age": 30}
        )

        await driver.create_node(
            uuid=bob_id,
            name="Bob",
            node_type="entity",
            properties={"type": "person", "age": 25}
        )

        await driver.create_node(
            uuid=techcorp_id,
            name="TechCorp",
            node_type="entity",
            properties={"type": "company", "industry": "Technology"}
        )

        print("✓ Created 3 nodes: Alice, Bob, TechCorp")

        # Create relationships
        await driver.create_edge(
            uuid=str(uuid.uuid4()),
            source_uuid=alice_id,
            target_uuid=bob_id,
            relation_type="KNOWS"
        )

        await driver.create_edge(
            uuid=str(uuid.uuid4()),
            source_uuid=alice_id,
            target_uuid=techcorp_id,
            relation_type="WORKS_AT"
        )

        await driver.create_edge(
            uuid=str(uuid.uuid4()),
            source_uuid=bob_id,
            target_uuid=techcorp_id,
            relation_type="WORKS_AT"
        )

        print("✓ Created relationships: Alice KNOWS Bob, both WORKS_AT TechCorp\n")

        # Query relationships using Cypher
        print("2. Querying relationships with Cypher:")
        print("-" * 40)

        cypher_rel = """
        MATCH (a:Entity)-[r:KNOWS]->(b:Entity)
        RETURN a.name AS person1, b.name AS person2
        """
        print(f"Cypher: {cypher_rel.strip()}")

        results = await driver.execute_query(cypher_rel)
        print(f"\nKNOWS relationships:")
        for result in results:
            print(f"  - {result['person1']} knows {result['person2']}")

        # Query people working at the same company
        print("\n3. Finding coworkers:")
        print("-" * 40)

        cypher_coworkers = """
        MATCH (p:Entity)-[:WORKS_AT]->(c:Entity)<-[:WORKS_AT]-(coworker:Entity)
        WHERE p.name = 'Alice' AND p <> coworker
        RETURN coworker.name AS coworker_name
        """
        print(f"Cypher: {cypher_coworkers.strip()}")

        results = await driver.execute_query(cypher_coworkers)
        print(f"\nAlice's coworkers:")
        for result in results:
            print(f"  - {result['coworker_name']}")

    finally:
        await driver.close()
        print("\n✓ Connection closed")


async def example_advanced_cypher():
    """Example: Advanced Cypher queries with aggregation, sorting, and limiting"""
    print("\n" + "="*80)
    print("EXAMPLE 3: Advanced Cypher Queries")
    print("="*80)

    driver = PostgresDriver(
        host='localhost',
        port=5432,
        user='postgres',
        password='postgres',
        database='postgres',
        group_id='cypher_advanced'
    )

    try:
        await asyncio.sleep(0.5)
        await driver.build_indices_and_constraints(delete_existing=False)

        # Clean up
        async with driver.pool.acquire() as conn:
            await conn.execute("DELETE FROM graph_edges WHERE group_id = 'cypher_advanced'")
            await conn.execute("DELETE FROM graph_nodes WHERE group_id = 'cypher_advanced'")

        print("\n1. Creating sample data:")
        print("-" * 40)

        # Create multiple people
        people = [
            ("Alice", 30, "San Francisco", 120000),
            ("Bob", 25, "New York", 95000),
            ("Charlie", 35, "San Francisco", 150000),
            ("Diana", 28, "New York", 105000),
            ("Eve", 32, "San Francisco", 135000),
        ]

        for name, age, city, salary in people:
            await driver.create_node(
                uuid=str(uuid.uuid4()),
                name=name,
                node_type="entity",
                properties={
                    "type": "person",
                    "age": age,
                    "city": city,
                    "salary": salary
                }
            )

        print(f"✓ Created {len(people)} people\n")

        # Aggregation query
        print("2. Aggregation - Count people by city:")
        print("-" * 40)

        cypher_agg = """
        MATCH (p:Entity)
        WHERE p.city IS NOT NULL
        RETURN p.city AS city, COUNT(p) AS people_count
        ORDER BY people_count DESC
        """
        print(f"Cypher: {cypher_agg.strip()}")

        results = await driver.execute_query(cypher_agg)
        print(f"\nPeople per city:")
        for result in results:
            print(f"  - {result['city']}: {result['people_count']} people")

        # Average salary by city
        print("\n3. Average salary by city:")
        print("-" * 40)

        cypher_avg = """
        MATCH (p:Entity)
        WHERE p.salary IS NOT NULL
        RETURN p.city AS city, AVG(p.salary) AS avg_salary
        ORDER BY avg_salary DESC
        """
        print(f"Cypher: {cypher_avg.strip()}")

        results = await driver.execute_query(cypher_avg)
        print(f"\nAverage salary per city:")
        for result in results:
            avg_sal = result['avg_salary']
            print(f"  - {result['city']}: ${avg_sal:,.2f}")

        # Top earners with LIMIT
        print("\n4. Top 3 earners:")
        print("-" * 40)

        cypher_top = """
        MATCH (p:Entity)
        WHERE p.salary IS NOT NULL
        RETURN p.name AS name, p.salary AS salary
        ORDER BY p.salary DESC
        LIMIT 3
        """
        print(f"Cypher: {cypher_top.strip()}")

        results = await driver.execute_query(cypher_top)
        print(f"\nTop earners:")
        for i, result in enumerate(results, 1):
            print(f"  {i}. {result['name']}: ${int(result['salary']):,}")

        # Complex WHERE with multiple conditions
        print("\n5. Complex filtering - Young professionals in SF:")
        print("-" * 40)

        cypher_complex = """
        MATCH (p:Entity)
        WHERE p.age < 35
          AND p.city = 'San Francisco'
          AND p.salary > 100000
        RETURN p.name AS name, p.age AS age, p.salary AS salary
        ORDER BY p.age
        """
        print(f"Cypher: {cypher_complex.strip()}")

        results = await driver.execute_query(cypher_complex)
        print(f"\nYoung SF professionals (age < 35, salary > $100k):")
        for result in results:
            print(f"  - {result['name']}, age {result['age']}, ${int(result['salary']):,}")

    finally:
        await driver.close()
        print("\n✓ Connection closed")


async def example_direct_sql_generation():
    """Example: Using the Cypher parser directly for SQL generation"""
    print("\n" + "="*80)
    print("EXAMPLE 4: Direct SQL Generation (Advanced)")
    print("="*80)

    print("\nFor advanced use cases, you can use the parser directly:")
    print("-" * 40)

    # Initialize parser and SQL generator
    parser = CypherParser()
    sql_generator = SQLGenerator(group_id='my_app')

    # Example Cypher query
    cypher = """
    MATCH (a:Person)-[:KNOWS*1..3]->(b:Person)
    WHERE a.age > 25 AND b.city = 'NYC'
    RETURN a.name, COUNT(b) AS friend_count
    ORDER BY friend_count DESC
    LIMIT 10
    """

    print(f"Cypher Query:\n{cypher.strip()}\n")

    try:
        # Parse Cypher to AST
        ast = parser.parse(cypher)
        print("✓ Parsed successfully to AST\n")

        # Generate SQL
        sql, params = sql_generator.generate(ast, {})

        print("Generated SQL:")
        print("-" * 40)
        print(sql)
        print(f"\nParameters: {params}")

        print("\nYou can then execute this SQL directly:")
        print("-" * 40)
        print("async with driver.pool.acquire() as conn:")
        print("    results = await conn.fetch(sql, *params)")

    except Exception as e:
        print(f"Error: {e}")

    print("\nThis approach gives you:")
    print("  ✓ Full control over SQL execution")
    print("  ✓ Ability to inspect and modify generated SQL")
    print("  ✓ Better debugging capabilities")
    print("  ✓ Custom query optimization")


async def example_comparison():
    """Example: Comparison of different approaches"""
    print("\n" + "="*80)
    print("EXAMPLE 5: Comparison of Approaches")
    print("="*80)

    driver = PostgresDriver(
        host='localhost',
        port=5432,
        user='postgres',
        password='postgres',
        database='postgres',
        group_id='cypher_comparison'
    )

    try:
        await asyncio.sleep(0.5)
        await driver.build_indices_and_constraints(delete_existing=False)

        # Clean up
        async with driver.pool.acquire() as conn:
            await conn.execute("DELETE FROM graph_edges WHERE group_id = 'cypher_comparison'")
            await conn.execute("DELETE FROM graph_nodes WHERE group_id = 'cypher_comparison'")

        # Create test node
        node_id = str(uuid.uuid4())

        print("\n1. APPROACH A: Using driver.create_node() (Direct API)")
        print("-" * 40)
        print("Code:")
        print("  await driver.create_node(")
        print("      uuid=node_id,")
        print("      name='Alice',")
        print("      node_type='entity',")
        print("      properties={'age': 30, 'city': 'NYC'}")
        print("  )")

        await driver.create_node(
            uuid=node_id,
            name='Alice',
            node_type='entity',
            properties={'age': 30, 'city': 'NYC'}
        )
        print("✓ Node created\n")

        print("Pros: Simple, type-safe, direct")
        print("Cons: Less flexible, no query composition\n")

        print("2. APPROACH B: Using driver.execute_query() with Cypher")
        print("-" * 40)
        print("Code:")
        print("  cypher = \"MATCH (n:Entity {name: 'Alice'}) RETURN n\"")
        print("  results = await driver.execute_query(cypher)")

        cypher = "MATCH (n:Entity {name: 'Alice'}) RETURN n"
        results = await driver.execute_query(cypher)
        print(f"✓ Found {len(results)} nodes\n")

        print("Pros: Flexible, query composition, familiar syntax")
        print("Cons: Runtime translation overhead\n")

        print("3. APPROACH C: Direct SQL with parser (Advanced)")
        print("-" * 40)
        print("Code:")
        print("  parser = CypherParser()")
        print("  sql_gen = SQLGenerator(group_id='my_app')")
        print("  ast = parser.parse(cypher)")
        print("  sql, params = sql_gen.generate(ast, {})")
        print("  async with driver.pool.acquire() as conn:")
        print("      results = await conn.fetch(sql, *params)")

        parser = CypherParser()
        sql_gen = SQLGenerator(group_id='cypher_comparison')
        ast = parser.parse(cypher)
        sql, params = sql_gen.generate(ast, {})

        async with driver.pool.acquire() as conn:
            results = await conn.fetch(sql, *params)

        print(f"✓ Found {len(results)} nodes\n")

        print("Pros: Full control, can optimize SQL, best performance")
        print("Cons: More code, requires understanding of parser\n")

        print("RECOMMENDATION:")
        print("  - Use Approach A (direct API) for simple CRUD operations")
        print("  - Use Approach B (execute_query) for most queries")
        print("  - Use Approach C (direct SQL) for performance-critical paths")

    finally:
        await driver.close()
        print("\n✓ Connection closed")


async def main():
    """Run all examples"""
    print("\n" + "="*80)
    print("CYPHER DATABASE ACCESS - COMPLETE EXAMPLES")
    print("="*80)
    print("\nThese examples show how to use Cypher queries to access")
    print("the PostgreSQL database through the Graphiti driver.")
    print("\nMAKE SURE PostgreSQL is running and accessible!")
    print("="*80)

    try:
        await example_basic_cypher_operations()
        await example_cypher_relationships()
        await example_advanced_cypher()
        await example_direct_sql_generation()
        await example_comparison()

        print("\n" + "="*80)
        print("ALL EXAMPLES COMPLETED SUCCESSFULLY!")
        print("="*80)
        print("\nNext steps:")
        print("  1. Try modifying the Cypher queries")
        print("  2. Experiment with different WHERE clauses")
        print("  3. Create more complex relationship patterns")
        print("  4. Check the generated SQL with logging.DEBUG level")
        print("="*80 + "\n")

    except Exception as e:
        print(f"\n❌ Error running examples: {e}")
        print("\nMake sure:")
        print("  - PostgreSQL is running (docker-compose up -d)")
        print("  - Database credentials are correct")
        print("  - Schema is initialized (run example_usage.py first)")
        raise


if __name__ == '__main__':
    asyncio.run(main())
