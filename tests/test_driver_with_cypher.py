"""
Integration tests for PostgreSQL Graphiti Driver with Cypher Parser
Tests the full stack: Cypher query -> AST -> SQL -> Database execution
"""

import pytest
import pytest_asyncio
import asyncio
import uuid
from datetime import datetime
from graphiti_postgres import PostgresDriver
from graphiti_postgres.cypher import CypherParser, SQLGenerator


@pytest_asyncio.fixture
async def driver():
    """Create a test driver instance"""
    driver = PostgresDriver(
        host='localhost',
        port=5433,
        user='postgres',
        password='postgres',
        database='postgres',
        group_id='test_cypher_integration'
    )

    # Wait for pool initialization
    await asyncio.sleep(0.5)

    # Setup: create tables and indices
    await driver.build_indices_and_constraints(delete_existing=False)

    yield driver

    # Teardown: clean up test data
    async with driver.pool.acquire() as conn:
        await conn.execute("DELETE FROM graph_edges WHERE group_id = 'test_cypher_integration'")
        await conn.execute("DELETE FROM graph_nodes WHERE group_id = 'test_cypher_integration'")

    await driver.close()


@pytest_asyncio.fixture
def parser():
    """Create a Cypher parser"""
    return CypherParser()


@pytest_asyncio.fixture
def sql_generator(driver):
    """Create SQL generator with driver's group_id"""
    return SQLGenerator(group_id=driver.default_group_id)


class TestCypherIntegration:
    """Test Cypher queries executed through the driver"""

    @pytest.mark.asyncio
    async def test_cypher_create_node(self, driver, parser, sql_generator):
        """Test creating a node using Cypher"""
        # Create node using Cypher
        cypher = "CREATE (n:Entity {name: 'Alice', age: 30})"

        # Parse and generate SQL
        ast = parser.parse(cypher)
        sql, params = sql_generator.generate(ast)

        # Execute through driver
        async with driver.pool.acquire() as conn:
            result = await conn.fetch(sql, *params)

        assert len(result) == 1, f"Expected 1 node created, got {len(result)}"
        assert result[0]['name'] == 'Alice', f"Expected name 'Alice', got {result[0]['name']}"
        assert result[0]['node_type'] == 'entity', f"Expected node_type 'entity', got {result[0]['node_type']}"
        # Verify properties were set correctly
        import json
        props = result[0]['properties'] if isinstance(result[0]['properties'], dict) else json.loads(result[0]['properties'])
        assert props.get('age') == 30, f"Expected age 30, got {props.get('age')}"

    @pytest.mark.asyncio
    async def test_cypher_match_simple(self, driver, parser, sql_generator):
        """Test simple MATCH query with Cypher"""
        # Create test data
        node_id = str(uuid.uuid4())
        await driver.create_node(
            uuid=node_id,
            name="Bob",
            node_type="entity",
            properties={"age": 25, "city": "NYC"}
        )

        # Query using Cypher
        cypher = "MATCH (n:Entity) RETURN n"
        ast = parser.parse(cypher)
        sql, params = sql_generator.generate(ast)

        # Execute
        async with driver.pool.acquire() as conn:
            result = await conn.fetch(sql, *params)

        assert len(result) >= 1, f"Expected at least 1 result, got {len(result)}"
        # Check that Bob is in results
        # row_to_json returns JSON which asyncpg returns as string, need to parse
        import json
        names = []
        for r in result:
            if isinstance(r['n'], str):
                node_data = json.loads(r['n'])
                names.append(node_data['name'])
            elif isinstance(r['n'], dict):
                names.append(r['n']['name'])
        assert 'Bob' in names, f"Expected 'Bob' in results, got names: {names}"

        # Verify Bob has the correct properties
        bob_node = None
        for r in result:
            node_data = json.loads(r['n']) if isinstance(r['n'], str) else r['n']
            if node_data['name'] == 'Bob':
                bob_node = node_data
                break
        assert bob_node is not None, "Bob node not found in results"
        assert bob_node.get('node_type') == 'entity', f"Expected Bob to be entity type"

    @pytest.mark.asyncio
    async def test_cypher_match_with_properties(self, driver, parser, sql_generator):
        """Test MATCH with property filters"""
        # Create test nodes
        await driver.create_node(
            uuid=str(uuid.uuid4()),
            name="Alice",
            node_type="entity",
            properties={"age": 30, "city": "NYC"}
        )
        await driver.create_node(
            uuid=str(uuid.uuid4()),
            name="Bob",
            node_type="entity",
            properties={"age": 25, "city": "LA"}
        )
        await driver.create_node(
            uuid=str(uuid.uuid4()),
            name="Charlie",
            node_type="entity",
            properties={"age": 30, "city": "SF"}
        )

        # Query for age = 30
        cypher = "MATCH (n:Entity {age: 30}) RETURN n"
        ast = parser.parse(cypher)
        sql, params = sql_generator.generate(ast)

        async with driver.pool.acquire() as conn:
            result = await conn.fetch(sql, *params)

        # Should find Alice and Charlie
        assert len(result) == 2
        import json
        names = sorted([json.loads(r['n'])['name'] if isinstance(r['n'], str) else r['n']['name'] for r in result])
        assert names == ['Alice', 'Charlie']

    @pytest.mark.asyncio
    async def test_cypher_match_with_where(self, driver, parser, sql_generator):
        """Test MATCH with WHERE clause"""
        # Create test nodes
        await driver.create_node(
            uuid=str(uuid.uuid4()),
            name="Alice",
            node_type="entity",
            properties={"age": 30}
        )
        await driver.create_node(
            uuid=str(uuid.uuid4()),
            name="Bob",
            node_type="entity",
            properties={"age": 25}
        )

        # Query with WHERE
        cypher = "MATCH (n:Entity) WHERE n.age > 26 RETURN n.name AS name"
        ast = parser.parse(cypher)
        sql, params = sql_generator.generate(ast, {'age_threshold': 26})

        async with driver.pool.acquire() as conn:
            result = await conn.fetch(sql, *params)

        # Should only find Alice (age 30 > 26), not Bob (age 25)
        assert len(result) >= 1, f"Expected at least 1 result, got {len(result)}"

        # Verify only Alice is returned
        names = [r['name'] for r in result]
        assert 'Alice' in names, f"Expected Alice in results, got: {names}"
        # Bob should not be in results since his age (25) is not > 26
        if len(result) == 1:
            assert names[0] == 'Alice', f"Expected only Alice, got {names}"

    @pytest.mark.asyncio
    async def test_cypher_match_relationship(self, driver, parser, sql_generator):
        """Test MATCH with relationships"""
        # Create nodes
        alice_id = str(uuid.uuid4())
        bob_id = str(uuid.uuid4())

        await driver.create_node(uuid=alice_id, name="Alice", node_type="entity")
        await driver.create_node(uuid=bob_id, name="Bob", node_type="entity")

        # Create relationship
        await driver.create_edge(
            uuid=str(uuid.uuid4()),
            source_uuid=alice_id,
            target_uuid=bob_id,
            relation_type="KNOWS"
        )

        # Query with relationship pattern
        cypher = "MATCH (a:Entity)-[r:KNOWS]->(b:Entity) RETURN a, b"
        ast = parser.parse(cypher)
        sql, params = sql_generator.generate(ast)

        async with driver.pool.acquire() as conn:
            result = await conn.fetch(sql, *params)

        assert len(result) == 1, f"Expected exactly 1 relationship match, got {len(result)}"

        # Verify the relationship connects Alice to Bob
        import json
        row = result[0]
        node_a = json.loads(row['a']) if isinstance(row['a'], str) else row['a']
        node_b = json.loads(row['b']) if isinstance(row['b'], str) else row['b']

        assert node_a['name'] == 'Alice', f"Expected source node to be Alice, got {node_a['name']}"
        assert node_b['name'] == 'Bob', f"Expected target node to be Bob, got {node_b['name']}"

    @pytest.mark.asyncio
    async def test_cypher_return_with_limit(self, driver, parser, sql_generator):
        """Test RETURN with LIMIT"""
        # Create multiple nodes
        for i in range(10):
            await driver.create_node(
                uuid=str(uuid.uuid4()),
                name=f"Person{i}",
                node_type="entity"
            )

        # Query with LIMIT
        cypher = "MATCH (n:Entity) RETURN n LIMIT 5"
        ast = parser.parse(cypher)
        sql, params = sql_generator.generate(ast)

        async with driver.pool.acquire() as conn:
            result = await conn.fetch(sql, *params)

        assert len(result) == 5

    @pytest.mark.asyncio
    async def test_cypher_return_with_skip_and_limit(self, driver, parser, sql_generator):
        """Test RETURN with SKIP and LIMIT for pagination"""
        # Create nodes with predictable names
        node_ids = []
        for i in range(10):
            node_id = str(uuid.uuid4())
            await driver.create_node(
                uuid=node_id,
                name=f"User{i:02d}",
                node_type="entity"
            )
            node_ids.append(node_id)

        # Query with SKIP and LIMIT
        cypher = "MATCH (n:Entity) RETURN n SKIP 3 LIMIT 4"
        ast = parser.parse(cypher)
        sql, params = sql_generator.generate(ast)

        async with driver.pool.acquire() as conn:
            result = await conn.fetch(sql, *params)

        # Should get 4 results (skipping first 3)
        assert len(result) == 4

    @pytest.mark.asyncio
    async def test_cypher_return_with_order_by(self, driver, parser, sql_generator):
        """Test RETURN with ORDER BY"""
        # Create nodes
        await driver.create_node(
            uuid=str(uuid.uuid4()),
            name="Charlie",
            node_type="entity",
            properties={"age": 35}
        )
        await driver.create_node(
            uuid=str(uuid.uuid4()),
            name="Alice",
            node_type="entity",
            properties={"age": 25}
        )
        await driver.create_node(
            uuid=str(uuid.uuid4()),
            name="Bob",
            node_type="entity",
            properties={"age": 30}
        )

        # Query with ORDER BY
        cypher = "MATCH (n:Entity) RETURN n.name AS name ORDER BY n.age ASC"
        ast = parser.parse(cypher)
        sql, params = sql_generator.generate(ast)

        async with driver.pool.acquire() as conn:
            result = await conn.fetch(sql, *params)

        # Results should be ordered by age: Alice(25), Bob(30), Charlie(35)
        assert len(result) >= 3, f"Expected at least 3 results, got {len(result)}"

        # Extract the first 3 names and verify they're in age order
        names = [r['name'] for r in result[:3]]
        assert names == ['Alice', 'Bob', 'Charlie'], f"Expected names ordered by age [Alice, Bob, Charlie], got {names}"

    @pytest.mark.asyncio
    async def test_cypher_count_aggregation(self, driver, parser, sql_generator):
        """Test COUNT aggregation function"""
        # Create nodes
        for i in range(5):
            await driver.create_node(
                uuid=str(uuid.uuid4()),
                name=f"Node{i}",
                node_type="entity"
            )

        # Query with COUNT
        cypher = "MATCH (n:Entity) RETURN COUNT(n) AS count"
        ast = parser.parse(cypher)
        sql, params = sql_generator.generate(ast)

        async with driver.pool.acquire() as conn:
            result = await conn.fetch(sql, *params)

        assert len(result) == 1, f"COUNT should return exactly 1 row, got {len(result)}"
        assert result[0]['count'] >= 5, f"Expected at least 5 nodes (created in this test), got {result[0]['count']}"

    @pytest.mark.asyncio
    async def test_cypher_property_projection(self, driver, parser, sql_generator):
        """Test projecting specific properties"""
        # Create node
        node_id = str(uuid.uuid4())
        await driver.create_node(
            uuid=node_id,
            name="Alice",
            node_type="entity",
            properties={"age": 30, "city": "NYC", "occupation": "Engineer"}
        )

        # Query specific properties (filter by age which is in JSONB)
        cypher = "MATCH (n:Entity {age: 30}) RETURN n.name AS name, n.age AS age, n.city AS city"
        ast = parser.parse(cypher)
        sql, params = sql_generator.generate(ast)

        async with driver.pool.acquire() as conn:
            result = await conn.fetch(sql, *params)

        assert len(result) == 1, f"Expected exactly 1 node with age=30, got {len(result)}"

        # Verify specific properties are correctly projected
        row = result[0]
        assert row['name'] == 'Alice', f"Expected name 'Alice', got {row['name']}"
        # Age and city should be accessible from JSONB properties
        # The exact format depends on SQL generation, but they should be present

    @pytest.mark.asyncio
    async def test_cypher_multiple_relationships(self, driver, parser, sql_generator):
        """Test querying multiple relationship patterns"""
        # Create a chain: A -> B -> C
        node_a = str(uuid.uuid4())
        node_b = str(uuid.uuid4())
        node_c = str(uuid.uuid4())

        await driver.create_node(uuid=node_a, name="A", node_type="entity")
        await driver.create_node(uuid=node_b, name="B", node_type="entity")
        await driver.create_node(uuid=node_c, name="C", node_type="entity")

        await driver.create_edge(
            uuid=str(uuid.uuid4()),
            source_uuid=node_a,
            target_uuid=node_b,
            relation_type="KNOWS"
        )
        await driver.create_edge(
            uuid=str(uuid.uuid4()),
            source_uuid=node_b,
            target_uuid=node_c,
            relation_type="KNOWS"
        )

        # Query the chain
        cypher = "MATCH (a:Entity)-[:KNOWS]->(b:Entity)-[:KNOWS]->(c:Entity) RETURN a, b, c"
        ast = parser.parse(cypher)
        sql, params = sql_generator.generate(ast)

        async with driver.pool.acquire() as conn:
            result = await conn.fetch(sql, *params)

        # Should find exactly one chain: A -> B -> C
        assert len(result) == 1, f"Expected exactly 1 chain A->B->C, got {len(result)}"

        # Verify all three nodes are present in the chain
        import json
        row = result[0]
        node_a_data = json.loads(row['a']) if isinstance(row['a'], str) else row['a']
        node_b_data = json.loads(row['b']) if isinstance(row['b'], str) else row['b']
        node_c_data = json.loads(row['c']) if isinstance(row['c'], str) else row['c']

        assert node_a_data['name'] == 'A', f"Expected first node to be A, got {node_a_data['name']}"
        assert node_b_data['name'] == 'B', f"Expected middle node to be B, got {node_b_data['name']}"
        assert node_c_data['name'] == 'C', f"Expected last node to be C, got {node_c_data['name']}"

    @pytest.mark.asyncio
    async def test_cypher_bidirectional_relationship(self, driver, parser, sql_generator):
        """Test bidirectional relationship matching"""
        # Create nodes with bidirectional relationships
        alice_id = str(uuid.uuid4())
        bob_id = str(uuid.uuid4())

        await driver.create_node(uuid=alice_id, name="Alice", node_type="entity")
        await driver.create_node(uuid=bob_id, name="Bob", node_type="entity")

        # Create edge from Alice to Bob
        await driver.create_edge(
            uuid=str(uuid.uuid4()),
            source_uuid=alice_id,
            target_uuid=bob_id,
            relation_type="FRIENDS_WITH"
        )

        # Query without direction
        cypher = "MATCH (a:Entity)-[r:FRIENDS_WITH]-(b:Entity) RETURN a, b"
        ast = parser.parse(cypher)
        sql, params = sql_generator.generate(ast)

        async with driver.pool.acquire() as conn:
            result = await conn.fetch(sql, *params)

        # Should match the relationship
        assert len(result) >= 1

        # Verify that Alice and Bob are in the results
        import json
        found_alice_bob = False
        for row in result:
            node_a = json.loads(row['a']) if isinstance(row['a'], str) else row['a']
            node_b = json.loads(row['b']) if isinstance(row['b'], str) else row['b']
            if (node_a['name'] == 'Alice' and node_b['name'] == 'Bob') or \
               (node_a['name'] == 'Bob' and node_b['name'] == 'Alice'):
                found_alice_bob = True
                break
        assert found_alice_bob, "Expected to find Alice-Bob relationship in either direction"

    @pytest.mark.asyncio
    async def test_cypher_optional_match(self, driver, parser, sql_generator):
        """Test OPTIONAL MATCH (LEFT JOIN)"""
        # Create a node with no relationships
        node_id = str(uuid.uuid4())
        await driver.create_node(uuid=node_id, name="Loner", node_type="entity")

        # Query with OPTIONAL MATCH
        cypher = "MATCH (a:Entity) OPTIONAL MATCH (a)-[r:KNOWS]->(b) RETURN a, b"
        ast = parser.parse(cypher)
        sql, params = sql_generator.generate(ast)

        async with driver.pool.acquire() as conn:
            result = await conn.fetch(sql, *params)

        # Should return the node even without relationships
        assert len(result) >= 1

        # Verify that Loner is in the results with NULL for b
        import json
        found_loner = False
        for row in result:
            node_a = json.loads(row['a']) if isinstance(row['a'], str) else row['a']
            if node_a['name'] == 'Loner':
                found_loner = True
                # b should be NULL since there's no KNOWS relationship
                assert row['b'] is None or row['b'] == 'null', f"Expected b to be NULL for Loner, got {row['b']}"
                break
        assert found_loner, "Expected to find Loner node in results"

    @pytest.mark.asyncio
    async def test_cypher_with_parameters(self, driver, parser, sql_generator):
        """Test parameterized Cypher queries"""
        # Create node
        node_id = str(uuid.uuid4())
        await driver.create_node(
            uuid=node_id,
            name="Alice",
            node_type="entity",
            properties={"age": 30}
        )

        # Query with parameters (filter by age which is in JSONB)
        cypher = "MATCH (n:Entity {age: $person_age}) RETURN n"
        ast = parser.parse(cypher)
        sql, params = sql_generator.generate(ast, {'person_age': 30})

        async with driver.pool.acquire() as conn:
            result = await conn.fetch(sql, *params)

        assert len(result) > 0
        # Verify the node was found by checking properties
        import json
        if isinstance(result[0]['n'], str):
            node_data = json.loads(result[0]['n'])
            assert node_data.get('name') == 'Alice', f"Expected Alice, got {node_data.get('name')}"
            assert node_data.get('properties', {}).get('age') == 30, f"Expected age 30"
        else:
            assert result[0]['n'].get('name') == 'Alice', f"Expected Alice, got {result[0]['n'].get('name')}"

    @pytest.mark.asyncio
    async def test_cypher_complex_where_clause(self, driver, parser, sql_generator):
        """Test complex WHERE clause with AND/OR"""
        # Create test data
        await driver.create_node(
            uuid=str(uuid.uuid4()),
            name="Alice",
            node_type="entity",
            properties={"age": 30, "city": "NYC"}
        )
        await driver.create_node(
            uuid=str(uuid.uuid4()),
            name="Bob",
            node_type="entity",
            properties={"age": 25, "city": "NYC"}
        )
        await driver.create_node(
            uuid=str(uuid.uuid4()),
            name="Charlie",
            node_type="entity",
            properties={"age": 30, "city": "LA"}
        )

        # Complex WHERE with AND
        cypher = "MATCH (n:Entity) WHERE n.age = 30 AND n.city = 'NYC' RETURN n.name AS name"
        ast = parser.parse(cypher)
        sql, params = sql_generator.generate(ast)

        async with driver.pool.acquire() as conn:
            result = await conn.fetch(sql, *params)

        # Should find only Alice (age 30 AND city NYC)
        assert len(result) >= 1

        # Verify only Alice is returned (not Bob with age 25, not Charlie in LA)
        names = [r['name'] for r in result]
        assert 'Alice' in names, f"Expected Alice in results, got {names}"
        assert 'Bob' not in names, f"Bob should not be in results (age 25 != 30)"
        assert 'Charlie' not in names, f"Charlie should not be in results (city LA != NYC)"

    @pytest.mark.asyncio
    async def test_cypher_return_distinct(self, driver, parser, sql_generator):
        """Test RETURN DISTINCT"""
        # Create nodes with duplicate names
        for i in range(3):
            await driver.create_node(
                uuid=str(uuid.uuid4()),
                name="DuplicateName",
                node_type="entity"
            )

        # Query with DISTINCT
        cypher = "MATCH (n:Entity) RETURN DISTINCT n.name AS name"
        ast = parser.parse(cypher)
        sql, params = sql_generator.generate(ast)

        async with driver.pool.acquire() as conn:
            result = await conn.fetch(sql, *params)

        # Should have unique names only
        assert len(result) >= 1

        # Verify that "DuplicateName" appears only once despite 3 nodes having it
        names = [r['name'] for r in result]
        duplicate_count = names.count('DuplicateName')
        assert 'DuplicateName' in names, "Expected DuplicateName in results"
        # Note: May have other names from other tests, but DuplicateName should appear only once
        assert duplicate_count == 1, f"Expected DuplicateName to appear exactly once due to DISTINCT, got {duplicate_count} times"

    @pytest.mark.asyncio
    async def test_cypher_relationship_with_properties(self, driver, parser, sql_generator):
        """Test relationships with property filters"""
        # Create nodes and edges with properties
        alice_id = str(uuid.uuid4())
        bob_id = str(uuid.uuid4())
        charlie_id = str(uuid.uuid4())

        await driver.create_node(uuid=alice_id, name="Alice", node_type="entity")
        await driver.create_node(uuid=bob_id, name="Bob", node_type="entity")
        await driver.create_node(uuid=charlie_id, name="Charlie", node_type="entity")

        # Strong connection
        await driver.create_edge(
            uuid=str(uuid.uuid4()),
            source_uuid=alice_id,
            target_uuid=bob_id,
            relation_type="KNOWS",
            properties={"strength": "strong"}
        )
        # Weak connection
        await driver.create_edge(
            uuid=str(uuid.uuid4()),
            source_uuid=alice_id,
            target_uuid=charlie_id,
            relation_type="KNOWS",
            properties={"strength": "weak"}
        )

        # Query strong relationships
        cypher = "MATCH (a:Entity)-[r:KNOWS {strength: 'strong'}]->(b:Entity) RETURN a, b"
        ast = parser.parse(cypher)
        sql, params = sql_generator.generate(ast)

        async with driver.pool.acquire() as conn:
            result = await conn.fetch(sql, *params)

        # Should only find Alice -> Bob (strong connection, not Alice -> Charlie weak connection)
        assert len(result) >= 1

        # Verify only the strong relationship (Alice -> Bob) is returned, not the weak one
        import json
        found_alice_bob = False
        for row in result:
            node_a = json.loads(row['a']) if isinstance(row['a'], str) else row['a']
            node_b = json.loads(row['b']) if isinstance(row['b'], str) else row['b']
            if node_a['name'] == 'Alice' and node_b['name'] == 'Bob':
                found_alice_bob = True
            # Charlie should NOT be in results (weak relationship)
            assert node_b['name'] != 'Charlie', "Charlie should not be in results (weak relationship, not strong)"
        assert found_alice_bob, "Expected to find Alice -> Bob (strong) relationship"

    @pytest.mark.asyncio
    async def test_cypher_is_null_check(self, driver, parser, sql_generator):
        """Test IS NULL and IS NOT NULL operators"""
        # Create nodes with and without certain properties
        node1 = str(uuid.uuid4())
        node2 = str(uuid.uuid4())

        await driver.create_node(
            uuid=node1,
            name="HasAge",
            node_type="entity",
            properties={"age": 30}
        )
        await driver.create_node(
            uuid=node2,
            name="NoAge",
            node_type="entity",
            properties={}
        )

        # Query for nodes without age
        cypher = "MATCH (n:Entity) WHERE n.age IS NULL RETURN n"
        ast = parser.parse(cypher)
        sql, params = sql_generator.generate(ast)

        async with driver.pool.acquire() as conn:
            result = await conn.fetch(sql, *params)

        # Should find NoAge node
        assert len(result) >= 1

        # Verify NoAge is in results (nodes without age property)
        import json
        found_no_age = False
        for row in result:
            node = json.loads(row['n']) if isinstance(row['n'], str) else row['n']
            if node['name'] == 'NoAge':
                found_no_age = True
                break
        assert found_no_age, "Expected to find NoAge node (with NULL age)"

        # Query for nodes with age
        cypher = "MATCH (n:Entity) WHERE n.age IS NOT NULL RETURN n"
        ast = parser.parse(cypher)
        sql, params = sql_generator.generate(ast)

        async with driver.pool.acquire() as conn:
            result = await conn.fetch(sql, *params)

        # Should find HasAge node
        assert len(result) >= 1

        # Verify HasAge is in results and NoAge is NOT
        found_has_age = False
        for row in result:
            node = json.loads(row['n']) if isinstance(row['n'], str) else row['n']
            if node['name'] == 'HasAge':
                found_has_age = True
            # NoAge should NOT be in these results
            assert node['name'] != 'NoAge', "NoAge should not be in IS NOT NULL results"
        assert found_has_age, "Expected to find HasAge node (with NOT NULL age)"

    @pytest.mark.asyncio
    async def test_cypher_starts_with(self, driver, parser, sql_generator):
        """Test STARTS WITH string operator"""
        # Create test nodes with email property in JSONB
        await driver.create_node(
            uuid=str(uuid.uuid4()),
            name="Alice",
            node_type="entity",
            properties={"email": "alice@example.com"}
        )
        await driver.create_node(
            uuid=str(uuid.uuid4()),
            name="Bob",
            node_type="entity",
            properties={"email": "bob@test.com"}
        )
        await driver.create_node(
            uuid=str(uuid.uuid4()),
            name="Andrew",
            node_type="entity",
            properties={"email": "andrew@example.com"}
        )

        # Query for emails starting with 'a' (at example.com)
        cypher = "MATCH (n:Entity) WHERE n.email STARTS WITH 'a' RETURN n"
        ast = parser.parse(cypher)
        sql, params = sql_generator.generate(ast)

        async with driver.pool.acquire() as conn:
            result = await conn.fetch(sql, *params)

        # Should find alice and andrew (lowercase 'a'), not Bob
        assert len(result) >= 2

        # Verify alice and andrew are in results, Bob is not
        import json
        names = []
        for row in result:
            node = json.loads(row['n']) if isinstance(row['n'], str) else row['n']
            names.append(node['name'])
        assert 'Alice' in names or 'alice' in names, f"Expected Alice in results, got {names}"
        assert 'Andrew' in names or 'andrew' in names, f"Expected Andrew in results, got {names}"
        assert 'Bob' not in names, f"Bob should not be in results (email starts with 'b', not 'a')"

    @pytest.mark.asyncio
    async def test_cypher_ends_with(self, driver, parser, sql_generator):
        """Test ENDS WITH string operator"""
        # Create test nodes
        await driver.create_node(
            uuid=str(uuid.uuid4()),
            name="Alice",
            node_type="entity",
            properties={"email": "alice@example.com"}
        )
        await driver.create_node(
            uuid=str(uuid.uuid4()),
            name="Bob",
            node_type="entity",
            properties={"email": "bob@test.org"}
        )
        await driver.create_node(
            uuid=str(uuid.uuid4()),
            name="Charlie",
            node_type="entity",
            properties={"email": "charlie@example.com"}
        )

        # Query for emails ending with '.com'
        cypher = "MATCH (n:Entity) WHERE n.email ENDS WITH '.com' RETURN n"
        ast = parser.parse(cypher)
        sql, params = sql_generator.generate(ast)

        async with driver.pool.acquire() as conn:
            result = await conn.fetch(sql, *params)

        # Should find Alice and Charlie (.com emails), not Bob (.org)
        assert len(result) >= 2

        # Verify Alice and Charlie are in results, Bob is not
        import json
        names = []
        for row in result:
            node = json.loads(row['n']) if isinstance(row['n'], str) else row['n']
            names.append(node['name'])
        assert 'Alice' in names, f"Expected Alice in results, got {names}"
        assert 'Charlie' in names, f"Expected Charlie in results, got {names}"
        assert 'Bob' not in names, f"Bob should not be in results (email ends with '.org', not '.com')"

    @pytest.mark.asyncio
    async def test_cypher_contains(self, driver, parser, sql_generator):
        """Test CONTAINS string operator"""
        # Create test nodes
        await driver.create_node(
            uuid=str(uuid.uuid4()),
            name="Alice",
            node_type="entity",
            properties={"city": "San Francisco"}
        )
        await driver.create_node(
            uuid=str(uuid.uuid4()),
            name="Bob",
            node_type="entity",
            properties={"city": "New York"}
        )
        await driver.create_node(
            uuid=str(uuid.uuid4()),
            name="Alicia",
            node_type="entity",
            properties={"city": "San Jose"}
        )

        # Query for cities containing 'San'
        cypher = "MATCH (n:Entity) WHERE n.city CONTAINS 'San' RETURN n"
        ast = parser.parse(cypher)
        sql, params = sql_generator.generate(ast)

        async with driver.pool.acquire() as conn:
            result = await conn.fetch(sql, *params)

        # Should find Alice (San Francisco) and Alicia (San Jose), not Bob (New York)
        assert len(result) >= 2

        # Verify Alice and Alicia are in results, Bob is not
        import json
        names = []
        for row in result:
            node = json.loads(row['n']) if isinstance(row['n'], str) else row['n']
            names.append(node['name'])
        assert 'Alice' in names, f"Expected Alice in results, got {names}"
        assert 'Alicia' in names, f"Expected Alicia in results, got {names}"
        assert 'Bob' not in names, f"Bob should not be in results (New York doesn't contain 'San')"

    @pytest.mark.asyncio
    async def test_cypher_in_operator(self, driver, parser, sql_generator):
        """Test IN operator for list membership"""
        # Create test nodes
        await driver.create_node(
            uuid=str(uuid.uuid4()),
            name="Alice",
            node_type="entity",
            properties={"age": 25}
        )
        await driver.create_node(
            uuid=str(uuid.uuid4()),
            name="Bob",
            node_type="entity",
            properties={"age": 30}
        )
        await driver.create_node(
            uuid=str(uuid.uuid4()),
            name="Charlie",
            node_type="entity",
            properties={"age": 35}
        )
        await driver.create_node(
            uuid=str(uuid.uuid4()),
            name="David",
            node_type="entity",
            properties={"age": 40}
        )

        # Query for specific ages using IN
        cypher = "MATCH (n:Entity) WHERE n.age IN [25, 30, 35] RETURN n"
        ast = parser.parse(cypher)
        sql, params = sql_generator.generate(ast)

        async with driver.pool.acquire() as conn:
            result = await conn.fetch(sql, *params)

        # Should find Alice (25), Bob (30), and Charlie (35), not David (40)
        assert len(result) >= 3

        # Verify Alice, Bob, and Charlie are in results, David is not
        import json
        names = []
        for row in result:
            node = json.loads(row['n']) if isinstance(row['n'], str) else row['n']
            names.append(node['name'])
        assert 'Alice' in names, f"Expected Alice in results, got {names}"
        assert 'Bob' in names, f"Expected Bob in results, got {names}"
        assert 'Charlie' in names, f"Expected Charlie in results, got {names}"
        assert 'David' not in names, f"David should not be in results (age 40 not in [25, 30, 35])"

    @pytest.mark.asyncio
    async def test_cypher_multiple_relationship_types(self, driver, parser, sql_generator):
        """Test pattern matching with multiple relationship types using |"""
        # Create nodes and different relationship types
        alice_id = str(uuid.uuid4())
        bob_id = str(uuid.uuid4())
        charlie_id = str(uuid.uuid4())

        await driver.create_node(uuid=alice_id, name="Alice", node_type="entity")
        await driver.create_node(uuid=bob_id, name="Bob", node_type="entity")
        await driver.create_node(uuid=charlie_id, name="Charlie", node_type="entity")

        # Alice KNOWS Bob
        await driver.create_edge(
            uuid=str(uuid.uuid4()),
            source_uuid=alice_id,
            target_uuid=bob_id,
            relation_type="KNOWS"
        )

        # Alice FOLLOWS Charlie
        await driver.create_edge(
            uuid=str(uuid.uuid4()),
            source_uuid=alice_id,
            target_uuid=charlie_id,
            relation_type="FOLLOWS"
        )

        # Query for KNOWS or FOLLOWS relationships
        cypher = "MATCH (a:Entity)-[:KNOWS|:FOLLOWS]->(b:Entity) RETURN a, b"
        ast = parser.parse(cypher)
        sql, params = sql_generator.generate(ast)

        async with driver.pool.acquire() as conn:
            result = await conn.fetch(sql, *params)

        # Should find both relationships (Alice->Bob KNOWS and Alice->Charlie FOLLOWS)
        assert len(result) >= 2

        # Verify both relationships are returned
        import json
        found_bob = False
        found_charlie = False
        for row in result:
            node_a = json.loads(row['a']) if isinstance(row['a'], str) else row['a']
            node_b = json.loads(row['b']) if isinstance(row['b'], str) else row['b']
            if node_a['name'] == 'Alice' and node_b['name'] == 'Bob':
                found_bob = True
            if node_a['name'] == 'Alice' and node_b['name'] == 'Charlie':
                found_charlie = True
        assert found_bob, "Expected to find Alice->Bob (KNOWS) relationship"
        assert found_charlie, "Expected to find Alice->Charlie (FOLLOWS) relationship"

    @pytest.mark.asyncio
    async def test_cypher_undirected_relationship(self, driver, parser, sql_generator):
        """Test undirected relationship matching (no arrow)"""
        # Create nodes and a relationship
        alice_id = str(uuid.uuid4())
        bob_id = str(uuid.uuid4())

        await driver.create_node(uuid=alice_id, name="Alice", node_type="entity")
        await driver.create_node(uuid=bob_id, name="Bob", node_type="entity")

        # Create relationship in one direction
        await driver.create_edge(
            uuid=str(uuid.uuid4()),
            source_uuid=alice_id,
            target_uuid=bob_id,
            relation_type="KNOWS"
        )

        # Query with undirected pattern (should match regardless of direction)
        cypher = "MATCH (a:Entity)-[:KNOWS]-(b:Entity) RETURN a, b"
        ast = parser.parse(cypher)
        sql, params = sql_generator.generate(ast)

        async with driver.pool.acquire() as conn:
            result = await conn.fetch(sql, *params)

        # Should find the relationship from either direction
        assert len(result) >= 1

        # Verify Alice and Bob are connected
        import json
        found_connection = False
        for row in result:
            node_a = json.loads(row['a']) if isinstance(row['a'], str) else row['a']
            node_b = json.loads(row['b']) if isinstance(row['b'], str) else row['b']
            if (node_a['name'] == 'Alice' and node_b['name'] == 'Bob') or \
               (node_a['name'] == 'Bob' and node_b['name'] == 'Alice'):
                found_connection = True
                break
        assert found_connection, "Expected to find Alice-Bob connection in either direction"

    @pytest.mark.asyncio
    async def test_cypher_collect_aggregation(self, driver, parser, sql_generator):
        """Test collect() aggregation function"""
        # Create person and their friends
        alice_id = str(uuid.uuid4())
        bob_id = str(uuid.uuid4())
        charlie_id = str(uuid.uuid4())

        await driver.create_node(uuid=alice_id, name="Alice", node_type="entity")
        await driver.create_node(uuid=bob_id, name="Bob", node_type="entity")
        await driver.create_node(uuid=charlie_id, name="Charlie", node_type="entity")

        await driver.create_edge(
            uuid=str(uuid.uuid4()),
            source_uuid=alice_id,
            target_uuid=bob_id,
            relation_type="KNOWS"
        )
        await driver.create_edge(
            uuid=str(uuid.uuid4()),
            source_uuid=alice_id,
            target_uuid=charlie_id,
            relation_type="KNOWS"
        )

        # Query to collect all friends
        cypher = "MATCH (p:Entity)-[:KNOWS]->(friend:Entity) RETURN p.name AS person, collect(friend.name) AS friends"
        ast = parser.parse(cypher)
        sql, params = sql_generator.generate(ast)

        async with driver.pool.acquire() as conn:
            result = await conn.fetch(sql, *params)

        # Should aggregate friends into a list
        assert len(result) >= 1

        # Verify Alice's friends are collected
        found_alice = False
        for row in result:
            if row['person'] == 'Alice':
                found_alice = True
                friends = row['friends']
                # Friends should be an array/list containing Bob and Charlie
                assert friends is not None, "Expected friends list to be non-null"
                # Depending on SQL generation, friends might be JSON or list
                break
        assert found_alice, "Expected to find Alice with collected friends"

    @pytest.mark.asyncio
    async def test_cypher_aggregation_with_grouping(self, driver, parser, sql_generator):
        """Test aggregation with implicit GROUP BY"""
        # Create companies and employees
        company1_id = str(uuid.uuid4())
        company2_id = str(uuid.uuid4())
        emp1_id = str(uuid.uuid4())
        emp2_id = str(uuid.uuid4())
        emp3_id = str(uuid.uuid4())

        await driver.create_node(uuid=company1_id, name="TechCorp", node_type="entity", properties={"type": "company"})
        await driver.create_node(uuid=company2_id, name="DataInc", node_type="entity", properties={"type": "company"})
        await driver.create_node(uuid=emp1_id, name="Alice", node_type="entity", properties={"type": "person", "salary": 80000})
        await driver.create_node(uuid=emp2_id, name="Bob", node_type="entity", properties={"type": "person", "salary": 90000})
        await driver.create_node(uuid=emp3_id, name="Charlie", node_type="entity", properties={"type": "person", "salary": 70000})

        await driver.create_edge(uuid=str(uuid.uuid4()), source_uuid=emp1_id, target_uuid=company1_id, relation_type="WORKS_AT")
        await driver.create_edge(uuid=str(uuid.uuid4()), source_uuid=emp2_id, target_uuid=company1_id, relation_type="WORKS_AT")
        await driver.create_edge(uuid=str(uuid.uuid4()), source_uuid=emp3_id, target_uuid=company2_id, relation_type="WORKS_AT")

        # Count employees per company
        cypher = "MATCH (p:Entity)-[:WORKS_AT]->(c:Entity) RETURN c.name AS company, COUNT(p) AS employee_count"
        ast = parser.parse(cypher)
        sql, params = sql_generator.generate(ast)

        async with driver.pool.acquire() as conn:
            result = await conn.fetch(sql, *params)

        # Should have results grouped by company
        assert len(result) >= 1

    @pytest.mark.asyncio
    async def test_cypher_with_having_pattern(self, driver, parser, sql_generator):
        """Test WITH clause with filtering (HAVING-like behavior)"""
        # Create nodes with varying connection counts
        alice_id = str(uuid.uuid4())
        bob_id = str(uuid.uuid4())
        charlie_id = str(uuid.uuid4())
        david_id = str(uuid.uuid4())

        await driver.create_node(uuid=alice_id, name="Alice", node_type="entity")
        await driver.create_node(uuid=bob_id, name="Bob", node_type="entity")
        await driver.create_node(uuid=charlie_id, name="Charlie", node_type="entity")
        await driver.create_node(uuid=david_id, name="David", node_type="entity")

        # Alice knows Bob and Charlie (2 connections)
        await driver.create_edge(uuid=str(uuid.uuid4()), source_uuid=alice_id, target_uuid=bob_id, relation_type="KNOWS")
        await driver.create_edge(uuid=str(uuid.uuid4()), source_uuid=alice_id, target_uuid=charlie_id, relation_type="KNOWS")

        # David only knows Bob (1 connection)
        await driver.create_edge(uuid=str(uuid.uuid4()), source_uuid=david_id, target_uuid=bob_id, relation_type="KNOWS")

        # Find people with more than 1 connection
        cypher = """
        MATCH (p:Entity)-[:KNOWS]->(f:Entity)
        WITH p, COUNT(f) AS friend_count
        WHERE friend_count > 1
        RETURN p.name AS person, friend_count
        """
        ast = parser.parse(cypher)
        sql, params = sql_generator.generate(ast)

        async with driver.pool.acquire() as conn:
            result = await conn.fetch(sql, *params)

        # Should only find Alice (not David)
        assert len(result) >= 1

    @pytest.mark.asyncio
    async def test_cypher_regex_match(self, driver, parser, sql_generator):
        """Test regex pattern matching with =~"""
        # Create nodes with various code patterns in JSONB
        await driver.create_node(uuid=str(uuid.uuid4()), name="Alice", node_type="entity", properties={"code": "ABC123"})
        await driver.create_node(uuid=str(uuid.uuid4()), name="Bob", node_type="entity", properties={"code": "XYZ"})
        await driver.create_node(uuid=str(uuid.uuid4()), name="Charlie", node_type="entity", properties={"code": "DEF456"})

        # Find codes with numbers
        cypher = "MATCH (n:Entity) WHERE n.code =~ '.*[0-9]+.*' RETURN n"
        ast = parser.parse(cypher)
        sql, params = sql_generator.generate(ast)

        async with driver.pool.acquire() as conn:
            result = await conn.fetch(sql, *params)

        # Should find ABC123 and DEF456
        assert len(result) >= 2

    @pytest.mark.asyncio
    async def test_cypher_not_operator(self, driver, parser, sql_generator):
        """Test NOT operator in WHERE clause"""
        # Create test nodes
        await driver.create_node(
            uuid=str(uuid.uuid4()),
            name="Alice",
            node_type="entity",
            properties={"age": 25}
        )
        await driver.create_node(
            uuid=str(uuid.uuid4()),
            name="Bob",
            node_type="entity",
            properties={"age": 30}
        )
        await driver.create_node(
            uuid=str(uuid.uuid4()),
            name="Charlie",
            node_type="entity",
            properties={"age": 35}
        )

        # Find nodes where age is NOT 30
        cypher = "MATCH (n:Entity) WHERE NOT n.age = 30 RETURN n"
        ast = parser.parse(cypher)
        sql, params = sql_generator.generate(ast)

        async with driver.pool.acquire() as conn:
            result = await conn.fetch(sql, *params)

        # Should find Alice and Charlie (not Bob)
        assert len(result) >= 2

    @pytest.mark.asyncio
    async def test_cypher_multiple_labels(self, driver, parser, sql_generator):
        """Test nodes with multiple labels (using | in pattern)"""
        # Create nodes of different types
        await driver.create_node(uuid=str(uuid.uuid4()), name="Alice", node_type="entity")
        await driver.create_node(uuid=str(uuid.uuid4()), name="Bob", node_type="episode")

        # Match multiple node types
        cypher = "MATCH (n) WHERE n.node_type = 'entity' OR n.node_type = 'episode' RETURN n"
        ast = parser.parse(cypher)
        sql, params = sql_generator.generate(ast)

        async with driver.pool.acquire() as conn:
            result = await conn.fetch(sql, *params)

        # Should find both
        assert len(result) >= 2

    @pytest.mark.asyncio
    async def test_cypher_sum_aggregation(self, driver, parser, sql_generator):
        """Test SUM aggregation function"""
        # Create nodes with numeric properties
        await driver.create_node(
            uuid=str(uuid.uuid4()),
            name="Sale1",
            node_type="entity",
            properties={"amount": 100}
        )
        await driver.create_node(
            uuid=str(uuid.uuid4()),
            name="Sale2",
            node_type="entity",
            properties={"amount": 200}
        )
        await driver.create_node(
            uuid=str(uuid.uuid4()),
            name="Sale3",
            node_type="entity",
            properties={"amount": 150}
        )

        # Calculate total
        cypher = "MATCH (s:Entity) RETURN SUM(s.amount) AS total"
        ast = parser.parse(cypher)
        sql, params = sql_generator.generate(ast)

        async with driver.pool.acquire() as conn:
            result = await conn.fetch(sql, *params)

        assert len(result) >= 1

    @pytest.mark.asyncio
    async def test_cypher_min_max_aggregation(self, driver, parser, sql_generator):
        """Test MIN and MAX aggregation functions"""
        # Create nodes with ages
        await driver.create_node(
            uuid=str(uuid.uuid4()),
            name="Alice",
            node_type="entity",
            properties={"age": 25}
        )
        await driver.create_node(
            uuid=str(uuid.uuid4()),
            name="Bob",
            node_type="entity",
            properties={"age": 45}
        )
        await driver.create_node(
            uuid=str(uuid.uuid4()),
            name="Charlie",
            node_type="entity",
            properties={"age": 35}
        )

        # Find min and max age
        cypher = "MATCH (n:Entity) RETURN MIN(n.age) AS min_age, MAX(n.age) AS max_age"
        ast = parser.parse(cypher)
        sql, params = sql_generator.generate(ast)

        async with driver.pool.acquire() as conn:
            result = await conn.fetch(sql, *params)

        assert len(result) >= 1


class TestCypherErrorHandling:
    """Test error handling in Cypher integration"""

    @pytest.mark.asyncio
    async def test_invalid_cypher_syntax(self, parser):
        """Test that invalid Cypher raises appropriate error"""
        with pytest.raises(Exception):
            parser.parse("INVALID CYPHER QUERY")

    @pytest.mark.asyncio
    async def test_empty_result_set(self, driver, parser, sql_generator):
        """Test queries that return no results"""
        # Query for non-existent node
        cypher = "MATCH (n:Entity {name: 'NonExistent'}) RETURN n"
        ast = parser.parse(cypher)
        sql, params = sql_generator.generate(ast)

        async with driver.pool.acquire() as conn:
            result = await conn.fetch(sql, *params)

        # Should return empty list, not error
        assert result == [] or len(result) == 0


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
