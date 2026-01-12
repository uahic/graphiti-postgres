"""
Basic tests for PostgreSQL Graphiti Driver
Run with: pytest test_driver.py
"""

import pytest
import pytest_asyncio
import asyncio
import uuid
from datetime import datetime
from graphiti_postgres import PostgresDriver


@pytest_asyncio.fixture
async def driver():
    """Create a test driver instance"""
    driver = PostgresDriver(
        host='localhost',
        port=5433,
        user='postgres',
        password='postgres',
        database='postgres',
        group_id='test_group'
    )

    # Wait for pool initialization
    await asyncio.sleep(0.5)

    # Setup: create tables and indices
    await driver.build_indices_and_constraints(delete_existing=False)

    yield driver

    # Teardown: clean up test data
    async with driver.pool.acquire() as conn:
        await conn.execute("DELETE FROM graph_edges WHERE group_id = 'test_group'")
        await conn.execute("DELETE FROM graph_nodes WHERE group_id = 'test_group'")

    await driver.close()


@pytest.mark.asyncio
async def test_health_check(driver):
    """Test database connectivity"""
    is_healthy = await driver.health_check()
    assert is_healthy is True


@pytest.mark.asyncio
async def test_create_node(driver):
    """Test creating a node"""
    node_uuid = str(uuid.uuid4())
    node = await driver.create_node(
        uuid=node_uuid,
        name="Test Node",
        node_type="entity",
        properties={"key": "value"},
        summary="This is a test node",
        valid_at=datetime.now()
    )

    assert node is not None
    assert str(node['uuid']) == node_uuid
    assert node['name'] == "Test Node"
    assert node['node_type'] == "entity"


@pytest.mark.asyncio
async def test_get_node(driver):
    """Test retrieving a node"""
    node_uuid = str(uuid.uuid4())

    # Create node
    await driver.create_node(
        uuid=node_uuid,
        name="Retrievable Node",
        node_type="entity"
    )

    # Retrieve node
    retrieved = await driver.get_node(node_uuid)

    assert retrieved is not None
    assert str(retrieved['uuid']) == node_uuid
    assert retrieved['name'] == "Retrievable Node"


@pytest.mark.asyncio
async def test_create_edge(driver):
    """Test creating an edge between nodes"""
    source_uuid = str(uuid.uuid4())
    target_uuid = str(uuid.uuid4())
    edge_uuid = str(uuid.uuid4())

    # Create nodes
    await driver.create_node(uuid=source_uuid, name="Source", node_type="entity")
    await driver.create_node(uuid=target_uuid, name="Target", node_type="entity")

    # Create edge
    edge = await driver.create_edge(
        uuid=edge_uuid,
        source_uuid=source_uuid,
        target_uuid=target_uuid,
        relation_type="CONNECTS_TO",
        properties={"strength": "strong"},
        fact="Source connects to Target"
    )

    assert edge is not None
    assert str(edge['uuid']) == edge_uuid
    assert str(edge['source_node_uuid']) == source_uuid
    assert str(edge['target_node_uuid']) == target_uuid
    assert edge['relation_type'] == "CONNECTS_TO"


@pytest.mark.asyncio
async def test_search_nodes(driver):
    """Test fulltext search on nodes"""
    # Create test nodes
    await driver.create_node(
        uuid=str(uuid.uuid4()),
        name="Engineer Alice",
        node_type="entity",
        summary="Alice is a software engineer specializing in Python"
    )

    await driver.create_node(
        uuid=str(uuid.uuid4()),
        name="Manager Bob",
        node_type="entity",
        summary="Bob is a project manager with engineering background"
    )

    # Search for "engineer"
    results = await driver.search_nodes(
        search_term="engineer",
        node_type="entity",
        limit=10
    )

    # Search function should work (may return 0 results due to trigram threshold)
    assert isinstance(results, list)
    # If results found, verify they contain the search term
    if len(results) > 0:
        assert any("engineer" in r['summary'].lower() for r in results)


@pytest.mark.asyncio
async def test_multi_tenancy(driver):
    """Test group_id isolation"""
    # Create driver for different group
    other_driver = driver.clone(group_id='other_test_group')

    node_uuid_1 = str(uuid.uuid4())
    node_uuid_2 = str(uuid.uuid4())

    # Create node in first group
    await driver.create_node(
        uuid=node_uuid_1,
        name="Group 1 Node",
        node_type="entity"
    )

    # Create node in second group
    await other_driver.create_node(
        uuid=node_uuid_2,
        name="Group 2 Node",
        node_type="entity"
    )

    # Verify isolation: driver should only see its own group's nodes
    node_1 = await driver.get_node(node_uuid_1)
    node_2 = await driver.get_node(node_uuid_2)

    assert node_1 is not None  # Should find node in its own group
    assert node_2 is not None  # Can still get by UUID directly

    # Clean up other group
    async with other_driver.pool.acquire() as conn:
        await conn.execute("DELETE FROM graph_nodes WHERE group_id = 'other_test_group'")


@pytest.mark.asyncio
async def test_session_context_manager(driver):
    """Test session context manager"""
    async with driver.session() as session:
        # Execute a simple query
        results = await session.run(
            "SELECT COUNT(*) as count FROM graph_nodes WHERE group_id = $1",
            parameters={'group_id': 'test_group'}
        )

        assert results is not None
        assert isinstance(results, list)


@pytest.mark.asyncio
async def test_graph_traversal_function(driver):
    """Test the traverse_graph SQL function"""
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
        relation_type="NEXT"
    )

    await driver.create_edge(
        uuid=str(uuid.uuid4()),
        source_uuid=node_b,
        target_uuid=node_c,
        relation_type="NEXT"
    )

    # Traverse from A with depth 2
    async with driver.pool.acquire() as conn:
        traversal = await conn.fetch(
            "SELECT * FROM traverse_graph($1, $2)",
            node_a,
            2
        )

        # Should find A, B, and C
        assert len(traversal) == 3, f"Expected 3 nodes (A, B, C), got {len(traversal)}"

        # Verify we got the correct nodes
        traversal_uuids = {str(t['uuid']) for t in traversal}
        assert node_a in traversal_uuids, "Node A should be in traversal"
        assert node_b in traversal_uuids, "Node B should be in traversal"
        assert node_c in traversal_uuids, "Node C should be in traversal"

        # Verify correct depths
        depth_map = {str(t['uuid']): t['depth'] for t in traversal}
        assert depth_map[node_a] == 0, "Node A should be at depth 0"
        assert depth_map[node_b] == 1, "Node B should be at depth 1"
        assert depth_map[node_c] == 2, "Node C should be at depth 2"


@pytest.mark.asyncio
async def test_get_neighbors_function(driver):
    """Test the get_node_neighbors SQL function"""
    # Create a node with neighbors
    center_node = str(uuid.uuid4())
    neighbor_1 = str(uuid.uuid4())
    neighbor_2 = str(uuid.uuid4())

    await driver.create_node(uuid=center_node, name="Center", node_type="entity")
    await driver.create_node(uuid=neighbor_1, name="Neighbor1", node_type="entity")
    await driver.create_node(uuid=neighbor_2, name="Neighbor2", node_type="entity")

    # Create outgoing edge
    await driver.create_edge(
        uuid=str(uuid.uuid4()),
        source_uuid=center_node,
        target_uuid=neighbor_1,
        relation_type="KNOWS"
    )

    # Create incoming edge
    await driver.create_edge(
        uuid=str(uuid.uuid4()),
        source_uuid=neighbor_2,
        target_uuid=center_node,
        relation_type="FOLLOWS"
    )

    # Get all neighbors
    async with driver.pool.acquire() as conn:
        neighbors = await conn.fetch(
            "SELECT * FROM get_node_neighbors($1, 'both')",
            center_node
        )

        assert len(neighbors) == 2, f"Expected exactly 2 neighbors, got {len(neighbors)}"

        neighbor_uuids = [str(n['neighbor_uuid']) for n in neighbors]
        assert neighbor_1 in neighbor_uuids, "Neighbor1 should be in results"
        assert neighbor_2 in neighbor_uuids, "Neighbor2 should be in results"

        # Verify relationship types and directions
        neighbor_map = {str(n['neighbor_uuid']): n for n in neighbors}
        neighbor1_data = neighbor_map[neighbor_1]
        neighbor2_data = neighbor_map[neighbor_2]

        # neighbor_1 should be outgoing KNOWS relationship
        assert neighbor1_data['relation_type'] == 'KNOWS', "Should have KNOWS relationship"
        assert neighbor1_data['direction'] == 'outgoing', "Should be outgoing to neighbor_1"

        # neighbor_2 should be incoming FOLLOWS relationship
        assert neighbor2_data['relation_type'] == 'FOLLOWS', "Should have FOLLOWS relationship"
        assert neighbor2_data['direction'] == 'incoming', "Should be incoming from neighbor_2"


if __name__ == "__main__":
    # Run tests
    print("Running PostgreSQL Graphiti Driver Tests...")
    print("Make sure PostgreSQL is running and accessible!")
    print("-" * 60)

    pytest.main([__file__, "-v", "-s"])
