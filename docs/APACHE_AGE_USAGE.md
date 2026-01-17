# Apache Age Driver for Graphiti

## Table of Contents
- [Introduction](#introduction)
- [Installation](#installation)
- [Quick Start](#quick-start)
- [API Reference](#api-reference)
- [Cypher Queries](#cypher-queries)
- [Multi-Tenancy](#multi-tenancy)
- [Temporal Tracking](#temporal-tracking)
- [Differences from PostgreSQL Driver](#differences-from-postgresql-driver)
- [Performance Considerations](#performance-considerations)
- [Troubleshooting](#troubleshooting)

---

## Introduction

The Apache Age driver provides a native graph database backend for Graphiti using [Apache Age](https://age.apache.org/), a PostgreSQL extension that adds graph database capabilities with full Cypher query support.

### Why Apache Age?

- **Native Graph Storage**: True graph database with vertices and edges as first-class entities
- **Cypher Support**: Full OpenCypher query language support (no translation needed)
- **PostgreSQL Foundation**: Leverages PostgreSQL's reliability, ACID compliance, and ecosystem
- **Simpler Architecture**: No Cypher-to-SQL translation layer (unlike the PostgreSQL driver)
- **Graph Performance**: Optimized for graph traversals and pattern matching

### Key Features

- ✅ Full Graphiti `GraphDriver` interface compatibility
- ✅ Native Cypher query execution
- ✅ Async/await support via asyncpg
- ✅ Multi-tenancy using separate graphs
- ✅ Bi-temporal tracking (valid_at/invalid_at)
- ✅ Connection pooling
- ✅ No sql_id mapping (uses UUID directly)

---

## Installation

### Prerequisites

1. **Docker** (recommended for running Apache Age)
2. **Python 3.8+**
3. **asyncpg** library

### Step 1: Install Python Package

```bash
# Install the graphiti-postgres package
pip install -e .

# Or install with dependencies
pip install asyncpg
```

### Step 2: Start Apache Age with Docker

```bash
# Start Apache Age container
docker-compose -f docker/docker-compose-age.yml up -d

# Check status
docker ps | grep graphiti-age

# View logs
docker logs graphiti-age
```

The Age container will:
- Run on `localhost:5432`
- Create the `graphiti` default graph
- Load the Age extension automatically
- Mount the schema initialization script

### Step 3: Verify Installation

```python
import asyncio
from graphiti_postgres import AgeDriver

async def test_connection():
    driver = AgeDriver(
        host='localhost',
        port=5432,
        user='postgres',
        password='postgres',
        database='postgres',
        graph_name='graphiti'
    )

    await driver.initialize()
    is_healthy = await driver.health_check()
    print(f"Connection healthy: {is_healthy}")
    await driver.close()

asyncio.run(test_connection())
```

---

## Quick Start

### Basic Usage

```python
import asyncio
import uuid
from datetime import datetime
from graphiti_postgres import AgeDriver

async def main():
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

        # Build indices and create graph
        await driver.build_indices_and_constraints()

        # Create a node
        person_id = str(uuid.uuid4())
        await driver.create_node(
            uuid=person_id,
            name="Alice",
            node_type="entity",
            properties={"age": 30, "occupation": "Engineer"},
            summary="Alice is a software engineer",
            valid_at=datetime.now()
        )

        # Create another node
        company_id = str(uuid.uuid4())
        await driver.create_node(
            uuid=company_id,
            name="TechCorp",
            node_type="entity",
            properties={"industry": "Technology"},
            summary="TechCorp is a tech company"
        )

        # Create a relationship
        await driver.create_edge(
            uuid=str(uuid.uuid4()),
            source_uuid=person_id,
            target_uuid=company_id,
            relation_type="WORKS_AT",
            properties={"since": "2020"},
            fact="Alice works at TechCorp"
        )

        # Query using Cypher
        results = await driver.execute_query(
            """
            MATCH (p:Entity {name: 'Alice'})-[r:WORKS_AT]->(c:Entity)
            RETURN p.name as person, c.name as company, r.since as since
            """
        )

        print(results)  # [{'person': 'Alice', 'company': 'TechCorp', 'since': '2020'}]

    finally:
        await driver.close()

asyncio.run(main())
```

---

## API Reference

### AgeDriver

#### Constructor

```python
AgeDriver(
    host: str = 'localhost',
    port: int = 5432,
    user: str = 'postgres',
    password: str = 'postgres',
    database: str = 'postgres',
    graph_name: str = 'graphiti',
    group_id: str = '',
    min_pool_size: int = 1,
    max_pool_size: int = 10
)
```

**Parameters:**
- `host`: Database host
- `port`: Database port
- `user`: Database user
- `password`: Database password
- `database`: PostgreSQL database name
- `graph_name`: Apache Age graph name (used for multi-tenancy)
- `group_id`: Legacy compatibility parameter (Age uses graph names)
- `min_pool_size`: Minimum connection pool size
- `max_pool_size`: Maximum connection pool size

#### Methods

##### `initialize()`
Initialize the connection pool.

```python
await driver.initialize()
```

##### `execute_query(cypher_query, **kwargs)`
Execute a native Cypher query.

```python
results = await driver.execute_query(
    "MATCH (n:Entity) WHERE n.age > $min_age RETURN n",
    parameters={'min_age': 25}
)
```

##### `session(database=None)`
Create a new database session.

```python
async with driver.session() as session:
    results = await session.run("MATCH (n) RETURN n LIMIT 10")
```

##### `close()`
Close the connection pool.

```python
await driver.close()
```

##### `health_check()`
Check database connectivity and Age extension.

```python
is_healthy = await driver.health_check()
```

##### `build_indices_and_constraints(delete_existing=False)`
Create the Age graph and build indices.

```python
await driver.build_indices_and_constraints()
```

##### `delete_all_indexes()`
Delete all custom indexes.

```python
await driver.delete_all_indexes()
```

##### `clone(group_id=None, graph_name=None)`
Create a new driver instance for a different tenant.

```python
tenant_driver = driver.clone(graph_name='graphiti_tenant_1')
```

#### Helper Methods

##### `create_node(...)`
Create a new node.

```python
await driver.create_node(
    uuid="...",
    name="Alice",
    node_type="entity",
    properties={"age": 30},
    summary="...",
    valid_at=datetime.now()
)
```

##### `create_edge(...)`
Create a new edge.

```python
await driver.create_edge(
    uuid="...",
    source_uuid="...",
    target_uuid="...",
    relation_type="KNOWS",
    properties={"since": "2020"}
)
```

##### `get_node(uuid)`
Retrieve a node by UUID.

```python
node = await driver.get_node(uuid)
```

##### `search_nodes(search_term, node_type=None, group_id=None, limit=10)`
Search nodes by text.

```python
results = await driver.search_nodes("software engineer", limit=5)
```

---

## Cypher Queries

Apache Age supports the full OpenCypher query language. Here are common patterns:

### Basic Patterns

#### Match Nodes

```python
# All entities
await driver.execute_query("MATCH (n:Entity) RETURN n")

# Nodes with specific property
await driver.execute_query("MATCH (n {uuid: $uuid}) RETURN n", parameters={'uuid': node_id})

# Nodes with label and properties
await driver.execute_query("MATCH (n:Entity {name: 'Alice'}) RETURN n")
```

#### Create Nodes

```python
# Create with label and properties
await driver.execute_query(
    """
    CREATE (n:Entity {
        uuid: $uuid,
        name: $name,
        age: $age
    })
    RETURN n
    """,
    parameters={'uuid': str(uuid.uuid4()), 'name': 'Bob', 'age': 25}
)
```

#### Create Relationships

```python
# Create relationship between existing nodes
await driver.execute_query(
    """
    MATCH (a {uuid: $uuid1}), (b {uuid: $uuid2})
    CREATE (a)-[r:KNOWS {since: '2020'}]->(b)
    RETURN r
    """,
    parameters={'uuid1': id1, 'uuid2': id2}
)
```

### Graph Traversal

#### Variable-Length Paths

```python
# Find all nodes within 1-3 hops
await driver.execute_query(
    """
    MATCH (start {uuid: $uuid})-[*1..3]->(connected)
    RETURN DISTINCT connected
    """,
    parameters={'uuid': start_id}
)
```

#### Shortest Path

```python
# Find shortest path between two nodes
await driver.execute_query(
    """
    MATCH path = shortestPath((a {uuid: $uuid1})-[*]-(b {uuid: $uuid2}))
    RETURN path
    """,
    parameters={'uuid1': id1, 'uuid2': id2}
)
```

#### Pattern Matching

```python
# Find complex patterns
await driver.execute_query(
    """
    MATCH (person:Entity)-[:WORKS_AT]->(company:Entity),
          (person)-[:LIVES_IN]->(city:Entity)
    WHERE company.industry = 'Technology'
    RETURN person.name, company.name, city.name
    """
)
```

### Aggregation

```python
# Count and group
await driver.execute_query(
    """
    MATCH (p:Entity)-[:WORKS_AT]->(c:Entity)
    RETURN c.name as company, count(p) as employee_count
    ORDER BY employee_count DESC
    """
)
```

### Update and Delete

```python
# Update properties
await driver.execute_query(
    """
    MATCH (n {uuid: $uuid})
    SET n.age = $new_age, n.updated_at = timestamp()
    RETURN n
    """,
    parameters={'uuid': node_id, 'new_age': 31}
)

# Delete node and relationships
await driver.execute_query(
    """
    MATCH (n {uuid: $uuid})
    DETACH DELETE n
    """,
    parameters={'uuid': node_id}
)
```

---

## Multi-Tenancy

Apache Age supports multi-tenancy through separate graphs, providing strong data isolation.

### Pattern 1: Separate Graphs (Recommended)

```python
# Base driver
base_driver = AgeDriver(
    host='localhost',
    port=5432,
    user='postgres',
    password='postgres',
    database='postgres',
    graph_name='graphiti'
)

await base_driver.initialize()

# Tenant 1 driver (uses graph: graphiti_tenant_1)
tenant1_driver = base_driver.clone(graph_name='graphiti_tenant_1')
await tenant1_driver.build_indices_and_constraints()

# Tenant 2 driver (uses graph: graphiti_tenant_2)
tenant2_driver = base_driver.clone(graph_name='graphiti_tenant_2')
await tenant2_driver.build_indices_and_constraints()

# Insert data into tenant 1
await tenant1_driver.create_node(
    uuid=str(uuid.uuid4()),
    name="Tenant 1 Data",
    node_type="entity"
)

# Insert data into tenant 2
await tenant2_driver.create_node(
    uuid=str(uuid.uuid4()),
    name="Tenant 2 Data",
    node_type="entity"
)

# Data is completely isolated by graph
```

### Pattern 2: Property-Based Filtering

```python
# Alternative: Use group_id property within a single graph
await driver.create_node(
    uuid=str(uuid.uuid4()),
    name="Alice",
    node_type="entity",
    group_id="tenant_1"
)

# Query with group_id filter
await driver.execute_query(
    "MATCH (n:Entity {group_id: $group_id}) RETURN n",
    parameters={'group_id': 'tenant_1'}
)
```

**Recommendation**: Use separate graphs for better isolation and security.

---

## Temporal Tracking

Graphiti supports bi-temporal tracking with `valid_at` and `invalid_at` properties.

### Temporal Data Model

```python
# Create node with temporal metadata
await driver.create_node(
    uuid=str(uuid.uuid4()),
    name="Alice",
    node_type="entity",
    valid_at=datetime(2024, 1, 1),  # When this fact became true
    invalid_at=None  # Still valid (null means no end date)
)

# Later: Invalidate the node
await driver.execute_query(
    """
    MATCH (n {uuid: $uuid})
    SET n.invalid_at = $invalidation_time
    RETURN n
    """,
    parameters={
        'uuid': node_id,
        'invalidation_time': datetime.now().isoformat()
    }
)
```

### Temporal Queries

#### Point-in-Time Query

```python
# "What was the state of the graph at time T?"
target_time = datetime(2024, 6, 1)
await driver.execute_query(
    """
    MATCH (n:Entity)
    WHERE n.valid_at <= $target_time
      AND (n.invalid_at IS NULL OR n.invalid_at > $target_time)
    RETURN n
    """,
    parameters={'target_time': target_time.isoformat()}
)
```

#### Currently Valid Nodes

```python
# Nodes that are currently valid
await driver.execute_query(
    """
    MATCH (n:Entity)
    WHERE n.valid_at <= timestamp()
      AND (n.invalid_at IS NULL OR n.invalid_at > timestamp())
    RETURN n
    """
)
```

#### Time Range Query

```python
# Nodes valid within a specific time range
await driver.execute_query(
    """
    MATCH (n:Entity)
    WHERE n.valid_at <= $end_time
      AND (n.invalid_at IS NULL OR n.invalid_at >= $start_time)
    RETURN n
    """,
    parameters={
        'start_time': start.isoformat(),
        'end_time': end.isoformat()
    }
)
```

---

## Differences from PostgreSQL Driver

| Feature | PostgreSQL Driver | Apache Age Driver |
|---------|-------------------|-------------------|
| **Storage Model** | Relational tables (graph_nodes, graph_edges) | Native graph (vertices, edges) |
| **Cypher Execution** | Translated to SQL via AST parser (800+ lines) | Native via `cypher()` function |
| **Query Complexity** | High (requires translation layer) | Low (direct Cypher passthrough) |
| **Properties** | JSONB columns | agtype (Age's graph-native type) |
| **Multi-Tenancy** | group_id column filtering | Separate graphs (cleaner isolation) |
| **Indexes** | PostgreSQL B-tree, GIN, IVFFlat | Age vertex label indexes |
| **Vector Embeddings** | Native pgvector support | Store as arrays (no native ops yet) |
| **Performance** | Good for hybrid SQL+graph queries | Better for pure graph queries |
| **Code Complexity** | ~1500 lines (with parser) | ~500 lines (simpler) |

### When to Use Each Driver

**Use PostgreSQL Driver if:**
- You need hybrid SQL and graph queries
- You want vector similarity search (pgvector)
- You have existing PostgreSQL infrastructure
- You need advanced fulltext search (pg_trgm)

**Use Apache Age Driver if:**
- You want native graph database features
- You prefer pure Cypher queries
- You need simpler architecture (no translation)
- You want better graph traversal performance
- Multi-tenancy with strong isolation is important

---

## Performance Considerations

### Indexing

Age supports property indexes on vertex labels:

```sql
-- Currently experimental in Age
-- CREATE INDEX ON Entity (uuid);
-- CREATE INDEX ON Entity (name);
```

For now, queries may be slower without indexes. Age is actively developing index support.

### Query Optimization

```python
# Good: Specific node lookup
MATCH (n {uuid: $uuid}) RETURN n

# Bad: Full scan
MATCH (n) WHERE n.uuid = $uuid RETURN n

# Good: Directed relationship
MATCH (a)-[r:KNOWS]->(b) RETURN b

# Bad: Undirected (scans both directions)
MATCH (a)-[r:KNOWS]-(b) RETURN b
```

### Connection Pooling

```python
# Use connection pooling for better performance
driver = AgeDriver(
    min_pool_size=5,
    max_pool_size=20
)
```

### Batch Operations

```python
# For bulk inserts, use transaction batching
async with driver.session() as session:
    for node in nodes:
        await session.run("CREATE (n:Entity {...})")
```

---

## Troubleshooting

### Age Extension Not Loaded

**Error**: `extension "age" does not exist`

**Solution**:
```bash
# Recreate container with Age extension
docker-compose -f docker/docker-compose-age.yml down -v
docker-compose -f docker/docker-compose-age.yml up -d

# Or manually load in PostgreSQL
docker exec -it graphiti-age psql -U postgres -d postgres -c "CREATE EXTENSION IF NOT EXISTS age;"
```

### Graph Does Not Exist

**Error**: `graph "graphiti" does not exist`

**Solution**:
```python
# Create graph manually
await driver.build_indices_and_constraints()

# Or via SQL
await driver.execute_query("SELECT create_graph('graphiti');")
```

### Connection Refused

**Error**: `Connection refused on port 5432`

**Solution**:
```bash
# Check container is running
docker ps | grep graphiti-age

# Check logs
docker logs graphiti-age

# Restart container
docker-compose -f docker/docker-compose-age.yml restart
```

### Query Returns Empty Results

**Issue**: Cypher query returns no results

**Debugging**:
```python
# Check if graph has data
results = await driver.execute_query("MATCH (n) RETURN count(n) as node_count")
print(results)  # [{'node_count': 0}] means graph is empty

# Check graph name
print(driver.graph_name)  # Should match the graph you created

# Verify Age setup
await driver.health_check()
```

### Performance Issues

**Issue**: Queries are slow

**Solutions**:
1. Add property indexes (when Age supports them)
2. Use specific node lookups instead of scans
3. Limit result sets with `LIMIT`
4. Use directed relationships when possible
5. Increase connection pool size

---

## Examples

See [`examples/age_example.py`](../examples/age_example.py) for comprehensive examples covering:

1. **Basic CRUD** - Create, read, update, delete operations
2. **Graph Traversal** - BFS, variable-length paths, pattern matching
3. **Multi-Tenancy** - Separate graphs for data isolation
4. **Temporal Queries** - Bi-temporal tracking with valid_at/invalid_at

Run the examples:

```bash
# Start Age container
docker-compose -f docker/docker-compose-age.yml up -d

# Run examples
python examples/age_example.py
```

---

## Further Reading

- [Apache Age Documentation](https://age.apache.org/age-manual/master/index.html)
- [OpenCypher Query Language](https://opencypher.org/)
- [Graphiti Core Documentation](https://github.com/getzep/graphiti)
- [PostgreSQL Driver Documentation](./QUICK_START.md)

---

## License

This driver is part of the graphiti-postgres project and follows the same license.
