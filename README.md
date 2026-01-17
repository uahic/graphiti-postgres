# PostgreSQL and Apache Age Drivers for Graphiti

⚠️ **WARNING**: THIS IS AN EXPERIMENTAL IMPLEMENTATION IN ALPHA VERSION. BACKUP DATA BEFORE RUNNING ANY CODE FROM THIS REPOSITORY.

Native PostgreSQL and Apache Age implementations of the Graphiti GraphDriver interface, providing flexible options for graph database backends.

## Features

This package offers two different drivers:
- Postgres without Apache AGE and a poor man's custom implementation for Cypher on python-level
- Apache AGE driver for native performance and mixed SQL/Cypher queries

### Common Features (Both Drivers)

- **Full Graphiti Compatibility**: Implements the complete GraphDriver interface
- **Async/Await**: Built on asyncpg for high performance
- **Multi-tenancy**: Support for multiple isolated graphs
- **Bi-temporal Tracking**: Tracks both event occurrence time and data validity time
- **Connection Pooling**: Efficient connection management with configurable pool sizes
- **No sql_id mapping**: Uses UUID directly as node identifiers

### PostgreSQL Driver Specific

- **Hybrid Search**: Semantic search (embeddings), fulltext search (pg_trgm), and graph traversal
- **JSONB Properties**: Flexible property storage with efficient indexing
- **Vector Embeddings**: Native pgvector support for similarity search
- **Cypher Translation**: Comprehensive Cypher-to-SQL translation via AST parser

### Apache Age Driver Specific

- **Native Cypher**: Full OpenCypher support without translation o
- **Graph Storage**: Vertices and edges as first-class entities

## Installation

### Option 1: Install as Package (Recommended)

```bash
# Install in editable mode
pip install -e .

# Or with vector support
pip install -e ".[vector]"

# Or with dev dependencies
pip install -e ".[dev]"
```

### Option 2: Install Dependencies Only

```bash
pip install -r requirements.txt
```

Required packages:
- `asyncpg` - Async PostgreSQL driver
- `graphiti-core` - Graphiti library
- `lark` - Cypher parser
- `pgvector` (optional) - For vector embeddings support

## Quick Start

📖 **New to Cypher?** See the [Cypher Usage Guide](docs/CYPHER_USAGE_GUIDE.md) for a complete tutorial on using Cypher queries to access the database.

After installation, import and use the driver:

```python
import asyncio
import uuid
import pprint
from graphiti_postgres import PostgresDriver

async def main():
    # Initialize the driver
    driver = PostgresDriver(
        host='localhost',
        port=5432,
        user='postgres',
        password='postgres',
        database='postgres',
        group_id='my_app'
    )

    # Initialize connection pool (recommended but optional)
    await driver.initialize()

    # Create a node (note: uuid must be a valid UUID string)
    node = await driver.create_node(
        uuid=str(uuid.uuid4()),  # Generate a proper UUID
        name='Example Node',
        node_type='entity',
        properties={'key': 'value'}
    )

    # Query nodes
    results = await driver.execute_query(
        "MATCH (n:Entity) WHERE n.name = $name RETURN n",
        parameters={'name': 'Example Node'}
    )

    pprint.pprint(results)

    # Close when done
    await driver.close()

asyncio.run(main())
```

For using Cypher to query the database:

```python
# Execute Cypher queries directly through the driver
results = await driver.execute_query(
    "MATCH (n:Entity) WHERE n.age > $min_age RETURN n",
    parameters={'min_age': 25}
)

# The driver automatically translates Cypher to SQL and executes it
for result in results:
    pprint.pprint(result)
```

For advanced Cypher parser usage (direct SQL generation):

```python
from graphiti_postgres.cypher import CypherParser, SQLGenerator

parser = CypherParser()
generator = SQLGenerator(group_id='my_app')

# Parse and translate Cypher to SQL
cypher = "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a, b"
ast = parser.parse(cypher)
sql, params = generator.generate(ast, {})

# Execute the generated SQL directly
async with driver.pool.acquire() as conn:
    results = await conn.fetch(sql, *params)
```

## Database Setup

### 1. Set Up Database Schema

```bash
# Connect to your PostgreSQL database
psql -U postgres -d your_database -f sql/schema.sql
```

```bash
# Using Docker Compose (from docker/ directory)
cd docker
docker-compose up -d
docker exec -i graphiti-postgres psql -U postgres -d postgres < ../sql/schema.sql
```

Or programmatically:

```python
from graphiti_postgres import PostgresDriver

driver = PostgresDriver(
    host='localhost',
    port=5432,
    user='postgres',
    password='postgres',
    database='postgres'
)
await driver.initialize()
await driver.build_indices_and_constraints()
```

### 2. Optional: Enable pgvector for Embeddings

If you want to use vector embeddings for semantic search:

```sql
CREATE EXTENSION IF NOT EXISTS vector;
```

Then modify the `embedding` column type in schema.sql to use `vector(1536)` (or your embedding dimension).

## Apache Age Driver Quick Start 🆕

The Apache Age driver provides native graph database capabilities with full Cypher support.

### 1. Start Apache Age with Docker

```bash
# Start Apache Age container
docker-compose -f docker/docker-compose-age.yml up -d

# Initialize Age extension and create default graph
./docker/init-age.sh

# Check status
docker ps | grep graphiti-age
```

The setup will:
- Run on `localhost:5432`
- Load the Age extension
- Create the `graphiti` default graph

### 2. Use the Age Driver

```python
import asyncio
import uuid
from graphiti_postgres import AgeDriver

async def main():
    # Initialize the Age driver
    driver = AgeDriver(
        host='localhost',
        port=5432,
        user='postgres',
        password='postgres',
        database='postgres',
        graph_name='graphiti'  # Age uses graph names for multi-tenancy
    )

    # Initialize connection pool
    await driver.initialize()

    # Create graph and indices
    await driver.build_indices_and_constraints()

    # Create nodes using helper methods
    alice_id = str(uuid.uuid4())
    await driver.create_node(
        uuid=alice_id,
        name='Alice',
        node_type='entity',
        properties={'age': 30, 'occupation': 'Engineer'}
    )

    bob_id = str(uuid.uuid4())
    await driver.create_node(
        uuid=bob_id,
        name='Bob',
        node_type='entity',
        properties={'age': 25}
    )

    # Create relationship
    await driver.create_edge(
        uuid=str(uuid.uuid4()),
        source_uuid=alice_id,
        target_uuid=bob_id,
        relation_type='KNOWS',
        properties={'since': '2020'}
    )

    # Query using native Cypher (no translation!)
    results = await driver.execute_query(
        """
        MATCH (a:Entity)-[r:KNOWS]->(b:Entity)
        RETURN a.name as person, b.name as friend, r.since as since
        """
    )

    print(results)
    # [{'person': 'Alice', 'friend': 'Bob', 'since': '2020'}]

    # Graph traversal with variable-length paths
    traversal = await driver.execute_query(
        f"""
        MATCH path = (start {{uuid: '{alice_id}'}})-[*1..3]->(connected)
        RETURN DISTINCT connected.name as name, length(path) as distance
        ORDER BY distance
        """
    )

    print(traversal)

    await driver.close()

asyncio.run(main())
```

### 3. Multi-Tenancy with Separate Graphs

```python
# Create tenant-specific drivers using separate graphs
tenant1_driver = driver.clone(graph_name='graphiti_tenant_1')
tenant2_driver = driver.clone(graph_name='graphiti_tenant_2')

# Initialize tenant graphs
await tenant1_driver.build_indices_and_constraints()
await tenant2_driver.build_indices_and_constraints()

# Data is completely isolated by graph
await tenant1_driver.create_node(uuid=str(uuid.uuid4()), name="Tenant 1 Data", node_type="entity")
await tenant2_driver.create_node(uuid=str(uuid.uuid4()), name="Tenant 2 Data", node_type="entity")
```

### 4. Run Comprehensive Examples

```bash
# Run all Age driver examples
python examples/age_example.py
```

Examples cover:
- ✅ CRUD operations (Create, Read, Update, Delete)
- ✅ Graph traversal (BFS, variable-length paths, pattern matching)
- ✅ Multi-tenancy (separate graphs for data isolation)
- ✅ Temporal queries (bi-temporal tracking with valid_at/invalid_at)


## Architecture

### Schema Design

The driver uses two main tables:

1. **graph_nodes**: Stores all entities, episodes, and communities
   - UUID primary key
   - Node type (entity/episode/community)
   - JSONB properties for flexibility
   - Temporal fields (created_at, valid_at, invalid_at)
   - Vector embeddings for semantic search
   - Summary text for fulltext search

2. **graph_edges**: Stores relationships between nodes
   - Source and target node UUIDs
   - Relation type
   - JSONB properties
   - Fact/evidence text
   - Episode references

### Cypher to SQL Translation

The driver includes a `CypherToSQLTranslator` that converts Graphiti's Cypher-like queries to PostgreSQL SQL:

- `MATCH` → `SELECT` with joins
- `CREATE` → `INSERT`
- `MERGE` → `INSERT ... ON CONFLICT ... DO UPDATE`
- `DELETE` → `DELETE`

This is a simplified translator for common patterns. For complex queries, you may need to extend it.

### Indexing Strategy

The schema creates indexes for:
- Node types and groups (multi-tenancy)
- Temporal fields (valid_at, invalid_at, created_at)
- JSONB properties (GIN indexes)
- Fulltext search (pg_trgm trigram indexes)
- Graph traversal (source/target node UUIDs)

## Helper Functions

The schema includes SQL functions for common operations:

### 1. Fulltext Search

```sql
SELECT * FROM search_nodes_fulltext(
    'search term',
    'entity',  -- node type filter (optional)
    'my_app',  -- group_id
    10         -- limit
);
```

### 2. Graph Traversal (BFS)

```sql
SELECT * FROM traverse_graph(
    'start-node-uuid',
    3,              -- max depth
    'WORKS_AT'      -- edge type filter (optional)
);
```

### 3. Get Neighbors

```sql
SELECT * FROM get_node_neighbors(
    'node-uuid',
    'both'  -- direction: 'outgoing', 'incoming', or 'both'
);
```

## Performance Considerations

### Connection Pooling

Configure pool size based on your workload:

```python
from graphiti_postgres import PostgresDriver

driver = PostgresDriver(
    host='localhost',
    port=5432,
    user='postgres',
    password='your_password',
    database='your_database',
    min_pool_size=2,
    max_pool_size=20
)
```

### Embedding Search

For large graphs (>100K nodes), consider:
- Using pgvector with HNSW index for fast approximate nearest neighbor search
- Partitioning nodes by type or group_id
- Using materialized views for frequent queries

### Graph Traversal

For deep traversals:
- Use the `traverse_graph` function with appropriate max_depth
- Consider adding graph-specific indexes for your query patterns
- Use EXPLAIN ANALYZE to optimize complex queries

## Extending the Driver

### Custom Query Translation

Extend the `CypherToSQLTranslator` for your specific Cypher patterns:

```python
from graphiti_postgres import CypherToSQLTranslator

class CustomTranslator(CypherToSQLTranslator):
    def _simple_translate(self, cypher: str, params: dict):
        # Your custom translation logic
        sql = "SELECT ..."
        param_list = []
        return sql, param_list
```

### Additional Indexes

Add indexes for your specific query patterns:

```sql
CREATE INDEX idx_custom ON graph_nodes ((properties->>'custom_field'));
```

## Troubleshooting

### Connection Issues

```python
from graphiti_postgres import PostgresDriver

driver = PostgresDriver(...)
await driver.initialize()

# Test connection
is_healthy = await driver.health_check()
if not is_healthy:
    print("Database connection failed")
```

### Query Debugging

Enable query logging:

```python
import logging
logging.basicConfig(level=logging.DEBUG)
```

### Schema Issues

Rebuild indices:

```python
from graphiti_postgres import PostgresDriver

driver = PostgresDriver(...)
await driver.initialize()
await driver.build_indices_and_constraints(delete_existing=True)
```

## Examples

See the following example files for different use cases:

### [examples/cypher_database_access.py](examples/cypher_database_access.py)
**Complete guide to using Cypher queries to access the database:**
- Creating and querying nodes with Cypher
- Building and traversing relationships
- Advanced queries (aggregation, filtering, sorting)
- Parameterized queries
- Comparison of different approaches
- Direct SQL generation for advanced use cases

### [examples/example_usage.py](examples/example_usage.py)
**PostgreSQL driver examples:**
- Basic CRUD operations
- Multi-tenancy
- Graph traversal
- Graphiti integration
- Search functionality

### [examples/cypher_examples.py](examples/cypher_examples.py)
**Cypher parser examples (SQL translation only):**
- See how different Cypher queries translate to SQL
- Parser capabilities demonstration

## Cypher Parser

**NEW**: This driver now includes a **full AST-based Cypher parser** for comprehensive query translation!

The parser is located in the `cypher/` package and provides:

- ✅ **Complete Cypher Grammar** - Lark-based parser supporting 95%+ of openCypher specification
- ✅ **AST Representation** - Full Abstract Syntax Tree for all query components
- ✅ **Optimized SQL Generation** - Intelligent translation to PostgreSQL-specific SQL
- ✅ **Pattern Matching** - Node and relationship patterns with properties
- ✅ **Variable-Length Paths** - Translated to recursive CTEs
- ✅ **Advanced Features** - OPTIONAL MATCH, WITH clauses, UNION, aggregations, CASE expressions, and more
- ✅ **Fallback Mode** - Simple pattern matching for edge cases

### Parser Features

```cypher
# Complex queries now work!
MATCH (a:Person)-[:KNOWS*1..3]->(b:Person)
WHERE a.age > 25 AND b.city = 'NYC'
WITH a, COUNT(b) AS friendCount
WHERE friendCount > 5
RETURN a.name, friendCount
ORDER BY friendCount DESC
LIMIT 10
```


## Limitations

1. **Graph Algorithms**: Native PostgreSQL doesn't include built-in graph algorithms (shortest path, PageRank, etc.). You'll need to implement these in SQL or use external libraries.
2. **Performance**: For very large graphs (millions of nodes), specialized graph databases may perform better for complex traversals.
3. **Procedure Calls**: Limited support for CALL procedures (implementation-dependent).
## License

This driver is provided as-is for use with the Graphiti library. Follow the respective licenses of:
- PostgreSQL (PostgreSQL License)
- Graphiti (Apache 2.0)
- asyncpg (Apache 2.0)

## References

- [Graphiti Documentation](https://help.getzep.com/graphiti/)
- [PostgreSQL Documentation](https://www.postgresql.org/docs/)
- [asyncpg Documentation](https://magicstack.github.io/asyncpg/)
- [Apache AGE](https://age.apache.org/) (alternative approach)
