# Cypher Usage Guide

This guide explains how to use Cypher queries to access your PostgreSQL database through the Graphiti PostgreSQL driver.

## Table of Contents
- [Quick Start](#quick-start)
- [Three Ways to Use Cypher](#three-ways-to-use-cypher)
- [Common Use Cases](#common-use-cases)
- [Best Practices](#best-practices)
- [Examples](#examples)

## Quick Start

The simplest way to use Cypher is through the driver's `execute_query()` method:

```python
from graphiti_postgres import PostgresDriver

driver = PostgresDriver(
    host='localhost',
    port=5432,
    user='postgres',
    password='postgres',
    database='postgres',
    group_id='my_app'
)

# Query using Cypher
results = await driver.execute_query(
    "MATCH (n:Entity) WHERE n.age > 25 RETURN n.name, n.age"
)

for result in results:
    print(f"{result['name']}, age {result['age']}")
```

## Three Ways to Use Cypher

### 1. Through Driver (Recommended for Most Cases)

**Use this when:** You want simple, clean code and don't need to inspect the SQL.

```python
# The driver handles everything automatically
results = await driver.execute_query(
    "MATCH (n:Entity {city: $city}) RETURN n",
    parameters={'city': 'San Francisco'}
)
```

**Pros:**
- ✅ Simplest approach
- ✅ Automatic Cypher-to-SQL translation
- ✅ Parameter handling built-in
- ✅ Clean, readable code

**Cons:**
- ❌ No visibility into generated SQL
- ❌ Slight translation overhead

---

### 2. Direct SQL Generation (For Advanced Users)

**Use this when:** You need to see or modify the generated SQL.

```python
from graphiti_postgres.cypher import CypherParser, SQLGenerator

parser = CypherParser()
sql_gen = SQLGenerator(group_id='my_app')

# Parse and generate SQL
cypher = "MATCH (n:Entity) RETURN n"
ast = parser.parse(cypher)
sql, params = sql_gen.generate(ast, {})

# Inspect the SQL
print(f"Generated SQL: {sql}")

# Execute directly
async with driver.pool.acquire() as conn:
    results = await conn.fetch(sql, *params)
```

**Pros:**
- ✅ Full control over SQL execution
- ✅ Can inspect and debug SQL
- ✅ Can optimize queries
- ✅ Better for performance-critical code

**Cons:**
- ❌ More verbose
- ❌ Requires understanding of parser internals

---

### 3. Direct Driver API (For Simple Operations)

**Use this when:** You're doing basic CRUD without complex queries.

```python
# Direct API methods
node = await driver.create_node(
    uuid=str(uuid.uuid4()),
    name='Alice',
    node_type='entity',
    properties={'age': 30}
)

retrieved = await driver.get_node(node_id)
```

**Pros:**
- ✅ Type-safe
- ✅ Very simple
- ✅ No query string parsing

**Cons:**
- ❌ Less flexible
- ❌ Can't compose complex queries
- ❌ Limited to basic operations

## Common Use Cases

### Creating Nodes

```python
# Option 1: Using Cypher
await driver.execute_query("""
    CREATE (p:Entity {
        name: 'Alice',
        age: 30,
        city: 'NYC'
    })
""")

# Option 2: Using direct API
await driver.create_node(
    uuid=str(uuid.uuid4()),
    name='Alice',
    node_type='entity',
    properties={'age': 30, 'city': 'NYC'}
)
```

### Querying with Filters

```python
# Simple filter
results = await driver.execute_query("""
    MATCH (n:Entity)
    WHERE n.age > 25
    RETURN n.name, n.age
""")

# Complex filter with parameters
results = await driver.execute_query("""
    MATCH (n:Entity)
    WHERE n.age BETWEEN $min_age AND $max_age
      AND n.city = $city
    RETURN n.name AS name, n.age AS age
    ORDER BY n.age DESC
""", parameters={'min_age': 25, 'max_age': 50, 'city': 'NYC'})
```

### Working with Relationships

```python
# Create relationship (use driver API)
await driver.create_edge(
    uuid=str(uuid.uuid4()),
    source_uuid=alice_id,
    target_uuid=bob_id,
    relation_type='KNOWS'
)

# Query relationships (use Cypher)
results = await driver.execute_query("""
    MATCH (a:Entity)-[:KNOWS]->(b:Entity)
    WHERE a.name = 'Alice'
    RETURN b.name AS friend_name
""")
```

### Aggregations

```python
# Count by group
results = await driver.execute_query("""
    MATCH (p:Entity)
    RETURN p.city AS city, COUNT(p) AS count
    ORDER BY count DESC
""")

# Average, min, max
results = await driver.execute_query("""
    MATCH (p:Entity)
    WHERE p.salary IS NOT NULL
    RETURN
        p.city AS city,
        AVG(p.salary) AS avg_salary,
        MIN(p.salary) AS min_salary,
        MAX(p.salary) AS max_salary
""")
```

### Graph Traversal

```python
# Variable-length paths
results = await driver.execute_query("""
    MATCH (start:Entity)-[:KNOWS*1..3]->(friend:Entity)
    WHERE start.name = 'Alice'
    RETURN DISTINCT friend.name
""")

# Multiple hops
results = await driver.execute_query("""
    MATCH (a:Entity)-[:KNOWS]->(b:Entity)-[:KNOWS]->(c:Entity)
    RETURN a.name AS person1, b.name AS connector, c.name AS person2
""")
```

## Best Practices

### 1. Use Parameters for Dynamic Values

❌ **Bad:**
```python
city = "New York"
cypher = f"MATCH (n:Entity) WHERE n.city = '{city}' RETURN n"
results = await driver.execute_query(cypher)
```

✅ **Good:**
```python
results = await driver.execute_query(
    "MATCH (n:Entity) WHERE n.city = $city RETURN n",
    parameters={'city': 'New York'}
)
```

### 2. Choose the Right Approach

| Task | Recommended Approach |
|------|---------------------|
| Simple CRUD | Direct API (`create_node`, `get_node`) |
| Complex queries | `execute_query()` with Cypher |
| Relationship traversal | `execute_query()` with Cypher |
| Performance-critical | Direct SQL generation |
| Debugging queries | Direct SQL generation |

### 3. Use Labels Consistently

```python
# Entity nodes (people, companies, etc.)
MATCH (n:Entity) WHERE n.type = 'person'

# Episode nodes (events, facts)
MATCH (e:Episode) WHERE e.timestamp > $start_time

# Community nodes (groups, clusters)
MATCH (c:Community) WHERE c.size > 10
```

### 4. Index Your Properties

For better performance on properties you frequently query:

```sql
CREATE INDEX idx_entity_age ON graph_nodes ((properties->>'age'));
CREATE INDEX idx_entity_city ON graph_nodes ((properties->>'city'));
```

### 5. Handle JSON Properties Correctly

Properties are stored in JSONB. Access them like this:

```python
# In Cypher
WHERE n.age > 25        # Automatically handles JSONB extraction

# In direct SQL (if you need it)
WHERE (properties->>'age')::int > 25
```

## Examples

See the complete working examples:

- **[examples/cypher_database_access.py](../examples/cypher_database_access.py)** - Comprehensive guide with 5 detailed examples
- **[examples/example_usage.py](../examples/example_usage.py)** - Driver API examples
- **[examples/cypher_examples.py](../examples/cypher_examples.py)** - Parser demonstrations

## Supported Cypher Features

✅ **Fully Supported:**
- `MATCH` with node and relationship patterns
- `WHERE` clauses with complex conditions
- `RETURN` with projections and aliasing
- `CREATE` nodes and relationships
- `MERGE` (upsert) operations
- `ORDER BY`, `LIMIT`, `SKIP`
- `DISTINCT`
- Aggregations: `COUNT`, `SUM`, `AVG`, `MIN`, `MAX`, `COLLECT`
- `WITH` clauses (query chaining)
- `OPTIONAL MATCH` (LEFT JOIN)
- Variable-length paths (`*1..3`)
- Multiple relationship types (`[:TYPE1|:TYPE2]`)
- String operators: `STARTS WITH`, `ENDS WITH`, `CONTAINS`
- Comparison operators: `=`, `<>`, `<`, `>`, `<=`, `>=`
- `IN` operator
- `IS NULL`, `IS NOT NULL`
- Regular expressions (`=~`)
- `UNION` queries
- `CASE` expressions

⚠️ **Limitations:**
- No stored procedures (`CALL`)
- No path-specific functions
- Some advanced graph algorithms need custom implementation

## Troubleshooting

### Query Not Working?

1. **Enable debug logging:**
```python
import logging
logging.basicConfig(level=logging.DEBUG)
```

2. **Check the generated SQL:**
```python
from graphiti_postgres.cypher import CypherParser, SQLGenerator

parser = CypherParser()
sql_gen = SQLGenerator(group_id='my_app')

ast = parser.parse(your_cypher_query)
sql, params = sql_gen.generate(ast, {})

print(f"SQL: {sql}")
print(f"Params: {params}")
```

3. **Test the SQL directly:**
```python
async with driver.pool.acquire() as conn:
    results = await conn.fetch(sql, *params)
    print(results)
```

### Common Issues

**Problem:** `KeyError` when accessing results
```python
# Make sure your RETURN clause matches your access
result['name']  # Requires: RETURN n.name AS name
```

**Problem:** Empty results
```python
# Check group_id matches
driver = PostgresDriver(group_id='my_app')  # Must match data's group_id
```

**Problem:** Slow queries
```python
# Add indexes for frequently queried properties
CREATE INDEX idx_name ON graph_nodes ((properties->>'name'));
```

## Next Steps

1. ✅ Run the comprehensive examples: `python examples/cypher_database_access.py`
2. ✅ Read the parser documentation: [cypher/README.md](../cypher/README.md)
3. ✅ Check the test suite for more examples: [tests/test_driver_with_cypher.py](../tests/test_driver_with_cypher.py)
4. ✅ Explore advanced features in the main README: [README.md](../README.md)

## Getting Help

- Check the [main README](../README.md) for installation and setup
- See [QUICK_START.md](QUICK_START.md) for basic usage
- Review [PARSER_IMPLEMENTATION.md](PARSER_IMPLEMENTATION.md) for parser details
- Open an issue on GitHub for bugs or feature requests
