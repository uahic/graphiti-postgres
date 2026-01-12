# Quick Start Guide - Cypher Parser

Get started with the new Cypher parser in 5 minutes!

## Installation

```bash
cd experimental/graphiti-postgres
pip install -r requirements.txt
```

The key dependency is `lark>=1.1.9` for parsing.

## Basic Usage

### 1. Direct Parser Usage

```python
from cypher import CypherParser, SQLGenerator

# Initialize
parser = CypherParser()
generator = SQLGenerator(group_id='my_app')

# Parse and translate
cypher = "MATCH (n:Person {age: 30}) RETURN n.name"
ast = parser.parse(cypher)
sql, params = generator.generate(ast)

print(sql)
# Output: SELECT n1.properties->>'name' FROM graph_nodes n1 WHERE ...
```

### 2. With PostgresDriver (Automatic)

```python
from postgres_driver import PostgresDriver

# Driver automatically uses the parser
driver = PostgresDriver(
    host='localhost',
    port=5432,
    database='graphiti',
    group_id='my_app'
)

# Write Cypher, get SQL automatically
results = await driver.execute_query(
    "MATCH (a:Person)-[:KNOWS]->(b) WHERE a.age > 25 RETURN a, b"
)
```

The driver translates Cypher queries transparently!

## Common Patterns

### Pattern Matching

```cypher
# Simple node
MATCH (n:Person) RETURN n

# With properties
MATCH (n:Person {city: 'NYC', age: 30}) RETURN n

# Relationships
MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN a, r, b

# Bidirectional
MATCH (a)-[r:KNOWS]-(b) RETURN a, b

# Variable-length
MATCH (a)-[:KNOWS*1..3]->(b) RETURN a, b

# Multiple types
MATCH (a)-[r:KNOWS|WORKS_WITH]->(b) RETURN a, b
```

### Filtering

```cypher
# Simple WHERE
MATCH (n:Person) WHERE n.age > 25 RETURN n

# Boolean logic
MATCH (n:Person)
WHERE n.age > 18 AND n.age < 65 AND n.city = 'NYC'
RETURN n

# String matching
MATCH (n:Person)
WHERE n.name STARTS WITH 'A'
  AND n.email CONTAINS '@example.com'
RETURN n

# List membership
MATCH (n:Person)
WHERE n.city IN ['NYC', 'SF', 'LA']
RETURN n

# Null checks
MATCH (n:Person)
WHERE n.email IS NOT NULL
RETURN n
```

### Aggregations

```cypher
# Count
MATCH (n:Person) RETURN COUNT(n) AS total

# Group by
MATCH (p:Person)-[:WORKS_AT]->(c:Company)
RETURN c.name, COUNT(p) AS employees

# Multiple aggregates
MATCH (p:Person)
RETURN AVG(p.age) AS avgAge,
       MIN(p.age) AS minAge,
       MAX(p.age) AS maxAge
```

### Sorting and Pagination

```cypher
# ORDER BY
MATCH (n:Person)
RETURN n.name, n.age
ORDER BY n.age DESC, n.name ASC

# LIMIT and SKIP
MATCH (n:Person)
RETURN n
ORDER BY n.createdAt DESC
SKIP 20 LIMIT 10
```

### Query Chaining (WITH)

```cypher
# Filter aggregated results
MATCH (p:Person)-[:LIVES_IN]->(c:City)
WITH c.name AS city, COUNT(p) AS population
WHERE population > 10000
RETURN city, population
ORDER BY population DESC

# Multi-step processing
MATCH (a:Person)-[:KNOWS]->(b:Person)
WITH a, COUNT(b) AS friendCount
WHERE friendCount > 5
MATCH (a)-[:LIKES]->(m:Movie)
RETURN a.name, friendCount, COUNT(m) AS movieCount
```

### Optional Matches

```cypher
# LEFT JOIN behavior
MATCH (p:Person)
OPTIONAL MATCH (p)-[:LIKES]->(m:Movie)
RETURN p.name, m.title

# Multiple optional matches
MATCH (p:Person)
OPTIONAL MATCH (p)-[:WORKS_AT]->(c:Company)
OPTIONAL MATCH (p)-[:LIVES_IN]->(city:City)
RETURN p.name, c.name, city.name
```

### Creating Data

```cypher
# Create node
CREATE (p:Person {name: 'Alice', age: 30, city: 'NYC'})

# Create relationship
MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'})
CREATE (a)-[r:KNOWS {since: 2020}]->(b)

# Merge (upsert)
MERGE (p:Person {email: 'alice@example.com'})
SET p.name = 'Alice', p.lastLogin = timestamp()
```

### Updating Data

```cypher
# SET properties
MATCH (p:Person {name: 'Alice'})
SET p.age = 31, p.city = 'SF'

# REMOVE properties
MATCH (p:Person {name: 'Alice'})
REMOVE p.temporaryField

# Conditional update with CASE
MATCH (p:Person)
SET p.status = CASE
  WHEN p.age < 18 THEN 'minor'
  WHEN p.age < 65 THEN 'adult'
  ELSE 'senior'
END
```

### Deleting Data

```cypher
# Delete node (must have no relationships)
MATCH (p:Person {name: 'Alice'})
DELETE p

# Delete with relationships
MATCH (p:Person {name: 'Alice'})
DETACH DELETE p
```

### Union Queries

```cypher
# Combine results
MATCH (p:Person) RETURN p.name AS name
UNION
MATCH (c:Company) RETURN c.name AS name

# Keep duplicates
MATCH (p:Person) RETURN p.city
UNION ALL
MATCH (c:Company) RETURN c.city
```

## Using Parameters

```python
from cypher import CypherParser, SQLGenerator

parser = CypherParser()
generator = SQLGenerator(group_id='my_app')

# Query with parameters
cypher = """
MATCH (p:Person {id: $personId})
WHERE p.age > $minAge
RETURN p
"""

# Provide parameter values
params = {'personId': 123, 'minAge': 25}

ast = parser.parse(cypher)
sql, sql_params = generator.generate(ast, params)
```

In Cypher queries, use `$paramName`. The generator will convert them to PostgreSQL positional parameters ($1, $2, etc.).

## Advanced Features

### CASE Expressions

```cypher
MATCH (p:Person)
RETURN p.name,
       CASE
         WHEN p.age < 18 THEN 'minor'
         WHEN p.age < 65 THEN 'adult'
         ELSE 'senior'
       END AS ageGroup
```

### List Comprehensions

```cypher
RETURN [x IN [1, 2, 3, 4, 5] WHERE x % 2 = 0 | x * 2] AS evenDoubled
```

### Pattern Comprehensions

```cypher
MATCH (a:Person)
RETURN a.name,
       [(a)-[:KNOWS]->(friend) WHERE friend.age > 25 | friend.name] AS adultFriends
```

## Running Examples

```bash
# Run the example file to see translations
cd experimental/graphiti-postgres
python examples/cypher_examples.py
```

This will show 15+ examples of Cypher queries and their SQL translations.

## Running Tests

```bash
# All tests
python -m pytest tests/ -v

# Specific test
python -m pytest tests/test_cypher_parser.py::TestCypherParser::test_simple_match

# With output
python -m pytest tests/ -v -s
```

## Troubleshooting

### Parse Errors

If you get a parse error, the parser will show exactly where it failed:

```python
try:
    ast = parser.parse("MATCH (n:Person")  # Missing closing paren
except ValueError as e:
    print(e)
    # Output: Failed to parse Cypher query: ...
```

### Unsupported Features

If a Cypher feature isn't supported yet, the translator falls back to simple pattern matching:

```python
# The driver automatically handles this
results = await driver.execute_query(unsupported_query)
# Falls back to simple translator with a warning
```

### Debug Mode

Enable logging to see SQL generation:

```python
import logging
logging.basicConfig(level=logging.DEBUG)

# Now you'll see translation details
```

## Next Steps

1. **Read the full documentation**: [../cypher/README.md](../cypher/README.md)
2. **Check implementation details**: [PARSER_IMPLEMENTATION.md](PARSER_IMPLEMENTATION.md)
3. **Explore examples**: [../examples/cypher_examples.py](../examples/cypher_examples.py)
4. **Run tests**: `pytest ../tests/`
5. **Try your own queries**!

## Quick Reference

| Cypher | SQL Translation |
|--------|----------------|
| `(n:Label)` | `FROM graph_nodes WHERE node_type = 'label'` |
| `-[r:TYPE]->` | `JOIN graph_edges ON ...` |
| `WHERE` | `WHERE ...` |
| `RETURN` | `SELECT ...` |
| `ORDER BY` | `ORDER BY ...` |
| `LIMIT/SKIP` | `LIMIT ... OFFSET ...` |
| `[:TYPE*1..3]` | `WITH RECURSIVE ...` (CTE) |
| `OPTIONAL MATCH` | `LEFT JOIN ...` |
| `WITH` | `WITH cte AS (...)` |
| `UNION` | `UNION` / `UNION ALL` |

## Support

For issues or questions:
- Check the [main README](../README.md)
- Review [test examples](../tests/test_cypher_parser.py)
- See [implementation docs](PARSER_IMPLEMENTATION.md)
