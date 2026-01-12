# Cypher Parser and SQL Generator

Full AST-based Cypher query parser with PostgreSQL SQL generation for the Graphiti PostgreSQL driver.

## Overview

This package provides a complete implementation of Cypher query parsing and translation to PostgreSQL SQL:

1. **Lark-based Grammar Parser** - Parses Cypher queries into an Abstract Syntax Tree (AST)
2. **AST Node Classes** - Comprehensive representation of all Cypher query components
3. **SQL Generator** - Traverses AST and generates optimized PostgreSQL SQL

## Architecture

```
Cypher Query
    ↓
[Lark Parser + Grammar]
    ↓
Abstract Syntax Tree (AST)
    ↓
[SQL Generator]
    ↓
PostgreSQL SQL + Parameters
```

## Features

### Supported Cypher Features

#### Pattern Matching
- ✅ Node patterns: `(n:Label {prop: value})`
- ✅ Relationship patterns: `-[r:TYPE]->`, `<-[r:TYPE]-`, `-[r:TYPE]-`
- ✅ Variable-length paths: `[:TYPE*1..3]`, `[:TYPE*]`
- ✅ Multiple patterns: `MATCH (a)-[:R1]->(b), (c)-[:R2]->(d)`
- ✅ Named paths: `p = (a)-[:R]->(b)`

#### Clauses
- ✅ `MATCH` - Pattern matching with WHERE filters
- ✅ `OPTIONAL MATCH` - Translates to LEFT JOIN
- ✅ `RETURN` - Projection with aliases
- ✅ `WITH` - Query chaining via CTEs
- ✅ `WHERE` - Complex filter expressions
- ✅ `CREATE` - Node and relationship creation
- ✅ `MERGE` - Upsert operations (INSERT ... ON CONFLICT)
- ✅ `DELETE` / `DETACH DELETE` - Deletion with cascade
- ✅ `SET` - Property updates
- ✅ `REMOVE` - Property and label removal
- ✅ `ORDER BY` - Sorting with ASC/DESC
- ✅ `LIMIT` / `SKIP` - Pagination
- ✅ `UNION` / `UNION ALL` - Set operations

#### Expressions
- ✅ Boolean operators: `AND`, `OR`, `NOT`
- ✅ Comparison: `=`, `<>`, `<`, `>`, `<=`, `>=`
- ✅ String operators: `CONTAINS`, `STARTS WITH`, `ENDS WITH`, `=~`
- ✅ Arithmetic: `+`, `-`, `*`, `/`, `%`, `^`
- ✅ Null checks: `IS NULL`, `IS NOT NULL`
- ✅ List membership: `IN`
- ✅ Property access: `n.property`, `n.nested.prop`
- ✅ Index access: `list[0]`

#### Data Types
- ✅ Integers, floats, strings, booleans, null
- ✅ Lists: `[1, 2, 3]`, `[x IN list WHERE x > 5]`
- ✅ Maps: `{key: value, ...}`
- ✅ Parameters: `$paramName`

#### Functions
- ✅ Aggregations: `COUNT`, `SUM`, `AVG`, `MIN`, `MAX`
- ✅ String functions: `toLower`, `toUpper`, `length`
- ✅ List functions: `size`, `head`, `tail`
- ✅ `CASE` expressions (simple and searched)
- ✅ `DISTINCT` modifier

#### Advanced Features
- ✅ List comprehensions: `[x IN list WHERE condition | expression]`
- ✅ Pattern comprehensions: `[p = pattern WHERE condition | expression]`
- ✅ Quantifiers: `ALL`, `ANY`, `NONE`, `SINGLE`
- ✅ Subqueries with `CALL`

## Usage

### Basic Example

```python
from cypher import CypherParser, SQLGenerator

# Initialize parser and generator
parser = CypherParser()
generator = SQLGenerator(group_id='my_group')

# Parse Cypher query
cypher = "MATCH (n:Person)-[:KNOWS]->(friend) WHERE n.age > 25 RETURN n.name, friend.name"
ast = parser.parse(cypher)

# Generate SQL
sql, params = generator.generate(ast)
print(sql)
# Output:
# SELECT n1.properties->>'name', n2.properties->>'name'
# FROM graph_nodes n1
# JOIN graph_edges e1 ON n1.uuid = e1.source_id
# JOIN graph_nodes n2 ON e1.target_id = n2.uuid
# WHERE n1.group_id = $1 AND n1.node_type = $2
#   AND e1.relation_type = $3 AND n1.properties->>'age' > $4
```

### With Parameters

```python
cypher = "MATCH (n:Person {id: $personId}) RETURN n"
parameters = {'personId': 12345}

ast = parser.parse(cypher)
sql, params = generator.generate(ast, parameters)
```

### Integration with PostgresDriver

The parser is automatically used by the PostgresDriver:

```python
from postgres_driver import PostgresDriver

driver = PostgresDriver(
    host='localhost',
    port=5432,
    database='graphiti',
    group_id='my_group'
)

# Cypher queries are automatically translated
results = await driver.execute_query(
    "MATCH (n:Entity)-[:RELATES_TO]->(m) RETURN n, m LIMIT 10"
)
```

## SQL Translation Patterns

### Node Patterns → Table Scans

```cypher
MATCH (n:Entity {name: 'Alice'})
```
↓
```sql
SELECT * FROM graph_nodes n1
WHERE n1.group_id = $1
  AND n1.node_type = $2
  AND n1.properties->>'name' = $3
```

### Relationships → JOINs

```cypher
MATCH (a)-[r:KNOWS]->(b)
```
↓
```sql
FROM graph_nodes n1
JOIN graph_edges e1 ON n1.uuid = e1.source_id
JOIN graph_nodes n2 ON e1.target_id = n2.uuid
WHERE e1.relation_type = $1
```

### Variable-Length Paths → Recursive CTEs

```cypher
MATCH (a)-[:KNOWS*1..3]->(b)
```
↓
```sql
WITH RECURSIVE path AS (
  SELECT source_id, target_id, 1 as depth
  FROM graph_edges WHERE relation_type = 'KNOWS'
  UNION ALL
  SELECT p.source_id, e.target_id, p.depth + 1
  FROM path p
  JOIN graph_edges e ON p.target_id = e.source_id
  WHERE e.relation_type = 'KNOWS' AND p.depth < 3
)
```

### OPTIONAL MATCH → LEFT JOIN

```cypher
MATCH (a) OPTIONAL MATCH (a)-[r]->(b)
```
↓
```sql
FROM graph_nodes n1
LEFT JOIN graph_edges e1 ON n1.uuid = e1.source_id
LEFT JOIN graph_nodes n2 ON e1.target_id = n2.uuid
```

### WITH → CTEs

```cypher
MATCH (n) WITH n.age AS age WHERE age > 25 RETURN age
```
↓
```sql
WITH cte_1 AS (
  SELECT n1.properties->>'age' AS age
  FROM graph_nodes n1
  WHERE n1.properties->>'age' > 25
)
SELECT age FROM cte_1
```

## File Structure

```
cypher/
├── __init__.py          # Package exports
├── grammar.lark         # Lark grammar definition
├── ast_nodes.py         # AST node classes
├── parser.py            # CypherParser + AST transformer
├── sql_generator.py     # SQLGenerator for PostgreSQL
└── README.md            # This file
```

## Extending the Parser

### Adding New Cypher Features

1. **Update Grammar** (`grammar.lark`)
   ```lark
   new_clause: "NEW"i pattern action
   ```

2. **Add AST Node** (`ast_nodes.py`)
   ```python
   @dataclass
   class NewClause(ASTNode):
       pattern: Pattern
       action: str
   ```

3. **Add Transformer Rule** (`parser.py`)
   ```python
   def new_clause(self, items):
       return NewClause(pattern=items[0], action=items[1])
   ```

4. **Add SQL Generation** (`sql_generator.py`)
   ```python
   def _generate_new_clause(self, clause: NewClause) -> str:
       # Generate SQL for new clause
       return sql_string
   ```

### Custom SQL Generation

Subclass `SQLGenerator` to customize SQL output:

```python
class CustomSQLGenerator(SQLGenerator):
    def _generate_node_filters(self, node, alias):
        filters = super()._generate_node_filters(node, alias)
        # Add custom filtering logic
        return filters
```

## Testing

Run the test suite:

```bash
# Run all tests
python -m pytest tests/

# Run specific test class
python -m pytest tests/test_cypher_parser.py::TestCypherParser

# Run with verbose output
python -m pytest -v tests/
```

## Performance Considerations

1. **Grammar Parsing**: Uses LALR parser for O(n) parsing speed
2. **AST Traversal**: Single-pass SQL generation
3. **Parameter Binding**: Prepared statement support for query caching
4. **Fallback Mode**: Simple pattern matching for unsupported queries

## Limitations

1. **Schema Mapping**: Assumes specific PostgreSQL schema (graph_nodes, graph_edges)
2. **Procedure Calls**: Limited CALL support (implementation-dependent)
3. **Spatial Functions**: Not yet supported
4. **Full-Text Search**: Uses PostgreSQL-specific syntax (different from Neo4j)

## Future Enhancements

- [ ] Graph algorithm implementations (shortest path, PageRank)
- [ ] Streaming query results for large datasets
- [ ] Query optimization hints
- [ ] Cost-based query planning
- [ ] Spatial/GIS function support
- [ ] Full-text search integration

## References

- [openCypher Specification](https://opencypher.org/)
- [Lark Parser Documentation](https://lark-parser.readthedocs.io/)
- [PostgreSQL JSON Functions](https://www.postgresql.org/docs/current/functions-json.html)
- [PostgreSQL Recursive Queries](https://www.postgresql.org/docs/current/queries-with.html)
