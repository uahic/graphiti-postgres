# Documentation Index

Quick navigation to all documentation files in the graphiti-postgres project.

## Getting Started

- **[Main README](../README.md)** - Project overview, installation, and basic usage
- **[Quick Start Guide](QUICK_START.md)** - Get up and running in 5 minutes
- **[Original Quick Start](QUICKSTART.md)** - Original getting started guide

## Parser Documentation

- **[Cypher Parser Overview](../cypher/README.md)** - Complete parser documentation
- **[Parser Implementation Details](PARSER_IMPLEMENTATION.md)** - Technical implementation details
- **[Grammar Definition](../cypher/grammar.lark)** - EBNF grammar for Cypher

## Code Examples

- **[Parser Examples](../examples/cypher_examples.py)** - 15+ Cypher translation examples
- **[Driver Examples](../examples/example_usage.py)** - PostgresDriver usage examples

## Database

- **[PostgreSQL Schema](../sql/schema.sql)** - Database schema definition
- **[Docker Setup](../docker/docker-compose.yml)** - Docker Compose configuration

## Development

- **[Contributing Guide](CONTRIBUTING.md)** - How to contribute to the project
- **[Test Suite](../tests/test_cypher_parser.py)** - Parser test suite
- **[Apache AGE Analysis](APACHE_AGE_ANALYSIS.md)** - Analysis of Apache AGE integration

## Architecture

### Core Components

1. **[postgres_driver.py](../postgres_driver.py)** - Main driver implementation
   - PostgresDriver class
   - CypherToSQLTranslator (integrated)
   - Connection pooling
   - Query execution

2. **[cypher/](../cypher/)** - Cypher parser package
   - `grammar.lark` - Cypher grammar
   - `ast_nodes.py` - AST node classes
   - `parser.py` - Parser + transformer
   - `sql_generator.py` - SQL generation

### Directory Structure

```
graphiti-postgres/
├── cypher/                    # Cypher parser package
├── docs/                      # Documentation (you are here!)
├── examples/                  # Code examples
├── tests/                     # Test suite
├── sql/                       # Database schemas
├── docker/                    # Docker configuration
├── postgres_driver.py         # Main driver
├── setup.py                   # Package setup
└── requirements.txt           # Dependencies
```

## Quick Links by Topic

### For Users

- [Installation Guide](../README.md#installation)
- [Quick Start](QUICK_START.md)
- [Usage Examples](../examples/cypher_examples.py)
- [Cypher Query Support](../cypher/README.md#supported-cypher-features)

### For Developers

- [Contributing Guide](CONTRIBUTING.md)
- [Parser Implementation](PARSER_IMPLEMENTATION.md)
- [Running Tests](../tests/test_cypher_parser.py)
- [Database Schema](../sql/schema.sql)

### For Integration

- [Driver API](../postgres_driver.py)
- [Cypher to SQL Translation](../cypher/README.md#sql-translation-patterns)
- [Integration Tests](../tests/test_driver.py)

## Feature Documentation

### Cypher Parser

- **Grammar**: [grammar.lark](../cypher/grammar.lark)
- **Supported Features**: [Cypher README](../cypher/README.md#supported-cypher-features)
- **Translation Examples**: [Parser Docs](../cypher/README.md#sql-translation-patterns)
- **Usage Guide**: [Quick Start](QUICK_START.md#basic-usage)

### PostgreSQL Driver

- **Connection Pooling**: [Main README](../README.md#architecture)
- **Multi-tenancy**: [Main README](../README.md#features)
- **Query Execution**: [Driver Examples](../examples/example_usage.py)

### Database Schema

- **Schema Definition**: [schema.sql](../sql/schema.sql)
- **Indexes**: [Main README](../README.md#architecture)
- **Helper Functions**: [schema.sql](../sql/schema.sql)

## Version History

- **v1.0** - Initial PostgreSQL driver implementation
- **v2.0** - Full AST-based Cypher parser added (latest)

## External Resources

- [Graphiti Documentation](https://help.getzep.com/graphiti/)
- [PostgreSQL Documentation](https://www.postgresql.org/docs/)
- [openCypher Specification](https://opencypher.org/)
- [Lark Parser Documentation](https://lark-parser.readthedocs.io/)

## Support

For issues or questions:
1. Check the [main README](../README.md)
2. Review [examples](../examples/)
3. Read the [parser documentation](../cypher/README.md)
4. Check [implementation details](PARSER_IMPLEMENTATION.md)
