# Contributing to graphiti-postgres

Thank you for your interest in contributing!

## Getting Started

1. Fork the repository
2. Clone your fork: `git clone https://github.com/YOUR_USERNAME/graphiti-postgres.git`
3. Create a branch: `git checkout -b feature/your-feature-name`
4. Set up development environment: `./setup_env.sh`

## Development Setup

```bash
# Install dependencies
pip install -r requirements.txt

# Start PostgreSQL
docker-compose up -d

# Run tests
pytest test_driver.py -v

# Run example
python example_usage.py
```

## Areas for Contribution

### High Priority
- **Full Cypher Parser** - Complete Cypher-to-SQL translation
- **Graph Algorithms** - Shortest path, PageRank, community detection
- **Performance** - Query optimization, batch operations
- **Documentation** - More examples, tutorials

### Medium Priority
- **Additional Tests** - Edge cases, performance tests
- **Error Handling** - Better error messages, validation
- **Monitoring** - Query logging, metrics
- **Type Hints** - Complete type annotations

### Ideas
- GraphQL API wrapper
- Streaming query results
- Apache AGE optional mode
- Additional database backends (MySQL, SQLite)

## Code Guidelines

- Follow PEP 8 style guide
- Add type hints to new functions
- Write tests for new features
- Update documentation
- Keep commits atomic and well-described

## Testing

```bash
# Run all tests
pytest test_driver.py -v

# Run specific test
pytest test_driver.py::test_create_node -v

# Run with coverage
pytest test_driver.py --cov=postgres_driver
```

## Pull Request Process

1. Update README.md if adding features
2. Add tests for new functionality
3. Ensure all tests pass
4. Update CHANGELOG.md (if exists)
5. Submit PR with clear description

## Code Review

- PRs require at least one approval
- Address review comments
- Keep PRs focused and reasonably sized

## Questions?

Open an issue for discussion before starting major work.

Thank you for contributing! 🚀
