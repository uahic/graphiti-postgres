# Quick Start Guide - PostgreSQL Driver for Graphiti

## 5-Minute Setup

### Step 1: Install Dependencies

```bash
cd experimental/graphiti-postgres
pip install -r requirements.txt
```

### Step 2: Set Up Database

Make sure PostgreSQL is running, then create the schema:

```bash
# Using psql
psql -U postgres -d your_database -f schema.sql

# Or using connection string
psql postgresql://user:password@localhost:5432/dbname -f schema.sql
```

For Supabase users:
```bash
# Use the Supabase SQL Editor to paste and run schema.sql
# Or use their connection string
psql "postgresql://postgres:[YOUR-PASSWORD]@db.[YOUR-PROJECT].supabase.co:5432/postgres" -f schema.sql
```

### Step 3: Configure Environment

```bash
cp .env.example .env
# Edit .env with your database credentials
```

### Step 4: Test the Driver

```python
# test_connection.py
import asyncio
from postgres_driver import PostgresDriver

async def test():
    driver = PostgresDriver(
        host='localhost',
        port=5432,
        user='postgres',
        password='your_password',
        database='postgres'
    )

    await asyncio.sleep(0.5)  # Wait for pool init

    # Test connection
    healthy = await driver.health_check()
    print(f"Connection: {'✓ OK' if healthy else '✗ Failed'}")

    await driver.close()

asyncio.run(test())
```

Run it:
```bash
python test_connection.py
```

### Step 5: Run Example

```bash
python example_usage.py
```

## Using with Graphiti

### Installation

```bash
pip install graphiti-core
```

### Basic Usage

```python
import asyncio
from graphiti_core import Graphiti
from graphiti_core.llm_client import OpenAIClient
from graphiti_core.embedder import OpenAIEmbedder
from postgres_driver import PostgresDriver

async def main():
    # Initialize driver
    driver = PostgresDriver(
        host='localhost',
        port=5432,
        user='postgres',
        password='your_password',
        database='postgres',
        group_id='my_app'
    )

    # Wait for initialization
    await asyncio.sleep(0.5)

    # Setup schema (first time only)
    await driver.build_indices_and_constraints()

    # Initialize Graphiti
    graphiti = Graphiti(
        driver=driver,
        llm_client=OpenAIClient(api_key='your-openai-key'),
        embedder=OpenAIEmbedder(api_key='your-openai-key')
    )

    # Add knowledge
    await graphiti.add_episode(
        name='user_interaction',
        episode_body='Alice ordered a pizza from Pizza Palace.',
        source_description='Chat log from 2024-01-15'
    )

    # Query knowledge
    results = await graphiti.search(
        query='What did Alice order?',
        num_results=5
    )

    print("Search Results:", results)

    await driver.close()

asyncio.run(main())
```

## Common Issues

### Issue: asyncpg not found
```bash
pip install asyncpg
```

### Issue: Connection refused
Check PostgreSQL is running:
```bash
# Linux/Mac
sudo service postgresql status

# Or
pg_isready
```

### Issue: Authentication failed
Verify credentials:
```bash
psql -U postgres -d postgres -c "SELECT 1"
```

### Issue: Schema creation fails
Make sure you have CREATE privileges:
```sql
GRANT CREATE ON DATABASE your_database TO your_user;
```

## Next Steps

1. Read the full [README.md](./README.md) for detailed documentation
2. Review [example_usage.py](./example_usage.py) for more examples
3. Check [APACHE_AGE_ANALYSIS.md](./APACHE_AGE_ANALYSIS.md) for architecture decisions
4. Run tests: `pytest test_driver.py`

## Production Checklist

- [ ] Configure connection pooling (min_pool_size, max_pool_size)
- [ ] Set up proper database backups
- [ ] Enable pgvector extension for embeddings (optional)
- [ ] Configure appropriate indexes for your query patterns
- [ ] Set up monitoring and logging
- [ ] Use connection string from environment variables
- [ ] Enable SSL for database connections
- [ ] Set up multi-tenancy with group_id if needed

## Support

- Driver issues: Check the README troubleshooting section
- Graphiti questions: https://github.com/getzep/graphiti
- PostgreSQL help: https://www.postgresql.org/support/

## Performance Tips

1. **Use connection pooling**: Already configured in the driver
2. **Add custom indexes**: Based on your query patterns
3. **Monitor slow queries**: Enable PostgreSQL slow query log
4. **Use EXPLAIN ANALYZE**: Optimize complex queries
5. **Consider partitioning**: For very large graphs (>1M nodes)

## Database Maintenance

```sql
-- Check table sizes
SELECT
    schemaname,
    tablename,
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) AS size
FROM pg_tables
WHERE tablename IN ('graph_nodes', 'graph_edges')
ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC;

-- Vacuum and analyze
VACUUM ANALYZE graph_nodes;
VACUUM ANALYZE graph_edges;

-- Reindex if needed
REINDEX TABLE graph_nodes;
REINDEX TABLE graph_edges;
```

Happy graphing! 🚀
