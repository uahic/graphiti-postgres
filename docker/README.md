# Docker Setup for Graphiti Drivers

This directory contains Docker configurations for both PostgreSQL and Apache Age graph databases.

## PostgreSQL Driver

Uses standard PostgreSQL with pgvector extension.

### Start PostgreSQL

```bash
docker-compose -f docker/docker-compose.yml up -d
```

### Stop PostgreSQL

```bash
docker-compose -f docker/docker-compose.yml down
```

### Remove data (clean start)

```bash
docker-compose -f docker/docker-compose.yml down -v
```

---

## Apache Age Driver

Uses Apache Age extension for PostgreSQL with native graph capabilities.

### Quick Start

1. **Start the Apache Age container:**

```bash
docker-compose -f docker/docker-compose-age.yml up -d
```

2. **Initialize the Age extension:**

```bash
./docker/init-age.sh
```

This will:
- Create the Age extension
- Set up the default `graphiti` graph
- Verify the installation

3. **Verify the setup:**

```bash
docker exec -it graphiti-age psql -U postgres -d postgres -c "SELECT * FROM ag_catalog.ag_graph;"
```

### Stop Apache Age

```bash
docker-compose -f docker/docker-compose-age.yml down
```

### Remove data (clean start)

```bash
docker-compose -f docker/docker-compose-age.yml down -v
```

### Manual Initialization (Alternative)

If you prefer to initialize manually:

```bash
docker exec -it graphiti-age psql -U postgres -d postgres
```

Then run:

```sql
CREATE EXTENSION IF NOT EXISTS age;
LOAD 'age';
SET search_path = ag_catalog, "$user", public;
SELECT create_graph('graphiti');
```

### Troubleshooting

#### Container won't start

Check logs:
```bash
docker logs graphiti-age
```

#### Permission issues

The init script now runs after container startup to avoid permission issues.

#### Age extension not found

Make sure you're using the correct Apache Age image:
```bash
docker pull apache/age:latest
```

#### Port 5432 already in use

If you have both PostgreSQL and Age running, change the port in `docker-compose-age.yml`:

```yaml
ports:
  - "5433:5432"  # Use 5433 on host instead
```

Then connect with `port=5433` in your driver configuration.

---

## Using Both Drivers Simultaneously

To run both PostgreSQL and Apache Age at the same time, modify one of the port mappings:

**Option 1: Change Age to use port 5433**

Edit `docker-compose-age.yml`:
```yaml
ports:
  - "5433:5432"
```

Connect to Age:
```python
driver = AgeDriver(host='localhost', port=5433, ...)
```

**Option 2: Use Docker networks**

Both containers can communicate via Docker network names:
```python
# From host machine
postgres_driver = PostgresDriver(host='localhost', port=5432, ...)
age_driver = AgeDriver(host='localhost', port=5433, ...)

# From another Docker container
postgres_driver = PostgresDriver(host='graphiti-postgres', port=5432, ...)
age_driver = AgeDriver(host='graphiti-age', port=5432, ...)
```

---

## Container Details

### PostgreSQL Container
- **Image**: `pgvector/pgvector:pg16`
- **Container name**: `graphiti-postgres`
- **Default port**: `5432`
- **Extensions**: pgvector, uuid-ossp, pg_trgm
- **Data volume**: `graphiti_postgres_data`

### Apache Age Container
- **Image**: `apache/age:latest`
- **Container name**: `graphiti-age`
- **Default port**: `5432`
- **Extensions**: age
- **Data volume**: `graphiti_age_data`

---

## Health Checks

Both containers include health checks. Verify status:

```bash
# Check PostgreSQL
docker inspect graphiti-postgres | grep -A 5 Health

# Check Apache Age
docker inspect graphiti-age | grep -A 5 Health
```

Or use the driver health check methods:

```python
import asyncio
from graphiti_postgres import PostgresDriver, AgeDriver

async def check_health():
    # PostgreSQL
    pg_driver = PostgresDriver(host='localhost', port=5432)
    await pg_driver.initialize()
    print(f"PostgreSQL healthy: {await pg_driver.health_check()}")
    await pg_driver.close()

    # Apache Age
    age_driver = AgeDriver(host='localhost', port=5432)
    await age_driver.initialize()
    print(f"Apache Age healthy: {await age_driver.health_check()}")
    await age_driver.close()

asyncio.run(check_health())
```
