"""
PostgreSQL driver for Graphiti Knowledge Graph
Implements the GraphDriver interface using native PostgreSQL with asyncpg
"""

import asyncio
import logging
from abc import ABC
from contextlib import asynccontextmanager
from typing import Any, Coroutine, Optional
from datetime import datetime
import json

try:
    import asyncpg
except ImportError:
    raise ImportError(
        "asyncpg is required for PostgreSQL driver. Install with: pip install asyncpg"
    )

# These imports would come from graphiti_core in actual usage
# For now, we define minimal interfaces
from enum import Enum

# Import new Cypher parser
from .cypher import CypherParser, SQLGenerator

logger = logging.getLogger(__name__)


class GraphProvider(Enum):
    """Graph database provider types"""
    NEO4J = 'neo4j'
    FALKORDB = 'falkordb'
    KUZU = 'kuzu'
    NEPTUNE = 'neptune'
    POSTGRESQL = 'postgresql'  # New provider


class GraphDriverSession(ABC):
    """Abstract session interface for graph drivers"""
    async def __aenter__(self):
        raise NotImplementedError()

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        raise NotImplementedError()

    async def run(self, query: str, **kwargs):
        raise NotImplementedError()


class GraphDriver(ABC):
    """Abstract base class for graph database drivers"""
    provider: GraphProvider
    fulltext_syntax: str = ''
    _database: str
    default_group_id: str = ''

    async def execute_query(self, cypher_query: str, **kwargs: Any) -> Coroutine:
        raise NotImplementedError()

    def session(self, database: str | None = None) -> GraphDriverSession:
        raise NotImplementedError()

    async def close(self):
        raise NotImplementedError()

    async def delete_all_indexes(self) -> Coroutine:
        raise NotImplementedError()

    async def build_indices_and_constraints(self, delete_existing: bool = False):
        raise NotImplementedError()


class PostgresDriverSession(GraphDriverSession):
    """PostgreSQL session wrapper for executing queries"""

    def __init__(self, pool: asyncpg.Pool):
        self.pool = pool
        self.connection: Optional[asyncpg.Connection] = None

    async def __aenter__(self):
        self.connection = await self.pool.acquire()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.connection:
            await self.pool.release(self.connection)
            self.connection = None

    async def run(self, query: str, **kwargs):
        """Execute a translated SQL query"""
        if not self.connection:
            raise RuntimeError("Session not initialized. Use async with context.")

        # Convert Cypher parameters to PostgreSQL
        params = kwargs.get('parameters', {})

        try:
            # Execute and fetch results
            records = await self.connection.fetch(query, *params.values())
            return [dict(record) for record in records]
        except Exception as e:
            logger.error(f"Query execution failed: {e}\nQuery: {query}")
            raise


class CypherToSQLTranslator:
    """
    Full Cypher to SQL translator using AST-based parser
    Supports comprehensive Cypher query patterns
    """

    def __init__(self, group_id: str = ''):
        self.parser = CypherParser()
        self.generator = SQLGenerator(group_id=group_id)

    def translate(self, cypher_query: str, parameters: dict = None) -> tuple[str, list]:
        """
        Translate Cypher query to PostgreSQL SQL using AST parser

        Args:
            cypher_query: Cypher query string
            parameters: Parameter values (for $param references)

        Returns:
            (sql_query, param_list)
        """
        try:
            # Parse Cypher to AST
            ast = self.parser.parse(cypher_query)

            # Generate SQL from AST
            sql, params = self.generator.generate(ast, parameters or {})

            logger.debug(f"Translated Cypher to SQL:\n{cypher_query}\n->\n{sql}")

            return sql, params

        except Exception as e:
            logger.warning(f"AST parser failed, falling back to simple translator: {e}")
            # Fallback to simple pattern matching for backwards compatibility
            return self._simple_translate(cypher_query, parameters or {})

    def _simple_translate(self, cypher_query: str, params: dict) -> tuple[str, list]:
        """
        Simplified translator as fallback
        Handles basic patterns when AST parser fails
        """
        cypher_lower = cypher_query.strip().lower()

        # MATCH patterns
        if cypher_lower.startswith('match'):
            if 'return' in cypher_lower:
                sql = """
                    SELECT n.uuid, n.name, n.node_type, n.properties, n.summary, n.embedding,
                           n.created_at, n.valid_at, n.invalid_at, n.metadata
                    FROM graph_nodes n
                    WHERE n.group_id = $1
                """
                param_list = [params.get('group_id', self.generator.context.group_id)]

                if 'uuid' in params:
                    sql += " AND n.uuid = $2"
                    param_list.append(params['uuid'])

                return sql, param_list

        # CREATE patterns
        elif cypher_lower.startswith('create'):
            node_type = params.get('node_type', 'entity')

            sql = """
                INSERT INTO graph_nodes (
                    uuid, name, node_type, group_id, properties, summary,
                    embedding, valid_at, invalid_at, metadata
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
                RETURNING uuid, name, node_type, properties
            """

            param_list = [
                params.get('uuid'),
                params.get('name'),
                node_type,
                params.get('group_id', self.generator.context.group_id),
                json.dumps(params.get('properties', {})),
                params.get('summary'),
                params.get('embedding'),
                params.get('valid_at'),
                params.get('invalid_at'),
                json.dumps(params.get('metadata', {}))
            ]

            return sql, param_list

        # MERGE patterns
        elif cypher_lower.startswith('merge'):
            sql = """
                INSERT INTO graph_nodes (
                    uuid, name, node_type, group_id, properties, summary,
                    embedding, valid_at, invalid_at, metadata
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
                ON CONFLICT (uuid)
                DO UPDATE SET
                    name = EXCLUDED.name,
                    properties = EXCLUDED.properties,
                    summary = EXCLUDED.summary,
                    embedding = EXCLUDED.embedding,
                    valid_at = EXCLUDED.valid_at,
                    metadata = EXCLUDED.metadata
                RETURNING uuid, name, node_type
            """

            param_list = [
                params.get('uuid'),
                params.get('name'),
                params.get('node_type', 'entity'),
                params.get('group_id', self.generator.context.group_id),
                json.dumps(params.get('properties', {})),
                params.get('summary'),
                params.get('embedding'),
                params.get('valid_at'),
                params.get('invalid_at'),
                json.dumps(params.get('metadata', {}))
            ]

            return sql, param_list

        # DELETE patterns
        elif cypher_lower.startswith('delete') or 'detach delete' in cypher_lower:
            sql = "DELETE FROM graph_nodes WHERE uuid = $1"
            param_list = [params.get('uuid')]
            return sql, param_list

        # Default: return as-is (might be raw SQL)
        return cypher_query, list(params.values())


class PostgresDriver(GraphDriver):
    """
    PostgreSQL driver implementation for Graphiti
    Uses native PostgreSQL with asyncpg for graph storage
    """

    provider = GraphProvider.POSTGRESQL
    fulltext_syntax = ''  # PostgreSQL uses different syntax than Neo4j

    def __init__(
        self,
        host: str = 'localhost',
        port: int = 5433,
        user: str = 'postgres',
        password: str = '',
        database: str = 'postgres',
        group_id: str = '',
        min_pool_size: int = 1,
        max_pool_size: int = 10,
        **kwargs
    ):
        """
        Initialize PostgreSQL driver

        Args:
            host: Database host
            port: Database port
            user: Database user
            password: Database password
            database: Database name
            group_id: Default group ID for multi-tenancy
            min_pool_size: Minimum connection pool size
            max_pool_size: Maximum connection pool size
        """
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self._database = database
        self.default_group_id = group_id
        self.min_pool_size = min_pool_size
        self.max_pool_size = max_pool_size

        self.pool: Optional[asyncpg.Pool] = None
        self.translator = CypherToSQLTranslator(group_id=group_id)

        # Initialize connection pool
        asyncio.create_task(self._init_pool())

    async def _init_pool(self):
        """Initialize asyncpg connection pool"""
        if self.pool is None:
            try:
                self.pool = await asyncpg.create_pool(
                    host=self.host,
                    port=self.port,
                    user=self.user,
                    password=self.password,
                    database=self._database,
                    min_size=self.min_pool_size,
                    max_size=self.max_pool_size,
                )
                logger.info(f"PostgreSQL connection pool created for database: {self._database}")
            except Exception as e:
                logger.error(f"Failed to create connection pool: {e}")
                raise

    async def execute_query(self, cypher_query: str, **kwargs: Any) -> list[dict]:
        """
        Execute a Cypher-like query by translating to SQL

        Args:
            cypher_query: Cypher query string (will be translated to SQL)
            **kwargs: Query parameters

        Returns:
            List of result dictionaries
        """
        if not self.pool:
            await self._init_pool()

        try:
            # Translate Cypher to SQL
            parameters = kwargs.get('parameters', {})
            sql_query, param_list = self.translator.translate(cypher_query, parameters)

            logger.debug(f"Executing SQL: {sql_query}")
            logger.debug(f"Parameters: {param_list}")

            async with self.pool.acquire() as connection:
                records = await connection.fetch(sql_query, *param_list)
                return [dict(record) for record in records]

        except Exception as e:
            logger.error(f"Query execution failed: {e}\nQuery: {cypher_query}")
            raise

    def session(self, database: str | None = None) -> PostgresDriverSession:
        """
        Create a new database session

        Args:
            database: Optional database name (uses default if not provided)

        Returns:
            PostgresDriverSession instance
        """
        if database and database != self._database:
            logger.warning(
                f"PostgreSQL driver doesn't support switching databases in session. "
                f"Using configured database: {self._database}"
            )

        if not self.pool:
            raise RuntimeError("Connection pool not initialized")

        return PostgresDriverSession(self.pool)

    async def close(self):
        """Close the database connection pool"""
        if self.pool:
            await self.pool.close()
            self.pool = None
            logger.info("PostgreSQL connection pool closed")

    async def delete_all_indexes(self):
        """Delete all custom indexes (keeps primary/foreign keys)"""
        if not self.pool:
            await self._init_pool()

        drop_indexes_sql = """
            DO $$
            DECLARE
                r RECORD;
            BEGIN
                FOR r IN (
                    SELECT indexname
                    FROM pg_indexes
                    WHERE schemaname = 'public'
                    AND tablename IN ('graph_nodes', 'graph_edges')
                    AND indexname LIKE 'idx_%'
                )
                LOOP
                    EXECUTE 'DROP INDEX IF EXISTS ' || quote_ident(r.indexname);
                END LOOP;
            END$$;
        """

        async with self.pool.acquire() as connection:
            await connection.execute(drop_indexes_sql)
            logger.info("All custom indexes deleted")

    async def build_indices_and_constraints(self, delete_existing: bool = False):
        """
        Build database indices and constraints

        Args:
            delete_existing: If True, drop existing indexes first
        """
        if not self.pool:
            await self._init_pool()

        if delete_existing:
            await self.delete_all_indexes()

        # Read schema file and execute
        import os
        schema_path = os.path.join(os.path.dirname(__file__), 'sql', 'schema.sql')

        if os.path.exists(schema_path):
            with open(schema_path, 'r') as f:
                schema_sql = f.read()

            async with self.pool.acquire() as connection:
                await connection.execute(schema_sql)
                logger.info("Indices and constraints built successfully")
        else:
            logger.warning(f"Schema file not found: {schema_path}")

    async def health_check(self) -> bool:
        """
        Check database connectivity

        Returns:
            True if connection is healthy
        """
        try:
            if not self.pool:
                await self._init_pool()

            async with self.pool.acquire() as connection:
                result = await connection.fetchval("SELECT 1")
                return result == 1
        except Exception as e:
            logger.error(f"Health check failed: {e}")
            return False

    def clone(self, group_id: str) -> 'PostgresDriver':
        """
        Create a new driver instance for a different group (multi-tenancy)

        Args:
            group_id: Group ID for the new driver

        Returns:
            New PostgresDriver instance with same connection but different group_id
        """
        new_driver = PostgresDriver(
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            database=self._database,
            group_id=group_id,
            min_pool_size=self.min_pool_size,
            max_pool_size=self.max_pool_size,
        )
        # Share the pool
        new_driver.pool = self.pool
        return new_driver

    # Helper methods for common operations

    async def create_node(
        self,
        uuid: str,
        name: str,
        node_type: str = 'entity',
        properties: dict = None,
        summary: str = None,
        embedding: list = None,
        valid_at: datetime = None,
        group_id: str = None
    ) -> dict:
        """Create a new node"""
        sql = """
            INSERT INTO graph_nodes (
                uuid, name, node_type, group_id, properties, summary, embedding, valid_at
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            RETURNING *
        """

        async with self.pool.acquire() as connection:
            record = await connection.fetchrow(
                sql,
                uuid,
                name,
                node_type,
                group_id or self.default_group_id,
                json.dumps(properties or {}),
                summary,
                embedding,
                valid_at
            )
            return dict(record)

    async def create_edge(
        self,
        uuid: str,
        source_uuid: str,
        target_uuid: str,
        relation_type: str,
        properties: dict = None,
        fact: str = None,
        episodes: list = None,
        group_id: str = None
    ) -> dict:
        """Create a new edge"""
        sql = """
            INSERT INTO graph_edges (
                uuid, source_node_uuid, target_node_uuid, relation_type,
                group_id, properties, fact, episodes
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            RETURNING *
        """

        async with self.pool.acquire() as connection:
            record = await connection.fetchrow(
                sql,
                uuid,
                source_uuid,
                target_uuid,
                relation_type,
                group_id or self.default_group_id,
                json.dumps(properties or {}),
                fact,
                episodes or []
            )
            return dict(record)

    async def get_node(self, uuid: str) -> Optional[dict]:
        """Get a node by UUID"""
        sql = "SELECT * FROM graph_nodes WHERE uuid = $1"

        async with self.pool.acquire() as connection:
            record = await connection.fetchrow(sql, uuid)
            return dict(record) if record else None

    async def search_nodes(
        self,
        search_term: str,
        node_type: str = None,
        group_id: str = None,
        limit: int = 10
    ) -> list[dict]:
        """Search nodes using fulltext search"""
        sql = "SELECT * FROM search_nodes_fulltext($1, $2, $3, $4)"

        async with self.pool.acquire() as connection:
            records = await connection.fetch(
                sql,
                search_term,
                node_type,
                group_id or self.default_group_id,
                limit
            )
            return [dict(record) for record in records]
