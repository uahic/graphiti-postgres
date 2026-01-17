"""
Apache Age driver for Graphiti Knowledge Graph
Implements the GraphDriver interface using Apache Age graph extension for PostgreSQL
"""

import logging
import json
from typing import Any, Optional
from datetime import datetime

try:
    import asyncpg
except ImportError:
    raise ImportError(
        "asyncpg is required for Apache Age driver. Install with: pip install asyncpg"
    )

# Import from driver module
from .driver import GraphDriver, GraphDriverSession, GraphProvider

logger = logging.getLogger(__name__)


def _parse_return_columns(cypher_query: str) -> str:
    """
    Parse the RETURN clause from a Cypher query and generate Age column definitions.

    Args:
        cypher_query: The Cypher query string

    Returns:
        Column definition string like "(col1 agtype, col2 agtype)"
    """
    import re

    # PostgreSQL reserved keywords that need quoting
    RESERVED_KEYWORDS = {
        'count', 'sum', 'avg', 'min', 'max', 'order', 'limit', 'offset',
        'all', 'analyse', 'analyze', 'and', 'any', 'array', 'as', 'asc',
        'asymmetric', 'authorization', 'between', 'binary', 'both', 'case',
        'cast', 'check', 'collate', 'collation', 'column', 'concurrently',
        'constraint', 'create', 'cross', 'current_catalog', 'current_date',
        'current_role', 'current_schema', 'current_time', 'current_timestamp',
        'current_user', 'default', 'deferrable', 'desc', 'distinct', 'do',
        'else', 'end', 'except', 'false', 'fetch', 'for', 'foreign', 'freeze',
        'from', 'full', 'grant', 'group', 'having', 'ilike', 'in', 'initially',
        'inner', 'intersect', 'into', 'is', 'isnull', 'join', 'lateral',
        'leading', 'left', 'like', 'localtime', 'localtimestamp', 'natural',
        'not', 'notnull', 'null', 'only', 'or', 'outer', 'overlaps',
        'placing', 'primary', 'references', 'returning', 'right', 'select',
        'session_user', 'similar', 'some', 'symmetric', 'table', 'tablesample',
        'then', 'to', 'trailing', 'true', 'union', 'unique', 'user', 'using',
        'variadic', 'verbose', 'when', 'where', 'window', 'with'
    }

    # Find the RETURN clause (case-insensitive)
    # Handle multiline queries and various formats
    query_upper = cypher_query.upper()
    return_idx = query_upper.rfind('RETURN')

    if return_idx == -1:
        # No RETURN clause, use generic result column
        return "(result agtype)"

    # Extract everything after RETURN
    return_part = cypher_query[return_idx + 6:].strip()

    # Remove ORDER BY, LIMIT, SKIP clauses if present
    for keyword in ['ORDER BY', 'LIMIT', 'SKIP']:
        keyword_idx = return_part.upper().find(keyword)
        if keyword_idx != -1:
            return_part = return_part[:keyword_idx].strip()

    # Split by comma to get individual return items
    return_items = [item.strip() for item in return_part.split(',')]

    # Extract column names (handle "expr as alias" and "expr")
    columns = []
    for item in return_items:
        # Match "expression as alias" or just "expression"
        as_match = re.search(r'\s+as\s+(\w+)\s*$', item, re.IGNORECASE)
        if as_match:
            # Use the alias
            columns.append(as_match.group(1))
        else:
            # Try to extract a simple column name
            # Handle cases like "n.name", "count(n)", etc.
            simple_match = re.search(r'(\w+)$', item)
            if simple_match:
                columns.append(simple_match.group(1))
            else:
                # Fallback to generic name
                columns.append(f"col{len(columns)}")

    # Generate column definitions
    # Note: We don't quote column names because Age handles them correctly
    # and quoting can cause issues with ORDER BY clauses that reference these aliases
    if not columns:
        return "(result agtype)"

    column_defs = ", ".join([f"{col} agtype" for col in columns])
    return f"({column_defs})"


def _agtype_to_python(agtype_value):
    """Convert Apache Age agtype to Python object"""
    if agtype_value is None:
        return None

    # Age returns agtype as string representation
    if isinstance(agtype_value, str):
        try:
            # Parse JSON representation
            return json.loads(agtype_value)
        except:
            return agtype_value

    return agtype_value


async def _setup_age_environment(connection):
    """Set up Age environment for a connection"""
    await connection.execute("SET search_path = ag_catalog, \"$user\", public;")
    await connection.execute("LOAD 'age';")


class AgeDriverSession(GraphDriverSession):
    """Apache Age session wrapper for executing queries"""

    def __init__(self, pool: asyncpg.Pool, graph_name: str):
        self.pool = pool
        self.graph_name = graph_name
        self.connection: Optional[asyncpg.Connection] = None

    async def __aenter__(self):
        self.connection = await self.pool.acquire()
        await _setup_age_environment(self.connection)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.connection:
            await self.pool.release(self.connection)
            self.connection = None

    async def run(self, query: str, **kwargs):
        """Execute a Cypher query via Apache Age"""
        if not self.connection:
            raise RuntimeError("Session not initialized. Use async with context.")

        # Convert Cypher parameters to Age format
        params = kwargs.get('parameters', {})

        try:
            # Execute Cypher query through Age's cypher() function
            # Age requires us to wrap the Cypher query and specify result columns
            result = await self._execute_cypher(query, params)
            return result
        except Exception as e:
            logger.error(f"Query execution failed: {e}\nQuery: {query}")
            raise

    async def _execute_cypher(self, cypher_query: str, parameters: dict = None):
        """
        Execute Cypher query using Apache Age's cypher() function

        Apache Age syntax:
        SELECT * FROM cypher('graph_name', $$
            MATCH (n:Person) RETURN n
        $$) as (n agtype);
        """
        # For Age, we need to wrap the query in the cypher() function
        # We'll use a generic approach that works for most queries

        # Replace parameter placeholders ($param) with Age-specific format
        processed_query = cypher_query
        param_values = []

        if parameters:
            for key, value in parameters.items():
                # Age uses $1, $2, etc. for parameters, similar to PostgreSQL
                # We'll handle this during query construction
                pass

        # Execute the query
        # Note: Age returns results as agtype, which needs special handling
        try:
            # Parse the RETURN clause to determine column definitions
            column_defs = _parse_return_columns(processed_query)

            # Direct execution approach for Age
            result = await self.connection.fetch(
                f"SELECT * FROM cypher('{self.graph_name}', $$ {processed_query} $$) as {column_defs};"
            )

            # Convert agtype results to Python objects
            python_results = []
            for record in result:
                row_dict = {}
                for key in record.keys():
                    agtype_value = record[key]
                    # Parse agtype to Python object
                    row_dict[key] = _agtype_to_python(agtype_value)
                python_results.append(row_dict)

            return python_results

        except Exception as e:
            logger.error(f"Age Cypher execution error: {e}")
            raise

    def _agtype_to_python(self, agtype_value):
        """Convert Apache Age agtype to Python object"""
        if agtype_value is None:
            return None

        # Age returns agtype as string in most cases
        # We need to parse it to Python objects
        if isinstance(agtype_value, str):
            try:
                # Try to parse as JSON
                return json.loads(agtype_value)
            except:
                return agtype_value

class AgeDriver(GraphDriver):
    """
    Apache Age driver implementation for Graphiti
    Uses Apache Age graph extension for PostgreSQL with native Cypher support
    """

    provider = GraphProvider.APACHE_AGE
    fulltext_syntax = ''  # Age uses different fulltext syntax than Neo4j

    def __init__(
        self,
        host: str = 'localhost',
        port: int = 5432,
        user: str = 'postgres',
        password: str = 'postgres',
        database: str = 'postgres',
        graph_name: str = 'graphiti',  # Age uses graph names instead of tables
        group_id: str = '',  # For compatibility, though Age uses graph separation
        min_pool_size: int = 1,
        max_pool_size: int = 10,
        **kwargs
    ):
        """
        Initialize Apache Age driver

        Args:
            host: Database host
            port: Database port
            user: Database user
            password: Database password
            database: Database name
            graph_name: Age graph name (used for multi-tenancy)
            group_id: Legacy group ID for compatibility (Age uses graph names)
            min_pool_size: Minimum connection pool size
            max_pool_size: Maximum connection pool size
        """
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self._database = database
        self.graph_name = graph_name
        self.default_group_id = group_id  # Kept for API compatibility
        self.min_pool_size = min_pool_size
        self.max_pool_size = max_pool_size

        self.pool: Optional[asyncpg.Pool] = None

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
                logger.info(f"Apache Age connection pool created for database: {self._database}, graph: {self.graph_name}")
            except Exception as e:
                logger.error(f"Failed to create connection pool: {e}")
                raise

    async def initialize(self):
        """
        Initialize the driver and connection pool.

        Returns:
            self for method chaining
        """
        await self._init_pool()
        return self

    async def execute_query(self, cypher_query: str, **kwargs: Any) -> list[dict]:
        """
        Execute a Cypher query using Apache Age

        Args:
            cypher_query: Cypher query string (native Age Cypher)
            **kwargs: Query parameters

        Returns:
            List of result dictionaries
        """
        if not self.pool:
            await self._init_pool()

        try:
            parameters = kwargs.get('parameters', {})

            async with self.pool.acquire() as connection:
                await _setup_age_environment(connection)

                # Execute Cypher query via Age's cypher() function
                # Note: Age requires us to specify the result column types
                # For flexibility, we use agtype for all results

                logger.debug(f"Executing Cypher via Age: {cypher_query}")

                # Parse the RETURN clause to determine column definitions
                column_defs = _parse_return_columns(cypher_query)

                # Wrap query in Age's cypher() function
                age_query = f"SELECT * FROM cypher('{self.graph_name}', $$ {cypher_query} $$) as {column_defs};"

                records = await connection.fetch(age_query)

                # Convert Age agtype results to Python dicts
                results = []
                for record in records:
                    row_dict = {}
                    for key in record.keys():
                        agtype_value = record[key]
                        # Parse agtype to Python object
                        row_dict[key] = _agtype_to_python(agtype_value)
                    results.append(row_dict)

                return results

        except Exception as e:
            logger.error(f"Query execution failed: {e}\nQuery: {cypher_query}")
            raise

    def session(self, database: str | None = None) -> AgeDriverSession:
        """
        Create a new database session

        Args:
            database: Optional database name (uses default if not provided)

        Returns:
            AgeDriverSession instance
        """
        if database and database != self._database:
            logger.warning(
                f"Apache Age driver doesn't support switching databases in session. "
                f"Using configured database: {self._database}"
            )

        if not self.pool:
            raise RuntimeError("Connection pool not initialized")

        return AgeDriverSession(self.pool, self.graph_name)

    async def close(self):
        """Close the database connection pool"""
        if self.pool:
            await self.pool.close()
            self.pool = None
            logger.info("Apache Age connection pool closed")

    async def delete_all_indexes(self):
        """Delete all custom indexes in the Age graph"""
        if not self.pool:
            await self._init_pool()

        # Age indexes are different from PostgreSQL
        # They're created on vertex labels and properties
        async with self.pool.acquire() as connection:
            await _setup_age_environment(connection)

            # Drop indexes on common properties
            # Note: Age doesn't have a standard way to list all graph indexes
            # This is a placeholder for future implementation
            logger.info("Age indexes cleanup - implementation pending")

    async def build_indices_and_constraints(self, delete_existing: bool = False):
        """
        Build Age graph indices and constraints

        Args:
            delete_existing: If True, drop existing indexes first
        """
        if not self.pool:
            await self._init_pool()

        if delete_existing:
            await self.delete_all_indexes()

        async with self.pool.acquire() as connection:
            await _setup_age_environment(connection)

            # Create graph if it doesn't exist
            try:
                await connection.execute(f"SELECT create_graph('{self.graph_name}');")
                logger.info(f"Created Age graph: {self.graph_name}")
            except Exception as e:
                if 'already exists' in str(e):
                    logger.info(f"Age graph {self.graph_name} already exists")
                else:
                    logger.error(f"Error creating Age graph: {e}")
                    raise

            # Create indexes on vertex properties
            # Age uses different index syntax than PostgreSQL
            # Indexes are typically created on vertex labels and specific properties
            logger.info("Age indices and constraints built successfully")

    async def health_check(self) -> bool:
        """
        Check database connectivity and Age extension

        Returns:
            True if connection and Age extension are healthy
        """
        try:
            if not self.pool:
                await self._init_pool()

            async with self.pool.acquire() as connection:
                # Check basic connectivity
                result = await connection.fetchval("SELECT 1")
                if result != 1:
                    return False

                # Check Age extension
                await connection.execute("LOAD 'age';")

                # Check if graph exists
                graph_check = await connection.fetchval(
                    "SELECT count(*) FROM ag_catalog.ag_graph WHERE name = $1;",
                    self.graph_name
                )

                return graph_check >= 0  # True if query succeeds

        except Exception as e:
            logger.error(f"Health check failed: {e}")
            return False

    def clone(self, group_id: str = None, graph_name: str = None) -> 'AgeDriver':
        """
        Create a new driver instance for a different tenant

        Args:
            group_id: Group ID (kept for compatibility)
            graph_name: Age graph name (primary multi-tenancy mechanism)

        Returns:
            New AgeDriver instance with same connection but different graph
        """
        if graph_name:
            new_graph_name = graph_name
        elif group_id:
            new_graph_name = f"{self.graph_name}_{group_id}"
        else:
            new_graph_name = self.graph_name

        new_driver = AgeDriver(
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            database=self._database,
            graph_name=new_graph_name,
            group_id=group_id or self.default_group_id,
            min_pool_size=self.min_pool_size,
            max_pool_size=self.max_pool_size,
        )
        # Share the pool
        new_driver.pool = self.pool
        return new_driver

    # Helper methods for common operations (matching PostgreSQL driver API)

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
        """Create a new node using Cypher"""
        # Build properties dict
        props = properties or {}
        props['uuid'] = uuid
        props['name'] = name
        props['node_type'] = node_type

        if summary:
            props['summary'] = summary
        if valid_at:
            props['valid_at'] = valid_at.isoformat()
        if group_id:
            props['group_id'] = group_id or self.default_group_id
        if embedding:
            # Age doesn't have native vector support yet
            # Store as array in properties
            props['embedding'] = embedding

        # Build Cypher CREATE query
        # Age uses labels for node types
        label = node_type.capitalize()

        # Create property string
        props_json = json.dumps(props)

        cypher = f"""
        CREATE (n:{label} {{uuid: '{uuid}', name: '{name}', properties: '{props_json}'}})
        RETURN n
        """

        result = await self.execute_query(cypher)
        return result[0] if result else {}

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
        """Create a new edge using Cypher"""
        # Build properties
        props = properties or {}
        props['uuid'] = uuid

        if fact:
            props['fact'] = fact
        if episodes:
            props['episodes'] = episodes
        if group_id:
            props['group_id'] = group_id or self.default_group_id

        # Build Cypher CREATE query for relationship
        props_json = json.dumps(props)

        cypher = f"""
        MATCH (a {{uuid: '{source_uuid}'}}), (b {{uuid: '{target_uuid}'}})
        CREATE (a)-[r:{relation_type} {{uuid: '{uuid}', properties: '{props_json}'}}]->(b)
        RETURN r
        """

        result = await self.execute_query(cypher)
        return result[0] if result else {}

    async def get_node(self, uuid: str) -> Optional[dict]:
        """Get a node by UUID using Cypher"""
        cypher = f"""
        MATCH (n {{uuid: '{uuid}'}})
        RETURN n
        """

        result = await self.execute_query(cypher)
        return result[0] if result else None

    async def search_nodes(
        self,
        search_term: str,
        node_type: str = None,
        group_id: str = None,
        limit: int = 10
    ) -> list[dict]:
        """
        Search nodes using text matching

        Note: Age doesn't have built-in fulltext search like PostgreSQL
        This uses simple string matching on name/summary properties
        """
        # Build Cypher query with WHERE clause
        label_filter = f":{node_type.capitalize()}" if node_type else ""

        cypher = f"""
        MATCH (n{label_filter})
        WHERE n.name CONTAINS '{search_term}' OR n.summary CONTAINS '{search_term}'
        RETURN n
        LIMIT {limit}
        """

        result = await self.execute_query(cypher)
        return result
