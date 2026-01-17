-- Apache Age Schema for Graphiti Knowledge Graph
-- This schema initializes Apache Age extension and creates the default graph

-- Load Apache Age extension
CREATE EXTENSION IF NOT EXISTS age;

-- Load AGE into the current session
LOAD 'age';

-- Set search path to include ag_catalog
SET search_path = ag_catalog, "$user", public;

-- Create default graph for Graphiti
-- Note: If the graph already exists, this will fail (expected on subsequent runs)
SELECT create_graph('graphiti');

-- ============================================================================
-- APACHE AGE GRAPH STRUCTURE
-- ============================================================================
--
-- Apache Age stores graphs differently than relational databases:
-- - Vertices (nodes) are stored in ag_catalog with system-managed IDs
-- - Edges (relationships) are first-class graph entities
-- - Properties are stored as 'agtype' (Age's JSONB-like type)
-- - Labels define node types (Entity, Episode, Community)
-- - Relationship types are native (KNOWS, WORKS_AT, etc.)
--
-- ============================================================================

-- ============================================================================
-- NODE LABELS (equivalent to node_type in PostgreSQL driver)
-- ============================================================================
--
-- Graphiti uses three main node types:
-- 1. Entity: Real-world entities (people, places, organizations, concepts)
-- 2. Episode: Events or time-bound interactions
-- 3. Community: Clusters or groups of related entities
--
-- In Age, these are implemented as vertex labels.
-- Labels are created automatically when first vertex with that label is created.
--
-- Example node structure:
-- {
--   uuid: "550e8400-e29b-41d4-a716-446655440000",
--   name: "Alice",
--   node_type: "entity",
--   properties: {...},
--   summary: "Alice is a software engineer",
--   valid_at: "2024-01-15T10:30:00",
--   invalid_at: null,
--   group_id: "tenant_123"
-- }
--
-- ============================================================================

-- ============================================================================
-- PROPERTY INDEXES
-- ============================================================================
--
-- Apache Age supports indexes on vertex properties for performance.
-- Create indexes on frequently queried properties:
-- - uuid: Primary identifier (unique lookup)
-- - group_id: Multi-tenancy filtering
-- - valid_at, invalid_at: Temporal queries
-- - name: Text search
--
-- Note: Age index syntax differs from PostgreSQL
-- Age uses: CREATE INDEX ON vertex_label (property_name)
-- ============================================================================

-- Index on uuid property for Entity nodes
-- Uncomment when Age supports this syntax (currently experimental):
-- CREATE INDEX ON Entity (uuid);
-- CREATE INDEX ON Episode (uuid);
-- CREATE INDEX ON Community (uuid);

-- ============================================================================
-- MULTI-TENANCY SETUP
-- ============================================================================
--
-- For multi-tenancy, we use separate graphs per tenant:
-- - graphiti (default/system graph)
-- - graphiti_tenant_1
-- - graphiti_tenant_2
-- - etc.
--
-- To create a new tenant graph:
-- SELECT create_graph('graphiti_tenant_1');
-- SELECT create_graph('graphiti_tenant_2');
--
-- Alternative: Use group_id property filtering within a single graph
-- (less isolation, but simpler management)
--
-- ============================================================================

-- Example: Create tenant graphs (uncomment as needed)
-- SELECT create_graph('graphiti_tenant_1');
-- SELECT create_graph('graphiti_tenant_2');

-- ============================================================================
-- TEMPORAL TRACKING
-- ============================================================================
--
-- Graphiti uses bi-temporal tracking:
-- - valid_at: When the fact became true (event time)
-- - invalid_at: When the fact became false (or NULL if still valid)
--
-- Example temporal query:
-- MATCH (n:Entity)
-- WHERE n.valid_at <= '2024-01-15T10:00:00'
--   AND (n.invalid_at IS NULL OR n.invalid_at > '2024-01-15T10:00:00')
-- RETURN n
--
-- This returns all entities that were valid at the specified timestamp.
--
-- ============================================================================

-- ============================================================================
-- HELPER FUNCTIONS (Future Implementation)
-- ============================================================================
--
-- Unlike PostgreSQL driver, Age doesn't support SQL functions directly.
-- Graph operations are performed via Cypher queries.
--
-- Common patterns:
--
-- 1. Get node neighbors:
--    MATCH (n {uuid: $uuid})-[r]-(neighbor)
--    RETURN neighbor, r, type(r) as relation_type
--
-- 2. Graph traversal (BFS):
--    MATCH path = (start {uuid: $uuid})-[*1..3]->(end)
--    RETURN path
--
-- 3. Shortest path:
--    MATCH path = shortestPath((a {uuid: $uuid1})-[*]-(b {uuid: $uuid2}))
--    RETURN path
--
-- 4. Variable-length paths:
--    MATCH (a {uuid: $uuid})-[*1..5]->(b)
--    RETURN DISTINCT b
--
-- ============================================================================

-- ============================================================================
-- NOTES ON APACHE AGE VS POSTGRESQL DRIVER
-- ============================================================================
--
-- Key Differences:
--
-- 1. Storage Model:
--    - PostgreSQL: graph_nodes and graph_edges tables
--    - Age: Native graph with vertices and edges
--
-- 2. Cypher Support:
--    - PostgreSQL: Translated to SQL via AST parser
--    - Age: Native Cypher via cypher() function
--
-- 3. Properties:
--    - PostgreSQL: JSONB columns
--    - Age: agtype (similar to JSONB but graph-native)
--
-- 4. Multi-tenancy:
--    - PostgreSQL: group_id column filtering
--    - Age: Separate graphs (cleaner isolation)
--
-- 5. Indexes:
--    - PostgreSQL: B-tree, GIN, IVFFlat (for vectors)
--    - Age: Vertex label indexes, property indexes
--
-- 6. Embeddings:
--    - PostgreSQL: Native pgvector support
--    - Age: Store as array in properties (no native vector ops yet)
--
-- ============================================================================

-- Comments for documentation
COMMENT ON EXTENSION age IS 'Apache Age graph extension for PostgreSQL - enables native graph storage and Cypher queries';
