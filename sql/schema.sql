-- PostgreSQL Schema for Graphiti Knowledge Graph
-- This schema stores nodes and edges for the Graphiti graph database driver

-- Enable required extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_trgm"; -- For text search
CREATE EXTENSION IF NOT EXISTS "vector"; -- For embeddings

-- Graph Nodes table
-- Stores all entities, episodes, and communities
CREATE TABLE IF NOT EXISTS graph_nodes (
    uuid UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    name TEXT NOT NULL,
    node_type TEXT NOT NULL CHECK (node_type IN ('entity', 'episode', 'community')),
    group_id TEXT NOT NULL DEFAULT '',

    -- Temporal fields for bi-temporal tracking
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    valid_at TIMESTAMP, -- Event occurrence time
    invalid_at TIMESTAMP, -- When the node becomes invalid

    -- Node properties stored as JSONB for flexibility
    properties JSONB DEFAULT '{}'::jsonb,

    -- Embedding for semantic search (stored as array of floats)
    embedding vector(1536), -- Assumes OpenAI embeddings, adjust size as needed

    -- Summary text for BM25/fulltext search
    summary TEXT,

    -- Metadata
    metadata JSONB DEFAULT '{}'::jsonb
);

-- Graph Edges table
-- Stores relationships between nodes
CREATE TABLE IF NOT EXISTS graph_edges (
    uuid UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    source_node_uuid UUID NOT NULL REFERENCES graph_nodes(uuid) ON DELETE CASCADE,
    target_node_uuid UUID NOT NULL REFERENCES graph_nodes(uuid) ON DELETE CASCADE,

    -- Relationship type
    relation_type TEXT NOT NULL,

    -- Temporal fields
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    valid_at TIMESTAMP,
    invalid_at TIMESTAMP,

    -- Edge properties
    properties JSONB DEFAULT '{}'::jsonb,

    -- Group for multi-tenancy
    group_id TEXT NOT NULL DEFAULT '',

    -- Fact/evidence that led to this edge
    fact TEXT,
    episodes UUID[] DEFAULT ARRAY[]::UUID[],

    -- Metadata
    metadata JSONB DEFAULT '{}'::jsonb
);

-- ============================================================================
-- INDEXES FOR PERFORMANCE
-- Optimized for: Graph traversal, temporal queries, RAG (Vector + Text)
-- ============================================================================

-- ----------------------------------------------------------------------------
-- NODE INDEXES
-- ----------------------------------------------------------------------------

-- Basic node type index (used for filtering by entity/episode/community)
CREATE INDEX IF NOT EXISTS idx_nodes_type ON graph_nodes(node_type);

-- Multi-tenancy index (critical for group isolation)
CREATE INDEX IF NOT EXISTS idx_nodes_group ON graph_nodes(group_id);

-- Composite index for common query patterns (type + group filtering)
CREATE INDEX IF NOT EXISTS idx_nodes_type_group ON graph_nodes(node_type, group_id);

-- **CRITICAL: Bi-temporal index for historical queries**
-- Enables efficient "valid at time T" queries for temporal AI agents
-- Supports: historical state retrieval, time-travel queries, Graphiti temporal semantics
CREATE INDEX IF NOT EXISTS idx_nodes_temporal_range
ON graph_nodes (group_id, node_type, valid_at, invalid_at);

-- JSONB GIN indexes for flexible property queries
-- Allows efficient queries on arbitrary JSON fields
CREATE INDEX IF NOT EXISTS idx_nodes_properties ON graph_nodes USING GIN (properties);
CREATE INDEX IF NOT EXISTS idx_nodes_metadata ON graph_nodes USING GIN (metadata);

-- Fulltext/trigram search index for fuzzy text matching
-- Enables semantic text search with pg_trgm similarity
CREATE INDEX IF NOT EXISTS idx_nodes_summary_trgm ON graph_nodes USING GIN (summary gin_trgm_ops);

-- **CRITICAL: Vector similarity index for RAG**
-- IVFFlat index for approximate nearest neighbor (ANN) search
-- Without this index, vector similarity searches are prohibitively slow
-- lists=100: creates 100 centroids for clustering (tune based on dataset size)
-- Rule of thumb: lists ≈ sqrt(total_rows), adjust between 50-1000
CREATE INDEX IF NOT EXISTS idx_nodes_embedding_vector
ON graph_nodes
USING ivfflat (embedding vector_cosine_ops)
WITH (lists = 100);

-- ----------------------------------------------------------------------------
-- EDGE INDEXES
-- ----------------------------------------------------------------------------

-- Basic edge component indexes (kept for reverse traversals and analytics)
CREATE INDEX IF NOT EXISTS idx_edges_source ON graph_edges(source_node_uuid);
CREATE INDEX IF NOT EXISTS idx_edges_target ON graph_edges(target_node_uuid);
CREATE INDEX IF NOT EXISTS idx_edges_relation ON graph_edges(relation_type);

-- Multi-tenancy index (important for group-based edge filtering)
CREATE INDEX IF NOT EXISTS idx_edges_group ON graph_edges(group_id);

-- **CRITICAL: Hot-path traversal index (source → relation → target)**
-- Optimized for BFS/DFS graph traversals - covers the most common JOIN pattern
-- Enables index-only scans for "find all X-type edges from node Y"
-- This is the primary index for graph navigation queries
CREATE INDEX IF NOT EXISTS idx_edges_traversal_hot_path
ON graph_edges (source_node_uuid, relation_type, target_node_uuid);

-- **CRITICAL: Temporal index for time-aware traversals**
-- Enables "which relationships were valid at time T?" queries
-- Essential for historical graph analysis and temporal reasoning
CREATE INDEX IF NOT EXISTS idx_edges_temporal
ON graph_edges (source_node_uuid, relation_type, valid_at, invalid_at);

-- Legacy composite indexes (kept for backward compatibility and specific query patterns)
-- These may be redundant with hot-path index but kept to avoid query regressions
CREATE INDEX IF NOT EXISTS idx_edges_source_target ON graph_edges(source_node_uuid, target_node_uuid);
CREATE INDEX IF NOT EXISTS idx_edges_source_type ON graph_edges(source_node_uuid, relation_type);

-- JSONB indexes for edge properties
-- Supports queries on edge attributes and episode arrays
CREATE INDEX IF NOT EXISTS idx_edges_properties ON graph_edges USING GIN (properties);
CREATE INDEX IF NOT EXISTS idx_edges_episodes ON graph_edges USING GIN (episodes);

-- Helper function to search nodes by text
CREATE OR REPLACE FUNCTION search_nodes_fulltext(
    search_term TEXT,
    node_type_filter TEXT DEFAULT NULL,
    group_filter TEXT DEFAULT '',
    limit_count INT DEFAULT 10
)
RETURNS TABLE (
    uuid UUID,
    name TEXT,
    node_type TEXT,
    summary TEXT,
    similarity REAL
) AS $$
BEGIN
    RETURN QUERY
    SELECT
        n.uuid,
        n.name,
        n.node_type,
        n.summary,
        similarity(n.summary, search_term) as similarity
    FROM graph_nodes n
    WHERE
        n.group_id = group_filter
        AND (node_type_filter IS NULL OR n.node_type = node_type_filter)
        AND n.summary % search_term -- pg_trgm similarity operator
    ORDER BY similarity DESC
    LIMIT limit_count;
END;
$$ LANGUAGE plpgsql;

-- Helper function for graph traversal (BFS)
DROP FUNCTION IF EXISTS traverse_graph(UUID, INT, TEXT);
CREATE OR REPLACE FUNCTION traverse_graph(
    start_node_uuid UUID,
    max_depth INT DEFAULT 3,
    edge_type_filter TEXT DEFAULT NULL
)
RETURNS TABLE (
    uuid UUID,
    depth INT,
    path UUID[]
) AS $$
WITH RECURSIVE graph_traversal AS (
    -- Base case: start node
    SELECT
        start_node_uuid as node_uuid,
        0 as depth,
        ARRAY[start_node_uuid] as path

    UNION ALL

    -- Recursive case: follow edges
    SELECT
        e.target_node_uuid,
        gt.depth + 1,
        gt.path || e.target_node_uuid
    FROM graph_traversal gt
    JOIN graph_edges e ON e.source_node_uuid = gt.node_uuid
    WHERE
        gt.depth < max_depth
        AND NOT (e.target_node_uuid = ANY(gt.path)) -- Avoid cycles
        AND (edge_type_filter IS NULL OR e.relation_type = edge_type_filter)
)
SELECT DISTINCT ON (node_uuid)
    node_uuid as uuid,
    depth,
    path
FROM graph_traversal
ORDER BY node_uuid, depth;
$$ LANGUAGE sql;

-- Helper function to get node neighbors
DROP FUNCTION IF EXISTS get_node_neighbors(UUID, TEXT);
CREATE OR REPLACE FUNCTION get_node_neighbors(
    node_uuid UUID,
    direction_param TEXT DEFAULT 'both' -- 'outgoing', 'incoming', or 'both'
)
RETURNS TABLE (
    neighbor_uuid UUID,
    relation_type TEXT,
    direction TEXT
) AS $$
BEGIN
    RETURN QUERY
    SELECT
        e.target_node_uuid as neighbor_uuid,
        e.relation_type,
        'outgoing'::TEXT as direction
    FROM graph_edges e
    WHERE e.source_node_uuid = node_uuid
    AND direction_param IN ('both', 'outgoing')

    UNION ALL

    SELECT
        e.source_node_uuid as neighbor_uuid,
        e.relation_type,
        'incoming'::TEXT as direction
    FROM graph_edges e
    WHERE e.target_node_uuid = node_uuid
    AND direction_param IN ('both', 'incoming');
END;
$$ LANGUAGE plpgsql;

-- Comments for documentation
COMMENT ON TABLE graph_nodes IS 'Stores all graph nodes (entities, episodes, communities) for Graphiti knowledge graph';
COMMENT ON TABLE graph_edges IS 'Stores relationships between graph nodes';
COMMENT ON COLUMN graph_nodes.valid_at IS 'Timestamp when this node became valid (event occurrence time)';
COMMENT ON COLUMN graph_nodes.invalid_at IS 'Timestamp when this node became invalid (null = still valid)';
COMMENT ON COLUMN graph_nodes.embedding IS 'Vector embedding for semantic similarity search';
COMMENT ON COLUMN graph_edges.episodes IS 'Array of episode UUIDs that support this edge';
