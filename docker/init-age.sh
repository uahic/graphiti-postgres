#!/bin/bash
# Initialize Apache Age extension and create default graph
# Run this after starting the Age container: ./docker/init-age.sh

set -e

echo "=================================================="
echo "Apache Age Initialization Script"
echo "=================================================="
echo ""

# Check if container is running
if ! docker ps | grep -q graphiti-age; then
    echo "❌ Error: graphiti-age container is not running"
    echo ""
    echo "Start it with:"
    echo "  docker-compose -f docker/docker-compose-age.yml up -d"
    exit 1
fi

echo "✓ Container is running"
echo ""

# Wait for PostgreSQL to be ready
echo "Waiting for PostgreSQL to be ready..."
for i in {1..30}; do
    if docker exec graphiti-age pg_isready -U postgres > /dev/null 2>&1; then
        echo "✓ PostgreSQL is ready"
        break
    fi
    if [ $i -eq 30 ]; then
        echo "❌ Error: PostgreSQL did not become ready in time"
        exit 1
    fi
    sleep 1
done

echo ""
echo "Initializing Apache Age extension..."

# Execute the initialization SQL
docker exec -i graphiti-age psql -U postgres -d postgres <<'EOF'
-- Load Apache Age extension
CREATE EXTENSION IF NOT EXISTS age;

-- Load AGE into the current session
LOAD 'age';

-- Set search path to include ag_catalog
SET search_path = ag_catalog, "$user", public;

-- Create default graph for Graphiti
DO $$
BEGIN
    -- Try to create the graph, ignore if it already exists
    PERFORM create_graph('graphiti');
    RAISE NOTICE 'Created graph: graphiti';
EXCEPTION
    WHEN duplicate_object THEN
        RAISE NOTICE 'Graph graphiti already exists, skipping creation';
END $$;

-- Verify the setup
SELECT 'Apache Age version: ' || extversion AS info FROM pg_extension WHERE extname = 'age';
SELECT 'Graph count: ' || count(*) AS info FROM ag_catalog.ag_graph;
SELECT 'Graph name: ' || name AS info FROM ag_catalog.ag_graph;

EOF

if [ $? -eq 0 ]; then
    echo ""
    echo "=================================================="
    echo "✅ Apache Age initialized successfully!"
    echo "=================================================="
    echo ""
    echo "The following graph has been created:"
    echo "  - graphiti (default graph)"
    echo ""
    echo "You can now use the AgeDriver:"
    echo ""
    echo "  from graphiti_postgres import AgeDriver"
    echo ""
    echo "  driver = AgeDriver("
    echo "      host='localhost',"
    echo "      port=5432,"
    echo "      user='postgres',"
    echo "      password='postgres',"
    echo "      database='postgres',"
    echo "      graph_name='graphiti'"
    echo "  )"
    echo ""
else
    echo ""
    echo "❌ Error: Failed to initialize Apache Age"
    exit 1
fi
