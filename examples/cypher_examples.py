"""
Examples demonstrating the Cypher parser capabilities
Run these to see how Cypher queries are translated to PostgreSQL SQL
"""

import sys
from pathlib import Path

# Add parent directory to path so we can import cypher module
sys.path.insert(0, str(Path(__file__).parent.parent))

from cypher.parser import CypherParser
from cypher.sql_generator import SQLGenerator


def print_translation(title: str, cypher: str, params: dict = None):
    """Helper to show Cypher to SQL translation"""
    print(f"\n{'='*80}")
    print(f"Example: {title}")
    print(f"{'='*80}")
    print(f"\nCypher Query:")
    print(cypher)

    parser = CypherParser()
    generator = SQLGenerator(group_id='example_group')

    try:
        ast = parser.parse(cypher)
        sql, sql_params = generator.generate(ast, params or {})

        print(f"\nGenerated SQL:")
        print(sql)
        print(f"\nParameters: {sql_params}")
    except Exception as e:
        print(f"\nError: {e}")


def main():
    """Run all examples"""

    # Example 1: Simple MATCH
    print_translation(
        "Simple Node Match",
        "MATCH (n:Person) RETURN n.name, n.age"
    )

    # Example 2: Relationship traversal
    print_translation(
        "Relationship Traversal",
        """
        MATCH (a:Person)-[r:KNOWS]->(b:Person)
        WHERE a.age > 25
        RETURN a.name AS person, b.name AS friend
        """
    )

    # Example 3: Variable-length path
    print_translation(
        "Variable-Length Path (Friends of Friends)",
        """
        MATCH (a:Person)-[:KNOWS*1..3]->(b:Person)
        WHERE a.name = 'Alice'
        RETURN DISTINCT b.name AS connection
        """
    )

    # Example 4: OPTIONAL MATCH
    print_translation(
        "Optional Relationships",
        """
        MATCH (p:Person)
        OPTIONAL MATCH (p)-[:LIKES]->(m:Movie)
        RETURN p.name, m.title
        """
    )

    # Example 5: WITH clause (query chaining)
    print_translation(
        "Query Chaining with WITH",
        """
        MATCH (p:Person)-[:LIVES_IN]->(c:City)
        WITH c.name AS city, COUNT(p) AS population
        WHERE population > 1000
        RETURN city, population
        ORDER BY population DESC
        LIMIT 10
        """
    )

    # Example 6: Aggregation
    print_translation(
        "Aggregation and Grouping",
        """
        MATCH (p:Person)-[:WORKS_AT]->(c:Company)
        RETURN c.name AS company, COUNT(p) AS employeeCount, AVG(p.salary) AS avgSalary
        ORDER BY employeeCount DESC
        """
    )

    # Example 7: Complex WHERE conditions
    print_translation(
        "Complex Filtering",
        """
        MATCH (p:Person)
        WHERE p.age >= 18 AND p.age <= 65
          AND (p.city = 'NYC' OR p.city = 'SF')
          AND p.email IS NOT NULL
        RETURN p.name, p.age, p.city
        """
    )

    # Example 8: CASE expression
    print_translation(
        "CASE Expression",
        """
        MATCH (p:Person)
        RETURN p.name,
               CASE
                 WHEN p.age < 18 THEN 'minor'
                 WHEN p.age < 65 THEN 'adult'
                 ELSE 'senior'
               END AS ageGroup
        """
    )

    # Example 9: CREATE
    print_translation(
        "Create New Node",
        """
        CREATE (p:Person {name: 'Bob', age: 30, city: 'NYC'})
        """
    )

    # Example 10: MERGE (Upsert)
    print_translation(
        "Merge (Upsert) Node",
        """
        MERGE (p:Person {email: 'alice@example.com'})
        SET p.name = 'Alice', p.lastSeen = timestamp()
        """
    )

    # Example 11: Parameters
    print_translation(
        "Using Parameters",
        """
        MATCH (p:Person {id: $personId})
        WHERE p.age > $minAge
        RETURN p
        """,
        params={'personId': 123, 'minAge': 25}
    )

    # Example 12: UNION
    print_translation(
        "UNION Query",
        """
        MATCH (p:Person) RETURN p.name AS name
        UNION
        MATCH (c:Company) RETURN c.name AS name
        """
    )

    # Example 13: String operations
    print_translation(
        "String Matching",
        """
        MATCH (p:Person)
        WHERE p.name STARTS WITH 'A'
          AND p.email CONTAINS '@example.com'
        RETURN p.name
        """
    )

    # Example 14: List operations
    print_translation(
        "List Membership",
        """
        MATCH (p:Person)
        WHERE p.city IN ['NYC', 'SF', 'LA']
        RETURN p.name, p.city
        """
    )

    # Example 15: Multiple relationship types
    print_translation(
        "Multiple Relationship Types",
        """
        MATCH (p:Person)-[r:KNOWS|:WORKS_WITH]->(other:Person)
        WHERE p.name = 'Alice'
        RETURN other.name, type(r) AS relationshipType
        """
    )


if __name__ == '__main__':
    main()
