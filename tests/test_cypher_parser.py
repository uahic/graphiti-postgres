"""
Comprehensive test suite for Cypher parser and SQL generator
"""

import sys
import unittest
from pathlib import Path

# Add parent directory to path so we can import modules
sys.path.insert(0, str(Path(__file__).parent.parent))

from cypher import CypherParser, SQLGenerator
from cypher.ast_nodes import *


class TestCypherParser(unittest.TestCase):
    """Test Cypher parsing"""

    def setUp(self):
        self.parser = CypherParser()

    def test_simple_match(self):
        """Test simple MATCH query"""
        query = "MATCH (n:Person) RETURN n"
        ast = self.parser.parse(query)

        self.assertIsInstance(ast, Query)
        self.assertEqual(len(ast.clauses), 2)
        self.assertIsInstance(ast.clauses[0], MatchClause)
        self.assertIsInstance(ast.clauses[1], ReturnClause)

    def test_match_with_properties(self):
        """Test MATCH with property filters"""
        query = "MATCH (n:Person {name: 'Alice', age: 30}) RETURN n"
        ast = self.parser.parse(query)

        match_clause = ast.clauses[0]
        self.assertIsInstance(match_clause, MatchClause)

        pattern = match_clause.patterns[0]
        node = pattern.elements[0].nodes[0]
        self.assertEqual(node.variable, 'n')
        self.assertEqual(node.labels, ['Person'])
        self.assertIsNotNone(node.properties)

    def test_match_with_relationship(self):
        """Test MATCH with relationships"""
        query = "MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN a, r, b"
        ast = self.parser.parse(query)

        match_clause = ast.clauses[0]
        element = match_clause.patterns[0].elements[0]

        self.assertEqual(len(element.nodes), 2)
        self.assertEqual(len(element.relationships), 1)

        rel = element.relationships[0]
        self.assertEqual(rel.variable, 'r')
        self.assertEqual(rel.types, ['KNOWS'])
        self.assertEqual(rel.direction, Direction.OUTGOING)

    def test_variable_length_path(self):
        """Test variable-length relationships"""
        query = "MATCH (a)-[r:KNOWS*1..3]->(b) RETURN a, b"
        ast = self.parser.parse(query)

        rel = ast.clauses[0].patterns[0].elements[0].relationships[0]
        self.assertEqual(rel.min_hops, 1)
        self.assertEqual(rel.max_hops, 3)

    def test_where_clause(self):
        """Test WHERE clause"""
        query = "MATCH (n:Person) WHERE n.age > 25 AND n.city = 'NYC' RETURN n"
        ast = self.parser.parse(query)

        match_clause = ast.clauses[0]
        self.assertIsNotNone(match_clause.where)
        self.assertIsInstance(match_clause.where, BinaryOp)
        self.assertEqual(match_clause.where.operator, 'AND')

    def test_return_with_alias(self):
        """Test RETURN with aliases"""
        query = "MATCH (n:Person) RETURN n.name AS personName, n.age AS personAge"
        ast = self.parser.parse(query)

        return_clause = ast.clauses[1]
        self.assertEqual(len(return_clause.items), 2)
        self.assertEqual(return_clause.items[0].alias, 'personName')
        self.assertEqual(return_clause.items[1].alias, 'personAge')

    def test_order_by(self):
        """Test ORDER BY clause"""
        query = "MATCH (n:Person) RETURN n.name ORDER BY n.age DESC, n.name ASC"
        ast = self.parser.parse(query)

        return_clause = ast.clauses[1]
        self.assertIsNotNone(return_clause.order_by)
        self.assertEqual(len(return_clause.order_by), 2)
        self.assertEqual(return_clause.order_by[0].order, SortOrder.DESC)
        self.assertEqual(return_clause.order_by[1].order, SortOrder.ASC)

    def test_limit_skip(self):
        """Test LIMIT and SKIP"""
        query = "MATCH (n:Person) RETURN n SKIP 10 LIMIT 20"
        ast = self.parser.parse(query)

        return_clause = ast.clauses[1]
        self.assertIsNotNone(return_clause.skip)
        self.assertIsNotNone(return_clause.limit)

    def test_create(self):
        """Test CREATE clause"""
        query = "CREATE (n:Person {name: 'Bob', age: 25})"
        ast = self.parser.parse(query)

        self.assertIsInstance(ast.clauses[0], CreateClause)
        node = ast.clauses[0].patterns[0].elements[0].nodes[0]
        self.assertEqual(node.labels, ['Person'])

    def test_merge(self):
        """Test MERGE clause"""
        query = "MERGE (n:Person {id: 123})"
        ast = self.parser.parse(query)

        self.assertIsInstance(ast.clauses[0], MergeClause)

    def test_delete(self):
        """Test DELETE clause"""
        query = "MATCH (n:Person) DELETE n"
        ast = self.parser.parse(query)

        self.assertEqual(len(ast.clauses), 2)
        self.assertIsInstance(ast.clauses[1], DeleteClause)
        self.assertFalse(ast.clauses[1].detach)

    def test_detach_delete(self):
        """Test DETACH DELETE"""
        query = "MATCH (n:Person) DETACH DELETE n"
        ast = self.parser.parse(query)

        delete_clause = ast.clauses[1]
        self.assertTrue(delete_clause.detach)

    def test_set_clause(self):
        """Test SET clause"""
        query = "MATCH (n:Person) SET n.age = 30, n.city = 'NYC'"
        ast = self.parser.parse(query)

        self.assertIsInstance(ast.clauses[1], SetClause)
        self.assertEqual(len(ast.clauses[1].items), 2)

    def test_with_clause(self):
        """Test WITH clause"""
        query = "MATCH (n:Person) WITH n.age AS age WHERE age > 25 RETURN age"
        ast = self.parser.parse(query)

        self.assertEqual(len(ast.clauses), 3)
        self.assertIsInstance(ast.clauses[1], WithClause)

    def test_optional_match(self):
        """Test OPTIONAL MATCH"""
        query = "MATCH (a:Person) OPTIONAL MATCH (a)-[r:KNOWS]->(b) RETURN a, b"
        ast = self.parser.parse(query)

        self.assertEqual(len(ast.clauses), 3)
        self.assertTrue(ast.clauses[1].optional)

    def test_union(self):
        """Test UNION queries"""
        query = "MATCH (n:Person) RETURN n UNION MATCH (m:Company) RETURN m"
        ast = self.parser.parse(query)

        self.assertEqual(len(ast.unions), 1)
        self.assertFalse(ast.union_all)

    def test_union_all(self):
        """Test UNION ALL"""
        query = "MATCH (n:Person) RETURN n UNION ALL MATCH (m:Person) RETURN m"
        ast = self.parser.parse(query)

        self.assertEqual(len(ast.unions), 1)
        self.assertTrue(ast.union_all)

    def test_aggregation_functions(self):
        """Test aggregation functions"""
        query = "MATCH (n:Person) RETURN COUNT(n), AVG(n.age), MAX(n.age)"
        ast = self.parser.parse(query)

        return_clause = ast.clauses[1]
        self.assertEqual(len(return_clause.items), 3)
        self.assertIsInstance(return_clause.items[0].expression, FunctionCall)
        self.assertEqual(return_clause.items[0].expression.name, 'COUNT')

    def test_case_expression(self):
        """Test CASE expression"""
        query = """
        MATCH (n:Person)
        RETURN CASE WHEN n.age < 18 THEN 'minor' ELSE 'adult' END AS status
        """
        ast = self.parser.parse(query)

        return_clause = ast.clauses[1]
        self.assertIsInstance(return_clause.items[0].expression, CaseExpression)

    def test_list_literal(self):
        """Test list literals"""
        query = "RETURN [1, 2, 3, 4, 5] AS numbers"
        ast = self.parser.parse(query)

        return_clause = ast.clauses[0]
        self.assertIsInstance(return_clause.items[0].expression, ListLiteral)

    def test_map_literal(self):
        """Test map literals"""
        query = "RETURN {name: 'Alice', age: 30} AS person"
        ast = self.parser.parse(query)

        return_clause = ast.clauses[0]
        self.assertIsInstance(return_clause.items[0].expression, MapLiteral)

    def test_parameters(self):
        """Test parameter references"""
        query = "MATCH (n:Person {id: $personId}) RETURN n"
        ast = self.parser.parse(query)

        # Parameters should be in properties
        node = ast.clauses[0].patterns[0].elements[0].nodes[0]
        self.assertIsNotNone(node.properties)


class TestSQLGenerator(unittest.TestCase):
    """Test SQL generation from AST"""

    def setUp(self):
        self.parser = CypherParser()
        self.generator = SQLGenerator(group_id='test_group')

    def test_simple_match_sql(self):
        """Test SQL generation for simple MATCH"""
        query = "MATCH (n:Entity) RETURN n"
        ast = self.parser.parse(query)
        sql, params = self.generator.generate(ast)

        self.assertIn('SELECT', sql)
        self.assertIn('FROM graph_nodes', sql)
        self.assertIn('node_type', sql)
        self.assertIn('test_group', params)

    def test_match_with_where_sql(self):
        """Test SQL generation with WHERE clause"""
        query = "MATCH (n:Entity) WHERE n.name = 'Alice' RETURN n"
        ast = self.parser.parse(query)
        sql, params = self.generator.generate(ast, {'name': 'Alice'})

        self.assertIn('WHERE', sql)
        self.assertIn('AND', sql)

    def test_relationship_join_sql(self):
        """Test SQL generation for relationships"""
        query = "MATCH (a:Entity)-[r:RELATES_TO]->(b:Entity) RETURN a, b"
        ast = self.parser.parse(query)
        sql, params = self.generator.generate(ast)

        self.assertIn('JOIN graph_edges', sql)
        self.assertIn('source_node_uuid', sql)
        self.assertIn('target_node_uuid', sql)

    def test_order_by_sql(self):
        """Test SQL generation with ORDER BY"""
        query = "MATCH (n:Entity) RETURN n ORDER BY n.name DESC"
        ast = self.parser.parse(query)
        sql, params = self.generator.generate(ast)

        self.assertIn('ORDER BY', sql)
        self.assertIn('DESC', sql)

    def test_limit_skip_sql(self):
        """Test SQL generation with LIMIT and SKIP"""
        query = "MATCH (n:Entity) RETURN n SKIP 5 LIMIT 10"
        ast = self.parser.parse(query)
        sql, params = self.generator.generate(ast)

        self.assertIn('LIMIT', sql)
        self.assertIn('OFFSET', sql)

    def test_create_sql(self):
        """Test SQL generation for CREATE"""
        query = "CREATE (n:Entity {name: 'Test'})"
        ast = self.parser.parse(query)
        sql, params = self.generator.generate(ast)

        self.assertIn('INSERT INTO graph_nodes', sql)
        self.assertIn('RETURNING', sql)

    def test_merge_sql(self):
        """Test SQL generation for MERGE"""
        query = "MERGE (n:Entity {id: 123})"
        ast = self.parser.parse(query)
        sql, params = self.generator.generate(ast)

        self.assertIn('INSERT INTO graph_nodes', sql)
        self.assertIn('ON CONFLICT', sql)
        self.assertIn('DO UPDATE', sql)

    def test_delete_sql(self):
        """Test SQL generation for DELETE"""
        query = "MATCH (n:Entity) DELETE n"
        ast = self.parser.parse(query)
        sql, params = self.generator.generate(ast)

        self.assertIn('DELETE FROM graph_nodes', sql)

    def test_parameter_substitution(self):
        """Test parameter handling"""
        query = "MATCH (n:Entity {name: $name}) RETURN n"
        ast = self.parser.parse(query)
        sql, params = self.generator.generate(ast, {'name': 'Alice'})

        self.assertIn('Alice', params)
        self.assertIn('$', sql)

    def test_property_access_sql(self):
        """Test property access in SQL"""
        query = "MATCH (n:Entity) RETURN n.name, n.age"
        ast = self.parser.parse(query)
        sql, params = self.generator.generate(ast)

        # Should use JSONB operators for property access
        self.assertIn('properties', sql)

    def test_aggregation_sql(self):
        """Test aggregation functions"""
        query = "MATCH (n:Entity) RETURN COUNT(n) AS total"
        ast = self.parser.parse(query)
        sql, params = self.generator.generate(ast)

        self.assertIn('COUNT', sql)


class TestIntegration(unittest.TestCase):
    """Integration tests for complete translation pipeline"""

    def setUp(self):
        self.parser = CypherParser()
        self.generator = SQLGenerator(group_id='test_group')

    def translate(self, cypher, params=None):
        """Helper to translate Cypher to SQL"""
        ast = self.parser.parse(cypher)
        return self.generator.generate(ast, params or {})

    def test_complex_query(self):
        """Test complex query with multiple clauses"""
        query = """
        MATCH (a:Person)-[r:KNOWS]->(b:Person)
        WHERE a.age > 25 AND b.city = 'NYC'
        RETURN a.name AS name, COUNT(b) AS friendCount
        ORDER BY friendCount DESC
        LIMIT 10
        """
        sql, params = self.translate(query)

        self.assertIn('SELECT', sql)
        self.assertIn('JOIN', sql)
        self.assertIn('WHERE', sql)
        self.assertIn('ORDER BY', sql)
        self.assertIn('LIMIT', sql)

    def test_graph_traversal(self):
        """Test graph traversal with variable-length paths"""
        query = "MATCH (a:Person)-[:KNOWS*1..3]->(b:Person) RETURN a, b"
        sql, params = self.translate(query)

        # Should use recursive CTE for variable-length paths
        self.assertIn('RECURSIVE', sql)

    def test_optional_match_translation(self):
        """Test OPTIONAL MATCH translation to LEFT JOIN"""
        query = "MATCH (a:Person) OPTIONAL MATCH (a)-[r:LIKES]->(b:Movie) RETURN a, b"
        sql, params = self.translate(query)

        self.assertIn('LEFT JOIN', sql)

    def test_with_clause_translation(self):
        """Test WITH clause translation to CTE"""
        query = """
        MATCH (n:Person)
        WITH n.age AS age, COUNT(n) AS count
        WHERE count > 5
        RETURN age, count
        """
        sql, params = self.translate(query)
        print(sql)

        self.assertIn('WITH', sql)

    def test_union_translation(self):
        """Test UNION translation"""
        query = """
        MATCH (n:Person) RETURN n.name AS name
        UNION
        MATCH (m:Company) RETURN m.name AS name
        """
        sql, params = self.translate(query)

        self.assertIn('UNION', sql)


if __name__ == '__main__':
    unittest.main()
