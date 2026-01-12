"""
Cypher Parser using Lark
Transforms Cypher queries into AST
"""

from pathlib import Path
from typing import Any, List, Optional
from lark import Lark, Transformer, Token
from .ast_nodes import *


class CypherTransformer(Transformer):
    """Transforms Lark parse tree into AST nodes"""

    # Query structure
    def start(self, items):
        return items[0]

    def query(self, items):
        return items[0]

    def regular_query(self, items):
        first_query = items[0]
        unions = []
        union_types = []

        i = 1
        while i < len(items):
            if isinstance(items[i], Query):
                unions.append(items[i])
                i += 1
            elif isinstance(items[i], str):
                # This is a union type indicator
                union_types.append(items[i])
                i += 1
            else:
                i += 1

        if unions:
            first_query.unions = unions
            # Check if any union is UNION ALL
            first_query.union_all = 'UNION ALL' in union_types

        return first_query

    def single_query(self, items):
        return Query(clauses=items)

    def union_all(self, items):
        return 'UNION ALL'

    def union_distinct(self, items):
        return 'UNION'

    # Clauses
    def clause(self, items):
        return items[0]

    def match(self, items):
        patterns = []
        where = None

        for item in items:
            if isinstance(item, Pattern):
                patterns.append(item)
            elif isinstance(item, Expression):
                where = item

        return MatchClause(patterns=patterns, optional=False, where=where)

    def optional_match(self, items):
        patterns = []
        where = None

        for item in items:
            if isinstance(item, Pattern):
                patterns.append(item)
            elif isinstance(item, Expression):
                where = item

        return MatchClause(patterns=patterns, optional=True, where=where)

    def where(self, items):
        return items[0]

    def return_clause(self, items):
        body = items[0]
        return ReturnClause(**body)

    def with_clause(self, items):
        body = items[0]
        where = items[1] if len(items) > 1 else None
        return WithClause(**body, where=where)

    def distinct_marker(self, items):
        """Handle DISTINCT keyword"""
        return True

    def projection_body(self, items):
        distinct = False
        proj_items = None
        order_by = None
        skip = None
        limit = None

        for item in items:
            if item is True:  # distinct_marker returns True
                distinct = True
            elif isinstance(item, list) and all(isinstance(x, ProjectionItem) for x in item):
                proj_items = item
            elif isinstance(item, list) and all(isinstance(x, SortItem) for x in item):
                order_by = item
            elif isinstance(item, dict):
                # Handle skip and limit which are returned as dicts from their rules
                if 'skip' in item:
                    skip = item['skip']
                elif 'limit' in item:
                    limit = item['limit']

        return {
            'items': proj_items or [],
            'distinct': distinct,
            'order_by': order_by,
            'skip': skip,
            'limit': limit
        }

    def projection_with_star(self, items):
        result = [ProjectionItem(expression=Variable('*'), alias=None)]
        result.extend(items)
        return result

    def projection_list(self, items):
        return items

    def projection_item(self, items):
        expression = items[0]
        alias = items[1].name if len(items) > 1 and isinstance(items[1], Variable) else None
        return ProjectionItem(expression=expression, alias=alias)

    def order(self, items):
        return items

    def asc(self, items):
        return SortOrder.ASC

    def desc(self, items):
        return SortOrder.DESC

    def sort_order(self, items):
        return items[0]

    def sort_item(self, items):
        expression = items[0]
        order = SortOrder.ASC

        if len(items) > 1:
            order = items[1]

        return SortItem(expression=expression, order=order)

    def skip(self, items):
        return {'skip': items[0]}

    def limit(self, items):
        return {'limit': items[0]}

    def create(self, items):
        return CreateClause(patterns=items)

    def merge(self, items):
        pattern = items[0]
        on_match = None
        on_create = None

        for item in items[1:]:
            if hasattr(item, 'on_type'):
                if item.on_type == 'MATCH':
                    on_match = item.items
                elif item.on_type == 'CREATE':
                    on_create = item.items

        return MergeClause(pattern=pattern, on_match=on_match, on_create=on_create)

    def on_match(self, items):
        set_clause = items[0]
        set_clause.on_type = 'MATCH'
        return set_clause

    def on_create(self, items):
        set_clause = items[0]
        set_clause.on_type = 'CREATE'
        return set_clause

    def delete(self, items):
        return DeleteClause(expressions=items, detach=False)

    def detach_delete(self, items):
        return DeleteClause(expressions=items, detach=True)

    def set_clause(self, items):
        return SetClause(items=items)

    def set_property(self, items):
        # items[0] can be Variable or PropertyAccess (e.g., n.age)
        target = items[0]
        if isinstance(target, Variable):
            var = target.name
            prop_key = None
        elif isinstance(target, PropertyAccess):
            var = target.expression.name if isinstance(target.expression, Variable) else str(target.expression)
            prop_key = target.property_key
        else:
            var = str(target)
            prop_key = None
        return SetItem(variable=var, property_key=prop_key, expression=items[1])

    def set_properties_map(self, items):
        var = items[0].name if isinstance(items[0], Variable) else str(items[0])
        return SetItem(variable=var, expression=items[1], merge_properties=True)

    def set_label(self, items):
        var = items[0].name if isinstance(items[0], Variable) else str(items[0])
        label = str(items[1])
        return SetItem(variable=var, label=label)

    def remove(self, items):
        return RemoveClause(items=items)

    def remove_label(self, items):
        var = items[0].name if isinstance(items[0], Variable) else str(items[0])
        label = str(items[1])
        return RemoveItem(variable=var, label=label)

    def remove_property(self, items):
        var = items[0].name if isinstance(items[0], Variable) else str(items[0])
        prop = str(items[1])
        return RemoveItem(variable=var, property_key=prop)

    def standalone_call(self, items):
        proc_name = str(items[0])
        args = items[1] if len(items) > 1 and isinstance(items[1], list) else []
        yield_items = items[2] if len(items) > 2 else None
        return CallClause(procedure_name=proc_name, arguments=args, yield_items=yield_items)

    # Patterns
    def pattern(self, items):
        return Pattern(elements=items)

    def pattern_part(self, items):
        return items[0]

    def named_path(self, items):
        var = items[0]
        pattern = items[1]
        pattern.path_variable = var.name if isinstance(var, Variable) else str(var)
        return pattern

    def anonymous_pattern_part(self, items):
        return items[0]

    def pattern_element(self, items):
        nodes = []
        relationships = []

        for item in items:
            if isinstance(item, NodePattern):
                nodes.append(item)
            elif isinstance(item, RelationshipPattern):
                relationships.append(item)

        return PatternElement(nodes=nodes, relationships=relationships)

    def node_pattern(self, items):
        variable = None
        labels = []
        properties = None

        for item in items:
            if isinstance(item, Variable):
                variable = item.name
            elif isinstance(item, list) and all(isinstance(x, str) for x in item):
                labels = item
            elif isinstance(item, MapLiteral):
                properties = item

        return NodePattern(variable=variable, labels=labels, properties=properties)

    def left_arrow_head(self, items):
        return Token('left_arrow_head', '<')

    def right_arrow_head(self, items):
        return Token('right_arrow_head', '>')

    def dash(self, items):
        return Token('dash', '-')

    def relationship_pattern(self, items):
        direction = Direction.BOTH
        variable = None
        types = []
        properties = None
        min_hops = None
        max_hops = None

        has_left = False
        has_right = False

        for item in items:
            if isinstance(item, Token):
                if item.type == 'left_arrow_head':
                    has_left = True
                elif item.type == 'right_arrow_head':
                    has_right = True
            elif isinstance(item, dict):
                variable = item.get('variable')
                types = item.get('types', [])
                properties = item.get('properties')
                min_hops = item.get('min_hops')
                max_hops = item.get('max_hops')

        if has_left and not has_right:
            direction = Direction.INCOMING
        elif has_right and not has_left:
            direction = Direction.OUTGOING

        return RelationshipPattern(
            variable=variable,
            types=types,
            properties=properties,
            direction=direction,
            min_hops=min_hops,
            max_hops=max_hops
        )

    def relationship_detail(self, items):
        result = {'variable': None, 'types': [], 'properties': None, 'min_hops': None, 'max_hops': None}

        for item in items:
            if isinstance(item, Variable):
                result['variable'] = item.name
            elif isinstance(item, list) and all(isinstance(x, str) for x in item):
                result['types'] = item
            elif isinstance(item, MapLiteral):
                result['properties'] = item
            elif isinstance(item, dict) and 'min_hops' in item:
                result.update(item)

        return result

    def relationship_types(self, items):
        return items

    def rel_type(self, items):
        return str(items[0])

    def variable_length(self, items):
        if not items:
            return {'min_hops': 1, 'max_hops': None}
        return items[0]

    def range_explicit(self, items):
        return {'min_hops': int(items[0]), 'max_hops': int(items[1])}

    def range_min(self, items):
        return {'min_hops': int(items[0]), 'max_hops': None}

    def range_max(self, items):
        return {'min_hops': 1, 'max_hops': int(items[0])}

    def range_all(self, items):
        return {'min_hops': 1, 'max_hops': None}

    # Labels and properties
    def label_expression(self, items):
        return items

    def label_term(self, items):
        return str(items[0])

    def properties(self, items):
        if not items:
            return MapLiteral(items={})
        return items[0]

    def property_list(self, items):
        props = {}
        for key, value in items:
            props[key] = value
        return MapLiteral(items=props)

    def property(self, items):
        return (str(items[0]), items[1])

    def property_key(self, items):
        return str(items[0])

    # Expressions
    def expression(self, items):
        return items[0]

    def or_expression(self, items):
        if len(items) == 1:
            return items[0]

        result = items[0]
        for i in range(1, len(items)):
            result = BinaryOp(left=result, operator='OR', right=items[i])
        return result

    def and_expression(self, items):
        if len(items) == 1:
            return items[0]

        result = items[0]
        for i in range(1, len(items)):
            result = BinaryOp(left=result, operator='AND', right=items[i])
        return result

    def not_expression(self, items):
        # This handles the not_expression rule from grammar
        # If it's a NOT expr, items will be the negated expression
        # Otherwise it's just a comparison_expression that passes through
        return items[0]

    def not_expr(self, items):
        return UnaryOp(operator='NOT', operand=items[0])

    def comparison_expression(self, items):
        if len(items) == 1:
            return items[0]

        left = items[0]
        # items[1] could be either a null_check result or a comp_op
        item1 = items[1]

        # If item1 is a string and is a unary operator
        if isinstance(item1, str) and item1 in ['IS NULL', 'IS NOT NULL']:
            # Unary postfix operator (from null_check)
            return ComparisonOp(left=left, operator=item1, right=None)

        # Otherwise, it's a binary operator
        op_token = item1
        if isinstance(op_token, Token):
            operator = str(op_token.value)
        elif isinstance(op_token, str):
            operator = op_token
        else:
            operator = '='

        # Binary operators need a right operand
        right = items[2] if len(items) > 2 else None
        return ComparisonOp(left=left, operator=operator, right=right)

    def null_check(self, items):
        # This receives the result from is_null_op or is_not_null_op
        return items[0]

    def comp_op(self, items):
        # This receives the result from the named rules below
        return items[0] if items else '='

    def eq_op(self, items):
        return '='

    def ne_op(self, items):
        return '<>'

    def ne_op2(self, items):
        return '!='

    def lt_op(self, items):
        return '<'

    def gt_op(self, items):
        return '>'

    def lte_op(self, items):
        return '<='

    def gte_op(self, items):
        return '>='

    def in_op(self, items):
        return 'IN'

    def contains_op(self, items):
        return 'CONTAINS'

    def starts_with_op(self, items):
        return 'STARTS WITH'

    def ends_with_op(self, items):
        return 'ENDS WITH'

    def regex_op(self, items):
        return '=~'

    def is_null_op(self, items):
        return 'IS NULL'

    def is_not_null_op(self, items):
        return 'IS NOT NULL'

    def add_expression(self, items):
        if len(items) == 1:
            return items[0]

        result = items[0]
        i = 1
        while i < len(items):
            operator = str(items[i])
            result = BinaryOp(left=result, operator=operator, right=items[i + 1])
            i += 2
        return result

    def multiply_expression(self, items):
        if len(items) == 1:
            return items[0]

        result = items[0]
        i = 1
        while i < len(items):
            operator = str(items[i])
            result = BinaryOp(left=result, operator=operator, right=items[i + 1])
            i += 2
        return result

    def power_expression(self, items):
        if len(items) == 1:
            return items[0]
        return BinaryOp(left=items[0], operator='^', right=items[1])

    def unary_expression(self, items):
        if len(items) == 1:
            return items[0]
        return UnaryOp(operator=str(items[0]), operand=items[1])

    def postfix_expression(self, items):
        result = items[0]

        for item in items[1:]:
            if isinstance(item, str):
                result = PropertyAccess(expression=result, property_key=item)
            elif isinstance(item, Expression):
                result = IndexAccess(expression=result, index=item)

        return result

    def property_lookup(self, items):
        return str(items[0])

    def index_lookup(self, items):
        return items[0]

    # Atoms
    def atom(self, items):
        return items[0]

    def literal(self, items):
        return items[0]

    def number(self, items):
        return items[0]

    def integer(self, items):
        return IntegerLiteral(value=int(items[0]))

    def float_number(self, items):
        return FloatLiteral(value=float(items[0]))

    def string(self, items):
        # Remove quotes
        s = str(items[0])[1:-1]
        # Handle escape sequences
        s = s.replace('\\n', '\n').replace('\\t', '\t').replace('\\"', '"').replace("\\'", "'")
        return StringLiteral(value=s)

    def boolean(self, items):
        return items[0]

    def true_val(self, items):
        return BooleanLiteral(value=True)

    def false_val(self, items):
        return BooleanLiteral(value=False)

    def true_literal(self, items):
        return BooleanLiteral(value=True)

    def false_literal(self, items):
        return BooleanLiteral(value=False)

    def null_value(self, items):
        return NullLiteral()

    def null_literal(self, items):
        return NullLiteral()

    def parameter(self, items):
        return Parameter(name=str(items[0]))

    def variable(self, items):
        return Variable(name=str(items[0]))

    def list_literal(self, items):
        elements = items[0] if items else []
        return ListLiteral(elements=elements)

    def expression_list(self, items):
        return items

    def map_literal(self, items):
        if not items:
            return MapLiteral(items={})
        return items[0]

    def map_item_list(self, items):
        props = {}
        for key, value in items:
            props[key] = value
        return MapLiteral(items=props)

    def map_item(self, items):
        return (str(items[0]), items[1])

    # Case expression
    def case_expr(self, items):
        alternatives = []
        else_expr = None

        i = 0
        while i < len(items):
            if isinstance(items[i], tuple):
                alternatives.append(items[i])
                i += 1
            else:
                else_expr = items[i]
                i += 1

        return CaseExpression(alternatives=alternatives, else_expression=else_expr)

    def case_simple(self, items):
        test_expr = items[0]
        alternatives = []
        else_expr = None

        for item in items[1:]:
            if isinstance(item, tuple):
                alternatives.append(item)
            else:
                else_expr = item

        return CaseExpression(test_expression=test_expr, alternatives=alternatives, else_expression=else_expr)

    def case_alternative(self, items):
        return (items[0], items[1])

    # List comprehension
    def list_comprehension(self, items):
        return items[0]

    def filter_expression(self, items):
        variable = items[0].name if isinstance(items[0], Variable) else str(items[0])
        list_expr = items[1]
        where = None
        map_expr = None

        if len(items) > 2:
            if isinstance(items[2], Expression):
                where = items[2]
            if len(items) > 3:
                map_expr = items[3]

        return ListComprehension(variable=variable, list_expression=list_expr, where=where, map_expression=map_expr)

    # Pattern comprehension
    def pattern_comprehension(self, items):
        path_var = items[0].name if isinstance(items[0], Variable) else str(items[0])
        pattern = items[1]
        where = None
        map_expr = items[-1]

        if len(items) > 3:
            where = items[2]

        return PatternComprehension(path_variable=path_var, pattern=pattern, where=where, map_expression=map_expr)

    # Quantifiers
    def quantifier(self, items):
        quant_type = str(items[0])
        filter_expr = items[1]
        return Quantifier(
            quantifier_type=quant_type,
            variable=filter_expr.variable,
            list_expression=filter_expr.list_expression,
            where=filter_expr.where
        )

    # Function calls
    def function_invocation(self, items):
        name = str(items[0])
        distinct = False
        args = []

        for item in items[1:]:
            if item == 'DISTINCT':
                distinct = True
            elif isinstance(item, list):
                args = item

        return FunctionCall(name=name, arguments=args, distinct=distinct)

    def function_name(self, items):
        return str(items[0])

    def procedure_name(self, items):
        return '.'.join(str(item) for item in items)

    def procedure_result(self, items):
        return [item.name if isinstance(item, Variable) else str(item) for item in items]

    # Terminals
    def IDENTIFIER(self, token):
        return str(token)

    def STRING(self, token):
        return token

    def INT(self, token):
        return int(token)

    def FLOAT(self, token):
        return float(token)


class CypherParser:
    """Main Cypher parser class"""

    def __init__(self):
        grammar_path = Path(__file__).parent / "grammar.lark"
        with open(grammar_path, 'r') as f:
            grammar = f.read()

        self.parser = Lark(
            grammar,
            parser='lalr',
            transformer=CypherTransformer(),
            start='start'
        )

    def parse(self, cypher_query: str) -> Query:
        """
        Parse a Cypher query into an AST

        Args:
            cypher_query: Cypher query string

        Returns:
            Query AST node
        """
        try:
            ast = self.parser.parse(cypher_query)
            return ast
        except Exception as e:
            raise ValueError(f"Failed to parse Cypher query: {e}\nQuery: {cypher_query}")
