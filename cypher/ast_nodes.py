"""
AST Node Classes for Cypher Query Representation
Each node represents a component of a Cypher query
"""

from dataclasses import dataclass, field
from typing import List, Optional, Any, Union
from enum import Enum


class Direction(Enum):
    """Relationship direction"""
    OUTGOING = ">"
    INCOMING = "<"
    BOTH = "-"


class SortOrder(Enum):
    """Sort order for ORDER BY"""
    ASC = "ASC"
    DESC = "DESC"


# Base AST Node
@dataclass
class ASTNode:
    """Base class for all AST nodes"""
    pass


# Query Structure
@dataclass
class Query(ASTNode):
    """Top-level query node"""
    clauses: List[ASTNode]
    unions: List['Query'] = field(default_factory=list)
    union_all: bool = False


# Clauses
@dataclass
class MatchClause(ASTNode):
    """MATCH clause"""
    patterns: List['Pattern']
    optional: bool = False
    where: Optional['Expression'] = None


@dataclass
class ReturnClause(ASTNode):
    """RETURN clause"""
    items: List['ProjectionItem']
    distinct: bool = False
    order_by: Optional[List['SortItem']] = None
    skip: Optional['Expression'] = None
    limit: Optional['Expression'] = None


@dataclass
class WithClause(ASTNode):
    """WITH clause for query chaining"""
    items: List['ProjectionItem']
    distinct: bool = False
    where: Optional['Expression'] = None
    order_by: Optional[List['SortItem']] = None
    skip: Optional['Expression'] = None
    limit: Optional['Expression'] = None


@dataclass
class CreateClause(ASTNode):
    """CREATE clause"""
    patterns: List['Pattern']


@dataclass
class MergeClause(ASTNode):
    """MERGE clause"""
    pattern: 'Pattern'
    on_match: Optional[List['SetItem']] = None
    on_create: Optional[List['SetItem']] = None


@dataclass
class DeleteClause(ASTNode):
    """DELETE clause"""
    expressions: List['Expression']
    detach: bool = False


@dataclass
class SetClause(ASTNode):
    """SET clause"""
    items: List['SetItem']


@dataclass
class RemoveClause(ASTNode):
    """REMOVE clause"""
    items: List['RemoveItem']


@dataclass
class CallClause(ASTNode):
    """CALL procedure clause"""
    procedure_name: str
    arguments: List['Expression']
    yield_items: Optional[List[str]] = None


# Pattern Elements
@dataclass
class Pattern(ASTNode):
    """A pattern to match in the graph"""
    elements: List['PatternElement']
    path_variable: Optional[str] = None


@dataclass
class PatternElement(ASTNode):
    """Single element in a pattern (node-relationship-node chain)"""
    nodes: List['NodePattern']
    relationships: List['RelationshipPattern']


@dataclass
class NodePattern(ASTNode):
    """Node pattern (n:Label {prop: value})"""
    variable: Optional[str] = None
    labels: List[str] = field(default_factory=list)
    properties: Optional['MapLiteral'] = None


@dataclass
class RelationshipPattern(ASTNode):
    """Relationship pattern -[r:TYPE]->"""
    variable: Optional[str] = None
    types: List[str] = field(default_factory=list)
    properties: Optional['MapLiteral'] = None
    direction: Direction = Direction.BOTH
    min_hops: Optional[int] = None
    max_hops: Optional[int] = None


# Projections and Sorting
@dataclass
class ProjectionItem(ASTNode):
    """Item in RETURN or WITH clause"""
    expression: 'Expression'
    alias: Optional[str] = None


@dataclass
class SortItem(ASTNode):
    """Item in ORDER BY clause"""
    expression: 'Expression'
    order: SortOrder = SortOrder.ASC


# SET and REMOVE items
@dataclass
class SetItem(ASTNode):
    """Item in SET clause"""
    variable: str
    property_key: Optional[str] = None
    expression: Optional['Expression'] = None
    label: Optional[str] = None
    merge_properties: bool = False  # For += operator


@dataclass
class RemoveItem(ASTNode):
    """Item in REMOVE clause"""
    variable: str
    property_key: Optional[str] = None
    label: Optional[str] = None


# Expressions
@dataclass
class Expression(ASTNode):
    """Base expression node"""
    pass


@dataclass
class BinaryOp(Expression):
    """Binary operation (a + b, a AND b, etc.)"""
    left: Expression
    operator: str
    right: Expression


@dataclass
class UnaryOp(Expression):
    """Unary operation (NOT a, -a, etc.)"""
    operator: str
    operand: Expression


@dataclass
class ComparisonOp(Expression):
    """Comparison operation (a = b, a < b, etc.)"""
    left: Expression
    operator: str
    right: Expression


@dataclass
class PropertyAccess(Expression):
    """Property access (n.name)"""
    expression: Expression
    property_key: str


@dataclass
class IndexAccess(Expression):
    """Index access (list[0])"""
    expression: Expression
    index: Expression


@dataclass
class FunctionCall(Expression):
    """Function invocation"""
    name: str
    arguments: List[Expression]
    distinct: bool = False


@dataclass
class CaseExpression(Expression):
    """CASE expression"""
    test_expression: Optional[Expression] = None  # For simple CASE
    alternatives: List[tuple[Expression, Expression]] = field(default_factory=list)  # (when, then)
    else_expression: Optional[Expression] = None


@dataclass
class ListComprehension(Expression):
    """List comprehension [x IN list WHERE condition | expression]"""
    variable: str
    list_expression: Expression
    where: Optional[Expression] = None
    map_expression: Optional[Expression] = None


@dataclass
class PatternComprehension(Expression):
    """Pattern comprehension [path = pattern WHERE condition | expression]"""
    path_variable: str
    pattern: Pattern
    map_expression: Expression
    where: Optional[Expression] = None


@dataclass
class Quantifier(Expression):
    """Quantifier (ALL, ANY, NONE, SINGLE)"""
    quantifier_type: str  # ALL, ANY, NONE, SINGLE
    variable: str
    list_expression: Expression
    where: Optional[Expression] = None


# Literals
@dataclass
class Variable(Expression):
    """Variable reference"""
    name: str


@dataclass
class Parameter(Expression):
    """Query parameter ($param)"""
    name: str


@dataclass
class Literal(Expression):
    """Base literal value"""
    value: Any


@dataclass
class IntegerLiteral(Literal):
    """Integer literal"""
    value: int


@dataclass
class FloatLiteral(Literal):
    """Float literal"""
    value: float


@dataclass
class StringLiteral(Literal):
    """String literal"""
    value: str


@dataclass
class BooleanLiteral(Literal):
    """Boolean literal"""
    value: bool


@dataclass
class NullLiteral(Literal):
    """NULL literal"""
    value: None = None


@dataclass
class ListLiteral(Expression):
    """List literal [1, 2, 3]"""
    elements: List[Expression]


@dataclass
class MapLiteral(Expression):
    """Map literal {key: value, ...}"""
    items: dict[str, Expression]


# Helper functions for AST construction
def create_node_pattern(variable: Optional[str] = None,
                       labels: Optional[List[str]] = None,
                       properties: Optional[MapLiteral] = None) -> NodePattern:
    """Helper to create node pattern"""
    return NodePattern(
        variable=variable,
        labels=labels or [],
        properties=properties
    )


def create_relationship_pattern(variable: Optional[str] = None,
                               types: Optional[List[str]] = None,
                               properties: Optional[MapLiteral] = None,
                               direction: Direction = Direction.BOTH,
                               min_hops: Optional[int] = None,
                               max_hops: Optional[int] = None) -> RelationshipPattern:
    """Helper to create relationship pattern"""
    return RelationshipPattern(
        variable=variable,
        types=types or [],
        properties=properties,
        direction=direction,
        min_hops=min_hops,
        max_hops=max_hops
    )
