"""
SQL Generator for Cypher AST
Traverses AST and generates PostgreSQL SQL queries
"""

from typing import List, Tuple, Any, Optional, Set
from .ast_nodes import *
import json


class SQLContext:
    """Context for SQL generation"""

    def __init__(self, group_id: str = ''):
        self.group_id = group_id
        self.param_counter = 0
        self.params: List[Any] = []
        self.aliases: dict[str, str] = {}  # Variable name -> table alias
        self.alias_counter = 0

    def add_param(self, value: Any) -> str:
        """Add a parameter and return its placeholder"""
        self.param_counter += 1
        self.params.append(value)
        return f"${self.param_counter}"

    def get_alias(self, variable: str, table: str = 'graph_nodes') -> str:
        """Get or create table alias for variable"""
        if variable not in self.aliases:
            self.alias_counter += 1
            self.aliases[variable] = f"{table[0]}{self.alias_counter}"
        return self.aliases[variable]

    def reset(self):
        """Reset context for new query"""
        self.param_counter = 0
        self.params = []
        self.aliases = {}
        self.alias_counter = 0


class SQLGenerator:
    """Generates PostgreSQL SQL from Cypher AST"""

    # Known column names in graph_nodes and graph_edges tables
    NODE_COLUMNS = {'uuid', 'name', 'node_type', 'group_id', 'created_at', 'valid_at',
                    'invalid_at', 'embedding', 'summary', 'metadata'}
    EDGE_COLUMNS = {'uuid', 'source_node_uuid', 'target_node_uuid', 'relation_type',
                    'created_at', 'valid_at', 'invalid_at', 'group_id', 'fact', 'episodes', 'metadata'}

    def __init__(self, group_id: str = ''):
        self.context = SQLContext(group_id=group_id)

    def generate(self, query: Query, parameters: dict = None) -> Tuple[str, List[Any]]:
        """
        Generate SQL from AST

        Args:
            query: Query AST node
            parameters: Parameter values (for $param references)

        Returns:
            (sql_string, parameter_list)
        """
        self.context.reset()
        self.parameters = parameters or {}

        # Generate main query
        sql_parts = self._generate_query(query)

        # Handle UNION queries
        if query.unions:
            union_sqls = []
            for union_query in query.unions:
                union_sql = self._generate_query(union_query)
                union_sqls.append(union_sql)

            union_keyword = "UNION ALL" if query.union_all else "UNION"
            sql_parts = f"({sql_parts})\n{union_keyword}\n" + f"\n{union_keyword}\n".join(f"({s})" for s in union_sqls)

        return sql_parts, self.context.params

    def _generate_query(self, query: Query) -> str:
        """Generate SQL for a single query"""
        sql_parts = []
        cte_parts = []
        from_clause = None
        where_parts = []
        return_clause = None
        join_parts = []
        cte_name = None  # Track active CTE name
        cte_columns = {}  # Track CTE column mappings (variable -> column_name)

        for clause in query.clauses:
            if isinstance(clause, MatchClause):
                match_sql = self._generate_match(clause)
                if match_sql['cte']:
                    cte_parts.append(match_sql['cte'])
                # For first MATCH, set FROM clause. For subsequent MATCHes, append joins
                if match_sql['from']:
                    if from_clause is None:
                        from_clause = match_sql['from']
                    else:
                        # Subsequent MATCH - extract joins and append
                        join_parts.append(match_sql['from'])
                if match_sql['where']:
                    where_parts.append(match_sql['where'])

            elif isinstance(clause, ReturnClause):
                return_clause = clause

            elif isinstance(clause, WithClause):
                # WITH creates a CTE using accumulated clauses
                # Build the FROM clause with all joins
                full_from = from_clause
                if join_parts and full_from:
                    full_from += "\n" + "\n".join(join_parts)
                with_result = self._generate_with(clause, full_from, where_parts)
                cte_parts.append(with_result['sql'])
                cte_name = with_result['name']
                cte_columns = with_result['columns']
                # After WITH, reset FROM/WHERE for next clauses to use CTE
                from_clause = cte_name
                where_parts = []
                join_parts = []

            elif isinstance(clause, CreateClause):
                return self._generate_create(clause)

            elif isinstance(clause, MergeClause):
                return self._generate_merge(clause)

            elif isinstance(clause, DeleteClause):
                return self._generate_delete(clause)

            elif isinstance(clause, SetClause):
                return self._generate_set(clause)

            elif isinstance(clause, RemoveClause):
                return self._generate_remove(clause)

        # Build final SELECT query
        if return_clause:
            # Append any additional joins to FROM clause (only if not using CTE)
            if join_parts and from_clause and not cte_name:
                from_clause += "\n" + "\n".join(join_parts)
            select_sql = self._generate_return(return_clause, from_clause, where_parts, cte_columns)

            if cte_parts:
                cte_sql = "WITH " + ",\n".join(cte_parts)
                return f"{cte_sql}\n{select_sql}"
            else:
                return select_sql

        return ""

    def _generate_match(self, match: MatchClause) -> dict:
        """Generate SQL for MATCH clause"""
        result = {'cte': None, 'from': [], 'where': []}

        from_parts = []
        join_parts = []
        where_parts = []

        for pattern in match.patterns:
            pattern_sql = self._generate_pattern(pattern, match.optional)
            from_parts.extend(pattern_sql['from'])
            join_parts.extend(pattern_sql['joins'])
            where_parts.extend(pattern_sql['where'])

        # Add group_id filter
        if self.context.aliases:
            first_alias = list(self.context.aliases.values())[0]
            group_filter = f"{first_alias}.group_id = {self.context.add_param(self.context.group_id)}"
            where_parts.append(group_filter)

        # Add WHERE clause conditions
        if match.where:
            where_expr = self._generate_expression(match.where)
            where_parts.append(where_expr)

        # Combine FROM and JOINs
        from_clause = from_parts[0] if from_parts else ""
        if join_parts:
            from_clause += "\n" + "\n".join(join_parts)

        result['from'] = from_clause
        result['where'] = " AND ".join(where_parts) if where_parts else ""

        return result

    def _generate_pattern(self, pattern: Pattern, optional: bool = False) -> dict:
        """Generate SQL for a pattern"""
        result = {'from': [], 'joins': [], 'where': []}

        for element in pattern.elements:
            nodes = element.nodes
            relationships = element.relationships

            # First node becomes FROM clause (or JOIN if already exists)
            if nodes:
                first_node = nodes[0]
                node_var = first_node.variable or 'n'
                # Check if this node already has an alias (from previous MATCH)
                node_already_exists = node_var in self.context.aliases
                alias = self.context.get_alias(node_var)

                if not node_already_exists:
                    # New node - add to FROM
                    from_sql = f"graph_nodes {alias}"
                    result['from'].append(from_sql)

                    # Add node filters
                    node_filters = self._generate_node_filters(first_node, alias)
                    result['where'].extend(node_filters)

            # Process node-relationship-node chains
            for i, rel in enumerate(relationships):
                if i + 1 < len(nodes):
                    source_node = nodes[i]
                    target_node = nodes[i + 1]

                    rel_sql = self._generate_relationship_join(
                        source_node, rel, target_node, optional
                    )
                    result['joins'].extend(rel_sql['joins'])
                    result['where'].extend(rel_sql['where'])

        return result

    def _generate_node_filters(self, node: NodePattern, alias: str) -> List[str]:
        """Generate WHERE filters for node pattern"""
        filters = []

        # Label filters
        if node.labels:
            # In our schema, labels might be stored in node_type or metadata
            # For entity/episode/community, use node_type
            if node.labels[0].lower() in ['entity', 'episode', 'community']:
                filters.append(f"{alias}.node_type = {self.context.add_param(node.labels[0].lower())}")
            else:
                # Custom labels stored in metadata
                label_filter = f"{alias}.metadata->>'label' = {self.context.add_param(node.labels[0])}"
                filters.append(label_filter)

        # Property filters
        if node.properties and isinstance(node.properties, MapLiteral):
            for key, value_expr in node.properties.items.items():
                value = self._evaluate_literal(value_expr)
                param_placeholder = self.context.add_param(value)

                # Handle type conversions for JSONB comparisons
                if isinstance(value, (int, float)):
                    # For numeric values, cast JSONB to numeric for comparison
                    filters.append(f"({alias}.properties->'{key}')::numeric = {param_placeholder}")
                elif isinstance(value, bool):
                    # For boolean values, cast JSONB to boolean
                    filters.append(f"({alias}.properties->'{key}')::boolean = {param_placeholder}")
                else:
                    # For strings, use ->> operator
                    filters.append(f"{alias}.properties->>'{key}' = {param_placeholder}")

        return filters

    def _generate_relationship_join(self, source: NodePattern, rel: RelationshipPattern,
                                    target: NodePattern, optional: bool) -> dict:
        """Generate JOIN for relationship pattern"""
        result = {'joins': [], 'where': []}

        source_alias = self.context.get_alias(source.variable or 'n')
        target_alias = self.context.get_alias(target.variable or 'm')
        # Generate unique alias for unnamed relationships
        rel_var = rel.variable or f'_rel_{self.context.alias_counter}'
        edge_alias = self.context.get_alias(rel_var, 'graph_edges')

        join_type = "LEFT JOIN" if optional else "JOIN"

        # Handle variable-length paths with recursive CTE
        if rel.min_hops is not None or rel.max_hops is not None:
            path_sql = self._generate_variable_length_path(source_alias, target_alias, rel)
            result['joins'].append(path_sql)
        else:
            # Simple relationship join
            # Build relationship type filter (for ON clause if optional)
            type_filter = ""
            if rel.types:
                type_conditions = [
                    f"{edge_alias}.relation_type = {self.context.add_param(t)}"
                    for t in rel.types
                ]
                type_filter = f" AND ({' OR '.join(type_conditions)})"

            # Join edge table
            if rel.direction == Direction.OUTGOING or rel.direction == Direction.BOTH:
                edge_join = f"{join_type} graph_edges {edge_alias} ON {source_alias}.uuid = {edge_alias}.source_node_uuid{type_filter if optional else ''}"
            else:  # INCOMING
                edge_join = f"{join_type} graph_edges {edge_alias} ON {source_alias}.uuid = {edge_alias}.target_node_uuid{type_filter if optional else ''}"

            result['joins'].append(edge_join)

            # Join target node
            if rel.direction == Direction.OUTGOING or rel.direction == Direction.BOTH:
                node_join = f"{join_type} graph_nodes {target_alias} ON {edge_alias}.target_node_uuid = {target_alias}.uuid"
            else:  # INCOMING
                node_join = f"{join_type} graph_nodes {target_alias} ON {edge_alias}.source_node_uuid = {target_alias}.uuid"

            result['joins'].append(node_join)

            # For non-optional joins, add type filter to WHERE clause
            if not optional and type_filter:
                result['where'].append(type_filter.replace(' AND ', ''))

            # Add relationship property filters
            if rel.properties and isinstance(rel.properties, MapLiteral):
                for key, value_expr in rel.properties.items.items():
                    value = self._evaluate_literal(value_expr)
                    result['where'].append(
                        f"{edge_alias}.properties->>'{key}' = {self.context.add_param(value)}"
                    )

        # Add target node filters
        target_filters = self._generate_node_filters(target, target_alias)
        result['where'].extend(target_filters)

        return result

    def _generate_variable_length_path(self, source_alias: str, target_alias: str,
                                       rel: RelationshipPattern) -> str:
        """Generate recursive CTE for variable-length paths"""
        min_hops = rel.min_hops or 1
        max_hops = rel.max_hops or 999

        # Create CTE name
        cte_name = f"path_{self.context.alias_counter}"
        self.context.alias_counter += 1

        # Type filter
        type_filter = ""
        if rel.types:
            type_conditions = [f"relation_type = {self.context.add_param(t)}" for t in rel.types]
            type_filter = f"WHERE {' OR '.join(type_conditions)}"

        # Direction-specific logic
        if rel.direction == Direction.OUTGOING or rel.direction == Direction.BOTH:
            source_col = "source_node_uuid"
            target_col = "target_node_uuid"
        else:
            source_col = "target_node_uuid"
            target_col = "source_node_uuid"

        cte_sql = f"""
WITH RECURSIVE {cte_name} AS (
    SELECT {source_col} as start_id, {target_col} as end_id, 1 as depth,
           ARRAY[uuid] as path_edges
    FROM graph_edges
    {type_filter}
    UNION ALL
    SELECT p.start_id, e.{target_col}, p.depth + 1,
           p.path_edges || e.uuid
    FROM {cte_name} p
    JOIN graph_edges e ON p.end_id = e.{source_col}
    WHERE p.depth < {max_hops}
    {type_filter.replace('WHERE', 'AND') if type_filter else ''}
    AND NOT e.uuid = ANY(p.path_edges)
)
JOIN {cte_name} ON {source_alias}.uuid = {cte_name}.start_id
    AND {target_alias}.uuid = {cte_name}.end_id
    AND {cte_name}.depth >= {min_hops}
"""
        return cte_sql.strip()

    def _generate_return(self, ret: ReturnClause, from_clause: str, where_parts: List[str],
                         cte_columns: dict = None) -> str:
        """Generate RETURN clause as SELECT

        Args:
            ret: ReturnClause AST node
            from_clause: FROM clause (table name or CTE name)
            where_parts: WHERE conditions
            cte_columns: If selecting from CTE, mapping of variable -> CTE column name
        """
        # SELECT items
        distinct = "DISTINCT " if ret.distinct else ""
        select_items = []

        # Check if we have aggregations
        has_aggregation = False
        non_aggregated_exprs = []

        # If we're selecting from a CTE, we need to reference CTE columns
        using_cte = cte_columns is not None and len(cte_columns) > 0

        for item in ret.items:
            if using_cte:
                # Remap expressions to use CTE columns
                item_sql = self._generate_projection_item_from_cte(item, cte_columns)
            else:
                item_sql = self._generate_projection_item(item)
            select_items.append(item_sql)

            # Check if this item contains an aggregation function
            if self._contains_aggregation(item.expression):
                has_aggregation = True
            else:
                # Non-aggregated expression - will need GROUP BY
                if not using_cte:  # Only need GROUP BY if not using CTE
                    non_aggregated_exprs.append(self._generate_expression(item.expression))

        select_clause = f"SELECT {distinct}{', '.join(select_items)}"

        # FROM clause
        from_sql = f"\nFROM {from_clause}" if from_clause else ""

        # WHERE clause
        where_sql = ""
        if where_parts:
            where_sql = f"\nWHERE {' AND '.join(where_parts)}"

        # GROUP BY clause (if we have aggregations and non-aggregated columns)
        group_by_sql = ""
        if has_aggregation and non_aggregated_exprs and not using_cte:
            group_by_sql = f"\nGROUP BY {', '.join(non_aggregated_exprs)}"

        # ORDER BY
        order_sql = ""
        if ret.order_by:
            order_items = [
                f"{self._generate_expression(item.expression)} {item.order.value}"
                for item in ret.order_by
            ]
            order_sql = f"\nORDER BY {', '.join(order_items)}"

        # LIMIT and OFFSET
        limit_sql = ""
        if ret.limit:
            limit_value = self._generate_expression(ret.limit)
            limit_sql = f"\nLIMIT {limit_value}"

        if ret.skip:
            skip_value = self._generate_expression(ret.skip)
            limit_sql = f"\nOFFSET {skip_value}" + limit_sql

        return f"{select_clause}{from_sql}{where_sql}{group_by_sql}{order_sql}{limit_sql}"

    def _contains_aggregation(self, expr) -> bool:
        """Check if an expression contains an aggregation function"""
        if isinstance(expr, FunctionCall):
            # Check if it's an aggregation function
            agg_funcs = ['count', 'sum', 'avg', 'min', 'max', 'collect']
            if expr.name.lower() in agg_funcs:
                return True
        # For nested expressions, recursively check
        if isinstance(expr, PropertyAccess):
            return self._contains_aggregation(expr.expression)
        if isinstance(expr, BinaryOp):
            return self._contains_aggregation(expr.left) or self._contains_aggregation(expr.right)
        return False

    def _generate_projection_item(self, item: ProjectionItem) -> str:
        """Generate SQL for projection item"""
        expr = item.expression

        # Special case for * or variable that represents whole node
        if isinstance(expr, Variable) and expr.name == '*':
            return '*'
        elif isinstance(expr, Variable) and expr.name in self.context.aliases:
            alias = self.context.aliases[expr.name]
            # Return all columns from the node as JSON
            sql = f"row_to_json({alias}.*)"
            if item.alias:
                sql += f" AS {item.alias}"
            elif expr.name != alias:
                sql += f" AS {expr.name}"
            return sql
        else:
            expr_sql = self._generate_expression(expr)
            if item.alias:
                return f"{expr_sql} AS {item.alias}"
            return expr_sql

    def _generate_projection_item_from_cte(self, item: ProjectionItem, cte_columns: dict) -> str:
        """Generate SQL for projection item when selecting from CTE

        Args:
            item: ProjectionItem AST node
            cte_columns: Mapping of variable/alias name -> CTE column name
        """
        expr = item.expression

        # Check if this is a simple variable reference that maps to a CTE column
        if isinstance(expr, Variable) and expr.name in cte_columns:
            # Direct column reference from CTE
            cte_col = cte_columns[expr.name]
            if item.alias:
                return f"{cte_col} AS {item.alias}"
            else:
                return cte_col

        # Check if this is a property access on a CTE column (e.g., p.name where p is from CTE)
        elif isinstance(expr, PropertyAccess):
            if isinstance(expr.expression, Variable) and expr.expression.name in cte_columns:
                # The base variable is from CTE (which stores row as JSON)
                cte_col = cte_columns[expr.expression.name]
                # Access property from JSONB column (use literal, not parameter)
                property_sql = f"{cte_col}->>'{ expr.property_key}'"
                if item.alias:
                    return f"{property_sql} AS {item.alias}"
                else:
                    return property_sql

        # Otherwise, generate as normal
        expr_sql = self._generate_expression(expr)
        if item.alias:
            return f"{expr_sql} AS {item.alias}"
        else:
            return expr_sql

    def _generate_with(self, with_clause: WithClause, from_clause: str, where_parts: List[str]) -> dict:
        """Generate WITH clause as CTE

        Returns:
            dict with 'sql', 'name', and 'columns' (mapping of variable -> column_name)
        """
        # Similar to RETURN but creates a CTE
        select_items = []
        has_aggregation = False
        non_aggregated_exprs = []
        column_mappings = {}  # Track what each column represents
        # Track alias -> full expression mapping for HAVING clause
        alias_to_expr = {}

        for item in with_clause.items:
            item_sql = self._generate_projection_item(item)
            select_items.append(item_sql)

            # Track column name for this item
            if item.alias:
                # Aliased column
                column_name = item.alias
                # Store the full expression for this alias
                alias_to_expr[item.alias] = item.expression
            elif isinstance(item.expression, Variable):
                # Variable without alias - use variable name
                column_name = item.expression.name
            else:
                # Complex expression - will need to use ordinal or assume alias
                column_name = None

            # Map the variable/alias to column name
            if column_name:
                if isinstance(item.expression, Variable):
                    column_mappings[item.expression.name] = column_name
                if item.alias:
                    column_mappings[item.alias] = item.alias

            # Check if this item contains an aggregation function
            if self._contains_aggregation(item.expression):
                has_aggregation = True
            else:
                # Non-aggregated expression - will need GROUP BY
                non_aggregated_exprs.append(self._generate_expression(item.expression))

        cte_name = f"cte_{self.context.alias_counter}"
        self.context.alias_counter += 1

        distinct = "DISTINCT " if with_clause.distinct else ""
        select_clause = f"{cte_name} AS (SELECT {distinct}{', '.join(select_items)}"

        # Add FROM clause from preceding MATCH
        if from_clause:
            select_clause += f"\nFROM {from_clause}"

        # Add WHERE from preceding MATCH
        if where_parts:
            where_sql = " AND ".join(f"({w})" for w in where_parts)
            select_clause += f"\nWHERE {where_sql}"

        # Add GROUP BY if we have aggregations
        if has_aggregation and non_aggregated_exprs:
            select_clause += f"\nGROUP BY {', '.join(non_aggregated_exprs)}"

        # WITH clause WHERE becomes HAVING
        # We need to use full aggregate expressions in HAVING, not aliases
        if with_clause.where:
            having_expr = self._generate_having_expression(with_clause.where, alias_to_expr)
            select_clause += f"\nHAVING {having_expr}"

        # Add ORDER BY
        if with_clause.order_by:
            order_items = [
                f"{self._generate_expression(item.expression)} {item.order.value}"
                for item in with_clause.order_by
            ]
            select_clause += f" ORDER BY {', '.join(order_items)}"

        # Add LIMIT/OFFSET
        if with_clause.limit:
            limit_value = self._generate_expression(with_clause.limit)
            select_clause += f" LIMIT {limit_value}"

        if with_clause.skip:
            skip_value = self._generate_expression(with_clause.skip)
            select_clause += f" OFFSET {skip_value}"

        select_clause += ")"

        return {
            'sql': select_clause,
            'name': cte_name,
            'columns': column_mappings
        }

    def _generate_create(self, create: CreateClause) -> str:
        """Generate INSERT for CREATE"""
        # For simplicity, handle single node creation
        # Multi-node patterns would need multiple INSERTs or CTE
        if not create.patterns or not create.patterns[0].elements:
            return ""

        element = create.patterns[0].elements[0]
        if not element.nodes:
            return ""

        node = element.nodes[0]

        # Extract properties
        props = {}
        if node.properties and isinstance(node.properties, MapLiteral):
            props = {k: self._evaluate_literal(v) for k, v in node.properties.items.items()}

        # Determine node type from labels
        node_type = 'entity'
        if node.labels and node.labels[0].lower() in ['entity', 'episode', 'community']:
            node_type = node.labels[0].lower()

        sql = f"""
INSERT INTO graph_nodes (uuid, name, node_type, group_id, properties, valid_at)
VALUES (
    gen_random_uuid(),
    {self.context.add_param(props.get('name', ''))},
    {self.context.add_param(node_type)},
    {self.context.add_param(self.context.group_id)},
    {self.context.add_param(json.dumps(props))},
    CURRENT_TIMESTAMP
)
RETURNING uuid, name, node_type, properties
"""
        return sql.strip()

    def _generate_merge(self, merge: MergeClause) -> str:
        """Generate UPSERT for MERGE"""
        # Similar to CREATE but with ON CONFLICT
        # merge.pattern is a PatternElement, not a Pattern with elements list
        if hasattr(merge.pattern, 'elements'):
            element = merge.pattern.elements[0]
        else:
            element = merge.pattern
        node = element.nodes[0]

        props = {}
        if node.properties and isinstance(node.properties, MapLiteral):
            props = {k: self._evaluate_literal(v) for k, v in node.properties.items.items()}

        node_type = 'entity'
        if node.labels and node.labels[0].lower() in ['entity', 'episode', 'community']:
            node_type = node.labels[0].lower()

        # Determine conflict target (usually uuid or unique property)
        conflict_target = "uuid"

        sql = f"""
INSERT INTO graph_nodes (uuid, name, node_type, group_id, properties, valid_at)
VALUES (
    gen_random_uuid(),
    {self.context.add_param(props.get('name', ''))},
    {self.context.add_param(node_type)},
    {self.context.add_param(self.context.group_id)},
    {self.context.add_param(json.dumps(props))},
    CURRENT_TIMESTAMP
)
ON CONFLICT ({conflict_target}) DO UPDATE SET
    name = EXCLUDED.name,
    properties = EXCLUDED.properties,
    valid_at = CURRENT_TIMESTAMP
RETURNING uuid, name, node_type
"""
        return sql.strip()

    def _generate_delete(self, delete: DeleteClause) -> str:
        """Generate DELETE statement"""
        # DELETE must follow a MATCH, so we need context from previous clauses
        # For now, generate a simple DELETE based on variables
        delete_vars = []
        for expr in delete.expressions:
            if isinstance(expr, Variable):
                delete_vars.append(expr.name)

        if not delete_vars:
            return ""

        # The DELETE should reference the matched nodes
        # In a proper implementation, this would be part of the larger query generation
        # For now, return empty as DELETE needs to be handled in context of full query
        var = delete_vars[0]
        if var in self.context.aliases:
            alias = self.context.aliases[var]
            if delete.detach:
                # DETACH DELETE: first delete edges, then nodes
                return f"DELETE FROM graph_edges WHERE source_node_uuid IN (SELECT uuid FROM graph_nodes AS {alias}); DELETE FROM graph_nodes AS {alias}"
            else:
                return f"DELETE FROM graph_nodes WHERE uuid IN (SELECT {alias}.uuid FROM graph_nodes {alias})"

        return ""

    def _generate_set(self, set_clause: SetClause) -> str:
        """Generate UPDATE for SET"""
        updates = []

        for item in set_clause.items:
            if item.label:
                # Setting a label (update metadata)
                updates.append(f"metadata = jsonb_set(metadata, '{{label}}', {self.context.add_param(item.label)})")
            elif item.expression:
                if item.merge_properties:
                    # += operator: merge properties
                    expr_val = self._generate_expression(item.expression)
                    updates.append(f"properties = properties || {expr_val}")
                else:
                    # = operator: set specific property
                    expr_val = self._generate_expression(item.expression)
                    if item.property_key:
                        updates.append(f"properties = jsonb_set(properties, '{{{item.property_key}}}', {expr_val})")
                    else:
                        updates.append(f"properties = {expr_val}")

        alias = self.context.get_alias(set_clause.items[0].variable)
        update_sql = f"UPDATE graph_nodes SET {', '.join(updates)} WHERE uuid = {alias}.uuid"
        return update_sql

    def _generate_remove(self, remove: RemoveClause) -> str:
        """Generate UPDATE for REMOVE"""
        updates = []

        for item in remove.items:
            if item.label:
                updates.append(f"metadata = metadata - 'label'")
            elif item.property_key:
                updates.append(f"properties = properties - '{item.property_key}'")

        alias = self.context.get_alias(remove.items[0].variable)
        return f"UPDATE graph_nodes SET {', '.join(updates)} WHERE uuid = {alias}.uuid"

    def _generate_having_expression(self, expr: Expression, alias_to_expr: dict) -> str:
        """Generate SQL for HAVING expression, expanding aliases to their full expressions

        In PostgreSQL, HAVING clauses cannot reference SELECT aliases directly.
        We need to expand any alias references to their full aggregate expressions.

        Args:
            expr: The expression to generate
            alias_to_expr: Mapping of alias names to their original expressions
        """
        from lark import Tree
        if isinstance(expr, Tree):
            if len(expr.children) == 1:
                return self._generate_having_expression(expr.children[0], alias_to_expr)
            else:
                return str(expr)

        if isinstance(expr, Variable):
            # Check if this variable is an alias we defined in the SELECT clause
            if expr.name in alias_to_expr:
                # Expand the alias to its full expression
                return self._generate_expression(alias_to_expr[expr.name])
            # Otherwise, use normal variable resolution
            if expr.name in self.context.aliases:
                alias = self.context.aliases[expr.name]
                return f"{alias}.uuid"
            return expr.name

        elif isinstance(expr, ComparisonOp):
            # Recursively handle comparison operators
            left = self._generate_having_expression(expr.left, alias_to_expr)
            op = expr.operator
            right = self._generate_having_expression(expr.right, alias_to_expr) if expr.right is not None else None

            # Map Cypher operators to SQL
            op_map = {
                '=': '=',
                '<>': '!=',
                '!=': '!=',
                '<': '<',
                '>': '>',
                '<=': '<=',
                '>=': '>=',
            }
            sql_op = op_map.get(op, op)

            if right is None:
                return f"({left} {sql_op})"
            else:
                return f"({left} {sql_op} {right})"

        elif isinstance(expr, BinaryOp):
            # Handle AND, OR, etc.
            left = self._generate_having_expression(expr.left, alias_to_expr)
            right = self._generate_having_expression(expr.right, alias_to_expr)
            op = expr.operator.upper()
            return f"({left} {op} {right})"

        elif isinstance(expr, UnaryOp):
            operand = self._generate_having_expression(expr.operand, alias_to_expr)
            return f"{expr.operator.upper()} {operand}"

        # For all other expression types, use the standard expression generator
        else:
            return self._generate_expression(expr)

    def _generate_expression(self, expr: Expression) -> str:
        """Generate SQL for expression"""
        # Handle Tree nodes from lark parser (unwrap them)
        from lark import Tree
        if isinstance(expr, Tree):
            # Tree nodes should contain a single child that's the actual expression
            if len(expr.children) == 1:
                return self._generate_expression(expr.children[0])
            else:
                # If multiple children, it's a complex expression - shouldn't happen
                return str(expr)

        if isinstance(expr, Variable):
            if expr.name in self.context.aliases:
                alias = self.context.aliases[expr.name]
                return f"{alias}.uuid"
            return expr.name

        elif isinstance(expr, Parameter):
            # Look up parameter value
            value = self.parameters.get(expr.name)
            return self.context.add_param(value)

        elif isinstance(expr, IntegerLiteral):
            return str(expr.value)

        elif isinstance(expr, FloatLiteral):
            return str(expr.value)

        elif isinstance(expr, StringLiteral):
            return self.context.add_param(expr.value)

        elif isinstance(expr, BooleanLiteral):
            return "TRUE" if expr.value else "FALSE"

        elif isinstance(expr, NullLiteral):
            return "NULL"

        elif isinstance(expr, ListLiteral):
            elements = [self._generate_expression(e) for e in expr.elements]
            return f"ARRAY[{', '.join(elements)}]"

        elif isinstance(expr, MapLiteral):
            return self.context.add_param(json.dumps({k: self._evaluate_literal(v) for k, v in expr.items.items()}))

        elif isinstance(expr, PropertyAccess):
            base = self._generate_expression(expr.expression)
            # Access JSONB property
            if base.endswith('.uuid'):
                # If base is a variable reference to uuid, we need the table alias
                # to access properties. Change n.uuid -> n
                base = base[:-5]  # Remove '.uuid'

            # Check if this property is actually a table column
            property_name = expr.property_key
            if property_name in self.NODE_COLUMNS or property_name in self.EDGE_COLUMNS:
                # Direct column access
                return f"{base}.{property_name}"
            else:
                # JSONB property access
                return f"{base}.properties->>'{property_name}'"

        elif isinstance(expr, BinaryOp):
            left = self._generate_expression(expr.left)
            right = self._generate_expression(expr.right)
            op = expr.operator.upper()
            return f"({left} {op} {right})"

        elif isinstance(expr, UnaryOp):
            operand = self._generate_expression(expr.operand)
            return f"{expr.operator.upper()} {operand}"

        elif isinstance(expr, ComparisonOp):
            left = self._generate_expression(expr.left)
            op = expr.operator
            # For unary operators like IS NULL, there's no right operand
            right = self._generate_expression(expr.right) if expr.right is not None else None

            # Map Cypher operators to SQL
            op_map = {
                '=': '=',
                '<>': '!=',
                '!=': '!=',
                '<': '<',
                '>': '>',
                '<=': '<=',
                '>=': '>=',
                'IN': 'IN',
                'CONTAINS': 'LIKE',
                'STARTS WITH': 'LIKE',
                'ENDS WITH': 'LIKE',
                '=~': '~',
                'IS NULL': 'IS NULL',
                'IS NOT NULL': 'IS NOT NULL'
            }

            sql_op = op_map.get(op, op)

            # If left side is a property access (contains ->>), cast for numeric/boolean comparisons
            if '.properties->>' in left and sql_op in ['<', '>', '<=', '>=', '=', '!=']:
                # Check if right side is numeric
                if isinstance(expr.right, (IntegerLiteral, FloatLiteral)):
                    # Cast left side to numeric: change ->> to -> and cast to numeric
                    # e.g., "g1.properties->>'age'" becomes "(g1.properties->'age')::numeric"
                    left = left.replace("properties->>", "properties->")
                    left = f"({left})::numeric"

            # Special handling for IN operator
            if op == 'IN':
                # Cast JSONB property to appropriate type if needed
                if '.properties->>' in left and isinstance(expr.right, ListLiteral):
                    # Check if list contains numbers
                    if expr.right.elements and isinstance(expr.right.elements[0], (IntegerLiteral, FloatLiteral)):
                        left = left.replace("properties->>", "properties->")
                        left = f"({left})::numeric"
                # For IN with array, use = ANY(ARRAY[...]) pattern
                return f"{left} = ANY({right})"

            if op == 'CONTAINS':
                return f"{left} LIKE '%' || {right} || '%'"
            elif op == 'STARTS WITH':
                return f"{left} LIKE {right} || '%'"
            elif op == 'ENDS WITH':
                return f"{left} LIKE '%' || {right}"
            elif op in ['IS NULL', 'IS NOT NULL']:
                return f"{left} {sql_op}"
            else:
                return f"({left} {sql_op} {right})"

        elif isinstance(expr, FunctionCall):
            args = [self._generate_expression(arg) for arg in expr.arguments]
            distinct = "DISTINCT " if expr.distinct else ""
            # Map Cypher functions to PostgreSQL
            func_map = {
                'count': 'COUNT',
                'sum': 'SUM',
                'avg': 'AVG',
                'min': 'MIN',
                'max': 'MAX',
                'collect': 'array_agg',
                'toLower': 'LOWER',
                'toUpper': 'UPPER',
                'size': 'array_length',
                'length': 'length',
            }
            func_name = func_map.get(expr.name.lower(), expr.name.upper())

            # For numeric aggregations on JSONB properties, cast to numeric
            if func_name in ['SUM', 'AVG', 'MIN', 'MAX'] and len(args) > 0:
                arg = args[0]
                if '.properties->>' in arg:
                    # Cast JSONB text to numeric
                    arg = arg.replace("properties->>", "properties->")
                    arg = f"({arg})::numeric"
                    args[0] = arg

            return f"{func_name}({distinct}{', '.join(args)})"

        elif isinstance(expr, CaseExpression):
            case_sql = "CASE"
            if expr.test_expression:
                case_sql += f" {self._generate_expression(expr.test_expression)}"

            for when_expr, then_expr in expr.alternatives:
                when_sql = self._generate_expression(when_expr)
                then_sql = self._generate_expression(then_expr)
                case_sql += f" WHEN {when_sql} THEN {then_sql}"

            if expr.else_expression:
                else_sql = self._generate_expression(expr.else_expression)
                case_sql += f" ELSE {else_sql}"

            case_sql += " END"
            return case_sql

        else:
            # Default: try to convert to string
            return str(expr)

    def _evaluate_literal(self, expr: Expression) -> Any:
        """Evaluate a literal expression to its Python value"""
        # Handle Tree nodes from lark parser (unwrap them)
        from lark import Tree
        if isinstance(expr, Tree):
            if len(expr.children) == 1:
                return self._evaluate_literal(expr.children[0])
            else:
                return str(expr)

        if isinstance(expr, IntegerLiteral):
            return expr.value
        elif isinstance(expr, FloatLiteral):
            return expr.value
        elif isinstance(expr, StringLiteral):
            return expr.value
        elif isinstance(expr, BooleanLiteral):
            return expr.value
        elif isinstance(expr, NullLiteral):
            return None
        elif isinstance(expr, Parameter):
            # Look up parameter value
            return self.parameters.get(expr.name)
        elif isinstance(expr, ListLiteral):
            return [self._evaluate_literal(e) for e in expr.elements]
        elif isinstance(expr, MapLiteral):
            return {k: self._evaluate_literal(v) for k, v in expr.items.items()}
        else:
            return str(expr)
