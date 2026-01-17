#!/usr/bin/env python3
"""
Verification script to ensure all Age driver components are properly packaged
Run this after installing the package: pip install -e .
"""

import sys
import os


def verify_package():
    """Verify that all Age driver components are accessible"""
    print("=" * 70)
    print("Verifying graphiti-postgres package installation")
    print("=" * 70)

    errors = []
    warnings = []

    # 1. Check module imports
    print("\n[1] Checking module imports...")
    try:
        from graphiti_postgres import AgeDriver, AgeDriverSession
        print("  ✓ AgeDriver imported successfully")
        print("  ✓ AgeDriverSession imported successfully")
    except ImportError as e:
        errors.append(f"Failed to import Age driver classes: {e}")
        print(f"  ✗ Import error: {e}")

    try:
        from graphiti_postgres import PostgresDriver, GraphDriver, GraphProvider
        print("  ✓ PostgresDriver imported successfully")
        print("  ✓ GraphDriver imported successfully")
        print("  ✓ GraphProvider imported successfully")
    except ImportError as e:
        errors.append(f"Failed to import PostgreSQL driver classes: {e}")
        print(f"  ✗ Import error: {e}")

    # 2. Check GraphProvider enum includes APACHE_AGE
    print("\n[2] Checking GraphProvider enum...")
    try:
        from graphiti_postgres import GraphProvider
        if hasattr(GraphProvider, 'APACHE_AGE'):
            print(f"  ✓ GraphProvider.APACHE_AGE = {GraphProvider.APACHE_AGE.value}")
        else:
            errors.append("GraphProvider.APACHE_AGE not found")
            print("  ✗ GraphProvider.APACHE_AGE not found")
    except Exception as e:
        errors.append(f"Error checking GraphProvider: {e}")
        print(f"  ✗ Error: {e}")

    # 3. Check SQL schema files are accessible
    print("\n[3] Checking SQL schema files...")
    try:
        import graphiti_postgres
        package_dir = os.path.dirname(graphiti_postgres.__file__)

        schema_files = [
            ('schema.sql', 'PostgreSQL schema'),
            ('age_schema.sql', 'Apache Age schema')
        ]

        for filename, description in schema_files:
            schema_path = os.path.join(package_dir, 'sql', filename)
            if os.path.exists(schema_path):
                size = os.path.getsize(schema_path)
                print(f"  ✓ {description}: {filename} ({size} bytes)")
            else:
                errors.append(f"{description} not found at {schema_path}")
                print(f"  ✗ {description} not found: {schema_path}")

    except Exception as e:
        errors.append(f"Error checking SQL files: {e}")
        print(f"  ✗ Error: {e}")

    # 4. Check Cypher grammar file
    print("\n[4] Checking Cypher grammar file...")
    try:
        import graphiti_postgres
        package_dir = os.path.dirname(graphiti_postgres.__file__)
        grammar_path = os.path.join(package_dir, 'cypher', 'grammar.lark')

        if os.path.exists(grammar_path):
            size = os.path.getsize(grammar_path)
            print(f"  ✓ Cypher grammar: grammar.lark ({size} bytes)")
        else:
            warnings.append(f"Cypher grammar not found at {grammar_path}")
            print(f"  ⚠ Cypher grammar not found: {grammar_path}")
    except Exception as e:
        warnings.append(f"Error checking grammar file: {e}")
        print(f"  ⚠ Warning: {e}")

    # 5. Verify AgeDriver class structure
    print("\n[5] Checking AgeDriver class structure...")
    try:
        from graphiti_postgres import AgeDriver

        required_methods = [
            'initialize',
            'execute_query',
            'session',
            'close',
            'health_check',
            'build_indices_and_constraints',
            'delete_all_indexes',
            'clone',
            'create_node',
            'create_edge',
            'get_node',
            'search_nodes'
        ]

        missing_methods = []
        for method in required_methods:
            if hasattr(AgeDriver, method):
                print(f"  ✓ AgeDriver.{method}() exists")
            else:
                missing_methods.append(method)
                print(f"  ✗ AgeDriver.{method}() missing")

        if missing_methods:
            errors.append(f"AgeDriver missing methods: {', '.join(missing_methods)}")

    except Exception as e:
        errors.append(f"Error checking AgeDriver structure: {e}")
        print(f"  ✗ Error: {e}")

    # 6. Check __all__ exports
    print("\n[6] Checking package exports...")
    try:
        import graphiti_postgres

        expected_exports = [
            'AgeDriver',
            'AgeDriverSession',
            'PostgresDriver',
            'PostgresDriverSession',
            'GraphDriver',
            'GraphDriverSession',
            'GraphProvider',
            'CypherToSQLTranslator'
        ]

        missing_exports = []
        for export in expected_exports:
            if export in graphiti_postgres.__all__:
                print(f"  ✓ {export} in __all__")
            else:
                missing_exports.append(export)
                print(f"  ✗ {export} not in __all__")

        if missing_exports:
            errors.append(f"Missing exports: {', '.join(missing_exports)}")

    except Exception as e:
        errors.append(f"Error checking exports: {e}")
        print(f"  ✗ Error: {e}")

    # Summary
    print("\n" + "=" * 70)
    print("VERIFICATION SUMMARY")
    print("=" * 70)

    if errors:
        print(f"\n❌ FAILED: {len(errors)} error(s) found:")
        for i, error in enumerate(errors, 1):
            print(f"  {i}. {error}")
    else:
        print("\n✅ SUCCESS: All checks passed!")

    if warnings:
        print(f"\n⚠️  {len(warnings)} warning(s):")
        for i, warning in enumerate(warnings, 1):
            print(f"  {i}. {warning}")

    print("\n" + "=" * 70)

    return len(errors) == 0


if __name__ == "__main__":
    success = verify_package()
    sys.exit(0 if success else 1)
