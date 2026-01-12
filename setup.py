"""
Setup script for graphiti-postgres package
Install with: pip install -e .
"""

from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="graphiti-postgres",
    version="0.1.0",
    author="Martin Schulze",
    description="PostgreSQL driver for Graphiti with full Cypher parser",
    long_description=long_description,
    long_description_content_type="text/markdown",
    packages=find_packages(exclude=["tests", "examples", "docs"]),
    package_data={
        "graphiti_postgres": ["cypher/grammar.lark"],
    },
    include_package_data=True,
    python_requires=">=3.10",
    install_requires=[
        "asyncpg>=0.29.0",
        "graphiti-core>=0.8.0",
        "lark>=1.1.9",
    ],
    extras_require={
        "vector": ["pgvector>=0.2.0"],
        "dev": [
            "pytest>=7.4.0",
            "pytest-asyncio>=0.21.0",
        ],
    },
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: Apache Software License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
    ],
)
