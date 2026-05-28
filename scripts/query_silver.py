#!/usr/bin/env python3
"""Query silver layer data using DuckDB directly against MinIO S3.

Usage:
    uv run python scripts/query_silver.py --de                    # query DE silver
    uv run python scripts/query_silver.py --uk                    # query UK silver
    uv run python scripts/query_silver.py --quarantine-de         # query DE quarantine
    uv run python scripts/query_silver.py --quarantine-uk         # query UK quarantine
    uv run python scripts/query_silver.py --sql "SELECT * FROM data LIMIT 10"  # custom SQL
    uv run python scripts/query_silver.py --de --explain          # show query plan

Environment:
    MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY
"""

import argparse
import os
import sys

import duckdb


def get_minio_credential_env():
    return {
        "endpoint": os.getenv("MINIO_ENDPOINT", "localhost:9000"),
        "access_key": os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
        "secret_key": os.getenv("MINIO_SECRET_KEY", "minioadmin"),
    }


def configure_minio(con):
    endpoint = os.getenv("MINIO_ENDPOINT", "localhost:9000")
    access_key = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
    secret_key = os.getenv("MINIO_SECRET_KEY", "minioadmin")

    con.execute(f"SET s3_endpoint='{endpoint}'")
    con.execute(f"SET s3_access_key_id='{access_key}'")
    con.execute(f"SET s3_secret_access_key='{secret_key}'")
    con.execute("SET s3_use_ssl=false")
    con.execute("SET s3_url_style='path'")


def build_table_path(region: str, table: str) -> str:
    return f"'s3://tcg-silver/{table}/{region}/data.parquet'"


def run_query(query: str, explain: bool = False):
    con = duckdb.connect()
    configure_minio(con)

    if explain:
        result = con.sql(f"EXPLAIN {query}")
    else:
        result = con.sql(query)

    df = result.fetchdf()
    print(f"\n{'='*80}")
    print(f"Query: {query[:100]}{'...' if len(query) > 100 else ''}")
    print(f"Rows returned: {len(df)}")
    print(f"{'='*80}")
    print(df.to_string(max_rows=50, max_colwidth=50))
    con.close()


def query_table(region: str, table: str, limit: int = 20, where: str = None):
    table_path = build_table_path(region, table)
    where_clause = f"WHERE {where}" if where else ""
    query = f"SELECT * FROM {table_path} {where_clause} LIMIT {limit}"
    run_query(query)


def show_tables():
    con = duckdb.connect()
    configure_minio(con)

    print("\n=== Available tables in tcg-silver ===")
    for region in ["de", "uk"]:
        for table in ["data", "quarantine"]:
            path = f"s3://tcg-silver/{table}/{region}/data.parquet"
            try:
                count = con.sql(f"SELECT COUNT(*) FROM '{path}'").fetchone()[0]
                print(f"  tcg-silver/{table}/{region}: {count} rows")
            except Exception as e:
                print(f"  tcg-silver/{table}/{region}: error - {e}")

    con.close()


def main():
    parser = argparse.ArgumentParser(description="Query silver layer via DuckDB + MinIO")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--de", action="store_true", help="Query DE silver (data/de)")
    group.add_argument("--uk", action="store_true", help="Query UK silver (data/uk)")
    group.add_argument("--quarantine-de", action="store_true", help="Query DE quarantine")
    group.add_argument("--quarantine-uk", action="store_true", help="Query UK quarantine")
    group.add_argument("--sql", type=str, help="Custom SQL query, e.g. --sql 'SELECT * FROM data LIMIT 10'")
    group.add_argument("--tables", action="store_true", help="Show all available tables and row counts")

    parser.add_argument("--limit", type=int, default=20, help="LIMIT for table queries")
    parser.add_argument("--explain", action="store_true", help="Show query plan")
    parser.add_argument("--where", type=str, help="WHERE clause for table queries")

    args = parser.parse_args()

    if args.tables:
        show_tables()
        return

    if args.sql:
        run_query(args.sql, explain=args.explain)
        return

    if args.de:
        query_table("de", "data", limit=args.limit, where=args.where)
    elif args.uk:
        query_table("uk", "data", limit=args.limit, where=args.where)
    elif args.quarantine_de:
        query_table("de", "quarantine", limit=args.limit, where=args.where)
    elif args.quarantine_uk:
        query_table("uk", "quarantine", limit=args.limit, where=args.where)
    else:
        parser.print_help()
        print("\n=== Examples ===")
        print("  uv run python scripts/query_silver.py --tables")
        print("  uv run python scripts/query_silver.py --de")
        print("  uv run python scripts/query_silver.py --uk")
        print("  uv run python scripts/query_silver.py --quarantine-de")
        print("  uv run python scripts/query_silver.py --de --limit 5")
        print("  uv run python scripts/query_silver.py --de --where \"price > 100\"")
        print("  uv run python scripts/query_silver.py --sql \"SELECT card_id, price, currency FROM data LIMIT 10\"")


if __name__ == "__main__":
    main()