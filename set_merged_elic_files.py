import argparse
import json

import os
import psycopg2
from psycopg2.extras import execute_values

INSERT_BATCH_SIZE = 1000
TABLE_NAME = "file_index"

def parse_args():
    parser = argparse.ArgumentParser(description="Index files and paths into PostgreSQL. Supports config files via --config.")

    parser.add_argument('--input', help='Input file containing cross-reference data')
    parser.add_argument('--db-name', default='filedb', help='PostgreSQL database name')
    parser.add_argument('--db-user', default='postgres', help='PostgreSQL user')
    parser.add_argument('--db-password', default='', help='PostgreSQL password')
    parser.add_argument('--db-host', default='localhost', help='PostgreSQL host')
    parser.add_argument('--db-port', type=int, default=5432, help='PostgreSQL port')
    parser.add_argument('--config', help='Path to JSON config file containing arguments')

    return parser.parse_args()

def upsert_records(db_config, input: str):
    # Synchronous upsert using psycopg2
    conn = psycopg2.connect(
        dbname=db_config.get('database'),
        user=db_config.get('user'),
        password=db_config.get('password'),
        host=db_config.get('host'),
        port=db_config.get('port'),
    )
    try:
        with conn:
            with conn.cursor() as cur:
                with open(input, 'r') as f:
                    records = []
                    for line in f:
                        # Expect lines like: object_id,filename,fullpath
                        parts = [p.strip() for p in line.strip().split(',')]
                        if len(parts) < 2:
                            # skip malformed lines
                            continue
                        object_id, fullpath = parts[0], parts[1]
                        # Use OS-aware basename to handle platform-specific separators
                        filename = os.path.basename(fullpath)
                        records.append((object_id, filename, fullpath))
                        if len(records) >= INSERT_BATCH_SIZE:
                            sql = f"""
                                INSERT INTO {TABLE_NAME} (object_id, filename, fullpath)
                                VALUES %s
                                ON CONFLICT (object_id) DO UPDATE SET
                                    filename = EXCLUDED.filename,
                                    fullpath = EXCLUDED.fullpath
                            """
                            execute_values(cur, sql, records)
                            records = []
                    if records:
                        sql = f"""
                            INSERT INTO {TABLE_NAME} (object_id, filename, fullpath)
                            VALUES %s
                            ON CONFLICT (object_id) DO UPDATE SET
                                filename = EXCLUDED.filename,
                                fullpath = EXCLUDED.fullpath
                        """
                        execute_values(cur, sql, records)
    finally:
        conn.close()

def main():
    args = parse_args()

    if args.config:
        with open(args.config) as f:
            config_data = json.load(f)
        for key, value in config_data.items():
            if hasattr(args, key):
                setattr(args, key, value)

    db_config = {
        'user': args.db_user,
        'password': args.db_password,
        'database': args.db_name,
        'host': args.db_host,
        'port': args.db_port
    }
