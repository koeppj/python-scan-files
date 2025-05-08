import os
import re
import argparse
import asyncio
import asyncpg
import multiprocessing as mp
from queue import Empty
import time

INSERT_BATCH_SIZE = 1000
TABLE_NAME = "file_index"

def scan_dir(dir_path, root_path, filename_regex):
    """Scan a directory and return a list of files and subdirectories.
    If filename_regex is provided, it will extract the matching portion of the filename.
    """
    results = []
    subdirs = []
    try:
        with os.scandir(dir_path) as entries:
            for entry in entries:
                full_path = entry.path
                if entry.is_file():
                    full_filename = entry.name
                    extracted = full_filename
                    if filename_regex:
                        match = re.search(filename_regex, full_filename)
                        if match:
                            extracted = match.group(0)
                        else:
                            continue  # skip files that do not match
                    object_id = extracted if filename_regex else full_filename
                    results.append((object_id, full_filename, full_path, root_path))
                elif entry.is_dir(follow_symlinks=False):
                    subdirs.append(full_path)
    except (PermissionError, FileNotFoundError):
        pass
    return results, subdirs

def worker_task(task_queue, result_queue, root_path, filename_regex):
    """Worker function to process directories from the task queue."""
    while True:
        try:
            dir_path = task_queue.get(timeout=5)
            if dir_path is None:
                break
            if dir_path is None:
                break
            files, subdirs = scan_dir(dir_path, root_path, filename_regex)
            result_queue.put(('files', files))
            for sub in subdirs:
                task_queue.put(sub)
        except Empty:
            continue
        finally:
            task_queue.task_done()


async def prepare_table(conn, drop_existing):
    """Prepare the database table for indexing files."""
    if drop_existing:
        await conn.execute(f"DROP TABLE IF EXISTS {TABLE_NAME}")
    await conn.execute(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
        object_id TEXT PRIMARY KEY,
        filename TEXT NOT NULL,
        root_path TEXT NOT NULL,
        full_path TEXT NOT NULL
    );""")
    await conn.execute(f"CREATE INDEX IF NOT EXISTS idx_object_id ON {TABLE_NAME}(object_id);")

async def async_writer(result_queue, db_config, drop_existing):
    total_start = time.time()
    """Asynchronous writer function to insert file data into the database."""
    file_counter = 0
    conn = await asyncpg.connect(**db_config)
    await prepare_table(conn, drop_existing)

    insert_query = f"""
        INSERT INTO {TABLE_NAME} (object_id, filename, full_path, root_path)
        VALUES ($1, $2, $3, $4)
        ON CONFLICT (object_id) DO NOTHING
    """

    buffer = []
    loop = asyncio.get_running_loop()

    while True:
        try:
            msg_type, data = await loop.run_in_executor(None, result_queue.get, True, 10)
            if msg_type == 'done':
                break
            elif msg_type == 'files':
                buffer.extend(data)
                if len(buffer) >= INSERT_BATCH_SIZE:
                    start_time = time.time()
                    await conn.executemany(insert_query, buffer)
                    duration = time.time() - start_time
                    file_counter += len(buffer)
                    if file_counter % 100000 < INSERT_BATCH_SIZE:
                        print(f"[INFO] Indexed {file_counter:,} files in {duration:.2f} seconds...")
                    buffer.clear()
        except Empty:
            continue

    if buffer:
        start_time = time.time()
        await conn.executemany(insert_query, buffer)
        duration = time.time() - start_time
        file_counter += len(buffer)
        print(f"[INFO] Final indexed file count: {file_counter:,} in {duration:.2f} seconds")
    total_time = time.time() - total_start
    rate = file_counter / total_time if total_time > 0 else 0
    print(f"[SUMMARY] Indexed {file_counter:,} files total in {total_time:.2f} seconds ({rate:.2f} files/sec)")

    await conn.close()

def writer_process(result_queue, db_config, drop_existing):
    asyncio.run(async_writer(result_queue, db_config, drop_existing))

def traverse_parallel(root_dir, db_config, drop_existing=False, filename_regex=None, num_workers=mp.cpu_count()):
    """Traverse directories in parallel and index files into PostgreSQL."""
    ctx = mp.get_context('spawn')
    task_queue = ctx.JoinableQueue()
    result_queue = ctx.Queue(maxsize=1000)

    task_queue.put(root_dir)

    writer = ctx.Process(target=writer_process, args=(result_queue, db_config, drop_existing))
    writer.start()

    workers = [
        ctx.Process(target=worker_task, args=(task_queue, result_queue, root_dir, filename_regex))
        for _ in range(num_workers)
    ]
    for w in workers:
        w.start()

    task_queue.join()  # Wait until all tasks are done

    for _ in workers:
        task_queue.put(None)
    for w in workers:
        w.join()

    result_queue.put(('done', None))
    writer.join()

import json

def parse_args():
    parser = argparse.ArgumentParser(description="Index files and paths into PostgreSQL. Supports config files via --config.")

    parser.add_argument('--root', required=True, help='Root directory to start scanning')
    parser.add_argument('--db-name', default='filedb', help='PostgreSQL database name')
    parser.add_argument('--db-user', default='postgres', help='PostgreSQL user')
    parser.add_argument('--db-password', default='', help='PostgreSQL password')
    parser.add_argument('--db-host', default='localhost', help='PostgreSQL host')
    parser.add_argument('--db-port', type=int, default=5432, help='PostgreSQL port')
    parser.add_argument('--drop-existing', action='store_true', help='Drop and recreate table before indexing')
    parser.add_argument('--filename-regex', default=None, help='Regex pattern to extract portion of the filename')
    parser.add_argument('--config', help='Path to JSON config file containing arguments')

    return parser.parse_args()

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

    traverse_parallel(
        args.root,
        db_config,
        drop_existing=args.drop_existing,
        filename_regex=args.filename_regex
    )

if __name__ == '__main__':
    main()
