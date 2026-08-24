"""
Compares TetherSaver against LangGraph's SQLite checkpointer on checkpoint
write/read speed and peak memory usage.

Usage:
    pip install langgraph-checkpoint-sqlite
    python benchmarks/compare_checkpointers.py [--payload-kb 100] [--iterations 50]

Requires `langgraph-checkpoint-sqlite` as an extra dev dependency; it is
deliberately not a core dependency of this package, so it's installed only
when running this script.
"""

from __future__ import annotations

import argparse
import os
import sqlite3
import sys
import tempfile
import time
import tracemalloc
import uuid
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "python_pkg"))

from tether_langgraph import TetherSaver


def _make_checkpoint(payload_kb: int) -> tuple[dict, dict]:
    from langgraph.checkpoint.base import empty_checkpoint

    checkpoint = empty_checkpoint()
    checkpoint["channel_values"] = {"data": "x" * (payload_kb * 1024)}
    metadata = {"source": "benchmark", "step": 0, "parents": {}}
    return checkpoint, metadata


def _make_config(thread_id: str, checkpoint_id: str) -> dict:
    return {
        "configurable": {
            "thread_id": thread_id,
            "checkpoint_ns": "",
            "checkpoint_id": checkpoint_id,
        }
    }


def bench_tether(payload_kb: int, iterations: int) -> dict:
    with tempfile.TemporaryDirectory() as tmpdir:
        wal_path = os.path.join(tmpdir, "bench.wal")
        saver = TetherSaver(wal_path)

        tracemalloc.start()
        start = time.perf_counter()
        for _ in range(iterations):
            checkpoint, metadata = _make_checkpoint(payload_kb)
            checkpoint_id = str(uuid.uuid4())
            config = _make_config("bench-thread", checkpoint_id)
            saver.put(config, checkpoint, metadata, checkpoint.get("channel_versions", {}))
            saver.get_tuple(config)
        elapsed = time.perf_counter() - start
        _, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()

    return {"elapsed_s": elapsed, "peak_memory_bytes": peak}


def bench_sqlite(payload_kb: int, iterations: int) -> dict:
    try:
        from langgraph.checkpoint.sqlite import SqliteSaver
    except ImportError:
        return {"error": "langgraph-checkpoint-sqlite not installed"}

    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = os.path.join(tmpdir, "bench.sqlite")
        conn = sqlite3.connect(db_path, check_same_thread=False)
        saver = SqliteSaver(conn)
        saver.setup()

        tracemalloc.start()
        start = time.perf_counter()
        for _ in range(iterations):
            checkpoint, metadata = _make_checkpoint(payload_kb)
            checkpoint_id = str(uuid.uuid4())
            config = _make_config("bench-thread", checkpoint_id)
            saver.put(config, checkpoint, metadata, checkpoint.get("channel_versions", {}))
            saver.get_tuple(config)
        elapsed = time.perf_counter() - start
        _, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()
        conn.close()

    return {"elapsed_s": elapsed, "peak_memory_bytes": peak}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--payload-kb", type=int, default=100)
    parser.add_argument("--iterations", type=int, default=50)
    args = parser.parse_args()

    print(f"payload={args.payload_kb}KB iterations={args.iterations}\n")

    tether_result = bench_tether(args.payload_kb, args.iterations)
    print("TetherSaver:", tether_result)

    sqlite_result = bench_sqlite(args.payload_kb, args.iterations)
    print("SqliteSaver:", sqlite_result)


if __name__ == "__main__":
    main()
