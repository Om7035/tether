"""
Standalone worker script for chaos testing.

This script runs a durable @tether workflow with 3 steps:
- Step 1: Cheap operation, writes a call counter to disk.
- Step 2: On FIRST invocation (no marker file), kills the process via SIGKILL.
           On RETRY (marker file exists), completes normally.
- Step 3: Cheap operation.

The marker file on disk allows us to distinguish first run from retry run
without relying on in-memory state.

Usage:
    python chaos_worker.py <wal_dir> [marker_file]
"""

import asyncio
import os
import signal
import sys
from pathlib import Path

# Add parent packages to path so we can import tether
sys.path.insert(0, str(Path(__file__).parent.parent))

from tether import StepContext, tether


def main():
    """Main entry point."""
    if len(sys.argv) < 2:
        print("Usage: python chaos_worker.py <wal_dir> [marker_file]")
        sys.exit(1)

    wal_dir = sys.argv[1]
    marker_file = sys.argv[2] if len(sys.argv) > 2 else None

    # If marker_file is not provided, derive one from wal_dir
    if marker_file is None:
        marker_file = str(Path(wal_dir) / "chaos_marker.txt")

    # Change to a directory where .tether will be created
    # For simplicity, create it at the wal_dir location
    os.makedirs(wal_dir, exist_ok=True)

    # Create a custom .tether directory there
    tether_dir = Path(wal_dir) / ".tether"
    tether_dir.mkdir(exist_ok=True)

    # Override the default .tether location by changing cwd before calling @tether
    original_cwd = os.getcwd()
    try:
        os.chdir(wal_dir)
        asyncio.run(chaos_workflow())
    finally:
        os.chdir(original_cwd)

    # Signal success
    sys.exit(0)


async def chaos_workflow():
    """A @tether workflow with 3 steps: step1, step2 (kills on first run), step3."""

    @tether(name="chaos_test")
    async def workflow():
        context = StepContext()

        # Step 1: Write call counter to a file
        result1 = await context.step("step1", step1_fn)

        # After step1 completes and commits, check if we should kill
        check_and_kill_if_first_run()

        # Step 2: Will not be reached on first run (process killed above)
        result2 = await context.step("step2", step2_fn)

        # Step 3: Another cheap step
        result3 = await context.step("step3", step3_fn)

        return (result1, result2, result3)

    result = await workflow()
    return result


def step1_fn():
    """Step 1: Increment call counter to disk."""
    cwd = os.getcwd()
    counter_file = Path(cwd) / "step1_calls.txt"

    # Read current count (default 0)
    try:
        count = int(counter_file.read_text().strip())
    except (FileNotFoundError, ValueError):
        count = 0

    # Increment and write back
    count += 1
    counter_file.write_text(str(count))

    return "step1_result"


def check_and_kill_if_first_run():
    """
    After step1 commits, check if this is the first run and kill the process.
    This happens AFTER step1's result is committed to the WAL, so the first run
    should have step1 committed but step2+ not reached.
    """
    cwd = os.getcwd()
    marker_file = Path(cwd) / "chaos_marker.txt"

    if not marker_file.exists():
        # First run: create marker and kill process abruptly
        marker_file.write_text("killed_once")
        # Use SIGTERM (available on Windows; kills the process hard)
        os.kill(os.getpid(), signal.SIGTERM)
        # Should never reach here
        sys.exit(255)


def step2_fn():
    """Step 2: Cheap operation."""
    return "step2_result"


def step3_fn():
    """Step 3: Cheap operation."""
    return "step3_result"


if __name__ == "__main__":
    main()
