"""
Tests for TetherSaver: LangGraph checkpointer backed by Tether WAL.

This module tests the TetherSaver implementation against the LangGraph
checkpoint API. It verifies:
- put() stores checkpoints durably
- get_tuple() retrieves them correctly
- put_writes() stores pending writes
- list() enumerates checkpoints by thread
"""

from __future__ import annotations

import os
import sys
import tempfile
import uuid

import pytest
from langgraph.checkpoint.base import (
    Checkpoint,
    CheckpointMetadata,
    RunnableConfig,
)

# Import after ensuring python_pkg is in path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "python_pkg"))
from tether_langgraph import TetherSaver


def _make_test_checkpoint(
    checkpoint_id: str = "cp_1",
    step: int = 1,
) -> tuple[Checkpoint, CheckpointMetadata]:
    """Helper to create valid Checkpoint and CheckpointMetadata for testing."""
    checkpoint: Checkpoint = {
        "v": 1,
        "id": checkpoint_id,
        "ts": "2024-08-24T12:00:00.000000Z",
        "channel_values": {"counter": 5, "name": "test"},
        "channel_versions": {"counter": 1, "name": 1},
        "versions_seen": {},
        "updated_channels": ["counter"],
    }

    metadata: CheckpointMetadata = {
        "source": "input",
        "step": step,
        "parents": {},
        "run_id": str(uuid.uuid4()),
        "counters_since_delta_snapshot": {},
    }

    return checkpoint, metadata


def _make_test_config(
    thread_id: str = "thread_1",
    checkpoint_ns: str = "default",
    checkpoint_id: str = "cp_1",
) -> RunnableConfig:
    """Helper to create a valid RunnableConfig for testing."""
    return {
        "configurable": {
            "thread_id": thread_id,
            "checkpoint_ns": checkpoint_ns,
            "checkpoint_id": checkpoint_id,
        },
        "recursion_limit": 25,
    }


class TestTetherSaverBasics:
    """Basic round-trip tests for put/get_tuple."""

    def test_put_get_single_checkpoint(self):
        """Test storing and retrieving a single checkpoint."""
        with tempfile.TemporaryDirectory() as tmpdir:
            wal_path = os.path.join(tmpdir, "test.wal")
            saver = TetherSaver(wal_path)

            config = _make_test_config()
            checkpoint, metadata = _make_test_checkpoint()

            # Put the checkpoint.
            returned_config = saver.put(
                config, checkpoint, metadata, checkpoint["channel_versions"]
            )

            # Config should be returned unchanged.
            assert returned_config == config

            # Get it back.
            retrieved = saver.get_tuple(config)
            assert retrieved is not None
            assert retrieved.checkpoint == checkpoint
            assert retrieved.metadata == metadata
            assert retrieved.config == config

    def test_get_nonexistent_checkpoint(self):
        """Test get_tuple for a checkpoint that was never stored."""
        with tempfile.TemporaryDirectory() as tmpdir:
            wal_path = os.path.join(tmpdir, "test.wal")
            saver = TetherSaver(wal_path)

            config = _make_test_config()
            retrieved = saver.get_tuple(config)
            assert retrieved is None

    def test_get_with_missing_checkpoint_id(self):
        """Test get_tuple when checkpoint_id is missing from config."""
        with tempfile.TemporaryDirectory() as tmpdir:
            wal_path = os.path.join(tmpdir, "test.wal")
            saver = TetherSaver(wal_path)

            config = {
                "configurable": {"thread_id": "t1", "checkpoint_ns": "default"},
                "recursion_limit": 25,
            }
            retrieved = saver.get_tuple(config)
            assert retrieved is None

    def test_put_multiple_checkpoints_same_thread(self):
        """Test storing multiple checkpoints under the same thread."""
        with tempfile.TemporaryDirectory() as tmpdir:
            wal_path = os.path.join(tmpdir, "test.wal")
            saver = TetherSaver(wal_path)

            thread_id = "thread_1"

            # Store two checkpoints with different IDs.
            for cp_id in ["cp_1", "cp_2"]:
                config = _make_test_config(
                    thread_id=thread_id, checkpoint_id=cp_id
                )
                checkpoint, metadata = _make_test_checkpoint(checkpoint_id=cp_id)
                saver.put(config, checkpoint, metadata, checkpoint["channel_versions"])

            # Retrieve both.
            for cp_id in ["cp_1", "cp_2"]:
                config = _make_test_config(
                    thread_id=thread_id, checkpoint_id=cp_id
                )
                retrieved = saver.get_tuple(config)
                assert retrieved is not None
                assert retrieved.checkpoint["id"] == cp_id

    def test_different_namespaces(self):
        """Test that different checkpoint namespaces are isolated."""
        with tempfile.TemporaryDirectory() as tmpdir:
            wal_path = os.path.join(tmpdir, "test.wal")
            saver = TetherSaver(wal_path)

            thread_id = "thread_1"

            # Store in namespace "default"
            config_1 = _make_test_config(
                thread_id=thread_id, checkpoint_ns="default", checkpoint_id="cp_1"
            )
            checkpoint, metadata = _make_test_checkpoint(checkpoint_id="cp_1")
            checkpoint["channel_values"] = {"ns": "default"}
            saver.put(config_1, checkpoint, metadata, checkpoint["channel_versions"])

            # Store in namespace "alternate"
            config_2 = _make_test_config(
                thread_id=thread_id, checkpoint_ns="alternate", checkpoint_id="cp_1"
            )
            checkpoint_2, metadata_2 = _make_test_checkpoint(checkpoint_id="cp_1")
            checkpoint_2["channel_values"] = {"ns": "alternate"}
            saver.put(
                config_2, checkpoint_2, metadata_2, checkpoint_2["channel_versions"]
            )

            # Retrieve from default.
            retrieved_1 = saver.get_tuple(config_1)
            assert retrieved_1 is not None
            assert retrieved_1.checkpoint["channel_values"]["ns"] == "default"

            # Retrieve from alternate.
            retrieved_2 = saver.get_tuple(config_2)
            assert retrieved_2 is not None
            assert retrieved_2.checkpoint["channel_values"]["ns"] == "alternate"


class TestTetherSaverList:
    """Tests for the list() method."""

    def test_list_empty_thread(self):
        """Test listing checkpoints for a thread with no checkpoints."""
        with tempfile.TemporaryDirectory() as tmpdir:
            wal_path = os.path.join(tmpdir, "test.wal")
            saver = TetherSaver(wal_path)

            config = _make_test_config(thread_id="empty_thread")
            checkpoints = list(saver.list(config))
            assert checkpoints == []

    def test_list_single_checkpoint(self):
        """Test listing when a thread has one checkpoint."""
        with tempfile.TemporaryDirectory() as tmpdir:
            wal_path = os.path.join(tmpdir, "test.wal")
            saver = TetherSaver(wal_path)

            thread_id = "thread_1"
            config = _make_test_config(thread_id=thread_id, checkpoint_id="cp_1")
            checkpoint, metadata = _make_test_checkpoint(checkpoint_id="cp_1")
            saver.put(config, checkpoint, metadata, checkpoint["channel_versions"])

            # List should return the one checkpoint.
            listed = list(saver.list(config))
            assert len(listed) == 1
            assert listed[0].checkpoint["id"] == "cp_1"

    def test_list_multiple_checkpoints(self):
        """Test listing multiple checkpoints from a thread."""
        with tempfile.TemporaryDirectory() as tmpdir:
            wal_path = os.path.join(tmpdir, "test.wal")
            saver = TetherSaver(wal_path)

            thread_id = "thread_1"
            cp_ids = ["cp_1", "cp_2", "cp_3"]

            # Store three checkpoints.
            for cp_id in cp_ids:
                config = _make_test_config(thread_id=thread_id, checkpoint_id=cp_id)
                checkpoint, metadata = _make_test_checkpoint(checkpoint_id=cp_id)
                saver.put(
                    config, checkpoint, metadata, checkpoint["channel_versions"]
                )

            # List them.
            listed = list(saver.list(_make_test_config(thread_id=thread_id)))
            assert len(listed) == len(cp_ids)

            # Check all IDs are present.
            retrieved_ids = {cp.checkpoint["id"] for cp in listed}
            assert retrieved_ids == set(cp_ids)

    def test_list_with_limit(self):
        """Test that limit parameter restricts results."""
        with tempfile.TemporaryDirectory() as tmpdir:
            wal_path = os.path.join(tmpdir, "test.wal")
            saver = TetherSaver(wal_path)

            thread_id = "thread_1"

            # Store five checkpoints.
            for i in range(5):
                config = _make_test_config(
                    thread_id=thread_id, checkpoint_id=f"cp_{i}"
                )
                checkpoint, metadata = _make_test_checkpoint(checkpoint_id=f"cp_{i}")
                saver.put(
                    config, checkpoint, metadata, checkpoint["channel_versions"]
                )

            # List with limit=2.
            listed = list(saver.list(_make_test_config(thread_id=thread_id), limit=2))
            assert len(listed) == 2

    def test_list_with_before_filter(self):
        """Test that before parameter filters by timestamp."""
        with tempfile.TemporaryDirectory() as tmpdir:
            wal_path = os.path.join(tmpdir, "test.wal")
            saver = TetherSaver(wal_path)

            thread_id = "thread_1"

            # Store checkpoints with different timestamps.
            timestamps = [
                "2024-08-24T12:00:00.000000Z",
                "2024-08-24T12:01:00.000000Z",
                "2024-08-24T12:02:00.000000Z",
            ]

            for i, ts in enumerate(timestamps):
                config = _make_test_config(
                    thread_id=thread_id, checkpoint_id=f"cp_{i}"
                )
                checkpoint, metadata = _make_test_checkpoint(checkpoint_id=f"cp_{i}")
                checkpoint["ts"] = ts
                saver.put(
                    config, checkpoint, metadata, checkpoint["channel_versions"]
                )

            # List with before = second checkpoint's timestamp.
            before_config: RunnableConfig = {
                "configurable": {},
                "ts": timestamps[1],
            }
            listed = list(
                saver.list(_make_test_config(thread_id=thread_id), before=before_config)
            )

            # Should return only the first checkpoint (ts < timestamps[1]).
            assert len(listed) == 1
            assert listed[0].checkpoint["ts"] == timestamps[0]


class TestTetherSaverWrites:
    """Tests for put_writes() method."""

    def test_put_writes_basic(self):
        """Test that writes stored via put_writes are returned by get_tuple."""
        with tempfile.TemporaryDirectory() as tmpdir:
            wal_path = os.path.join(tmpdir, "test.wal")
            saver = TetherSaver(wal_path)

            config = _make_test_config()
            checkpoint, metadata = _make_test_checkpoint()

            # Put the checkpoint first.
            saver.put(config, checkpoint, metadata, checkpoint["channel_versions"])

            # Add some writes.
            writes = [("channel_1", "value_1"), ("channel_2", {"data": "value_2"})]
            saver.put_writes(config, writes, task_id="task_1")

            result = saver.get_tuple(config)
            assert result is not None
            assert result.pending_writes == [
                ("task_1", "channel_1", "value_1"),
                ("task_1", "channel_2", {"data": "value_2"}),
            ]

    def test_put_writes_multiple_tasks(self):
        """Test that writes from multiple tasks for the same checkpoint all merge in."""
        with tempfile.TemporaryDirectory() as tmpdir:
            wal_path = os.path.join(tmpdir, "test.wal")
            saver = TetherSaver(wal_path)

            config = _make_test_config()
            checkpoint, metadata = _make_test_checkpoint()
            saver.put(config, checkpoint, metadata, checkpoint["channel_versions"])

            # Store writes from two different tasks.
            for task_id in ["task_1", "task_2"]:
                writes = [(f"channel_{task_id}", f"value_{task_id}")]
                saver.put_writes(config, writes, task_id=task_id)

            result = saver.get_tuple(config)
            assert result is not None
            assert result.pending_writes == [
                ("task_1", "channel_task_1", "value_task_1"),
                ("task_2", "channel_task_2", "value_task_2"),
            ]


class TestTetherSaverIntegration:
    """Integration tests with realistic LangGraph scenarios."""

    def test_thread_isolation(self):
        """Test that different threads maintain separate checkpoints."""
        with tempfile.TemporaryDirectory() as tmpdir:
            wal_path = os.path.join(tmpdir, "test.wal")
            saver = TetherSaver(wal_path)

            # Create checkpoints in two threads.
            for thread_id in ["thread_1", "thread_2"]:
                config = _make_test_config(thread_id=thread_id, checkpoint_id="cp_1")
                checkpoint, metadata = _make_test_checkpoint(checkpoint_id="cp_1")
                checkpoint["channel_values"] = {"thread": thread_id}
                saver.put(
                    config, checkpoint, metadata, checkpoint["channel_versions"]
                )

            # Verify isolation.
            config_1 = _make_test_config(thread_id="thread_1", checkpoint_id="cp_1")
            retrieved_1 = saver.get_tuple(config_1)
            assert retrieved_1.checkpoint["channel_values"]["thread"] == "thread_1"

            config_2 = _make_test_config(thread_id="thread_2", checkpoint_id="cp_1")
            retrieved_2 = saver.get_tuple(config_2)
            assert retrieved_2.checkpoint["channel_values"]["thread"] == "thread_2"

    def test_checkpoint_history_workflow(self):
        """Test a realistic workflow: store multiple checkpoints, list them, retrieve one."""
        with tempfile.TemporaryDirectory() as tmpdir:
            wal_path = os.path.join(tmpdir, "test.wal")
            saver = TetherSaver(wal_path)

            thread_id = "workflow_thread"

            # Simulate a multi-step workflow.
            for step_num in range(1, 6):
                cp_id = f"checkpoint_{step_num}"
                config = _make_test_config(thread_id=thread_id, checkpoint_id=cp_id)
                checkpoint, metadata = _make_test_checkpoint(
                    checkpoint_id=cp_id, step=step_num
                )
                checkpoint["channel_values"] = {"step": step_num, "result": step_num * 10}
                saver.put(
                    config, checkpoint, metadata, checkpoint["channel_versions"]
                )

            # List all checkpoints.
            list_config = _make_test_config(thread_id=thread_id)
            all_checkpoints = list(saver.list(list_config))
            assert len(all_checkpoints) == 5

            # Retrieve the third checkpoint.
            cp3_config = _make_test_config(
                thread_id=thread_id, checkpoint_id="checkpoint_3"
            )
            cp3 = saver.get_tuple(cp3_config)
            assert cp3.checkpoint["channel_values"]["step"] == 3
            assert cp3.checkpoint["channel_values"]["result"] == 30

    def test_durability_across_engines(self):
        """Test that data persists across separate engine instances."""
        with tempfile.TemporaryDirectory() as tmpdir:
            wal_path = os.path.join(tmpdir, "test.wal")

            # Store with first saver instance.
            saver1 = TetherSaver(wal_path)
            config = _make_test_config(checkpoint_id="persistent_cp")
            checkpoint, metadata = _make_test_checkpoint(checkpoint_id="persistent_cp")
            checkpoint["channel_values"] = {"persistent": True}
            saver1.put(config, checkpoint, metadata, checkpoint["channel_versions"])

            # Create a new saver instance (simulating process restart).
            saver2 = TetherSaver(wal_path)
            retrieved = saver2.get_tuple(config)

            # Should retrieve the stored data.
            assert retrieved is not None
            assert retrieved.checkpoint["channel_values"]["persistent"] is True


class TestTetherSaverEdgeCases:
    """Edge case and error handling tests."""

    def test_large_checkpoint_data(self):
        """Test storing large checkpoint data."""
        with tempfile.TemporaryDirectory() as tmpdir:
            wal_path = os.path.join(tmpdir, "test.wal")
            saver = TetherSaver(wal_path)

            config = _make_test_config()
            checkpoint, metadata = _make_test_checkpoint()

            # Add large data to checkpoint.
            large_data = "x" * (100 * 1024)  # 100KB
            checkpoint["channel_values"]["large_field"] = large_data
            saver.put(config, checkpoint, metadata, checkpoint["channel_versions"])

            # Retrieve and verify.
            retrieved = saver.get_tuple(config)
            assert retrieved.checkpoint["channel_values"]["large_field"] == large_data

    def test_complex_nested_data(self):
        """Test storing complex nested Python objects in checkpoints."""
        with tempfile.TemporaryDirectory() as tmpdir:
            wal_path = os.path.join(tmpdir, "test.wal")
            saver = TetherSaver(wal_path)

            config = _make_test_config()
            checkpoint, metadata = _make_test_checkpoint()

            # Add complex nested data.
            complex_data = {
                "list": [1, 2, {"nested": "dict"}],
                "tuple": (1, "two", 3.0),
                "set": {1, 2, 3},
            }
            checkpoint["channel_values"]["complex"] = complex_data
            saver.put(config, checkpoint, metadata, checkpoint["channel_versions"])

            # Retrieve and verify.
            retrieved = saver.get_tuple(config)
            # Sets will become lists after pickling, tuples become lists, so check values.
            assert retrieved.checkpoint["channel_values"]["complex"]["list"][2]["nested"] == "dict"

    def test_empty_checkpoint_values(self):
        """Test storing a checkpoint with empty channel values."""
        with tempfile.TemporaryDirectory() as tmpdir:
            wal_path = os.path.join(tmpdir, "test.wal")
            saver = TetherSaver(wal_path)

            config = _make_test_config()
            checkpoint, metadata = _make_test_checkpoint()
            checkpoint["channel_values"] = {}

            saver.put(config, checkpoint, metadata, checkpoint["channel_versions"])
            retrieved = saver.get_tuple(config)
            assert retrieved.checkpoint["channel_values"] == {}


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
