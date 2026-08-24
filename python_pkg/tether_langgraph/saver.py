"""
TetherSaver: LangGraph BaseCheckpointSaver implementation backed by Tether WAL.

This module provides a drop-in replacement for LangGraph's default SQLite/Postgres
checkpointers, routing all checkpoint storage to Tether's high-performance Rust WAL.
"""

from __future__ import annotations

import pickle
from typing import Any, Iterator, Sequence

from langgraph.checkpoint.base import (
    BaseCheckpointSaver,
    Checkpoint,
    CheckpointMetadata,
    CheckpointTuple,
    RunnableConfig,
)
from tether import TetherEngine


def _make_checkpoint_key(thread_id: str, checkpoint_ns: str, checkpoint_id: str) -> str:
    """Compose a flat key for a checkpoint from its logical identifiers."""
    return f"{thread_id}:{checkpoint_ns}:{checkpoint_id}"


def _make_index_key(thread_id: str) -> str:
    """Compose the key for a thread's checkpoint index."""
    return f"__index__:{thread_id}"


def _make_writes_index_key(thread_id: str, checkpoint_ns: str, checkpoint_id: str) -> str:
    """Compose the key for a checkpoint's task_id index (which tasks wrote to it)."""
    return f"__writes_index__:{thread_id}:{checkpoint_ns}:{checkpoint_id}"


class TetherSaver(BaseCheckpointSaver):
    """
    LangGraph checkpointer backed by Tether's Rust WAL.

    Uses TetherEngine (the Rust-backed key-value store) to persist all checkpoint data.
    Checkpoints are serialized as pickled (checkpoint, metadata, parent_config, pending_writes)
    tuples. A secondary index maps thread_ids to lists of checkpoint keys for fast listing.

    Args:
        wal_path: Path to the Tether WAL file to use for storage.

    Example:
        >>> saver = TetherSaver(wal_path="/tmp/my_graph.wal")
        >>> # Use as drop-in replacement for LangGraph checkpointers:
        >>> graph = StateGraph(...).compile(checkpointer=saver)
    """

    def __init__(self, wal_path: str) -> None:
        """Initialize TetherSaver with a given WAL path."""
        self.engine = TetherEngine(wal_path)

    def put(
        self,
        config: RunnableConfig,
        checkpoint: Checkpoint,
        metadata: CheckpointMetadata,
        new_versions: dict[str, str | int | float],
    ) -> RunnableConfig:
        """
        Store a checkpoint.

        Serializes the checkpoint, metadata, parent_config, and (empty) pending_writes
        as a pickled CheckpointTuple, keyed by (thread_id, checkpoint_ns, checkpoint_id).
        Updates the thread's checkpoint index for fast listing.

        Args:
            config: RunnableConfig containing configurable dict with checkpoint identifiers.
            checkpoint: The checkpoint data (values, versions, etc.).
            metadata: Metadata about the checkpoint (source, step, parents, run_id).
            new_versions: Channel versions for this checkpoint.

        Returns:
            The same config dict (LangGraph convention).
        """
        # Extract checkpoint identifiers from config.
        configurable = config.get("configurable", {})
        thread_id = configurable.get("thread_id", "default")
        checkpoint_ns = configurable.get("checkpoint_ns", "default")
        checkpoint_id = checkpoint["id"]

        # Build the checkpoint tuple (without pending_writes for now, they're added by put_writes).
        checkpoint_tuple = CheckpointTuple(
            config=config,
            checkpoint=checkpoint,
            metadata=metadata,
            parent_config=configurable.get("parent_config"),
            pending_writes=None,
        )

        # Serialize the checkpoint.
        key = _make_checkpoint_key(thread_id, checkpoint_ns, checkpoint_id)
        serialized = pickle.dumps(checkpoint_tuple)

        # Update the thread index.
        index_key = _make_index_key(thread_id)
        index_data = self.engine.read(index_key)
        index_list = pickle.loads(index_data) if index_data else []
        if key not in index_list:
            index_list.append(key)

        # Buffer both writes, then commit them together: one disk sync
        # instead of two. The checkpoint and its index entry become durable
        # atomically -- there's no window where one exists without the other.
        self.engine.write(key, serialized)
        self.engine.write(index_key, pickle.dumps(index_list))
        self.engine.commit_batch([key, index_key])

        return config

    def put_writes(
        self,
        config: RunnableConfig,
        writes: Sequence[tuple[str, Any]],
        task_id: str,
        task_path: str = "",
    ) -> None:
        """
        Store pending writes linked to a checkpoint.

        Writes are stored separately, keyed by (thread_id, checkpoint_ns, checkpoint_id, task_id).
        On retrieval via get_tuple, they are merged into the checkpoint_tuple's pending_writes field.

        Args:
            config: RunnableConfig with checkpoint identifiers.
            writes: Sequence of (channel_name, value) tuples to store.
            task_id: Unique task identifier for these writes.
            task_path: Optional task path (not used for keying here).
        """
        configurable = config.get("configurable", {})
        thread_id = configurable.get("thread_id", "default")
        checkpoint_ns = configurable.get("checkpoint_ns", "default")
        # checkpoint_id may be in configurable or at config level
        checkpoint_id = configurable.get("checkpoint_id") or config.get("checkpoint_id", "")

        # Store writes under a separate key.
        writes_key = f"{thread_id}:{checkpoint_ns}:{checkpoint_id}:writes:{task_id}"
        serialized_writes = pickle.dumps(list(writes))

        # Record this task_id in the checkpoint's writes index so get_tuple
        # can find and merge all tasks' writes back in.
        writes_index_key = _make_writes_index_key(thread_id, checkpoint_ns, checkpoint_id)
        index_data = self.engine.read(writes_index_key)
        task_ids: list[str] = pickle.loads(index_data) if index_data else []
        if task_id not in task_ids:
            task_ids.append(task_id)

        self.engine.write(writes_key, serialized_writes)
        self.engine.write(writes_index_key, pickle.dumps(task_ids))
        self.engine.commit_batch([writes_key, writes_index_key])

    def get_tuple(self, config: RunnableConfig) -> CheckpointTuple | None:
        """
        Fetch a checkpoint tuple by config.

        Retrieves the checkpoint tuple and merges any pending writes stored via put_writes.

        Args:
            config: RunnableConfig specifying which checkpoint to retrieve.

        Returns:
            CheckpointTuple if found, None otherwise.
        """
        configurable = config.get("configurable", {})
        thread_id = configurable.get("thread_id", "default")
        checkpoint_ns = configurable.get("checkpoint_ns", "default")
        # checkpoint_id may be in configurable or at config level (LangGraph varies)
        checkpoint_id = configurable.get("checkpoint_id") or config.get("checkpoint_id", "")

        if not checkpoint_id:
            return None

        key = _make_checkpoint_key(thread_id, checkpoint_ns, checkpoint_id)
        data = self.engine.read(key)

        if data is None:
            return None

        checkpoint_tuple: CheckpointTuple = pickle.loads(data)

        # Merge in any pending writes recorded via put_writes(), across all tasks
        # that wrote to this checkpoint.
        pending_writes = self._read_pending_writes(thread_id, checkpoint_ns, checkpoint_id)
        if pending_writes:
            checkpoint_tuple = checkpoint_tuple._replace(pending_writes=pending_writes)

        return checkpoint_tuple

    def _read_pending_writes(
        self, thread_id: str, checkpoint_ns: str, checkpoint_id: str
    ) -> list[tuple[str, str, Any]]:
        """Collect (task_id, channel, value) writes recorded for a checkpoint."""
        writes_index_key = _make_writes_index_key(thread_id, checkpoint_ns, checkpoint_id)
        index_data = self.engine.read(writes_index_key)
        if not index_data:
            return []

        task_ids: list[str] = pickle.loads(index_data)
        pending_writes: list[tuple[str, str, Any]] = []
        for task_id in task_ids:
            writes_key = f"{thread_id}:{checkpoint_ns}:{checkpoint_id}:writes:{task_id}"
            writes_data = self.engine.read(writes_key)
            if writes_data is None:
                continue
            for channel, value in pickle.loads(writes_data):
                pending_writes.append((task_id, channel, value))

        return pending_writes

    def list(
        self,
        config: RunnableConfig | None,
        *,
        filter: dict[str, Any] | None = None,
        before: RunnableConfig | None = None,
        limit: int | None = None,
    ) -> Iterator[CheckpointTuple]:
        """
        List checkpoints matching the given criteria.

        Iterates over the thread's index to retrieve all checkpoint tuples.
        Applies Python-side filtering for before/limit (no backend range queries).

        Args:
            config: Base config; if provided, limits listing to that thread_id.
            filter: Not used (accepts for API compatibility).
            before: Only list checkpoints created before this config (by ts).
            limit: Maximum number of checkpoints to return.

        Yields:
            CheckpointTuple instances matching the criteria.
        """
        if config is None:
            # List all checkpoints across all threads.
            # For now, this is not efficiently implemented; iterate all threads.
            # In production, maintain a global checkpoint index.
            return

        configurable = config.get("configurable", {})
        thread_id = configurable.get("thread_id", "default")

        # Fetch the thread's index.
        index_key = _make_index_key(thread_id)
        index_data = self.engine.read(index_key)

        if index_data is None:
            return

        index_list = pickle.loads(index_data)

        # Retrieve each checkpoint tuple.
        count = 0
        for key in index_list:
            if limit is not None and count >= limit:
                break

            data = self.engine.read(key)
            if data is None:
                continue

            checkpoint_tuple: CheckpointTuple = pickle.loads(data)

            # Apply before filter (if provided, only include checkpoints before that timestamp).
            if before is not None:
                before_ts = before.get("ts")
                current_ts = checkpoint_tuple.checkpoint.get("ts")
                if before_ts and current_ts and current_ts >= before_ts:
                    continue

            yield checkpoint_tuple
            count += 1
