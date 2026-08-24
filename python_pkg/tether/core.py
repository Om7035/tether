"""
Python API for Tether: @tether decorator and StepContext for crash-proof execution.
"""

import contextvars
import functools
import inspect
import pickle
from pathlib import Path
from typing import Any, Callable, Coroutine, TypeVar, Union

from .tether import TetherEngine

# Module-level cache of TetherEngine instances, keyed by name.
_engine_cache: dict[str, TetherEngine] = {}

# ContextVar to hold the active engine for the current async context.
_active_engine: contextvars.ContextVar[TetherEngine] = contextvars.ContextVar(
    "tether_active_engine"
)

T = TypeVar("T")


class StepContext:
    """
    Manages durable step execution within a @tether-decorated function.

    Call context.step(name, fn, *args) to execute a crash-proof step:
    - If the step already completed (committed in the WAL), its result is returned immediately.
    - Otherwise, fn(*args) is called (awaited if async), result is serialized and committed.
    - If fn raises, nothing is committed; the exception propagates (next call will retry).
    """

    def __init__(self) -> None:
        """Initialize with the active engine from the ContextVar."""
        self.engine: TetherEngine = _active_engine.get()

    async def step(
        self,
        step_name: str,
        fn: Union[Callable[..., T], Callable[..., Coroutine[Any, Any, T]]],
        *args: Any,
    ) -> T:
        """
        Execute a durable step.

        Args:
            step_name: Unique name for this step (scoped to the decorated function's WAL).
            fn: Function to call (sync or async).
            *args: Arguments to pass to fn.

        Returns:
            The result of fn (either from cache if already completed, or freshly computed).

        Raises:
            Any exception fn raises (not caught or committed).
        """
        # Check if this step was already completed in a prior run.
        cached = self.engine.read(step_name)
        if cached is not None:
            # Step already committed; deserialize and return without re-executing fn.
            return pickle.loads(cached)

        # Step not yet completed; execute fn.
        if inspect.iscoroutinefunction(fn):
            result = await fn(*args)
        else:
            result = fn(*args)

        # Serialize, write (mark pending), and commit.
        serialized = pickle.dumps(result)
        self.engine.write(step_name, serialized)
        self.engine.commit(step_name)

        return result


def tether(name: str) -> Callable:
    """
    Decorator to make an async function crash-proof using Tether.

    Args:
        name: Name for this workflow (used to derive the WAL file path).

    Returns:
        A decorator that wraps the async function.

    Usage:
        @tether(name="my_workflow")
        async def my_workflow_task(arg):
            context = StepContext()
            result = await context.step("step1", some_fn, arg)
            return result
    """

    def decorator(func: Callable[..., Coroutine[Any, Any, T]]) -> Callable:
        @functools.wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> T:
            # Get or create the TetherEngine for this workflow name.
            if name not in _engine_cache:
                # Create .tether directory if it doesn't exist.
                tether_dir = Path(".tether")
                tether_dir.mkdir(exist_ok=True)
                wal_path = str(tether_dir / f"{name}.wal")
                _engine_cache[name] = TetherEngine(wal_path)

            engine = _engine_cache[name]

            # Set the engine in the ContextVar so StepContext() can find it.
            token = _active_engine.set(engine)
            try:
                return await func(*args, **kwargs)
            finally:
                # Restore the previous ContextVar value (for nested calls or cleanup).
                _active_engine.reset(token)

        return wrapper

    return decorator
