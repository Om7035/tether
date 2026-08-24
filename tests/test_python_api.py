"""
Tests for the Tether Python API (@tether decorator and StepContext).

Tests verify:
1. Basic @tether functionality with multiple steps
2. Crash recovery: a step that fails is retried on the next call, earlier steps are not re-executed
3. Cleanup of WAL files between tests
"""

import asyncio
import tempfile
from pathlib import Path
from typing import List
from unittest.mock import Mock

import pytest
from tether import StepContext, tether


@pytest.fixture
def temp_tether_dir():
    """Create a temporary .tether directory for tests."""
    # Save current dir
    original_dir = Path.cwd()
    # Create a temp dir and change to it
    with tempfile.TemporaryDirectory() as tmpdir:
        temp_path = Path(tmpdir)
        try:
            # Change to temp dir so .tether gets created there
            import os

            os.chdir(temp_path)
            yield temp_path
        finally:
            os.chdir(original_dir)


def test_basic_two_step_workflow(temp_tether_dir):
    """Test that a basic @tether workflow with two steps runs fully and returns correct values."""

    # Track which functions were called
    calls: List[str] = []

    def step1_fn(value: str) -> str:
        calls.append("step1_fn")
        return f"result1:{value}"

    def step2_fn(value: str) -> str:
        calls.append("step2_fn")
        return f"result2:{value}"

    @tether(name="test_basic_workflow")
    async def workflow(input_val: str):
        context = StepContext()
        r1 = await context.step("step1", step1_fn, input_val)
        r2 = await context.step("step2", step2_fn, r1)
        return r2

    # Run the workflow
    result = asyncio.run(workflow("input"))

    # Verify results
    assert result == "result2:result1:input"
    assert calls == ["step1_fn", "step2_fn"]


def test_crash_recovery_step2_fails_then_succeeds(temp_tether_dir):
    """
    Critical durability test: if step 2 raises an exception, step 1 should NOT be
    re-executed when the workflow is retried after step 2 is fixed.
    """

    call_counter1 = Mock(return_value="result1")
    call_counter2 = Mock(side_effect=RuntimeError("step2 failed"))

    @tether(name="test_crash_recovery")
    async def workflow_with_failure(value: str):
        context = StepContext()
        r1 = await context.step("step1", call_counter1, value)
        r2 = await context.step("step2", call_counter2, r1)
        return r2

    # First call: step 1 succeeds, step 2 fails
    with pytest.raises(RuntimeError, match="step2 failed"):
        asyncio.run(workflow_with_failure("input"))

    # Verify step1 was called once
    assert call_counter1.call_count == 1

    # Now fix step 2
    call_counter2.side_effect = None
    call_counter2.return_value = "result2"

    # Re-run the workflow with the same WAL
    result = asyncio.run(workflow_with_failure("input"))

    # Step 1 should NOT have been called again (still only 1 call total)
    assert call_counter1.call_count == 1

    # Step 2 should have been called a second time
    assert call_counter2.call_count == 2

    # Result should be correct
    assert result == "result2"


def test_async_step_functions(temp_tether_dir):
    """Test that @tether works with async step functions."""

    async def async_step1(value: str) -> str:
        await asyncio.sleep(0.001)
        return f"async1:{value}"

    async def async_step2(value: str) -> str:
        await asyncio.sleep(0.001)
        return f"async2:{value}"

    @tether(name="test_async_steps")
    async def async_workflow(input_val: str):
        context = StepContext()
        r1 = await context.step("step1", async_step1, input_val)
        r2 = await context.step("step2", async_step2, r1)
        return r2

    result = asyncio.run(async_workflow("input"))
    assert result == "async2:async1:input"


def test_step_with_complex_return_type(temp_tether_dir):
    """Test that pickle correctly serializes complex return types (dicts, lists, etc.)."""

    def complex_step() -> dict:
        return {"key": "value", "nested": [1, 2, 3], "data": {"a": 1}}

    @tether(name="test_complex_return")
    async def workflow():
        context = StepContext()
        result = await context.step("complex", complex_step)
        return result

    result = asyncio.run(workflow())
    assert result == {"key": "value", "nested": [1, 2, 3], "data": {"a": 1}}


def test_multiple_independent_workflows(temp_tether_dir):
    """Test that multiple workflows with different @tether names work independently."""

    call_log = []

    def step_a() -> str:
        call_log.append("workflow_a")
        return "a_result"

    def step_b() -> str:
        call_log.append("workflow_b")
        return "b_result"

    @tether(name="workflow_a")
    async def wf_a():
        context = StepContext()
        return await context.step("step", step_a)

    @tether(name="workflow_b")
    async def wf_b():
        context = StepContext()
        return await context.step("step", step_b)

    result_a = asyncio.run(wf_a())
    result_b = asyncio.run(wf_b())

    assert result_a == "a_result"
    assert result_b == "b_result"
    assert call_log == ["workflow_a", "workflow_b"]


def test_wal_file_created(temp_tether_dir):
    """Test that the .tether/name.wal file is actually created."""

    @tether(name="test_wal_creation")
    async def workflow():
        context = StepContext()
        await context.step("step1", lambda: "result")
        return "done"

    asyncio.run(workflow())

    # Verify the WAL file exists
    wal_path = Path(".tether") / "test_wal_creation.wal"
    assert wal_path.exists()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
