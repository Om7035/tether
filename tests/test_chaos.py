"""
Chaos engineering test suite for Tether durability guarantees.

These tests verify the core durability promise:
1. The system must NEVER lose a committed step.
2. The system must NEVER re-execute a committed step upon recovery.
3. Failed (uncommitted) steps SHOULD be retried on the next invocation.

Test strategy:
- Test 1 (SIGKILL recovery): Real process kill mid-workflow.
- Test 2 (Disk error): Verify Rust errors surface as clean Python exceptions.
- Test 3 (Network timeout): Simulate failed step with retry in same process.
"""

import asyncio
import os
import subprocess
import sys
from pathlib import Path

import pytest
from tether import StepContext, tether


class TestChaosKillMidWorkflow:
    """Test SIGKILL mid-workflow and recovery."""

    def test_sigkill_recovery_step2_killed_then_resumed(self, tmp_path):
        """
        Test the durability guarantee: after SIGKILL mid-workflow, the second run
        should NOT re-execute earlier completed steps.

        Steps:
        1. Run chaos_worker.py in a subprocess (will be killed at step 2).
        2. Assert the subprocess died abnormally.
        3. Run chaos_worker.py again with the same .tether WAL.
        4. Assert step1 was only called once (not re-executed on retry).
        """
        # Use tmp_path for isolated WAL and working state
        wal_and_work_dir = tmp_path / "chaos_run"
        wal_and_work_dir.mkdir()

        chaos_worker_script = Path(__file__).parent / "chaos_worker.py"
        assert chaos_worker_script.exists(), "chaos_worker.py must exist"

        # First invocation: process will be killed at step 2
        result1 = subprocess.run(
            [sys.executable, str(chaos_worker_script), str(wal_and_work_dir)],
            cwd=str(wal_and_work_dir),
            timeout=10,
            capture_output=False,  # Allow output to be seen if needed
            check=False,
        )

        # Verify the process died abnormally (non-zero exit code)
        # SIGKILL typically results in a negative return code on Unix or high positive on Windows
        assert result1.returncode != 0, (
            f"First invocation should have died, but exited with {result1.returncode}"
        )

        # Check that step1 was called once (counter file should show 1)
        counter_file = wal_and_work_dir / "step1_calls.txt"
        assert counter_file.exists(), "step1 should have written counter file"
        assert (
            counter_file.read_text().strip() == "1"
        ), "step1 should have been called exactly once"

        # Second invocation: same .tether directory, step2 should NOT kill this time
        result2 = subprocess.run(
            [sys.executable, str(chaos_worker_script), str(wal_and_work_dir)],
            cwd=str(wal_and_work_dir),
            timeout=10,
            capture_output=False,
            check=False,
        )

        # Second invocation must succeed
        assert result2.returncode == 0, (
            f"Second invocation should succeed, but exited with {result2.returncode}"
        )

        # **CRITICAL:** step1 must NOT have been called a second time
        # (the step was already committed in the first run)
        assert (
            counter_file.read_text().strip() == "1"
        ), "step1 must NOT be re-executed on recovery (must stay at 1 call)"


class TestChaosUnhandledError:
    """Test that Rust errors surface as clean Python exceptions (not panics/segfaults)."""

    def test_disk_error_invalid_path(self, tmp_path):
        """
        Simulate a disk-like error by trying to write to an invalid path.
        Assert that TetherEngine construction or write raises a clean Python exception.

        On Windows, use a nonexistent drive letter (e.g., Z:).
        """
        # Use a path that will fail when the Rust code tries to create/open a file
        # On Windows, Z: drive usually doesn't exist
        invalid_wal_path = "Z:\\nonexistent\\path\\test.wal" if sys.platform == "win32" else "/nonexistent/invalid/path/test.wal"

        # Try to create a TetherEngine with the invalid path
        from tether import TetherEngine

        try:
            engine = TetherEngine(invalid_wal_path)
            # If we got here, try to trigger a write
            engine.write("step1", b"test_data")
            pytest.fail(
                "TetherEngine should have raised an exception for invalid path, but didn't"
            )
        except (OSError, RuntimeError) as e:
            # Expect a clean, catchable exception (not a Rust panic that manifests as segfault)
            # RuntimeError can be raised by PyO3 wrapping Rust errors
            assert isinstance(e, (OSError, RuntimeError)), (
                f"Expected a clean exception, got {type(e).__name__}: {e}"
            )
        except Exception as e:  # noqa: BLE001  # intentional catch-all for unexpected exception types
            pytest.fail(
                f"Got an unexpected exception type {type(e).__name__}: {e}. "
                "Rust errors should surface as OSError/IOError/RuntimeError, not panics."
            )


class TestChaosNetworkTimeout:
    """Test retry behavior for failed (uncommitted) steps."""

    def test_timeout_on_first_call_succeeds_on_retry(self, tmp_path):
        """
        Test that a failed step (not committed) is retried on the next workflow invocation.

        Steps:
        1. Define a workflow with 3 steps.
        2. Step 2 raises TimeoutError on first call, succeeds on second.
        3. Call the workflow first time; expect TimeoutError and no commit.
        4. Call the workflow second time; expect it to retry step2 (and succeed this time).
        5. Verify step1 and step3 are NOT re-executed (they were already committed).
        """
        # Track call counts for each step
        call_counts = {"step1": 0, "step2": 0, "step3": 0}

        def step1_fn():
            call_counts["step1"] += 1
            return "step1_result"

        def step2_fn():
            call_counts["step2"] += 1
            if call_counts["step2"] == 1:
                # First call: fail
                raise TimeoutError("Network timeout on first call")
            # Second call: succeed
            return "step2_result"

        def step3_fn():
            call_counts["step3"] += 1
            return "step3_result"

        # Change to tmp_path so .tether is created there
        original_cwd = os.getcwd()
        try:
            os.chdir(tmp_path)

            @tether(name="timeout_test")
            async def timeout_workflow():
                context = StepContext()
                r1 = await context.step("step1", step1_fn)
                r2 = await context.step("step2", step2_fn)
                r3 = await context.step("step3", step3_fn)
                return (r1, r2, r3)

            # First invocation: step1 succeeds, step2 fails (TimeoutError)
            with pytest.raises(TimeoutError, match="Network timeout"):
                asyncio.run(timeout_workflow())

            # After first invocation:
            # - step1 was called once (and committed)
            # - step2 was called once (and failed, so NOT committed)
            # - step3 was NOT called (step2 failed before reaching it)
            assert call_counts["step1"] == 1
            assert call_counts["step2"] == 1
            assert call_counts["step3"] == 0

            # Second invocation: same workflow, same WAL
            result = asyncio.run(timeout_workflow())

            # After second invocation:
            # - step1 should be skipped (already committed) -> call_count stays 1
            # - step2 should be retried (was not committed) -> call_count becomes 2
            # - step3 should succeed (now reached) -> call_count becomes 1
            assert call_counts["step1"] == 1, (
                "step1 must NOT be re-executed (already committed)"
            )
            assert call_counts["step2"] == 2, (
                "step2 must be retried (was not committed on first run)"
            )
            assert call_counts["step3"] == 1, "step3 must be executed (first time reaching it)"

            # Verify the result is correct
            assert result == ("step1_result", "step2_result", "step3_result")

        finally:
            os.chdir(original_cwd)


class TestChaosMultipleWorkflows:
    """Test that multiple workflows don't interfere with each other during chaos."""

    def test_two_workflows_independent_during_failure(self, tmp_path):
        """
        Test that when one workflow fails, another workflow can succeed independently.
        """
        call_log = []

        def wf_a_step1():
            call_log.append("wf_a_step1")
            return "a_result"

        def wf_b_step1():
            call_log.append("wf_b_step1")
            if len([x for x in call_log if x == "wf_b_step1"]) == 1:
                raise RuntimeError("wf_b temp failure")
            return "b_result"

        original_cwd = os.getcwd()
        try:
            os.chdir(tmp_path)

            @tether(name="wf_chaos_a")
            async def workflow_a():
                context = StepContext()
                return await context.step("step1", wf_a_step1)

            @tether(name="wf_chaos_b")
            async def workflow_b():
                context = StepContext()
                return await context.step("step1", wf_b_step1)

            # Run workflow_a: succeeds
            result_a = asyncio.run(workflow_a())
            assert result_a == "a_result"
            assert call_log == ["wf_a_step1"]

            # Run workflow_b: fails on first try
            with pytest.raises(RuntimeError, match="wf_b temp failure"):
                asyncio.run(workflow_b())
            assert call_log == ["wf_a_step1", "wf_b_step1"]

            # Re-run workflow_b: should retry and succeed
            result_b = asyncio.run(workflow_b())
            assert result_b == "b_result"
            assert call_log == ["wf_a_step1", "wf_b_step1", "wf_b_step1"]

            # Re-run workflow_a: should return cached result (no new call)
            result_a2 = asyncio.run(workflow_a())
            assert result_a2 == "a_result"
            # call_log should NOT have a new "wf_a_step1" entry
            assert call_log == ["wf_a_step1", "wf_b_step1", "wf_b_step1"]

        finally:
            os.chdir(original_cwd)


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
