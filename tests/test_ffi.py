"""
Python FFI smoke test for TetherEngine.

This test file demonstrates the PyO3 FFI bridge for the Tether durable execution engine.
To run this test:
    1. Build the Rust extension: maturin develop
    2. Run pytest: pytest tests/test_ffi.py

NOTE: This test requires the compiled Rust module to be installed.
The extension module must be built via 'maturin develop' before running this test.
"""

import os
import sys
import tempfile


def test_tether_engine_basic_workflow():
    """Test the basic write-commit-read workflow."""
    # Import the compiled module
    # NOTE: This will fail if maturin develop has not been run.
    try:
        from tether import TetherEngine
    except ImportError as e:
        raise ImportError(
            "Failed to import tether module. "
            "Run 'maturin develop' from the project root first."
        ) from e

    # Create a temporary directory for the WAL
    with tempfile.TemporaryDirectory() as tmpdir:
        wal_path = os.path.join(tmpdir, "tether.wal")

        # Construct a TetherEngine
        engine = TetherEngine(wal_path)

        # Write a key-value pair (should mark pending and append to WAL)
        key = "test_step"
        value = b"step_result_data"
        engine.write(key, value)

        # At this point, the key is pending, so read should return None
        result = engine.read(key)
        assert result is None, "Pending keys should not be readable"

        # Commit the key
        engine.commit(key)

        # Now read should return the value
        result = engine.read(key)
        assert result == value, f"Expected {value}, got {result}"

        print("✓ Basic workflow test passed: write -> (pending) -> commit -> read")


def test_tether_engine_zero_copy_bytes():
    """Test that PyBytes are used for zero-copy semantics."""
    try:
        from tether import TetherEngine
    except ImportError as e:
        raise ImportError(
            "Failed to import tether module. "
            "Run 'maturin develop' from the project root first."
        ) from e

    with tempfile.TemporaryDirectory() as tmpdir:
        wal_path = os.path.join(tmpdir, "tether.wal")
        engine = TetherEngine(wal_path)

        # Write a large binary payload
        large_value = b"x" * (10 * 1024)  # 10KB
        engine.write("large_key", large_value)
        engine.commit("large_key")

        # Read it back
        result = engine.read("large_key")
        assert result == large_value, "Large payload round-trip failed"
        assert isinstance(result, bytes), "Read should return bytes"

        print("✓ Zero-copy bytes test passed: large payload handled efficiently")


def test_tether_engine_multiple_keys():
    """Test writing and committing multiple keys."""
    try:
        from tether import TetherEngine
    except ImportError as e:
        raise ImportError(
            "Failed to import tether module. "
            "Run 'maturin develop' from the project root first."
        ) from e

    with tempfile.TemporaryDirectory() as tmpdir:
        wal_path = os.path.join(tmpdir, "tether.wal")
        engine = TetherEngine(wal_path)

        # Write and commit multiple keys
        for i in range(5):
            key = f"key_{i}"
            value = f"value_{i}".encode()
            engine.write(key, value)
            engine.commit(key)

        # Verify all keys are readable
        for i in range(5):
            key = f"key_{i}"
            expected = f"value_{i}".encode()
            result = engine.read(key)
            assert result == expected, f"Key {key} mismatch"

        print("✓ Multiple keys test passed: 5 keys written, committed, and read back")


if __name__ == "__main__":
    print("Running Tether FFI smoke tests...")
    print("(Requires: maturin develop)")
    print()

    try:
        test_tether_engine_basic_workflow()
        test_tether_engine_zero_copy_bytes()
        test_tether_engine_multiple_keys()
        print()
        print("All smoke tests passed!")
    except Exception as e:  # noqa: BLE001  # intentional catch-all for the smoke test
        print(f"Test failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
