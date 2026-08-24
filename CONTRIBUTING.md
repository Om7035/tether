# Contributing to Tether

Tether is small on purpose. Before adding code, check whether the change
belongs in the Rust core, the Python API, or an adapter — see
[ARCHITECTURE.md](ARCHITECTURE.md) for the layer boundaries.

## Setup

```bash
pip install maturin
maturin develop --extras dev   # builds the Rust extension, installs dev deps
```

## Rules

- **No SQLite.** State lives in the custom WAL — see `rust_core/src/wal.rs`.
- **No panics across the FFI boundary.** Every Rust error must become a
  `PyResult` / clean Python exception, never a `panic!` or segfault.
- **No `std::sync::Mutex` across FFI.** Use `DashMap` or `crossbeam` so Rust
  work doesn't block the GIL.
- **Zero-copy where it matters.** Large payloads (LLM context, etc.) cross
  the Python/Rust boundary via buffer protocol, not JSON serialization.
- **Test first.** Write the failing test, then the implementation.
- **Keep files under ~300 lines.** Split into modules before they grow past
  that.

## Running tests

```bash
cargo test              # Rust core
pytest                  # Python API + LangGraph adapter + chaos suite
cargo clippy && cargo fmt --check
ruff check . && black --check .
```

## Pull requests

Small, focused PRs. Explain the *why* in the description; the diff shows
the *what*. If you're touching the WAL or state machine, add or update a
chaos test (`tests/`) that proves the guarantee still holds under a kill
mid-step.
