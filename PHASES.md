# Tether Build Phases

Source: master_spec.md. TDD strict: test before implementation. Commit after each phase passes.

- [x] Phase 1: Skeleton — Cargo.toml, pyproject.toml, empty structs/traits (lib.rs, wal.rs, state.rs, error.rs)
- [x] Phase 2: WAL — in-memory buffer + mmap flush, test-first, in rust_core/src/wal.rs (4/4 tests pass)
- [x] Phase 3: State Machine — 2PC (pending->committed) via DashMap, in rust_core/src/state.rs (5/5 tests pass)
- [x] Phase 4: FFI Bridge — PyO3 zero-copy (PyBytes), py.allow_threads() on all I/O (3/3 tests pass)
- [ ] Phase 5: Python API — @tether decorator, StepContext, python_pkg/tether/core.py
- [ ] Phase 6: Chaos tests — SIGKILL mid-step, disk full, network timeout fault injection
- [ ] Phase 7: LangGraph checkpointer adapter — tether_langgraph package
- [ ] Phase 8: Docs — README, ARCHITECTURE.md, BENCHMARKS.md

## Rules (from CLAUDE.md)
- No SQLite. No panics (PyResult everywhere). No std Mutex across FFI (DashMap/crossbeam only).
- Files <300 lines, split into modules if exceeded.
- Test first, then implement.
