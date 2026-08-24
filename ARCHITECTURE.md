# Architecture

Deep dive into Tether's write-ahead log, its two-phase commit protocol, the
zero-copy FFI boundary, and the GIL-release strategy. Written for reviewers
who want to know exactly what's happening on disk and across the FFI
boundary, not just the API surface.

## Layers

```
Python API (@tether, StepContext)
    |
PyO3 FFI bridge (TetherEngine)
    |
Rust core: StateManager (lock-free, in-memory) + WriteAheadLog (durable, on disk)
```

## The Write-Ahead Log

`rust_core/src/wal.rs` implements an append-only log: writes accumulate in an
in-memory `Vec<u8>` buffer, and `sync()` flushes that buffer to an `mmap`'d
file on disk, appending rather than rewriting. Each record on disk is
length-prefixed: `[u32 len][bytes]`, back to back, so `replay()` can walk the
file sequentially without a separate index.

On top of that raw framing, `rust_core/src/lib.rs` defines two record types
that carry Tether's own semantics:

- **WRITE** — `[u8 tag=0][u32 key_len][key bytes][value bytes]`
- **COMMIT** — `[u8 tag=1][u32 key_len][key bytes]`

A `write()` call appends a WRITE record. A `commit()` call appends a COMMIT
record for the same key. Both go through `sync()` before the call returns, so
by the time `write()`/`commit()` return control to Python, the record is on
disk — not just buffered in memory.

### Why two record types, not one

An earlier version of this replay logic treated every WRITE record found on
disk as committed. That's wrong: `write()` and `commit()` are two separate
calls, and a process can die between them — after the WRITE record hits disk,
before the COMMIT record does. Replaying that state as "committed" would mean
a step that was interrupted mid-flight gets treated as done and is never
retried, silently losing the work it was supposed to do.

Replay (`TetherEngine::new`) now does a two-pass reconstruction: it walks the
WAL keeping a `HashMap` of the most recent WRITE per key, and only promotes a
key into `StateManager` (as `Committed`) when a matching COMMIT record shows
up for it later in the log. A WRITE with no following COMMIT is dropped —
that step will re-run on the next attempt, which is the correct behavior.

## Two-Phase Commit State Machine

`rust_core/src/state.rs`'s `StateManager` mirrors the WAL's two-phase
structure in memory, backed by a lock-free `DashMap<String, (EntryState,
Vec<u8>)>`:

- `mark_pending(key, value)` inserts the entry as `Pending`.
- `commit(key)` transitions `Pending -> Committed`, or returns an error
  (never panics) if there's no pending entry to commit.
- `get(key)` only returns a value if its entry is `Committed` — a pending
  write is invisible to readers until it's committed.

This is what gives `StepContext.step()` its exactly-once guarantee: it calls
`engine.read(step_name)` before running anything, and a pending (uncommitted)
write reads back as `None`, so an interrupted step looks exactly like a step
that was never attempted.

## The FFI Bridge

`TetherEngine` (in `rust_core/src/lib.rs`) is the `#[pyclass]` Python talks
to. It holds:

- `wal: Arc<parking_lot::Mutex<WriteAheadLog>>` — `WriteAheadLog`'s
  `append`/`sync`/`truncate` need `&mut self`, so concurrent access from
  Python needs synchronization. `parking_lot::Mutex` is used instead of
  `std::sync::Mutex` because it's faster and never poisons — a panicking
  thread would otherwise leave every future lock acquisition failing, which
  is exactly the kind of instability a durability engine cannot afford.
- `state: Arc<StateManager>` — no lock needed; `DashMap` gives it interior
  concurrency directly.

### Zero-copy reads

`read()` returns `Option<Py<PyBytes>>`, built via `PyBytes::new_bound(py,
&bytes)`. To be precise about what "zero-copy" means here: this is one
controlled copy from Rust's `Vec<u8>` into a Python-owned `bytes` buffer, not
literally zero memory movement. What it avoids is the extra round trip a
naive implementation would pay for — serializing to JSON/UTF-8 text and
parsing it back on the Python side. Handing PyO3 the raw bytes directly and
letting it construct the Python object in one step is the "zero-copy" pattern
PyO3 offers over that alternative.

### GIL release during I/O

Every operation that touches disk — `write()`'s WAL append+sync, `commit()`'s
WAL append+sync — runs inside `py.allow_threads(|| { ... })`. Without this,
Rust would hold the Python GIL for the full duration of a disk sync, freezing
every other Python thread (including, in an async context, the whole event
loop) for as long as the OS takes to flush. `allow_threads` releases the GIL
before entering the closure and re-acquires it after, so other Python work
can proceed while Tether is waiting on disk.

`read()` does not release the GIL — it only touches the in-memory `DashMap`,
which is fast and lock-free, so there's nothing to gain from releasing it and
doing so would just add overhead.

## Error handling

`TetherError` (`rust_core/src/error.rs`) is the single error type for
anything that can go wrong in the Rust core — I/O failures, malformed WAL
records, state-machine misuse. It implements `From<TetherError> for PyErr`,
converting to `PyRuntimeError` with the error's message. Every fallible path
in the crate returns a `Result`/`PyResult`; nothing panics. This matters
specifically at the FFI boundary: a Rust panic crossing into Python is
undefined behavior (in the best case, an ugly abort; in the worst case, a
silent memory-safety violation), so every error has to be a value, not a
panic.

## What isn't built yet

- No compaction/truncation policy — the WAL grows unboundedly. `truncate()`
  exists but only supports dropping the whole log (`up_to == 0`), and nothing
  calls it automatically.
- No cross-process locking on the WAL file itself — two processes pointed at
  the same WAL path would corrupt each other's writes. Tether assumes one
  writer process per WAL file.
- `tether_langgraph.TetherSaver` implements only the synchronous half of
  LangGraph's `BaseCheckpointSaver` interface (`put`, `put_writes`,
  `get_tuple`, `list`); the async variants are unimplemented.
