# Benchmarks

Status: **no results yet.** This file ships the comparison script and the
methodology; running it against LangGraph's SQLite checkpointer and
publishing real numbers is still open work. Anything below is a placeholder
for a real run, not a claim.

## Running the comparison

```bash
pip install langgraph-checkpoint-sqlite
python benchmarks/compare_checkpointers.py --payload-kb 100 --iterations 50
```

[`benchmarks/compare_checkpointers.py`](benchmarks/compare_checkpointers.py)
puts and reads a series of checkpoints through `TetherSaver` and, if
`langgraph-checkpoint-sqlite` is installed, through LangGraph's default
`SqliteSaver`, timing both and tracking peak memory via `tracemalloc`. The
Tether half has been run manually during development (10KB payload, 5
iterations) and produces real numbers; the SQLite comparison has not been run
in this environment, since `langgraph-checkpoint-sqlite` isn't installed
here.

## What to measure

- **Checkpoint write+read latency** at a few payload sizes (1KB, 100KB,
  1MB — LLM contexts skew large) — this is what `compare_checkpointers.py`
  reports today.
- **Peak memory** during a checkpoint cycle, via `tracemalloc` — also
  reported today.
- **Recovery time**: time from process start to a resumed workflow reaching
  its first un-completed step, at increasing WAL sizes. Not yet scripted.

## Flamegraph

_Placeholder — not yet generated._ To produce one: profile
`benchmarks/compare_checkpointers.py` with `cargo flamegraph` (for the Rust
side, via a small Rust-only benchmark harness) or `py-spy record` (for the
end-to-end Python path), and drop the resulting SVG here.

## Memory allocation graph

_Placeholder — not yet generated._ `tracemalloc.take_snapshot()` at intervals
during a long-running benchmark, plotted with matplotlib, would show whether
memory stays flat (expected, given the WAL streams to disk rather than
accumulating in memory) or grows with iteration count.
