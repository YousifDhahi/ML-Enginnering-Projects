# Lecture 01 — OOM-Safe Streaming Pipeline with Crash Recovery

## What This Is

A production-grade data pipeline that processes a simulated 50GB Parquet dataset
on a memory-constrained machine (2GB RAM limit) without ever crashing from memory
exhaustion. Built under FinOps constraints with automated cost reporting.

## The Problem It Solves

A naive pipeline loads the entire dataset into RAM — on a 50GB file, this triggers
the Linux OOM Killer, kills the process, and loses all progress. This pipeline
solves three production problems simultaneously:

- Memory exhaustion → streaming chunk-by-chunk (one row group at a time)
- Data loss on crash → checkpoint ledger with fsync-guaranteed writes
- Cost overruns → automated FinOps gate with PASS/FAIL verdict

## Architecture

dataset_generator.py   →   streaming_pipeline.py   →   checkpoint_recovery.py
       ↓                            ↓                            ↓
 Generates 200 chunks        Transforms data             Recovers from crash
 × 250K rows each         (risk score + segment)       without reprocessing
 ZSTD compressed           Arrow-native, no loops       completed chunks
                                                              ↓
                                                      finops_report.py
                                                      Cost gate: PASS/FAIL

## Files

| File | Responsibility |
|---|---|
| `dataset_generator.py` | Generates fake 50GB Parquet dataset (200 row groups × 250K rows) |
| `streaming_pipeline.py` | Streams data chunk by chunk, computes risk score and segment |
| `checkpoint_recovery.py` | Writes one file per chunk, tracks progress, resumes after crash |
| `finops_report.py` | Measures wall time, estimates spot cost, enforces $9.00 budget |

## Key Engineering Decisions

**Why one file per chunk instead of one output file?**
If the process crashes mid-write on a single output file, the entire file becomes
corrupted and unreadable. One file per chunk means a crash destroys at most one
chunk — all previously completed chunks remain valid.

**Why fsync() after every checkpoint write?**
The Linux kernel buffers disk writes in page cache for performance. Without fsync(),
a crash between the Python write() call and the actual disk commit loses the
checkpoint entry — causing silent reprocessing of already-completed work on restart.

**Why 250K rows per chunk?**
Three-way trade-off: small enough to keep peak RAM under 2GB, large enough for
efficient NVMe sequential I/O (~15MB per read), and fine enough checkpoint
granularity that a crash wastes at most one chunk of compute.

**Why PyArrow instead of Pandas?**
Arrow uses contiguous typed memory buffers — the CPU prefetcher can stream cache
lines sequentially. Pandas object dtype columns are heap-allocated Python strings,
causing random DRAM pointer-chasing and L1/L2 cache misses at scale.

## Core Concepts Learned

- Memory hierarchy: L1/L2/L3 cache → DRAM → NVMe and the cost of each miss
- Columnar storage: why Arrow outperforms Pandas at scale
- OOM-safe streaming: processing arbitrarily large files in fixed RAM
- Crash recovery: checkpoint ledger pattern with kernel-level write guarantees
- FinOps: translating wall-clock time into cloud compute cost

## How to Run

```bash
# Step 1 — generate the dataset
python dataset_generator.py

# Step 2 — run the streaming transformation
python streaming_pipeline.py

# Step 3 — run with crash recovery (safe to interrupt and restart)
python checkpoint_recovery.py

# Step 4 — view cost report
python finops_report.py
```

## Infrastructure Context

- Instance: AWS r6i.2xlarge (64GB RAM, NVMe SSD)
- Spot price: $0.045/hour
- Budget ceiling: $9.00 per run
- RAM ceiling: 2GB peak RSS
- Dataset: 200 row groups × 250,000 rows = 50,000,000 rows total
