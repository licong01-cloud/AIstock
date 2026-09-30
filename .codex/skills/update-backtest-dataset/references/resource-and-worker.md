# Worker and resource execution

The worker owns one direct monthly build at a time and supports durable status and cooperative cancellation. It does not launch a source-freeze or source-recheck child.

## Resource policy

CPU, memory, commit headroom, pagefile, swap, WSL availability estimates, predicted disk growth and concurrent model workloads are telemetry only. They cannot cause admission denial, waiting, checkpoint, pressure-rung changes, automatic cancellation or terminal failure.

Actual OOM/process termination, DB/WSL/filesystem errors, timeout or ENOSPC may end the attempt. There is no automatic retry.

## Bounded implementation

- stream SQL rows;
- batch by instrument/month;
- bound writer buffers and logs;
- never keep the full market/history in one DataFrame;
- write only a new candidate-private path;
- cooperate with durable cancellation at batch boundaries.

Batching is an implementation detail, not a data-scope gate.
