# Local benchmarks

The benchmark runs the existing producer and worker binaries without modifying application source code. The `send_email` handler simulates work with a two-second sleep, then prints a message. It does not deliver email.

Two measurements are kept separate:

- **Producer ingestion:** POST requests to `/enqueue`, with 32 concurrent HTTP clients and no running workers. Each successful response is checked and the final Redis queue length must match the accepted request count. This measures jobs accepted into Redis, not jobs completed.
- **Worker processing:** 60 simulated jobs per trial, with either one or two worker processes. Each unmodified worker process starts three goroutines, so the comparison is three versus six concurrent task executors. Completion time includes enqueueing, execution, and success logging. Every expected task ID must appear exactly once in the success logs, and the queue must end empty. Shared application metrics counters are not used to establish completion.

Each configuration runs three times after warmup. Reported summaries use the median of the three trials. Completion is checked every 20 ms. All requests use fictional addresses from `example.com`.

Results describe this local machine and workload. Redis runs in a dedicated container on localhost with persistence disabled. They are not production capacity, reliability, or real email delivery measurements. General hardware and software specifications are recorded; usernames, home paths, hostnames, and device IDs are excluded.

## Reproduce on Windows / PowerShell

Prerequisites: Go, Node.js, and Docker Desktop with Linux containers running. Ports 16379, 18080, 18081, and 18082 must be free. Run from the repository root.

```powershell
docker run --detach --rm --name workqueue-benchmark --publish 127.0.0.1:16379:6379 redis:7.4.11-alpine redis-server --appendonly no

$benchmarkRuntime = Join-Path $env:TEMP 'workqueue-benchmark'
$benchmarkBinaries = Join-Path $benchmarkRuntime 'bin'
New-Item -ItemType Directory -Path $benchmarkBinaries -Force | Out-Null
$env:CGO_ENABLED = '0'
go build -trimpath -o $benchmarkBinaries ./cmd/producer ./cmd/worker

# Full test: 3 x 100,000 enqueue requests and both processing configurations.
node benchmarks/run.cjs --runtime $benchmarkRuntime

# Optional: repeat only the larger producer test.
node benchmarks/run.cjs --runtime $benchmarkRuntime --enqueue-only --enqueue-jobs 100000

docker stop workqueue-benchmark
```

Use this dedicated Redis container only: the harness requires an initially empty database 15, disables snapshot persistence, and clears its `task_queue` between trials. It refuses other hosts, ports, and database numbers. Runtime binaries and logs are stored outside the repository.

`results.json` contains the initial full run with 5,000 enqueue requests per trial and the worker completion tests. `enqueue-results.json` contains the larger follow-up producer measurement with 100,000 requests per trial. To reproduce the initial full run exactly, add `--enqueue-jobs 5000` to the full-test command. The completed measurements and CV wording are summarized in [CV.md](CV.md).
