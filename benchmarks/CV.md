# WorkQueue CV entry

Copy the following project entry:

**WorkQueue — Background Task Queue | Go, Redis, HTTP, Goroutines**

- Built a Redis-backed background task queue with separate HTTP producer and worker services, concurrent processing, task logging, and a metrics endpoint.
- Benchmarked 300,000 enqueue requests with 32 concurrent clients, achieving approximately 17,000 requests/second and p95 response latency below 3 ms in local producer-only tests.
- Demonstrated approximately 2x throughput scaling from 3 to 6 worker goroutines across two processes, reducing completion time for 60 simulated two-second jobs from 40.09 s to 20.06 s.

## Measurement evidence

Measured on 26 October 2026 using the existing application source without functional changes. Hardware: Intel Core i7-12700H, 20 logical CPUs, approximately 16 GB RAM. Software: Windows amd64, Go 1.27.3, Redis 7.4.11 in Docker, and a Node.js HTTP load generator. Redis persistence was disabled.

| Measurement | Result | Method |
| --- | --- | --- |
| Producer enqueue throughput | 17,072.96 requests/s | Median of three 100,000-request trials, 32 concurrent clients |
| Median per-trial p95 enqueue latency | 2.8345 ms | Successful HTTP response latency, median of three trial p95 values |
| Highest trial p95 enqueue latency | 2.9150 ms | All three larger trials had p95 below 3 ms |
| Enqueue verification | 300,000 accepted; zero observed HTTP errors | Response bodies and final Redis queue lengths checked |
| Three worker goroutines, one process | 40.0948 s; 1.4965 completed jobs/s | Median of three 60-job trials |
| Six worker goroutines, two processes | 20.0621 s; 2.9907 completed jobs/s | Median of three 60-job trials |
| Processing speedup | 1.9985x | Ratio of median batch completion times |
| Completion verification | 360 jobs; each expected task ID appeared exactly once | Three 60-job trials per configuration; no missing or duplicate IDs; queue empty after each trial |

The producer test measured acceptance into Redis with workers stopped. It did not measure 17,000 completed jobs per second. The completion test used the existing `send_email` simulation, including its two-second sleep, printing, and file logging. No email was delivered. Increasing the number of worker processes increased the number of goroutines; no application code was modified to change concurrency. These are local benchmark observations, not production capacity or reliability guarantees.

The HTTP generator used a closed loop: each of the 32 clients submitted its next request after receiving a response. Each larger producer trial lasted approximately six seconds. Job completion was checked every 20 ms using actual success log entries rather than the shared application metrics counters.

- [Larger producer results](enqueue-results.json)
- [Initial full benchmark and processing results](results.json)
- [Methodology and reproduction commands](README.md)
- [Benchmark harness](run.cjs)
