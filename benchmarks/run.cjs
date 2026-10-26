// Measures the existing binaries without changing application source code.
// Requires Node.js, producer/worker binaries, and a dedicated local Redis.
const fs = require('node:fs');
const path = require('node:path');
const os = require('node:os');
const net = require('node:net');
const http = require('node:http');
const { spawn, execFileSync } = require('node:child_process');
const { performance } = require('node:perf_hooks');
const assert = require('node:assert/strict');

const args = process.argv.slice(2);
const option = (name, fallback) => args.includes(name) ? args[args.indexOf(name) + 1] : fallback;
const runtime = path.resolve(option('--runtime', path.join(os.tmpdir(), 'workqueue-benchmark')));
const goCommand = option('--go', 'go');
const enqueueOnly = args.includes('--enqueue-only');
const enqueueJobs = Number(option('--enqueue-jobs', '100000'));
assert(Number.isInteger(enqueueJobs) && enqueueJobs > 0 && enqueueJobs <= 1000000, 'Invalid enqueue batch size');
const redisURL = new URL(option('--redis-url', 'redis://127.0.0.1:16379/15'));
assert.equal(redisURL.hostname, '127.0.0.1', 'Use a dedicated localhost Redis');
assert.equal(redisURL.port, '16379', 'Use the isolated benchmark Redis port');
assert.equal(redisURL.pathname, '/15', 'Use benchmark database 15');
const producerPort = 18080;
const workerPorts = [18081, 18082];
const output = path.join(__dirname, enqueueOnly ? 'enqueue-results.json' : 'results.json');
const root = path.resolve(__dirname, '..');
const processes = new Set();
const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));
const agent = new http.Agent({ keepAlive: true, maxSockets: 32 });
let redis;
let runDirectory;

class RedisClient {
  constructor(socket) {
    this.socket = socket;
    this.buffer = Buffer.alloc(0);
    this.pending = [];
    socket.on('data', chunk => {
      this.buffer = Buffer.concat([this.buffer, chunk]);
      while (this.pending.length) {
        const response = this.parse(0);
        if (!response) break;
        this.buffer = this.buffer.subarray(response.end);
        const { resolve, reject } = this.pending.shift();
        response.value instanceof Error ? reject(response.value) : resolve(response.value);
      }
    });
    socket.on('error', error => { for (const item of this.pending.splice(0)) item.reject(error); });
  }
  parse(offset) {
    const end = this.buffer.indexOf('\r\n', offset);
    if (end < 0) return null;
    const kind = String.fromCharCode(this.buffer[offset]);
    const text = this.buffer.toString('utf8', offset + 1, end);
    if (kind === '+' || kind === ':' || kind === '-') {
      return { value: kind === ':' ? Number(text) : kind === '-' ? Error(text) : text, end: end + 2 };
    }
    if (kind === '$') {
      const length = Number(text);
      if (length < 0) return { value: null, end: end + 2 };
      if (this.buffer.length < end + 2 + length + 2) return null;
      return { value: this.buffer.toString('utf8', end + 2, end + 2 + length), end: end + 2 + length + 2 };
    }
    throw Error('Unexpected Redis response type');
  }
  command(...parts) {
    return new Promise((resolve, reject) => {
      this.pending.push({ resolve, reject });
      const encoded = ['*' + parts.length + '\r\n'];
      for (const part of parts) { const value = String(part); encoded.push('$' + Buffer.byteLength(value) + '\r\n' + value + '\r\n'); }
      this.socket.write(encoded.join(''));
    });
  }
  close() { this.socket.destroy(); }
}

async function connectRedis() {
  const socket = await new Promise((resolve, reject) => {
    const socket = net.createConnection({ host: redisURL.hostname, port: Number(redisURL.port) });
    socket.once('connect', () => resolve(socket));
    socket.once('error', reject);
  });
  const client = new RedisClient(socket);
  await client.command('SELECT', 15);
  assert.equal(await client.command('DBSIZE'), 0, 'Benchmark database must initially be empty');
  return client;
}

function request(port, route, task) {
  const data = task ? JSON.stringify(task) : null;
  return new Promise((resolve, reject) => {
    const start = performance.now();
    const req = http.request({ hostname: '127.0.0.1', port, path: route, method: task ? 'POST' : 'GET', agent,
      headers: data ? { 'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(data) } : {} }, res => {
      let body = '';
      res.setEncoding('utf8');
      res.on('data', chunk => { body += chunk; });
      res.on('end', () => resolve({ status: res.statusCode, body, latencyMs: performance.now() - start }));
    });
    req.setTimeout(15000, () => req.destroy(Error('HTTP timeout')));
    req.on('error', reject);
    req.end(data);
  });
}

async function waitUntilReady(port, child) {
  const deadline = performance.now() + 15000;
  while (performance.now() < deadline) {
    if (child.exitCode !== null) throw Error('Application process exited during startup');
    try { await request(port, '/metrics'); return; } catch { await sleep(50); }
  }
  throw Error('Application startup timeout');
}

async function assertPortFree(port) {
  await new Promise((resolve, reject) => {
    const server = net.createServer();
    server.once('error', reject);
    server.listen(port, '0.0.0.0', () => server.close(resolve));
  });
}

async function startApplication(kind, port, directory) {
  fs.mkdirSync(directory, { recursive: true });
  const stdout = fs.openSync(path.join(directory, 'stdout.log'), 'w');
  const stderr = fs.openSync(path.join(directory, 'stderr.log'), 'w');
  const executable = path.join(runtime, 'bin', kind + (process.platform === 'win32' ? '.exe' : ''));
  const child = spawn(executable, [], { cwd: directory, windowsHide: true, env: { ...process.env,
    REDIS_URL: redisURL.toString(), PORT_PRODUCER: String(producerPort), PORT_WORKER: String(port) }, stdio: ['ignore', stdout, stderr] });
  fs.closeSync(stdout); fs.closeSync(stderr);
  child.on('error', error => { child.startError = error; });
  processes.add(child);
  await waitUntilReady(port, child);
  if (child.startError) throw child.startError;
  return child;
}

async function stopApplication(child) {
  if (child.exitCode === null) {
    const exited = new Promise(resolve => child.once('exit', resolve));
    child.kill();
    await exited;
  }
  processes.delete(child);
}

const task = id => ({ type: 'send_email', retries: 0, payload: { id, to: 'recipient@example.com', subject: 'Synthetic benchmark' } });
const percentile = (sorted, fraction) => sorted[Math.max(0, Math.ceil(sorted.length * fraction) - 1)];
const median = numbers => { const sorted = [...numbers].sort((a, b) => a - b); return percentile(sorted, 0.5); };
const round = value => Number(value.toFixed(4));

async function enqueueBatch(count, concurrency, prefix) {
  let next = 0, accepted = 0;
  const latencies = [], errors = [];
  const start = performance.now();
  await Promise.all(Array.from({ length: concurrency }, async () => {
    while (next < count) {
      const index = next++;
      try {
        const response = await request(producerPort, '/enqueue', task(prefix + '-' + index));
        if (response.status === 200 && response.body === "Task of type 'send_email' has been successfully added to the queue") {
          accepted++; latencies.push(response.latencyMs);
        } else errors.push({ index, status: response.status });
      } catch (error) { errors.push({ index, message: error.message }); }
    }
  }));
  const seconds = (performance.now() - start) / 1000;
  const sorted = latencies.sort((a, b) => a - b);
  return { requested: count, accepted, errors: errors.length, errorExamples: errors.slice(0, 5), elapsedSeconds: round(seconds),
    requestsPerSecond: round(accepted / seconds), p50Ms: round(percentile(sorted, 0.5)), p95Ms: round(percentile(sorted, 0.95)),
    p99Ms: round(percentile(sorted, 0.99)), meanMs: round(latencies.reduce((sum, value) => sum + value, 0) / latencies.length) };
}

function completedJobs(directories) {
  const ids = [];
  for (const directory of directories) {
    const file = path.join(directory, 'logs.txt');
    if (!fs.existsSync(file)) continue;
    for (const match of fs.readFileSync(file, 'utf8').matchAll(/SUCCESS: Task type: send_email Task payload: (\{[^\r\n]+\}) Retries left: 0/g)) {
      ids.push(JSON.parse(match[1]).id);
    }
  }
  return ids;
}

async function waitForCompletions(directories, expected, children) {
  const deadline = performance.now() + 90000;
  while (performance.now() < deadline) {
    assert(children.every(child => child.exitCode === null), 'A worker exited unexpectedly');
    const ids = completedJobs(directories);
    if (ids.length >= expected.size) {
      assert.equal(ids.length, expected.size, 'Unexpected completion count');
      assert.deepEqual(new Set(ids), expected, 'Missing, duplicate, or unexpected completed job');
      return ids.length;
    }
    await sleep(20);
  }
  throw Error('Timed out waiting for verified job completions');
}

async function main() {
  for (const port of [producerPort, ...workerPorts]) await assertPortFree(port);
  fs.mkdirSync(runtime, { recursive: true });
  runDirectory = fs.mkdtempSync(path.join(runtime, 'trial-'));
  redis = await connectRedis();
  await redis.command('CONFIG', 'SET', 'save', '');
  const info = await redis.command('INFO', 'server');
  const result = { schemaVersion: 1, measuredAtUTC: new Date().toISOString(),
    sourceCommit: execFileSync('git', ['rev-parse', 'HEAD'], { cwd: root, encoding: 'utf8' }).trim(),
    environment: { os: process.platform, architecture: process.arch, cpu: os.cpus()[0].model, logicalCPUs: os.cpus().length,
      memoryGiB: round(os.totalmem() / 2 ** 30), goVersion: execFileSync(goCommand, ['version'], { encoding: 'utf8' }).trim(),
      nodeVersion: process.version, redisVersion: info.match(/^redis_version:(.+)$/m)[1].trim(),
      redisPersistence: 'disabled', topology: 'Native Go processes; dedicated Docker Redis on localhost',
      applicationSourceModified: false, completionVerification: 'Unique task IDs in actual success log entries; not shared metrics counters' },
    producerEnqueue: { jobsPerRun: enqueueJobs, concurrency: 32, repetitions: 3, warmupRequests: 200, runningWorkerProcesses: 0, runs: [] },
    workerProcessing: { taskType: 'send_email', workload: 'Simulated 2-second delay plus printing and file logging; no emails sent',
      jobsPerRun: 60, repetitions: 3, enqueueConcurrency: 16, goroutinesPerProcess: 3, completionPollIntervalMs: 20, configurations: [] } };
  const producer = await startApplication('producer', producerPort, path.join(runDirectory, 'producer'));
  await enqueueBatch(200, 16, 'producer-warmup');
  await redis.command('DEL', 'task_queue');
  for (let run = 1; run <= 3; run++) {
    const measurement = await enqueueBatch(enqueueJobs, 32, 'enqueue-' + run);
    measurement.queueLengthVerified = await redis.command('LLEN', 'task_queue');
    assert.equal(measurement.errors, 0); assert.equal(measurement.accepted, enqueueJobs); assert.equal(measurement.queueLengthVerified, enqueueJobs);
    result.producerEnqueue.runs.push(measurement);
    await redis.command('DEL', 'task_queue');
    console.log(JSON.stringify({ phase: 'enqueue', run, ...measurement }));
  }
  result.producerEnqueue.medianRequestsPerSecond = round(median(result.producerEnqueue.runs.map(run => run.requestsPerSecond)));
  result.producerEnqueue.medianP95Ms = round(median(result.producerEnqueue.runs.map(run => run.p95Ms)));
  if (enqueueOnly) {
    delete result.workerProcessing;
    fs.writeFileSync(output, JSON.stringify(result, null, 2) + '\n');
    await stopApplication(producer);
    console.log(JSON.stringify({ phase: 'complete', enqueueRequestsPerSecond: result.producerEnqueue.medianRequestsPerSecond, enqueueP95Ms: result.producerEnqueue.medianP95Ms }));
    return;
  }
  for (const processCount of [1, 2]) {
    const configuration = { workerProcesses: processCount, totalWorkerGoroutines: processCount * 3, runs: [] };
    result.workerProcessing.configurations.push(configuration);
    for (let run = 1; run <= 3; run++) {
      const directories = [], workers = [];
      for (let worker = 0; worker < processCount; worker++) {
        const directory = path.join(runDirectory, 'processing-' + processCount + '-' + run + '-' + worker);
        directories.push(directory);
        workers.push(await startApplication('worker', workerPorts[worker], directory));
      }
      const warmupCount = 3 * processCount, warmupPrefix = 'warmup-' + processCount + '-' + run;
      await enqueueBatch(warmupCount, warmupCount, warmupPrefix);
      await waitForCompletions(directories, new Set(Array.from({ length: warmupCount }, (_, index) => warmupPrefix + '-' + index)), workers);
      for (const directory of directories) fs.writeFileSync(path.join(directory, 'logs.txt'), '');
      const prefix = 'processing-' + processCount + '-' + run;
      const start = performance.now();
      const enqueue = await enqueueBatch(60, 16, prefix);
      assert.equal(enqueue.errors, 0); assert.equal(enqueue.accepted, 60);
      const completed = await waitForCompletions(directories, new Set(Array.from({ length: 60 }, (_, index) => prefix + '-' + index)), workers);
      const seconds = (performance.now() - start) / 1000;
      const measurement = { elapsedSeconds: round(seconds), completedJobs: completed, uniqueCompletedJobs: completed, duplicateCompletions: 0,
        acceptedJobs: enqueue.accepted, httpErrors: enqueue.errors, queueLengthAtEnd: await redis.command('LLEN', 'task_queue'), jobsPerSecond: round(completed / seconds) };
      assert.equal(measurement.queueLengthAtEnd, 0);
      configuration.runs.push(measurement);
      for (const worker of workers) await stopApplication(worker);
      console.log(JSON.stringify({ phase: 'processing', workerProcesses: processCount, run, ...measurement }));
      fs.writeFileSync(output, JSON.stringify(result, null, 2) + '\n');
    }
    configuration.medianElapsedSeconds = round(median(configuration.runs.map(run => run.elapsedSeconds)));
    configuration.medianJobsPerSecond = round(median(configuration.runs.map(run => run.jobsPerSecond)));
  }
  result.producerEnqueue.medianRequestsPerSecond = round(median(result.producerEnqueue.runs.map(run => run.requestsPerSecond)));
  result.producerEnqueue.medianP95Ms = round(median(result.producerEnqueue.runs.map(run => run.p95Ms)));
  const [baseline, scaled] = result.workerProcessing.configurations;
  result.workerProcessing.speedupTwoVersusOneProcess = round(baseline.medianElapsedSeconds / scaled.medianElapsedSeconds);
  fs.writeFileSync(output, JSON.stringify(result, null, 2) + '\n');
  await stopApplication(producer);
  console.log(JSON.stringify({ phase: 'complete', enqueueRequestsPerSecond: result.producerEnqueue.medianRequestsPerSecond,
    enqueueP95Ms: result.producerEnqueue.medianP95Ms, processing: result.workerProcessing.configurations,
    speedup: result.workerProcessing.speedupTwoVersusOneProcess }));
}

main().catch(error => { console.error(error.stack); process.exitCode = 1; }).finally(async () => {
  for (const child of [...processes]) await stopApplication(child);
  if (redis) { try { await redis.command('DEL', 'task_queue'); } finally { redis.close(); } }
  agent.destroy();
});
