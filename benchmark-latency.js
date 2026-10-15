const http = require('http');
const { performance } = require('perf_hooks');

function request(method, path, body = null) {
  return new Promise((resolve, reject) => {
    const data = body ? JSON.stringify(body) : null;
    const req = http.request(
      {
        hostname: 'localhost',
        port: 3000,
        path,
        method,
        headers: {
          'Content-Type': 'application/json',
          ...(data ? { 'Content-Length': Buffer.byteLength(data) } : {})
        }
      },
      res => {
        let raw = '';
        res.on('data', chunk => (raw += chunk));
        res.on('end', () => {
          try {
            resolve({ statusCode: res.statusCode, body: JSON.parse(raw) });
          } catch {
            resolve({ statusCode: res.statusCode, body: raw });
          }
        });
      }
    );
    req.on('error', reject);
    if (data) req.write(data);
    req.end();
  });
}

async function measureSingleWorkflow(failCredit = false) {
  const start = performance.now();
  const post = await request('POST', '/workflows', {
    customerId: `C-BENCH-${Date.now()}`,
    failCredit,
    failVerification: false
  });
  const id = post.body.workflow.id;

  // High-frequency polling to measure true Kafka + DB transition latency
  while (true) {
    await new Promise(r => setTimeout(r, 15));
    const get = await request('GET', `/workflows/${id}`);
    const status = get.body.workflow?.status;
    if (status === 'COMPLETED' || status === 'FAILED_COMPENSATED' || status === 'FAILED') {
      const elapsed = performance.now() - start;
      return { id, status, elapsed };
    }
  }
}

function calculateStats(latencies) {
  const sorted = [...latencies].sort((a, b) => a - b);
  const min = sorted[0];
  const max = sorted[sorted.length - 1];
  const sum = sorted.reduce((acc, val) => acc + val, 0);
  const avg = sum / sorted.length;
  const p50 = sorted[Math.floor(sorted.length * 0.5)];
  const p90 = sorted[Math.floor(sorted.length * 0.9)];
  const p95 = sorted[Math.floor(sorted.length * 0.95)];

  return { min, max, avg, p50, p90, p95 };
}

async function runBenchmark() {
  console.log('========================================================');
  console.log('       FLOWMASTER END-TO-END LATENCY BENCHMARK          ');
  console.log('========================================================\n');

  // Warmup run
  console.log('Running warmup workflow...');
  await measureSingleWorkflow(false);
  console.log('Warmup complete.\n');

  // Benchmark 1: Normal workflows (Happy path -> COMPLETED)
  const NORMAL_ITERATIONS = 15;
  console.log(`Running ${NORMAL_ITERATIONS} Happy Path workflows...`);
  const normalLatencies = [];

  for (let i = 1; i <= NORMAL_ITERATIONS; i++) {
    const res = await measureSingleWorkflow(false);
    normalLatencies.push(res.elapsed);
    process.stdout.write(`  [${i}/${NORMAL_ITERATIONS}] ${res.elapsed.toFixed(1)}ms (${res.status})\n`);
  }

  const normalStats = calculateStats(normalLatencies);

  // Benchmark 2: Compensation workflows (failCredit = true -> FAILED_COMPENSATED)
  const COMP_ITERATIONS = 10;
  console.log(`\nRunning ${COMP_ITERATIONS} Saga Compensation workflows (failCredit: true)...`);
  const compLatencies = [];

  for (let i = 1; i <= COMP_ITERATIONS; i++) {
    const res = await measureSingleWorkflow(true);
    compLatencies.push(res.elapsed);
    process.stdout.write(`  [${i}/${COMP_ITERATIONS}] ${res.elapsed.toFixed(1)}ms (${res.status})\n`);
  }

  const compStats = calculateStats(compLatencies);

  console.log('\n========================================================');
  console.log('                BENCHMARK MEASUREMENT RESULTS           ');
  console.log('========================================================');
  console.log('1. Happy Path Workflow (Verification -> Credit Check -> Completed):');
  console.log(`   - Sample size:        ${NORMAL_ITERATIONS} runs`);
  console.log(`   - Min Latency:        ${normalStats.min.toFixed(2)} ms`);
  console.log(`   - Max Latency:        ${normalStats.max.toFixed(2)} ms`);
  console.log(`   - Average Latency:    ${normalStats.avg.toFixed(2)} ms`);
  console.log(`   - Median (P50):       ${normalStats.p50.toFixed(2)} ms`);
  console.log(`   - P90 Latency:        ${normalStats.p90.toFixed(2)} ms`);
  console.log(`   - P95 Latency:        ${normalStats.p95.toFixed(2)} ms`);
  console.log('\n2. Saga Compensation Workflow (Verification -> Credit Reject -> Compensation -> Failed_Compensated):');
  console.log(`   - Sample size:        ${COMP_ITERATIONS} runs`);
  console.log(`   - Min Latency:        ${compStats.min.toFixed(2)} ms`);
  console.log(`   - Max Latency:        ${compStats.max.toFixed(2)} ms`);
  console.log(`   - Average Latency:    ${compStats.avg.toFixed(2)} ms`);
  console.log(`   - Median (P50):       ${compStats.p50.toFixed(2)} ms`);
  console.log(`   - P90 Latency:        ${compStats.p90.toFixed(2)} ms`);
  console.log(`   - P95 Latency:        ${compStats.p95.toFixed(2)} ms`);
  console.log('========================================================\n');
}

runBenchmark().catch(err => {
  console.error('Benchmark failed:', err);
  process.exit(1);
});
