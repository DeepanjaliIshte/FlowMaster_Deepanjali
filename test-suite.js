const http = require('http');

function request(method, path, body = null, headers = {}) {
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
          ...(data ? { 'Content-Length': Buffer.byteLength(data) } : {}),
          ...headers
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

async function pollWorkflow(id, maxAttempts = 15) {
  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    await new Promise(r => setTimeout(r, 1000));
    const res = await request('GET', `/workflows/${id}`);
    const wf = res.body.workflow;
    if (['COMPLETED', 'FAILED', 'FAILED_COMPENSATED'].includes(wf.status)) {
      return res.body;
    }
  }
  throw new Error(`Timeout waiting for workflow ${id}`);
}

async function main() {
  console.log('====================================================');
  console.log('       FLOWMASTER COMPREHENSIVE TEST SUITE          ');
  console.log('====================================================\n');

  // 1. Health check
  console.log('--- TEST 1: Health Check ---');
  const health = await request('GET', '/health');
  console.log('Status code:', health.statusCode);
  console.log('Health response:', JSON.stringify(health.body));
  if (health.statusCode !== 200 || health.body.status !== 'ok') {
    throw new Error('Health check failed');
  }
  console.log('>>> TEST 1 PASSED\n');

  // 2. Happy Path
  console.log('--- TEST 2: Normal Successful Workflow ---');
  const happySubmit = await request('POST', '/workflows', {
    customerId: 'C-NORMAL-1',
    failCredit: false,
    failVerification: false
  });
  const happyId = happySubmit.body.workflow.id;
  console.log(`Submitted workflow: ${happyId}`);
  const happyFinal = await pollWorkflow(happyId);
  console.log(`Workflow status: ${happyFinal.workflow.status}, step: ${happyFinal.workflow.current_step}`);
  console.log('Event history count:', happyFinal.events.length);
  const happyEvents = happyFinal.events.map(e => e.event_type);
  console.log('Event types:', happyEvents.join(' -> '));

  if (happyFinal.workflow.status !== 'COMPLETED') {
    throw new Error(`Expected COMPLETED, got ${happyFinal.workflow.status}`);
  }
  console.log('>>> TEST 2 PASSED (Happy path reached COMPLETED)\n');

  // 3. Saga Compensation (failCredit = true)
  console.log('--- TEST 3: Saga Compensation (failCredit = true) ---');
  const compSubmit = await request('POST', '/workflows', {
    customerId: 'C-FAIL-CREDIT',
    failCredit: true,
    failVerification: false
  });
  const compId = compSubmit.body.workflow.id;
  console.log(`Submitted workflow: ${compId}`);
  const compFinal = await pollWorkflow(compId);
  console.log(`Workflow status: ${compFinal.workflow.status}, step: ${compFinal.workflow.current_step}`);
  console.log('Event history count:', compFinal.events.length);
  const compEvents = compFinal.events.map(e => e.event_type);
  console.log('Event types:', compEvents.join(' -> '));

  if (compFinal.workflow.status !== 'FAILED_COMPENSATED') {
    throw new Error(`Expected FAILED_COMPENSATED, got ${compFinal.workflow.status}`);
  }
  if (!compEvents.includes('COMPENSATION_SENT') || !compEvents.includes('COMPENSATION_COMPLETED')) {
    throw new Error('Missing compensation events in event trail');
  }
  console.log('>>> TEST 3 PASSED (Compensation confirmed and status is FAILED_COMPENSATED)\n');

  // 4. Early Failure (failVerification = true)
  console.log('--- TEST 4: Early Failure (failVerification = true) ---');
  const failVerifSubmit = await request('POST', '/workflows', {
    customerId: 'C-FAIL-VERIF',
    failCredit: false,
    failVerification: true
  });
  const failVerifId = failVerifSubmit.body.workflow.id;
  console.log(`Submitted workflow: ${failVerifId}`);
  const failVerifFinal = await pollWorkflow(failVerifId);
  console.log(`Workflow status: ${failVerifFinal.workflow.status}, step: ${failVerifFinal.workflow.current_step}`);
  const failEvents = failVerifFinal.events.map(e => e.event_type);
  console.log('Event types:', failEvents.join(' -> '));

  if (failVerifFinal.workflow.status !== 'FAILED') {
    throw new Error(`Expected FAILED, got ${failVerifFinal.workflow.status}`);
  }
  if (failEvents.includes('CREDIT_CHECK_COMMAND_SENT')) {
    throw new Error('Credit check should NOT be invoked if verification failed');
  }
  console.log('>>> TEST 4 PASSED (Verification failed directly, credit not invoked)\n');

  // 5. HTTP Idempotency Key
  console.log('--- TEST 5: HTTP Idempotency Key Header ---');
  const customKey = 'idemp-test-' + Date.now();
  const firstReq = await request(
    'POST',
    '/workflows',
    { customerId: 'C-IDEMP', failCredit: false },
    { 'Idempotency-Key': customKey }
  );
  console.log('First request status:', firstReq.statusCode, 'id:', firstReq.body.workflow.id);

  const secondReq = await request(
    'POST',
    '/workflows',
    { customerId: 'C-IDEMP', failCredit: false },
    { 'Idempotency-Key': customKey }
  );
  console.log('Second request status:', secondReq.statusCode, 'reused:', secondReq.body.reused, 'id:', secondReq.body.workflow.id);

  if (secondReq.body.workflow.id !== firstReq.body.workflow.id || secondReq.body.reused !== true) {
    throw new Error('Idempotency key failed to return existing workflow');
  }
  console.log('>>> TEST 5 PASSED (Duplicate request safely returned existing workflow)\n');

  console.log('====================================================');
  console.log('       ALL TEST SUITE SCENARIOS PASSED!             ');
  console.log('====================================================');
}

main().catch(err => {
  console.error('\nXXX TEST FAILED:', err);
  process.exit(1);
});
