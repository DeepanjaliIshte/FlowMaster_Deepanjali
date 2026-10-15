const http = require('http');
const { execSync } = require('child_process');

function request(method, path) {
  return new Promise((resolve, reject) => {
    const req = http.request(
      {
        hostname: 'localhost',
        port: 3000,
        path,
        method,
        headers: { 'Content-Type': 'application/json' }
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
    req.end();
  });
}

async function main() {
  console.log('====================================================');
  console.log('       ORCHESTRATOR CRASH & RECOVERY TEST           ');
  console.log('====================================================\n');

  // Step 1: Insert interrupted workflows directly into PostgreSQL
  console.log('1. Seeding interrupted workflows in PostgreSQL...');
  const sql = `
    DELETE FROM workflow_events WHERE workflow_id IN ('rec-running-1', 'rec-comp-1');
    DELETE FROM workflows WHERE id IN ('rec-running-1', 'rec-comp-1');
    INSERT INTO workflows(id, status, current_step, input) VALUES
      ('rec-running-1', 'RUNNING', 'verification', '{"customerId":"C-REC-1","failCredit":false}'),
      ('rec-comp-1', 'COMPENSATING', 'verification', '{"customerId":"C-REC-2","failCredit":true}');
    INSERT INTO workflow_events(event_key, workflow_id, event_type, payload) VALUES
      ('rec-running-1:created', 'rec-running-1', 'WORKFLOW_CREATED', '{"workflowId":"rec-running-1"}'),
      ('rec-comp-1:created', 'rec-comp-1', 'WORKFLOW_CREATED', '{"workflowId":"rec-comp-1"}');
  `;
  execSync('docker exec -i flowmaster-postgres psql -U flowmaster -d flowmaster', { input: sql, stdio: ['pipe', 'pipe', 'inherit'] });
  console.log('Seeded workflows: rec-running-1 (RUNNING) and rec-comp-1 (COMPENSATING)');

  // Step 2: Restart orchestrator container to trigger recovery
  console.log('\n2. Restarting orchestrator container to simulate crash & reboot...');
  execSync('docker restart flowmaster_deepanjali-orchestrator-1');
  console.log('Orchestrator restarted.');

  // Wait for orchestrator to boot up and run recovery
  console.log('\n3. Waiting for orchestrator to complete recovery...');
  let runningFinal = null;
  let compFinal = null;

  for (let attempt = 1; attempt <= 45; attempt++) {
    await new Promise(r => setTimeout(r, 1000));
    try {
      const res1 = await request('GET', '/workflows/rec-running-1');
      const res2 = await request('GET', '/workflows/rec-comp-1');

      if (res1.statusCode === 200 && res1.body.workflow) {
        if (['COMPLETED', 'FAILED_COMPENSATED'].includes(res1.body.workflow.status)) {
          runningFinal = res1.body;
        }
      }
      if (res2.statusCode === 200 && res2.body.workflow) {
        if (['COMPLETED', 'FAILED_COMPENSATED'].includes(res2.body.workflow.status)) {
          compFinal = res2.body;
        }
      }

      if (runningFinal && compFinal) break;
    } catch (err) {
      // Orchestrator might still be booting up
    }
  }

  if (!runningFinal || !compFinal) {
    throw new Error('Recovery timeout: workflows did not reach expected terminal states.');
  }

  console.log('\n--- RESULTS ---');
  console.log(`Workflow 'rec-running-1' recovered status: ${runningFinal.workflow.status} (step: ${runningFinal.workflow.current_step})`);
  console.log('Event trail for rec-running-1:', runningFinal.events.map(e => e.event_type).join(' -> '));

  console.log(`\nWorkflow 'rec-comp-1' recovered status: ${compFinal.workflow.status} (step: ${compFinal.workflow.current_step})`);
  console.log('Event trail for rec-comp-1:', compFinal.events.map(e => e.event_type).join(' -> '));

  if (runningFinal.workflow.status !== 'COMPLETED') {
    throw new Error(`Expected rec-running-1 to reach COMPLETED, got ${runningFinal.workflow.status}`);
  }
  if (compFinal.workflow.status !== 'FAILED_COMPENSATED') {
    throw new Error(`Expected rec-comp-1 to reach FAILED_COMPENSATED, got ${compFinal.workflow.status}`);
  }

  console.log('\n====================================================');
  console.log('       CRASH & RECOVERY TEST PASSED SUCCESSFULLY!   ');
  console.log('====================================================');
}

main().catch(err => {
  console.error('\nXXX RECOVERY TEST FAILED:', err);
  process.exit(1);
});
