const express = require('express');
const { Kafka } = require('kafkajs');
const { Pool } = require('pg');
const fs = require('fs');
const path = require('path');
const YAML = require('yaml');
const crypto = require('crypto');

const PORT = Number(process.env.PORT || 3000);
const brokers = (process.env.KAFKA_BROKERS || 'localhost:9092').split(',');
const pool = new Pool({ connectionString: process.env.DATABASE_URL });
const kafka = new Kafka({ clientId: 'flowmaster-orchestrator', brokers });
const producer = kafka.producer();
const consumer = kafka.consumer({ groupId: 'flowmaster-orchestrator-results-v1' });

// Robust relative path fallback for workflow.yaml
const workflowPath = process.env.WORKFLOW_FILE || path.join(__dirname, '../../../config/workflow.yaml');
let workflowDefinition;
try {
  workflowDefinition = YAML.parse(fs.readFileSync(workflowPath, 'utf8'));
} catch (err) {
  console.error(`[orchestrator] Failed to read workflow file at ${workflowPath}:`, err);
  process.exit(1);
}

const steps = workflowDefinition.steps || [];
if (steps.length === 0) {
  console.error('[orchestrator] No steps defined in workflow configuration!');
  process.exit(1);
}

// Helpers to work with YAML workflow configuration dynamically
function getStepById(stepId) {
  return steps.find(s => s.id === stepId);
}

function getStepByResultTopic(topic) {
  return steps.find(s => s.resultTopic === topic);
}

function getNextStep(currentStepId) {
  const idx = steps.findIndex(s => s.id === currentStepId);
  if (idx >= 0 && idx + 1 < steps.length) {
    return steps[idx + 1];
  }
  return null;
}

function getPreviousCompensableSteps(failedStepId) {
  const idx = steps.findIndex(s => s.id === failedStepId);
  if (idx <= 0) return [];
  // Return completed previous steps that have a compensationTopic in reverse order
  return steps.slice(0, idx).reverse().filter(s => !!s.compensationTopic);
}

const app = express();
app.use(express.json());

async function initDb() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS workflows (
      id TEXT PRIMARY KEY,
      status TEXT NOT NULL,
      current_step TEXT NOT NULL,
      input JSONB NOT NULL,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    CREATE TABLE IF NOT EXISTS workflow_events (
      id BIGSERIAL PRIMARY KEY,
      event_key TEXT UNIQUE NOT NULL,
      workflow_id TEXT NOT NULL REFERENCES workflows(id),
      event_type TEXT NOT NULL,
      payload JSONB NOT NULL,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
  `);
}

async function ensureKafkaTopics() {
  const admin = kafka.admin();
  try {
    await admin.connect();
    const existingTopics = await admin.listTopics();
    const allTopics = new Set();

    for (const s of steps) {
      if (s.commandTopic) allTopics.add(s.commandTopic);
      if (s.resultTopic) allTopics.add(s.resultTopic);
      if (s.compensationTopic) allTopics.add(s.compensationTopic);
    }

    const topicsToCreate = Array.from(allTopics)
      .filter(topic => !existingTopics.includes(topic))
      .map(topic => ({ topic, numPartitions: 3, replicationFactor: 1 }));

    if (topicsToCreate.length > 0) {
      console.log(`[orchestrator] Pre-creating Kafka topics: ${topicsToCreate.map(t => t.topic).join(', ')}`);
      await admin.createTopics({ topics: topicsToCreate });
    }
  } catch (err) {
    console.warn('[orchestrator] Kafka topic initialization warning (broker might auto-create):', err.message);
  } finally {
    await admin.disconnect().catch(() => {});
  }
}

async function recordEvent(workflowId, eventType, payload, eventKey) {
  await pool.query(
    `INSERT INTO workflow_events(event_key, workflow_id, event_type, payload)
     VALUES($1,$2,$3,$4) ON CONFLICT(event_key) DO NOTHING`,
    [eventKey, workflowId, eventType, payload]
  );
}

async function updateWorkflow(id, status, step) {
  await pool.query(
    'UPDATE workflows SET status=$2, current_step=$3, updated_at=NOW() WHERE id=$1',
    [id, status, step]
  );
}

async function publish(topic, payload) {
  await producer.send({
    topic,
    messages: [{ key: payload.workflowId, value: JSON.stringify(payload) }]
  });
}

async function dispatchStep(wf, step) {
  const command = { workflowId: wf.id, input: wf.input, eventId: crypto.randomUUID() };
  const eventKey = `${wf.id}:${step.id}:command`;
  await recordEvent(wf.id, `${step.id.toUpperCase()}_COMMAND_SENT`, command, eventKey);
  await updateWorkflow(wf.id, 'RUNNING', step.id);
  console.log(`[orchestrator] Workflow ${wf.id}: dispatched step '${step.id}' to topic '${step.commandTopic}'`);
  await publish(step.commandTopic, command);
}

async function handleResult(topic, payload) {
  if (!payload || !payload.workflowId) return;
  const { workflowId, success, reason, compensation } = payload;

  const found = await pool.query('SELECT * FROM workflows WHERE id=$1', [workflowId]);
  if (!found.rowCount) return;
  const wf = found.rows[0];

  // Ignore results if workflow is already in a terminal state
  if (['COMPLETED', 'FAILED_COMPENSATED', 'FAILED'].includes(wf.status)) {
    return;
  }

  // 1. Check if this is a compensation acknowledgement (e.g. from verification service)
  if (compensation) {
    console.log(`[orchestrator] Workflow ${workflowId}: compensation acknowledged by service`);
    const compEventKey = `${workflowId}:compensation:completed`;
    await recordEvent(workflowId, 'COMPENSATION_COMPLETED', payload, compEventKey);
    await updateWorkflow(workflowId, 'FAILED_COMPENSATED', 'compensated');
    console.log(`[orchestrator] Workflow ${workflowId}: moved to terminal status 'FAILED_COMPENSATED'`);
    return;
  }

  // 2. Normal forward step result
  const step = getStepByResultTopic(topic);
  if (!step) {
    console.warn(`[orchestrator] Received result for unrecognized topic: ${topic}`);
    return;
  }

  const resultKey = `${workflowId}:${step.id}:result`;
  await recordEvent(workflowId, `${step.id.toUpperCase()}_RESULT`, payload, resultKey);

  if (!success) {
    console.log(`[orchestrator] Workflow ${workflowId}: step '${step.id}' failed (${reason || 'rejected'})`);
    const compensableSteps = getPreviousCompensableSteps(step.id);

    if (compensableSteps.length > 0) {
      // Step failed after prior steps succeeded -> trigger Saga compensation
      console.log(`[orchestrator] Workflow ${workflowId}: entering 'COMPENSATING' state`);
      await updateWorkflow(workflowId, 'COMPENSATING', step.id);

      for (const prev of compensableSteps) {
        const compPayload = {
          workflowId,
          failedStep: step.id,
          reason: reason || `${step.id} failed`,
          eventId: crypto.randomUUID()
        };
        const compSentKey = `${workflowId}:${prev.id}:compensation:sent`;
        await recordEvent(workflowId, 'COMPENSATION_SENT', compPayload, compSentKey);
        console.log(`[orchestrator] Workflow ${workflowId}: publishing compensation command to '${prev.compensationTopic}'`);
        await publish(prev.compensationTopic, compPayload);
      }
    } else {
      // First step failed or no prior compensations required -> mark FAILED directly
      console.log(`[orchestrator] Workflow ${workflowId}: no prior steps to compensate. Status marked 'FAILED'`);
      await updateWorkflow(workflowId, 'FAILED', step.id);
    }
    return;
  }

  // 3. Step succeeded -> advance to next step or complete workflow
  console.log(`[orchestrator] Workflow ${workflowId}: step '${step.id}' succeeded`);
  const nextStep = getNextStep(step.id);

  if (nextStep) {
    // Durable idempotency guard: do not re-send if next step command was already recorded
    const sent = await pool.query(
      'SELECT 1 FROM workflow_events WHERE event_key=$1',
      [`${workflowId}:${nextStep.id}:command`]
    );
    if (sent.rowCount) return;

    await dispatchStep(wf, nextStep);
  } else {
    // No more steps defined in YAML -> Workflow is complete
    console.log(`[orchestrator] Workflow ${workflowId}: all steps finished. Status marked 'COMPLETED'`);
    await updateWorkflow(workflowId, 'COMPLETED', 'done');
    await recordEvent(workflowId, 'WORKFLOW_COMPLETED', { workflowId }, `${workflowId}:completed`);
  }
}

// REST API Endpoints
app.get('/health', (_req, res) => {
  res.json({
    status: 'ok',
    service: 'orchestrator',
    workflow: workflowDefinition.name,
    steps: steps.map(s => s.id)
  });
});

app.post('/workflows', async (req, res) => {
  try {
    const id = req.header('Idempotency-Key') || crypto.randomUUID();
    const existing = await pool.query('SELECT * FROM workflows WHERE id=$1', [id]);
    if (existing.rowCount) {
      console.log(`[orchestrator] POST /workflows: Idempotent replay for workflow ${id}`);
      return res.status(200).json({ workflow: existing.rows[0], reused: true });
    }

    const input = req.body || {};
    const firstStep = steps[0];

    await pool.query(
      `INSERT INTO workflows(id, status, current_step, input) VALUES($1, 'PENDING', $2, $3)`,
      [id, firstStep.id, input]
    );
    await recordEvent(id, 'WORKFLOW_CREATED', { workflowId: id, input }, `${id}:created`);
    console.log(`[orchestrator] Created workflow ${id} with input:`, JSON.stringify(input));

    await dispatchStep({ id, input }, firstStep);
    const result = await pool.query('SELECT * FROM workflows WHERE id=$1', [id]);
    res.status(202).json({ workflow: result.rows[0] });
  } catch (err) {
    console.error('[orchestrator] POST /workflows failed:', err);
    res.status(500).json({ error: 'Could not start workflow' });
  }
});

app.get('/workflows/:id', async (req, res) => {
  try {
    const result = await pool.query('SELECT * FROM workflows WHERE id=$1', [req.params.id]);
    if (!result.rowCount) return res.status(404).json({ error: 'Workflow not found' });
    const events = await pool.query(
      'SELECT event_type, payload, created_at FROM workflow_events WHERE workflow_id=$1 ORDER BY id ASC',
      [req.params.id]
    );
    res.json({ workflow: result.rows[0], events: events.rows });
  } catch (err) {
    console.error(`[orchestrator] GET /workflows/${req.params.id} failed:`, err);
    res.status(500).json({ error: 'Could not read workflow' });
  }
});

app.get('/workflows/:id/replay', async (req, res) => {
  try {
    const result = await pool.query('SELECT * FROM workflows WHERE id=$1', [req.params.id]);
    if (!result.rowCount) return res.status(404).json({ error: 'Workflow not found' });
    const currentWf = result.rows[0];

    const events = await pool.query(
      'SELECT id, event_type, payload, created_at FROM workflow_events WHERE workflow_id=$1 ORDER BY id ASC',
      [req.params.id]
    );

    // Event Sourcing Replay: Reconstruct state step-by-step purely from the audit trail
    let reconstructedState = {
      id: req.params.id,
      status: 'UNKNOWN',
      current_step: 'none',
      input: currentWf.input,
      replayHistory: []
    };

    for (const evt of events.rows) {
      const prevStatus = reconstructedState.status;
      switch (evt.event_type) {
        case 'WORKFLOW_CREATED':
          reconstructedState.status = 'PENDING';
          reconstructedState.current_step = steps[0].id;
          break;
        case 'VERIFICATION_COMMAND_SENT':
          reconstructedState.status = 'RUNNING';
          reconstructedState.current_step = 'verification';
          break;
        case 'VERIFICATION_RESULT':
          if (evt.payload.success) {
            reconstructedState.status = 'RUNNING';
          } else {
            reconstructedState.status = 'FAILED';
            reconstructedState.current_step = 'verification';
          }
          break;
        case 'CREDIT_CHECK_COMMAND_SENT':
          reconstructedState.status = 'RUNNING';
          reconstructedState.current_step = 'credit_check';
          break;
        case 'CREDIT_CHECK_RESULT':
          if (evt.payload.success) {
            reconstructedState.status = 'COMPLETED';
            reconstructedState.current_step = 'done';
          } else {
            reconstructedState.status = 'COMPENSATING';
            reconstructedState.current_step = 'credit_check';
          }
          break;
        case 'COMPENSATION_SENT':
          reconstructedState.status = 'COMPENSATING';
          reconstructedState.current_step = 'verification';
          break;
        case 'COMPENSATION_COMPLETED':
          reconstructedState.status = 'FAILED_COMPENSATED';
          reconstructedState.current_step = 'compensated';
          break;
        case 'WORKFLOW_COMPLETED':
          reconstructedState.status = 'COMPLETED';
          reconstructedState.current_step = 'done';
          break;
      }
      reconstructedState.replayHistory.push({
        eventId: evt.id,
        eventType: evt.event_type,
        stateTransition: `${prevStatus} -> ${reconstructedState.status}`,
        timestamp: evt.created_at
      });
    }

    res.json({
      workflowId: req.params.id,
      currentStateInDb: currentWf,
      reconstructedState: {
        id: reconstructedState.id,
        status: reconstructedState.status,
        current_step: reconstructedState.current_step
      },
      eventsProcessed: events.rowCount,
      replayHistory: reconstructedState.replayHistory,
      stateIntegrityVerified: reconstructedState.status === currentWf.status
    });
  } catch (err) {
    console.error(`[orchestrator] GET /workflows/${req.params.id}/replay failed:`, err);
    res.status(500).json({ error: 'Could not replay workflow' });
  }
});

// Restart recovery: safely resume incomplete workflows
async function recoverWorkflows() {
  try {
    const rows = await pool.query(
      "SELECT * FROM workflows WHERE status IN ('PENDING', 'RUNNING', 'COMPENSATING')"
    );

    if (rows.rowCount === 0) {
      console.log('[orchestrator] Recovery: No interrupted workflows found.');
      return;
    }

    console.log(`[orchestrator] Recovery: Found ${rows.rowCount} interrupted workflow(s) to inspect.`);

    for (const wf of rows.rows) {
      console.log(`[orchestrator] Inspecting workflow ${wf.id} (status: ${wf.status}, step: ${wf.current_step})`);

      if (wf.status === 'PENDING') {
        const firstStep = steps[0];
        await dispatchStep(wf, firstStep);
        await recordEvent(wf.id, 'WORKFLOW_RECOVERED', { action: 'dispatched_first_step' }, `${wf.id}:recovery:${Date.now()}`);
      } else if (wf.status === 'RUNNING') {
        const step = getStepById(wf.current_step);
        if (!step) continue;

        // Check if result was already recorded while offline
        const resultCheck = await pool.query(
          'SELECT payload FROM workflow_events WHERE workflow_id=$1 AND event_type=$2',
          [wf.id, `${step.id.toUpperCase()}_RESULT`]
        );

        if (resultCheck.rowCount > 0) {
          console.log(`[orchestrator] Recovery: Workflow ${wf.id} already has result for '${step.id}', advancing...`);
          await handleResult(step.resultTopic, resultCheck.rows[0].payload);
        } else {
          // Re-send command (worker persistent idempotency prevents duplicate execution)
          console.log(`[orchestrator] Recovery: Re-dispatching command for step '${step.id}'...`);
          const command = { workflowId: wf.id, input: wf.input, eventId: crypto.randomUUID() };
          await publish(step.commandTopic, command);
          await recordEvent(wf.id, 'WORKFLOW_RECOVERED', { action: 're-dispatched_command', step: step.id }, `${wf.id}:recovery:${Date.now()}`);
        }
      } else if (wf.status === 'COMPENSATING') {
        // Interrupted during compensation: check if compensation was acknowledged
        const compCheck = await pool.query(
          "SELECT 1 FROM workflow_events WHERE workflow_id=$1 AND event_type='COMPENSATION_COMPLETED'",
          [wf.id]
        );

        if (compCheck.rowCount > 0) {
          await updateWorkflow(wf.id, 'FAILED_COMPENSATED', 'compensated');
        } else {
          const step = getStepById(wf.current_step);
          let compensableSteps = [];
          if (step) {
            compensableSteps = getPreviousCompensableSteps(step.id);
            if (compensableSteps.length === 0 && step.compensationTopic) {
              compensableSteps = [step];
            }
          }
          if (compensableSteps.length === 0) {
            compensableSteps = steps.filter(s => !!s.compensationTopic);
          }

          for (const prev of compensableSteps) {
            const compPayload = {
              workflowId: wf.id,
              failedStep: wf.current_step,
              reason: 'Recovery compensation re-dispatch',
              eventId: crypto.randomUUID()
            };
            console.log(`[orchestrator] Recovery: Re-publishing compensation to '${prev.compensationTopic}'`);
            await publish(prev.compensationTopic, compPayload);
          }
          await recordEvent(wf.id, 'WORKFLOW_RECOVERED', { action: 're-dispatched_compensation' }, `${wf.id}:recovery:${Date.now()}`);
        }
      }
    }
  } catch (err) {
    console.error('[orchestrator] Error during workflow recovery:', err);
  }
}

async function main() {
  console.log(`[orchestrator] Initializing FlowMaster with workflow: '${workflowDefinition.name}'...`);
  await initDb();
  await ensureKafkaTopics();

  await producer.connect();
  await consumer.connect();

  const resultTopics = steps.map(s => s.resultTopic).filter(Boolean);
  await consumer.subscribe({ topics: resultTopics, fromBeginning: true });

  await consumer.run({
    eachMessage: async ({ topic, message }) => {
      try {
        if (!message.value) return;
        const payload = JSON.parse(message.value.toString());
        await handleResult(topic, payload);
      } catch (err) {
        console.error('[orchestrator] Result handler error:', err);
      }
    }
  });

  // Execute crash recovery
  await recoverWorkflows();

  app.listen(PORT, () => {
    console.log(`[orchestrator] Listening on port ${PORT}; Workflow '${workflowDefinition.name}' ready (${steps.length} steps configured)`);
  });
}

main().catch(err => {
  console.error('[orchestrator] Fatal startup error:', err);
  process.exit(1);
});

const shutdown = async () => {
  console.log('[orchestrator] Shutting down gracefully...');
  await consumer.disconnect().catch(() => {});
  await producer.disconnect().catch(() => {});
  await pool.end().catch(() => {});
  process.exit(0);
};

process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);

