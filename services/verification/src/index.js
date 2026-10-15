const express = require('express');
const { Kafka } = require('kafkajs');
const { Pool } = require('pg');

const PORT = Number(process.env.PORT || 3001);
const app = express();
app.use(express.json());

app.get('/health', (_req, res) => {
  res.json({ status: 'ok', service: 'verification' });
});

const kafka = new Kafka({
  clientId: 'flowmaster-verification',
  brokers: (process.env.KAFKA_BROKERS || 'localhost:9092').split(',')
});

const pool = new Pool({
  connectionString: process.env.DATABASE_URL
});

const producer = kafka.producer();
const consumer = kafka.consumer({ groupId: 'flowmaster-verification-v1' });

async function initDb() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS processed_commands (
      id BIGSERIAL PRIMARY KEY,
      service_name TEXT NOT NULL,
      command_key TEXT NOT NULL,
      workflow_id TEXT NOT NULL,
      status TEXT NOT NULL,
      output JSONB,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      UNIQUE(service_name, command_key)
    );
  `);
}

async function handleVerificationCommand(data) {
  const { workflowId, input } = data;
  const commandKey = `${workflowId}:command`;

  // Persistent Idempotency check: verify if command was already processed
  const existing = await pool.query(
    'SELECT status, output FROM processed_commands WHERE service_name = $1 AND command_key = $2',
    ['verification', commandKey]
  );

  if (existing.rowCount > 0) {
    console.log(`[verification] Duplicate command ignored for ${workflowId}`);
    // Re-send the cached result so orchestrator is guaranteed to receive it
    const saved = existing.rows[0].output;
    if (saved) {
      await producer.send({
        topic: 'verification.results',
        messages: [{ key: workflowId, value: JSON.stringify(saved) }]
      });
    }
    return;
  }

  // Business logic: check failVerification flag
  const success = input?.failVerification !== true;
  const result = {
    workflowId,
    success,
    reason: success ? null : 'Identity verification rejected'
  };

  // Persist result before publishing to guarantee durable state
  await pool.query(
    `INSERT INTO processed_commands (service_name, command_key, workflow_id, status, output)
     VALUES ('verification', $1, $2, $3, $4)
     ON CONFLICT (service_name, command_key) DO NOTHING`,
    [commandKey, workflowId, success ? 'SUCCESS' : 'FAILED', result]
  );

  console.log(`[verification] ${workflowId} => ${success ? 'SUCCESS' : 'FAILURE'}`);
  await producer.send({
    topic: 'verification.results',
    messages: [{ key: workflowId, value: JSON.stringify(result) }]
  });
}

async function handleCompensation(data) {
  const { workflowId, reason } = data;
  const compKey = `${workflowId}:compensation`;

  // Persistent Idempotency check: verify if compensation was already processed
  const existing = await pool.query(
    'SELECT status, output FROM processed_commands WHERE service_name = $1 AND command_key = $2',
    ['verification', compKey]
  );

  if (existing.rowCount > 0) {
    console.log(`[verification] Duplicate compensation ignored for ${workflowId}`);
    const saved = existing.rows[0].output;
    if (saved) {
      await producer.send({
        topic: 'verification.results',
        messages: [{ key: workflowId, value: JSON.stringify(saved) }]
      });
    }
    return;
  }

  console.log(`[verification] Compensation executed for ${workflowId}: ${reason || 'Rollback initiated'}`);

  const compResult = {
    workflowId,
    compensation: true,
    success: true,
    reason: reason || 'Verification hold released'
  };

  // Persist compensation record in PostgreSQL
  await pool.query(
    `INSERT INTO processed_commands (service_name, command_key, workflow_id, status, output)
     VALUES ('verification', $1, $2, 'COMPENSATED', $3)
     ON CONFLICT (service_name, command_key) DO NOTHING`,
    [compKey, workflowId, compResult]
  );

  // Send compensation acknowledgement back to orchestrator
  await producer.send({
    topic: 'verification.results',
    messages: [{
      key: workflowId,
      value: JSON.stringify(compResult)
    }]
  });
}

async function main() {
  await initDb();
  await producer.connect();
  await consumer.connect();
  await consumer.subscribe({
    topics: ['verification.commands', 'verification.compensations'],
    fromBeginning: true
  });

  console.log('[verification] Service connected to Kafka and PostgreSQL, listening for commands...');

  await consumer.run({
    eachMessage: async ({ topic, message }) => {
      try {
        if (!message.value) return;
        const data = JSON.parse(message.value.toString());
        if (!data || !data.workflowId) return;

        if (topic === 'verification.compensations') {
          await handleCompensation(data);
        } else if (topic === 'verification.commands') {
          await handleVerificationCommand(data);
        }
      } catch (err) {
        console.error('[verification] Error processing message:', err);
      }
    }
  });

  const server = app.listen(PORT, () => {
    console.log(`[verification] Express server listening on ${PORT}`);
  });

  const shutdown = async () => {
    console.log('[verification] Shutting down gracefully...');
    server.close();
    await consumer.disconnect().catch(() => {});
    await producer.disconnect().catch(() => {});
    await pool.end().catch(() => {});
    process.exit(0);
  };

  process.on('SIGINT', shutdown);
  process.on('SIGTERM', shutdown);
}

main().catch(err => {
  console.error('[verification] Fatal startup error:', err);
  process.exit(1);
});

