const express = require('express');
const { Kafka } = require('kafkajs');
const { Pool } = require('pg');

const PORT = Number(process.env.PORT || 3002);
const app = express();
app.use(express.json());

app.get('/health', (_req, res) => {
  res.json({ status: 'ok', service: 'credit' });
});

const kafka = new Kafka({
  clientId: 'flowmaster-credit',
  brokers: (process.env.KAFKA_BROKERS || 'localhost:9092').split(',')
});

const pool = new Pool({
  connectionString: process.env.DATABASE_URL
});

const producer = kafka.producer();
const consumer = kafka.consumer({ groupId: 'flowmaster-credit-v1' });

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

async function handleCreditCommand(data) {
  const { workflowId, input } = data;
  const commandKey = `${workflowId}:command`;

  // Persistent Idempotency check: verify if command was already processed
  const existing = await pool.query(
    'SELECT status, output FROM processed_commands WHERE service_name = $1 AND command_key = $2',
    ['credit', commandKey]
  );

  if (existing.rowCount > 0) {
    console.log(`[credit] Duplicate command ignored for ${workflowId}`);
    const saved = existing.rows[0].output;
    if (saved) {
      await producer.send({
        topic: 'credit.results',
        messages: [{ key: workflowId, value: JSON.stringify(saved) }]
      });
    }
    return;
  }

  // Demo rule: set failCredit:true in input to test Saga compensation.
  const success = input?.failCredit !== true;
  const result = {
    workflowId,
    success,
    reason: success ? null : 'Credit check rejected'
  };

  // Persist result before publishing to guarantee durable state
  await pool.query(
    `INSERT INTO processed_commands (service_name, command_key, workflow_id, status, output)
     VALUES ('credit', $1, $2, $3, $4)
     ON CONFLICT (service_name, command_key) DO NOTHING`,
    [commandKey, workflowId, success ? 'SUCCESS' : 'FAILED', result]
  );

  console.log(`[credit] ${workflowId} => ${success ? 'SUCCESS' : 'FAILURE'}`);
  await producer.send({
    topic: 'credit.results',
    messages: [{ key: workflowId, value: JSON.stringify(result) }]
  });
}

async function main() {
  await initDb();
  await producer.connect();
  await consumer.connect();
  await consumer.subscribe({ topic: 'credit.commands', fromBeginning: true });

  console.log('[credit] Service connected to Kafka and PostgreSQL, listening for commands...');

  await consumer.run({
    eachMessage: async ({ message }) => {
      try {
        if (!message.value) return;
        const data = JSON.parse(message.value.toString());
        if (!data || !data.workflowId) return;

        await handleCreditCommand(data);
      } catch (err) {
        console.error('[credit] Error processing message:', err);
      }
    }
  });

  const server = app.listen(PORT, () => {
    console.log(`[credit] Express server listening on ${PORT}`);
  });

  const shutdown = async () => {
    console.log('[credit] Shutting down gracefully...');
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
  console.error('[credit] Fatal startup error:', err);
  process.exit(1);
});

