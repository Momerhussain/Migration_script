// publish.cjs  (CommonJS)
const amqplib = require('amqplib');

async function main() {
  const url = process.env.AMQP_URL || 'amqp://admin:admin123@localhost:5672';
  const queue = process.env.QUEUE || 'test.alert';

  const conn = await amqplib.connect(url);
  const ch = await conn.createChannel();
  await ch.assertQueue(queue, { durable: true }); // policy ki max-length yahin apply ho jayegi

  for (let i = 1; i <= 100; i++) {
    ch.sendToQueue(queue, Buffer.from(`msg ${i}`), { persistent: true });
  }
  console.log('Published 100 msgs');
  await ch.close();
  await conn.close();
}

main().catch(err => {
  console.error('Publish error:', err);
  process.exit(1);
});
