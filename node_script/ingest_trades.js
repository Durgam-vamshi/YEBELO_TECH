const { Kafka } = require('kafkajs');
const fs = require('fs');
const csv = require('csv-parser');

const kafka = new Kafka({
  clientId: 'trade-ingestor',
  brokers: ['localhost:19092'],
    // brokers: ['redpanda-0:9092'],
});

// const kafka = new Kafka({
//   clientId: 'trade-ingestor',
//   brokers: [
//     'redpanda-0:9092',
//     'redpanda-1:9092',
//     'redpanda-2:9092'
//   ],
// });

const producer = kafka.producer();

const publishTrade = async (trade) => {
  try {
    await producer.send({
      topic: 'trade-data',
      messages: [{ value: JSON.stringify(trade) }],
    });
    console.log('Published:', trade);
  } catch (err) {
    console.error('Error publishing trade:', err);
  }
};

const run = async () => {
  try {
    const admin = kafka.admin();
    await admin.connect();
    await admin.createTopics({
      topics: [{ topic: 'trade-data', numPartitions: 1, replicationFactor: 1 }],
      waitForLeaders: true,
    });
    await admin.disconnect();

    await producer.connect();
    console.log('Producer connected to Redpanda!');

    const trades = [];
    fs.createReadStream('trades_data.csv')
      .pipe(csv())
      .on('data', (row) => {
        trades.push({
          token_address: row.token_address || 'UNKNOWN',
          price_in_sol: row.price_in_sol ? parseFloat(row.price_in_sol) : 0,
          block_time: row.block_time || new Date().toISOString(),
        });
      })
      .on('end', async () => {
        console.log(`Read ${trades.length} trades from CSV`);

        for (const trade of trades) {
          await publishTrade(trade);
        }

        console.log('✅ All trades ingested from CSV!');
        await producer.disconnect();
        process.exit(0);
      })
      .on('error', (err) => {
        console.error('Error reading CSV:', err);
      });
  } catch (err) {
    console.error('Error in ingestion script:', err);
    process.exit(1);
  }
};

run();
















