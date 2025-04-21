module.exports = {
  clientId: process.env.KAFKA_CLIENT_ID || 'kafka-visualizer',
  brokers: (process.env.KAFKA_BROKERS || 'localhost:9092').split(','),
  topic: process.env.KAFKA_TOPIC || 'visualization-topic',
  partitions: parseInt(process.env.KAFKA_PARTITIONS, 10) || 3,
  replicationFactor: parseInt(process.env.KAFKA_REPLICATION_FACTOR, 10) || 1,
};