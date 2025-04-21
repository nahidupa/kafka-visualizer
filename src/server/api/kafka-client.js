const kafkaConfig = require('../../config/kafka');
const crypto = require('crypto');

// helpers for complex record
function generateRandomId() {
  return crypto.randomBytes(4).toString('hex');
}
function createComplexRecord(payload) {
  const record = { id: generateRandomId(), payload, ts: Date.now() };
  // 25% chance to mark as corrupted
  if (Math.random() < 0.25) record.corrupted = true;
  return record;
}

class KafkaSimulator {
  constructor(config) {
    this.topics = {};
    this.consumers = [];
    this.clientId = config.clientId;
    this.config = config;
    this.deadLetterTopic = config.deadLetterTopic || 'dead-letter';
    this.createTopic(config.topic, config.partitions);
    // Dead-letter topic with single partition
    this.createTopic(this.deadLetterTopic, 1);
  }

  createTopic(topic, partitions = 3) {
    this.topics[topic] = Array(partitions).fill().map(() => []);
    return true;
  }

  getPartition(key, numPartitions) {
    if (!key) {
      return Math.floor(Math.random() * numPartitions);
    }
    
    // Direct mapping for demo country codes
    const keyStr = String(key);
    if (keyStr === 'US') return 0;
    if (keyStr === 'FR') return 1;
    if (keyStr === 'DE') return 2;
    
    // Fallback for other keys
    let hash = 0;
    for (let i = 0; i < keyStr.length; i++) {
      const char = keyStr.charCodeAt(i);
      hash = ((hash << 5) - hash) + char;
      hash = hash & hash; // Convert to 32bit integer
    }
    
    return Math.abs(hash) % numPartitions;
  }
}

class Producer {
  constructor(simulator) {
    this.simulator = simulator;
    this.connected = false;
  }

  async connect() {
    this.connected = true;
    return Promise.resolve();
  }

  async disconnect() {
    this.connected = false;
    return Promise.resolve();
  }

  async send({ topic, messages }) {
    if (!this.connected) {
      throw new Error('Producer not connected');
    }

    if (!this.simulator.topics[topic]) {
      this.simulator.createTopic(topic);
    }

    const numPartitions = this.simulator.topics[topic].length;
    const results = [];

    for (const message of messages) {
      const partition = this.simulator.getPartition(message.key, numPartitions);
      
      this.simulator.topics[topic][partition].push({
        key: message.key,
        value: message.value,
        timestamp: Date.now(),
        offset: this.simulator.topics[topic][partition].length
      });

      results.push({
        topic,
        partition,
        offset: this.simulator.topics[topic][partition].length - 1,
        key: message.key,
        value: message.value
      });
    }

    for (const consumer of this.simulator.consumers) {
      if (consumer.subscriptions.includes(topic)) {
        setTimeout(() => consumer.processMessages(topic), 500);
      }
    }

    return { results };
  }
}

class Consumer {
  constructor(simulator, { groupId }) {
    this.simulator = simulator;
    this.groupId = groupId;
    this.connected = false;
    this.subscriptions = [];
    this.callback = null;
    this.offsets = {};
    this.simulator.consumers.push(this);
  }

  async connect() {
    this.connected = true;
    return Promise.resolve();
  }

  async disconnect() {
    this.connected = false;
    return Promise.resolve();
  }

  async subscribe({ topic, fromBeginning }) {
    if (!this.connected) {
      throw new Error('Consumer not connected');
    }

    if (!this.simulator.topics[topic]) {
      this.simulator.createTopic(topic);
    }

    this.subscriptions.push(topic);
    
    if (!this.offsets[topic]) {
      this.offsets[topic] = {};
      for (let i = 0; i < this.simulator.topics[topic].length; i++) {
        this.offsets[topic][i] = fromBeginning ? 0 : this.simulator.topics[topic][i].length;
      }
    }

    return Promise.resolve();
  }

  async run({ eachMessage }) {
    this.callback = eachMessage;
    
    for (const topic of this.subscriptions) {
      this.processMessages(topic);
    }
  }

  async processMessages(topic) {
    if (!this.callback || !this.connected) return;

    const partitions = this.simulator.topics[topic];
    
    for (let partition = 0; partition < partitions.length; partition++) {
      const messages = partitions[partition];
      const currentOffset = this.offsets[topic][partition] || 0;
      
      if (currentOffset < messages.length) {
        const message = messages[currentOffset];
        
        this.offsets[topic][partition] = currentOffset + 1;
        
        await this.callback({
          topic,
          partition,
          message: {
            key: message.key,
            value: message.value,
            timestamp: message.timestamp,
            offset: message.offset,
            toString: () => JSON.stringify(message),
            key: {
              toString: () => message.key,
            },
            value: {
              toString: () => message.value,
            }
          }
        });
      }
    }
  }
}

const simulator = new KafkaSimulator(kafkaConfig);
const producer = new Producer(simulator);
const consumer = new Consumer(simulator, { 
  groupId: process.env.KAFKA_CONSUMER_GROUP || 'kafka-visualizer-group' 
});

async function connectProducer() {
  await producer.connect();
  console.log('Producer connected (simulated)');
}

async function connectConsumer() {
  await consumer.connect();
  await consumer.subscribe({ topic: kafkaConfig.topic, fromBeginning: true });
  console.log('Consumer connected and subscribed to topic (simulated)');
}

// Predefined keys set
const COUNTRY_CODES = ['US', 'FR', 'DE'];
async function produceMessage(_, value, topic = kafkaConfig.topic) {
  // choose random country code key
  const key = COUNTRY_CODES[Math.floor(Math.random() * COUNTRY_CODES.length)];
  // build complex record payload
  const record = createComplexRecord(value);
  const { results } = await producer.send({
    topic,
    messages: [{ key, value: record }],
  });
  // return the first message metadata for UI
  const { partition, offset } = results[0];
  return { topic, partition, key, value: record, offset };
}

async function consumeMessages(callback) {
  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      console.log(`Consumed message from partition ${partition}: key=${message.key}, value=${message.value}`);
      callback({
        topic,
        partition,
        key: message.key.toString(),
        value: message.value.toString(),
      });
    },
  });
}

async function disconnectProducer() {
  await producer.disconnect();
  console.log('Producer disconnected (simulated)');
}

async function disconnectConsumer() {
  await consumer.disconnect();
  console.log('Consumer disconnected (simulated)');
}

// Fetch the next available message in FIFO order across partitions
function fetchNextMessage(topic) {
  const partitions = simulator.topics[topic];
  if (!partitions) throw new Error(`Topic ${topic} does not exist`);
  // initialize offsets if missing
  if (!consumer.offsets[topic]) {
    consumer.offsets[topic] = {};
    for (let i = 0; i < partitions.length; i++) {
      consumer.offsets[topic][i] = 0;
    }
  }
  for (let partition = 0; partition < partitions.length; partition++) {
    const messages = partitions[partition];
    const offset = consumer.offsets[topic][partition];
    if (offset < messages.length) {
      const msg = messages[offset];
      consumer.offsets[topic][partition] = offset + 1;
      return { topic, partition, key: msg.key, value: msg.value };
    }
  }
  return null;
}

// Pending peeked messages (not yet committed)
const pendingPeeks = {};

// Peek next message without advancing committed offset
function peekNextMessage(topic) {
  const partitions = simulator.topics[topic];
  if (!partitions) throw new Error(`Topic ${topic} does not exist`);

  // initialize consumer offsets for topic if missing
  if (!consumer.offsets[topic]) {
    consumer.offsets[topic] = {};
    for (let i = 0; i < partitions.length; i++) {
      consumer.offsets[topic][i] = 0;
    }
  }

  // initialize pending list
  pendingPeeks[topic] = pendingPeeks[topic] || [];

  // find next available in FIFO order
  for (let partition = 0; partition < partitions.length; partition++) {
    const offset = consumer.offsets[topic][partition] + pendingPeeks[topic].filter(p => p.partition === partition).length;
    const messages = partitions[partition];
    if (offset < messages.length) {
      const msg = messages[offset];
      const record = { topic, partition, key: msg.key, value: msg.value, offset };
      pendingPeeks[topic].push(record);
      return record;
    }
  }
  return null;
}

// Commit a single peeked message (the earliest)
function commitSingleMessage(topic) {
  const list = pendingPeeks[topic] || [];
  if (list.length === 0) return null;
  const record = list.shift();
  // handle corrupted -> send to dead-letter
  if (record.value && record.value.corrupted) {
    // Add to dead letter topic (partition 0 since DLQ has only one partition)
    simulator.topics[simulator.deadLetterTopic][0].push({
      key: record.key,
      value: record.value,
      originalPartition: record.partition,
      originalTopic: topic,
      timestamp: Date.now(),
      reason: 'corrupted'
    });
    record.deadLetter = true;
  }
  consumer.offsets[topic][record.partition] = record.offset + 1;
  return record;
}

// Commit all peeked messages in batch
function commitBatchMessages(topic) {
  const list = pendingPeeks[topic] || [];
  const results = [];
  
  list.forEach(record => {
    // handle corrupted -> send to dead-letter
    if (record.value && record.value.corrupted) {
      // Add to dead letter topic (partition 0 since DLQ has only one partition)
      simulator.topics[simulator.deadLetterTopic][0].push({
        key: record.key,
        value: record.value,
        originalPartition: record.partition,
        originalTopic: topic,
        timestamp: Date.now(),
        reason: 'corrupted'
      });
      record.deadLetter = true;
    }
    consumer.offsets[topic][record.partition] = record.offset + 1;
    results.push(record);
  });
  
  pendingPeeks[topic] = [];
  return results;
}

// Get current consumer group status for a topic
function getConsumerStatus(topic) {
  const partitions = simulator.topics[topic] || [];
  const status = partitions.map((_, partition) => {
    const committed = consumer.offsets[topic]?.[partition] ?? 0;
    const pending = (pendingPeeks[topic] || []).filter(p => p.partition === partition).length;
    return { partition, committedOffset: committed, pendingCount: pending };
  });
  return { topic, partitions: status };
}

module.exports = {
  connectProducer,
  connectConsumer,
  produceMessage,
  consumeMessages,
  disconnectProducer,
  disconnectConsumer,
  createTopic: (name, partitions) => simulator.createTopic(name, partitions),
  getTopics: () => Object.keys(simulator.topics),
  getTopicDetails: (topic) => ({
    name: topic,
    partitions: simulator.topics[topic].map((partition, index) => ({
      id: index,
      messages: partition.length
    }))
  }),
  fetchNextMessage,
  peekNextMessage,
  commitSingleMessage,
  commitBatchMessages,
  getConsumerStatus
};