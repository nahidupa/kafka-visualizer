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
    this.consumerGroups = {}; // Track consumer groups
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
    
    // Fallback for other keys - consistent hashing
    let hash = 0;
    for (let i = 0; i < keyStr.length; i++) {
      const char = keyStr.charCodeAt(i);
      hash = ((hash << 5) - hash) + char;
      hash = hash & hash; // Convert to 32bit integer
    }
    
    return Math.abs(hash) % numPartitions;
  }

  // Get or create a consumer group
  getConsumerGroup(groupId) {
    if (!this.consumerGroups[groupId]) {
      this.consumerGroups[groupId] = {
        members: [],
        offsets: {}, // Offsets are now stored at the group level
        partitionAssignments: {} // Track which consumer owns which partition
      };
    }
    return this.consumerGroups[groupId];
  }
  
  // Simulate partition rebalancing within a consumer group
  rebalancePartitions(groupId, topic) {
    const group = this.getConsumerGroup(groupId);
    const activeMembers = group.members.filter(c => c.connected);
    
    if (activeMembers.length === 0) {
      return;
    }
    
    const numPartitions = this.topics[topic].length;
    
    // Reset current assignments for this topic
    group.partitionAssignments[topic] = group.partitionAssignments[topic] || {};
    
    // Assign partitions to consumers using simple round-robin
    for (let partition = 0; partition < numPartitions; partition++) {
      const consumerIndex = partition % activeMembers.length;
      const consumer = activeMembers[consumerIndex];
      group.partitionAssignments[topic][partition] = consumer.id;
    }
    
    console.log(`Rebalanced partitions for group ${groupId} on topic ${topic}`);
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
      
      // Create complete message with headers
      const kafkaMessage = {
        key: message.key,
        value: message.value,
        headers: message.headers || {}, // Support for Kafka headers
        timestamp: Date.now(),
        offset: this.simulator.topics[topic][partition].length
      };
      
      this.simulator.topics[topic][partition].push(kafkaMessage);

      results.push({
        topic,
        partition,
        offset: this.simulator.topics[topic][partition].length - 1,
        key: message.key,
        value: message.value,
        headers: message.headers || {}
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
    this.id = generateRandomId(); // Unique ID for this consumer instance
    this.connected = false;
    this.subscriptions = [];
    this.callback = null;
    this.simulator.consumers.push(this);

    // Register consumer in the group
    const group = this.simulator.getConsumerGroup(groupId);
    group.members.push(this);
  }

  async connect() {
    this.connected = true;
    return Promise.resolve();
  }

  async disconnect() {
    this.connected = false;
    // Trigger rebalancing when consumer disconnects
    this.subscriptions.forEach(topic => {
      this.simulator.rebalancePartitions(this.groupId, topic);
    });
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
    
    const group = this.simulator.getConsumerGroup(this.groupId);
    if (!group.offsets[topic]) {
      group.offsets[topic] = {};
      for (let i = 0; i < this.simulator.topics[topic].length; i++) {
        group.offsets[topic][i] = fromBeginning ? 0 : this.simulator.topics[topic][i].length;
      }
    }

    // Trigger partition rebalancing
    this.simulator.rebalancePartitions(this.groupId, topic);

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

    const group = this.simulator.getConsumerGroup(this.groupId);
    const partitions = this.simulator.topics[topic];
    
    // Process only partitions assigned to this consumer
    for (let partition = 0; partition < partitions.length; partition++) {
      // Check if this partition is assigned to this consumer instance
      if (group.partitionAssignments[topic]?.[partition] !== this.id) {
        continue; // Skip partitions not assigned to this consumer
      }

      const messages = partitions[partition];
      const currentOffset = group.offsets[topic][partition] || 0;
      
      if (currentOffset < messages.length) {
        const message = messages[currentOffset];
        
        group.offsets[topic][partition] = currentOffset + 1;
        
        await this.callback({
          topic,
          partition,
          message: {
            key: message.key,
            value: message.value,
            headers: message.headers || {}, // Include headers
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
async function produceMessage(key, value, topic = kafkaConfig.topic, headers = {}) {
  // If key not provided, choose random country code
  if (!key) {
    key = COUNTRY_CODES[Math.floor(Math.random() * COUNTRY_CODES.length)];
  }
  
  // Build complex record payload
  const record = createComplexRecord(value);
  
  // Standard Kafka headers
  const messageHeaders = {
    'content-type': 'application/json',
    'timestamp': Date.now().toString(),
    ...headers
  };
  
  const { results } = await producer.send({
    topic,
    messages: [{ 
      key, 
      value: record,
      headers: messageHeaders 
    }],
  });
  
  // Return the first message metadata for UI
  const { partition, offset } = results[0];
  return { 
    topic, 
    partition, 
    key, 
    value: record, 
    offset,
    headers: messageHeaders 
  };
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

// Fetch the next available message for this consumer
function fetchNextMessage(topic) {
  const partitions = simulator.topics[topic];
  if (!partitions) throw new Error(`Topic ${topic} does not exist`);
  
  const group = simulator.getConsumerGroup(consumer.groupId);
  if (!group.offsets[topic]) {
    group.offsets[topic] = {};
    for (let i = 0; i < partitions.length; i++) {
      group.offsets[topic][i] = 0;
    }
  }

  // Note: In real Kafka, consumers only read from partitions assigned to them
  // and ordering is guaranteed only within a partition
  
  // Get partitions assigned to this consumer
  const assignedPartitions = [];
  for (let partition = 0; partition < partitions.length; partition++) {
    if (group.partitionAssignments[topic]?.[partition] === consumer.id) {
      assignedPartitions.push(partition);
    }
  }

  // If no partitions are assigned, return null
  if (assignedPartitions.length === 0) {
    console.log('No partitions assigned to this consumer');
    return null;
  }

  // Check for available messages in assigned partitions
  for (const partition of assignedPartitions) {
    const messages = partitions[partition];
    const offset = group.offsets[topic][partition];
    if (offset < messages.length) {
      const msg = messages[offset];
      group.offsets[topic][partition] = offset + 1;
      return { 
        topic, 
        partition, 
        key: msg.key, 
        value: msg.value,
        headers: msg.headers,
        offset 
      };
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

  // Get consumer group
  const group = simulator.getConsumerGroup(consumer.groupId);
  if (!group.offsets[topic]) {
    group.offsets[topic] = {};
    for (let i = 0; i < partitions.length; i++) {
      group.offsets[topic][i] = 0;
    }
  }

  // Initialize pending list
  pendingPeeks[topic] = pendingPeeks[topic] || [];

  // Get partitions assigned to this consumer
  const assignedPartitions = [];
  for (let partition = 0; partition < partitions.length; partition++) {
    if (group.partitionAssignments[topic]?.[partition] === consumer.id) {
      assignedPartitions.push(partition);
    }
  }

  // For visualization purposes, if no partitions are assigned,
  // temporarily assign all partitions to this consumer
  if (assignedPartitions.length === 0) {
    for (let partition = 0; partition < partitions.length; partition++) {
      group.partitionAssignments[topic] = group.partitionAssignments[topic] || {};
      group.partitionAssignments[topic][partition] = consumer.id;
      assignedPartitions.push(partition);
    }
  }

  // Find next available message in assigned partitions
  for (const partition of assignedPartitions) {
    const offset = group.offsets[topic][partition] + pendingPeeks[topic].filter(p => p.partition === partition).length;
    const messages = partitions[partition];
    if (offset < messages.length) {
      const msg = messages[offset];
      const record = { 
        topic, 
        partition, 
        key: msg.key, 
        value: msg.value, 
        headers: msg.headers || {},
        offset 
      };
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
  const group = simulator.getConsumerGroup(consumer.groupId);
  group.offsets[topic][record.partition] = record.offset + 1;
  
  // Note: Dead Letter Queue handling is an application-level pattern, not a Kafka feature.
  // In real applications, this would be implemented by the consuming application.
  if (record.value && record.value.corrupted) {
    // Add to dead letter topic (partition 0 since DLQ typically has fewer partitions)
    simulator.topics[simulator.deadLetterTopic][0].push({
      key: record.key,
      value: record.value,
      headers: {
        'original-topic': topic,
        'original-partition': record.partition.toString(),
        'original-offset': record.offset.toString(),
        'error-reason': 'corrupted',
        'dead-letter-timestamp': Date.now().toString(),
      },
      originalPartition: record.partition,
      originalTopic: topic,
      timestamp: Date.now(),
      reason: 'corrupted'
    });
    record.deadLetter = true;
  }
  
  return record;
}

// Commit all peeked messages in batch
function commitBatchMessages(topic) {
  const list = pendingPeeks[topic] || [];
  const results = [];
  const group = simulator.getConsumerGroup(consumer.groupId);
  
  // Note: In real Kafka, batch commits are atomic per partition
  // But internally messages are still processed per partition
  
  // Group pending messages by partition for proper batch commit behavior
  const partitionCommits = {};
  list.forEach(record => {
    partitionCommits[record.partition] = partitionCommits[record.partition] || [];
    partitionCommits[record.partition].push(record);
  });

  // Process each partition's batch of messages
  Object.keys(partitionCommits).forEach(partition => {
    const partitionRecords = partitionCommits[partition];
    // Sort by offset to ensure proper ordering
    partitionRecords.sort((a, b) => a.offset - b.offset);
    
    // Find highest offset in this partition's batch
    const highestOffset = Math.max(...partitionRecords.map(r => r.offset));
    
    // Commit up to the highest offset
    group.offsets[topic][partition] = highestOffset + 1;
    
    // Process each record (e.g., handle corrupted messages)
    partitionRecords.forEach(record => {
      // Note: Dead Letter Queue handling is an application-level pattern, not a Kafka feature
      if (record.value && record.value.corrupted) {
        // Add to dead letter topic with proper headers
        simulator.topics[simulator.deadLetterTopic][0].push({
          key: record.key,
          value: record.value,
          headers: {
            'original-topic': topic,
            'original-partition': record.partition.toString(),
            'original-offset': record.offset.toString(),
            'error-reason': 'corrupted',
            'dead-letter-timestamp': Date.now().toString(),
            'batch-processed': 'true'
          },
          originalPartition: record.partition,
          originalTopic: topic,
          timestamp: Date.now(),
          reason: 'corrupted'
        });
        record.deadLetter = true;
      }
      
      results.push(record);
    });
  });
  
  pendingPeeks[topic] = [];
  return results;
}

// Get current consumer group status for a topic
function getConsumerStatus(topic) {
  const partitions = simulator.topics[topic] || [];
  const group = simulator.getConsumerGroup(consumer.groupId);
  const status = partitions.map((_, partition) => {
    const committed = group.offsets[topic]?.[partition] ?? 0;
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