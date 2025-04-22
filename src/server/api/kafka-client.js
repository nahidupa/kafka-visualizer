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
    
    // Configuration values (in ms) - similar to real Kafka defaults
    this.sessionTimeout = config.sessionTimeout || 10000; // Default 10 seconds
    this.heartbeatInterval = config.heartbeatInterval || 3000; // Default 3 seconds
    
    // Start heartbeat checker
    this.heartbeatChecker = setInterval(() => this.checkHeartbeats(), this.heartbeatInterval);
    
    // Topic configuration defaults
    this.defaultTopicConfig = {
      'retention.ms': 604800000, // 7 days
      'cleanup.policy': 'delete',
      'min.insync.replicas': 1,
      'compression.type': 'producer'
    };
  }
  
  createTopic(topic, partitions = 3, config = {}) {
    // Merge supplied config with defaults
    const topicConfig = {...this.defaultTopicConfig, ...config};
    
    this.topics[topic] = Array(partitions).fill().map(() => []);
    
    // Store topic configuration
    this.topics[topic].config = topicConfig;
    console.log(`Created topic ${topic} with ${partitions} partitions and config:`, topicConfig);
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
        partitionAssignments: {}, // Track which consumer owns which partition
        lastHeartbeats: {} // Track last heartbeat time for each member
      };
    }
    return this.consumerGroups[groupId];
  }
  
  // Record heartbeat from a consumer
  recordHeartbeat(groupId, consumerId) {
    const group = this.getConsumerGroup(groupId);
    group.lastHeartbeats[consumerId] = Date.now();
  }
  
  // Check consumer heartbeats and handle timeouts
  checkHeartbeats() {
    const now = Date.now();
    
    // Check each consumer group
    Object.entries(this.consumerGroups).forEach(([groupId, group]) => {
      let needRebalance = false;
      
      // Check each member's heartbeat
      group.members.forEach(consumer => {
        const lastHeartbeat = group.lastHeartbeats[consumer.id] || 0;
        
        // If heartbeat expired and consumer is marked as connected
        if (now - lastHeartbeat > this.sessionTimeout && consumer.connected) {
          console.log(`Consumer ${consumer.id} in group ${groupId} timed out`);
          
          // Mark consumer as disconnected
          consumer.connected = false;
          needRebalance = true;
        }
      });
      
      // If any consumers timed out, trigger rebalance for all topics
      if (needRebalance) {
        const activeConsumer = group.members.find(c => c.connected);
        if (activeConsumer) {
          activeConsumer.subscriptions.forEach(topic => {
            this.rebalancePartitions(groupId, topic);
          });
        }
      }
    });
  }
  
  // Simulate partition rebalancing within a consumer group
  rebalancePartitions(groupId, topic, strategy = 'range') {
    const group = this.getConsumerGroup(groupId);
    const activeMembers = group.members.filter(c => c.connected);
    const numPartitions = this.topics[topic].length;
    
    // Initialize partitionAssignments for this topic if it doesn't exist
    if (!group.partitionAssignments) {
      group.partitionAssignments = {};
    }
    
    // Initialize this topic's partition assignments object
    if (!group.partitionAssignments[topic]) {
      group.partitionAssignments[topic] = {};
    }
    
    // Range Assignor implementation (Kafka's default)
    if (strategy === 'range') {
      const membersCount = activeMembers.length;
      if (membersCount === 0) return;
      
      // Sort members by ID for consistent assignment
      activeMembers.sort((a, b) => a.id.localeCompare(b.id));
      
      const partitionsPerConsumer = Math.floor(numPartitions / membersCount);
      const remainder = numPartitions % membersCount;
      
      let assignedPartitions = 0;
      for (let i = 0; i < membersCount; i++) {
        const consumer = activeMembers[i];
        const partitionCount = i < remainder ? partitionsPerConsumer + 1 : partitionsPerConsumer;
        
        for (let j = 0; j < partitionCount; j++) {
          const partitionId = assignedPartitions++;
          group.partitionAssignments[topic][partitionId] = consumer.id;
        }
      }
    }
    // Round robin implementation could be added as an option
  }
  
  // Clean up resources
  shutdown() {
    if (this.heartbeatChecker) {
      clearInterval(this.heartbeatChecker);
    }
  }
}

class Producer {
  constructor(simulator) {
    this.simulator = simulator;
    this.connected = false;
    this.acks = 1; // Default to acks=1, similar to Kafka default
    this.retries = 3; // Default retries
    this.errorRate = 0.03; // 3% chance of transient errors
    this.enableIdempotence = false;
    this.producerIds = {};  // Track sequence IDs per topic-partition
    this.transactionInProgress = false; // Track transaction state
  }

  // Allow configuration
  setConfig({ enableIdempotence, acks, retries }) {
    if (enableIdempotence !== undefined) {
      this.enableIdempotence = enableIdempotence === true;
    }
    if (acks !== undefined) {
      this.acks = acks;
    }
    if (retries !== undefined) {
      this.retries = retries;
    }
    return this;
  }

  async connect() {
    // Simulate network errors on connect
    if (Math.random() < 0.01) {
      return Promise.reject(new Error('Connection refused: broker may be down'));
    }
    
    this.connected = true;
    return Promise.resolve();
  }

  async disconnect() {
    this.connected = false;
    return Promise.resolve();
  }

  async send({ topic, messages, acks = 1 }) {
    if (!this.connected) {
      throw new Error('Producer not connected');
    }

    // Simulate network errors/timeouts
    if (Math.random() < this.errorRate) {
      throw new Error('Network error: request timed out');
    }

    if (!this.simulator.topics[topic]) {
      this.simulator.createTopic(topic);
    }

    const numPartitions = this.simulator.topics[topic].length;
    const results = [];

    for (const message of messages) {
      // Simulate serialization errors
      if (message.value === undefined || message.value === null) {
        throw new Error('Cannot serialize null or undefined value');
      }
      
      const partition = this.simulator.getPartition(message.key, numPartitions);
      
      // Check for duplicates if idempotence is enabled
      if (this.enableIdempotence) {
        // Implementation of idempotent delivery...
      }

      // Create complete message with headers
      const kafkaMessage = {
        key: message.key,
        value: message.value,
        headers: message.headers || {}, // Support for Kafka headers
        timestamp: Date.now(),
        offset: this.simulator.topics[topic][partition].length
      };
      
      this.simulator.topics[topic][partition].push(kafkaMessage);

      // Simulate leader epoch (used for replication)
      const leaderEpoch = 0;

      results.push({
        topic,
        partition,
        offset: this.simulator.topics[topic][partition].length - 1,
        key: message.key,
        value: message.value,
        headers: message.headers || {},
        timestamp: kafkaMessage.timestamp,
        leaderEpoch
      });
    }

    // Simulate acks behavior
    if (acks === 0) {
      // Don't wait, return immediately (no delivery guarantees)
      return { results: [] };
    } else if (acks === 'all') {
      // Simulate waiting for all replicas (most reliable)
      await new Promise(resolve => setTimeout(resolve, 100));
    } else {
      // acks=1, wait for leader only (default)
      await new Promise(resolve => setTimeout(resolve, 50));
    }

    // Notify consumers of new messages
    for (const consumer of this.simulator.consumers) {
      if (consumer.subscriptions.includes(topic)) {
        setTimeout(() => consumer.processMessages(topic), 500);
      }
    }

    return { results };
  }

  beginTransaction() {
    if (!this.connected) throw new Error('Producer not connected');
    this.transactionInProgress = true;
    return { transactionId: `txn-${Date.now()}` };
  }

  commitTransaction() {
    if (!this.transactionInProgress) throw new Error('No transaction in progress');
    this.transactionInProgress = false;
    // Process the transaction...
  }

  abortTransaction() {
    if (!this.transactionInProgress) throw new Error('No transaction in progress');
    this.transactionInProgress = false;
    // Revert any pending changes...
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
    this.heartbeatInterval = null;
    this.autoCommitEnabled = false;
    this.autoCommitInterval = 5000; // 5 seconds default, like Kafka's default
    this.autoCommitTimer = null;
    this.errorRate = 0.05; // Simulate 5% error rate in processing

    // Register consumer in the group
    const group = this.simulator.getConsumerGroup(groupId);
    group.members.push(this);
  }

  async connect() {
    try {
      this.connected = true;
      
      // Start sending heartbeats
      this.heartbeatInterval = setInterval(() => {
        if (this.connected) {
          this.simulator.recordHeartbeat(this.groupId, this.id);
          // console.log(`Consumer ${this.id} heartbeat`);
        }
      }, 1000); // Send heartbeat every 1 second
      
      return Promise.resolve();
    } catch (err) {
      // Simulate network errors
      if (Math.random() < 0.01) { // 1% chance of connection error
        return Promise.reject(new Error('Connection refused: broker may be down'));
      }
      return Promise.resolve();
    }
  }

  async disconnect() {
    this.connected = false;
    
    // Stop heartbeats
    if (this.heartbeatInterval) {
      clearInterval(this.heartbeatInterval);
    }
    
    // Stop auto-commit if enabled
    if (this.autoCommitTimer) {
      clearInterval(this.autoCommitTimer);
    }
    
    // In real Kafka, rebalancing has a delay and is triggered by missed heartbeats
    // Here we'll simulate that with a slight delay before triggering rebalance
    setTimeout(() => {
      this.subscriptions.forEach(topic => {
        this.simulator.rebalancePartitions(this.groupId, topic);
      });
    }, 1000);
    
    return Promise.resolve();
  }

  async subscribe({ topic, fromBeginning, autoOffsetReset = 'latest', autoCommit = false }) {
    if (!this.connected) {
      throw new Error('Consumer not connected');
    }

    if (!this.simulator.topics[topic]) {
      this.simulator.createTopic(topic);
    }

    this.subscriptions.push(topic);
    this.autoCommitEnabled = autoCommit;  // FIXED: autoCommit is now defined with default value
    
    // Start auto-commit timer if enabled
    if (autoCommit && !this.autoCommitTimer) {
      this.autoCommitTimer = setInterval(() => {
        if (this.connected) {
          this._autoCommitOffsets();
        }
      }, this.autoCommitInterval);
    }
    
    const group = this.simulator.getConsumerGroup(this.groupId);
    if (!group.offsets[topic]) {
      group.offsets[topic] = {};
      for (let i = 0; i < this.simulator.topics[topic].length; i++) {
        if (fromBeginning || autoOffsetReset === 'earliest') {
          group.offsets[topic][i] = 0;
        } else {
          group.offsets[topic][i] = this.simulator.topics[topic][i].length;
        }
      }
    }

    // Record initial heartbeat
    this.simulator.recordHeartbeat(this.groupId, this.id);
    
    // Trigger partition rebalancing
    this.simulator.rebalancePartitions(this.groupId, topic);

    return Promise.resolve();
  }
  
  // Simulate auto commit behavior
  _autoCommitOffsets() {
    const pendingKeysToRemove = [];
    
    // For each topic with pending messages
    Object.keys(pendingPeeks).forEach(topic => {
      if (this.subscriptions.includes(topic) && pendingPeeks[topic].length > 0) {
        // Commit all pending peeks for this topic
        commitBatchMessages(topic);
        pendingKeysToRemove.push(topic);
      }
    });
    
    // Remove committed messages from pending
    pendingKeysToRemove.forEach(key => {
      pendingPeeks[key] = [];
    });
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
        
        try {
          // Simulate random processing errors (real Kafka consumers face these)
          if (Math.random() < this.errorRate) {
            throw new Error('Error processing message: serialization failure');
          }
          
          group.offsets[topic][partition] = currentOffset + 1;
          
          await this.callback({
            topic,
            partition,
            message: {
              key: message.key,
              value: message.value,
              headers: message.headers || {}, 
              timestamp: message.timestamp,
              offset: message.offset,
              leaderEpoch: 0, // Real Kafka has this for replication tracking
              toString: () => JSON.stringify(message),
              key: {
                toString: () => message.key,
              },
              value: {
                toString: () => message.value,
              }
            }
          });
          
        } catch (err) {
          console.error(`Error processing message: ${err.message}`);
          // In real Kafka, the consumer would decide whether to continue, retry, or 
          // move message to dead letter queue based on the error
        }
      }
    }
  }
  
  // Kafka consumers can manually commit offsets
  async commitOffset({ topic, partition, offset }) {
    const group = this.simulator.getConsumerGroup(this.groupId);
    
    if (!group.offsets[topic]) {
      group.offsets[topic] = {};
    }
    
    // Kafka only allows committing offsets for partitions assigned to this consumer
    if (group.partitionAssignments[topic]?.[partition] !== this.id) {
      throw new Error(`Cannot commit offset for partition ${partition} - not assigned to this consumer`);
    }
    
    // Update the offset
    group.offsets[topic][partition] = offset + 1; // +1 because Kafka commits the next offset to read
    
    return { topic, partition, offset: offset + 1 };
  }
}

const simulator = new KafkaSimulator(kafkaConfig);
const producer = new Producer(simulator);
const consumer = new Consumer(simulator, { 
  groupId: process.env.KAFKA_CONSUMER_GROUP || 'kafka-visualizer-group' 
});

const pendingPeeks = {};

async function connectProducer() {
  await producer.connect();
  console.log('Producer connected (simulated)');
}

async function connectConsumer() {
  await consumer.connect();
  await consumer.subscribe({ 
    topic: kafkaConfig.topic, 
    fromBeginning: true,
    autoCommit: false  // Explicitly set the autoCommit value
  });
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

// Store last accessed partition for round-robin consumption
let lastAccessedPartition = -1;

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

  // If still no assigned partitions (shouldn't happen), return null
  if (assignedPartitions.length === 0) {
    return null;
  }

  // True round-robin approach: check partitions starting after the last accessed one
  const partitionCount = assignedPartitions.length;
  
  // Check each partition exactly once in round-robin order
  for (let i = 0; i < partitionCount; i++) {
    // Calculate next partition index in round-robin fashion
    lastAccessedPartition = (lastAccessedPartition + 1) % partitionCount;
    const partition = assignedPartitions[lastAccessedPartition];
    
    // Calculate next offset for this partition considering pending peeks
    const pendingForPartition = pendingPeeks[topic].filter(p => p.partition === partition).length;
    const offset = group.offsets[topic][partition] + pendingForPartition;
    const messages = partitions[partition];
    
    if (offset < messages.length) {
      // Found a message to peek
      const msg = messages[offset];
      const record = { 
        topic, 
        partition, 
        key: msg.key, 
        value: msg.value, 
        headers: msg.headers || {},
        offset,
        // Include lag information for educational purposes
        lag: messages.length - offset - 1
      };
      pendingPeeks[topic].push(record);
      return record;
    }
    // This partition has no new messages, continue to next one
  }
  
  // If we get here, no messages are available in any partition
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
  
  if (list.length === 0) {
    return results;
  }
  
  // Group pending messages by partition for proper batch commit behavior
  const partitionCommits = {};
  list.forEach(record => {
    partitionCommits[record.partition] = partitionCommits[record.partition] || [];
    partitionCommits[record.partition].push(record);
  });

  // Process each partition's batch of messages separately (important Kafka behavior)
  Object.keys(partitionCommits).forEach(partition => {
    const partitionNumber = parseInt(partition, 10);
    const partitionRecords = partitionCommits[partition];
    
    // Sort by offset to ensure proper ordering within partition
    partitionRecords.sort((a, b) => a.offset - b.offset);
    
    // In a real Kafka consumer, we might have message processing failures
    // that would cause some messages to be skipped, but all offsets up to
    // the highest successful one would be committed
    let highestProcessedOffset = -1;
    
    // Process each record in order
    for (const record of partitionRecords) {
      // Simulate processing the message
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
      
      // Track the highest successfully processed offset
      highestProcessedOffset = record.offset;
      
      // Add to results
      results.push(record);
    }
    
    // Only update the committed offset if we processed at least one message
    if (highestProcessedOffset >= 0) {
      // In Kafka, we commit the NEXT offset to consume
      group.offsets[topic][partitionNumber] = highestProcessedOffset + 1;
    }
  });
  
  // Clear all pending records after committing
  pendingPeeks[topic] = [];
  
  // Return the list of processed messages
  return results;
}

// Get current consumer group status for a topic
function getConsumerStatus(topic) {
  const result = { partitions: [] };
  const partitions = simulator.topics[topic];
  const group = simulator.getConsumerGroup(consumer.groupId);
  
  for (let partition = 0; partition < partitions.length; partition++) {
    const committedOffset = group.offsets[topic]?.[partition] || 0;
    const latestOffset = partitions[partition].length;
    const lag = latestOffset - committedOffset;
    
    result.partitions.push({
      partition,
      committedOffset,
      lag,
      pendingCount: (pendingPeeks[topic] || [])
        .filter(p => p.partition === partition).length
    });
  }
  
  return result;
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