const kafkaConfig = require('../../config/kafka');

class KafkaSimulator {
  constructor(config) {
    this.topics = {};
    this.consumers = [];
    this.clientId = config.clientId;
    this.config = config;
    
    this.createTopic(config.topic, config.partitions);
  }

  createTopic(topic, partitions = 3) {
    this.topics[topic] = Array(partitions).fill().map(() => []);
    return true;
  }

  getPartition(key, numPartitions) {
    if (!key) {
      return Math.floor(Math.random() * numPartitions);
    }
    
    const hash = String(key).split('').reduce((acc, char) => {
      return (acc * 31 + char.charCodeAt(0)) & 0x7fffffff;
    }, 0);
    
    return hash % numPartitions;
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

async function produceMessage(key, value, topic = kafkaConfig.topic) {
  console.log(`Producing message: key=${key}, value=${value}, topic=${topic}`);
  const { results } = await producer.send({
    topic,
    messages: [{ key, value }],
  });
  // return the first message metadata for UI
  const { partition, offset } = results[0];
  return { topic, partition, key, value, offset };
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
  fetchNextMessage
};