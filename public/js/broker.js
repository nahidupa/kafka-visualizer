// filepath: /Users/nahidul.kibria/Documents/gitclone/kafka-visualizer/kafka-visualizer/public/js/broker.js
class Broker {
  constructor() {
    this.topics = {};
  }

  createTopic(topicName, partitions) {
    if (!this.topics[topicName]) {
      this.topics[topicName] = Array.from({ length: partitions }, () => []);
      return true;
    }
    return false;
  }

  produceMessage(topic, message) {
    if (!this.topics[topic]) {
      throw new Error(`Topic ${topic} does not exist`);
    }

    const partition = this.getPartition(message.key, this.topics[topic].length);
    this.topics[topic][partition].push(message);
    return { partition, message };
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

  consumeMessage(topic, partition) {
    if (!this.topics[topic] || !this.topics[topic][partition]) {
      throw new Error(`No messages in topic ${topic} partition ${partition}`);
    }

    return this.topics[topic][partition].shift();
  }

  getTopicDetails(topic) {
    if (!this.topics[topic]) {
      throw new Error(`Topic ${topic} does not exist`);
    }

    return {
      name: topic,
      partitions: this.topics[topic].map((partition, index) => ({
        id: index,
        messageCount: partition.length,
      })),
    };
  }
}

const broker = new Broker();
export default broker;