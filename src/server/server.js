const express = require('express');
const path = require('path');
const http = require('http');
const socketIo = require('socket.io');
const kafkaClient = require('./api/kafka-client');

const app = express();
const server = http.createServer(app);
const io = socketIo(server);
const PORT = process.env.PORT || 3000;

// Serve static files
app.use(express.static(path.join(__dirname, '../../public')));
app.use(express.json());

// API endpoints
app.get('/api/topics', (req, res) => {
  const topics = kafkaClient.getTopics();
  res.json({ topics });
});

app.get('/api/topics/:topic', (req, res) => {
  const topicInfo = kafkaClient.getTopicDetails(req.params.topic);
  res.json(topicInfo);
});

app.post('/api/topics', (req, res) => {
  const { name, partitions } = req.body;
  const result = kafkaClient.createTopic(name, partitions);
  res.json({ success: result });
});

async function startServer() {
  try {
    // Set up simulated Kafka
    await kafkaClient.connectProducer();
    await kafkaClient.connectConsumer();
    
    // Socket.io connection
    io.on('connection', (socket) => {
      console.log('Client connected');
      
      // Handle message production
      socket.on('produce', async ({ key, value, topic }) => {
        try {
          // produce and get metadata
          const result = await kafkaClient.produceMessage(key, value, topic);
          // notify producer
          socket.emit('produceSuccess', result);
          // broadcast to all clients for visualization
          io.emit('message', result);
        } catch (error) {
          socket.emit('produceError', { error: error.message });
        }
      });

      socket.on('disconnect', () => {
        console.log('Client disconnected');
      });

      // Handle single-message consume requests
      socket.on('consume', ({ topic }) => {
        try {
          const msg = kafkaClient.fetchNextMessage(topic);
          socket.emit('consumeResult', msg || { error: 'No available messages' });
        } catch (err) {
          socket.emit('consumeResult', { error: err.message });
        }
      });

      // Peek without committing offset
      socket.on('peek', ({ topic }) => {
        try {
          const msg = kafkaClient.peekNextMessage(topic);
          socket.emit('peekResult', msg || { error: 'No available messages' });
        } catch (err) {
          socket.emit('peekResult', { error: err.message });
        }
      });

      // Commit single message
      socket.on('commitSingle', ({ topic }) => {
        try {
          const rec = kafkaClient.commitSingleMessage(topic);
          socket.emit('commitSingleResult', rec || { error: 'No messages to commit' });
        } catch (err) {
          socket.emit('commitSingleResult', { error: err.message });
        }
      });

      // Commit batch messages
      socket.on('commitBatch', ({ topic }) => {
        try {
          const recs = kafkaClient.commitBatchMessages(topic);
          socket.emit('commitBatchResult', { records: recs });
        } catch (err) {
          socket.emit('commitBatchResult', { error: err.message });
        }
      });
    });
    
    // Manual consumption only; auto-push disabled
    // kafkaClient.consumeMessages((message) => {
    //   io.emit('message', message);
    // });
    
    server.listen(PORT, () => {
      console.log(`Kafka Visualizer running on http://localhost:${PORT}`);
    });
  } catch (error) {
    console.error('Error starting server:', error);
  }
}

startServer();

// Handle graceful shutdown
process.on('SIGINT', async () => {
  try {
    await kafkaClient.disconnectProducer();
    await kafkaClient.disconnectConsumer();
    console.log('Shutdown complete');
    process.exit(0);
  } catch (error) {
    console.error('Error during shutdown:', error);
    process.exit(1);
  }
});