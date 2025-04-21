# Kafka Visualizer

## Overview
The Kafka Visualizer is a web application that demonstrates how Apache Kafka works, including message production, consumption, and partitioning. This project serves as a tutorial for users to learn about Kafka's architecture and functionality through a visual interface.

## Features
- **Message Production**: Users can produce messages to a configurable Kafka topic.
- **Partition Visualization**: The application visualizes how messages are distributed across Kafka partitions based on key-based partitioning.
- **Message Consumption**: Users can consume messages from the topic and see how offsets are managed.
- **Configurable Topics and Partitions**: Users can create topics with a specified number of partitions.
- **Batch and Single Commits**: The application demonstrates both batch and single message commits.
- **Real-time Updates**: The UI updates in real-time to reflect the state of the Kafka broker and the messages in each partition.

## Project Structure
```
kafka-visualizer
├── public
│   ├── index.html          # Main HTML entry point
│   ├── css
│   │   └── styles.css      # Styles for the application
│   └── js
│       ├── app.js          # Main JavaScript logic
│       ├── broker.js       # Simulates the Kafka broker
│       └── animation.js     # Animation effects for message visualizations
├── src
│   ├── config
│   │   └── kafka.js        # Kafka configuration settings
│   └── server
│       ├── api
│       │   └── kafka-client.js # Simulates the Kafka client
│       └── server.js       # Sets up the Express server and socket.io connection
├── .env                     # Environment variables for configuration
├── package.json             # npm configuration file
└── README.md                # Project documentation
```

## Installation
1. Clone the repository:
   ```
   git clone <repository-url>
   cd kafka-visualizer
   ```

2. Install the dependencies:
   ```
   npm install
   ```

3. Create a `.env` file in the root directory and configure the necessary environment variables:
   ```
   KAFKA_CLIENT_ID=<your-client-id>
   KAFKA_BROKERS=<your-broker-address>
   KAFKA_TOPIC=<your-default-topic>
   KAFKA_CONSUMER_GROUP=<your-consumer-group>
   ```

## Usage
1. Start the server:
   ```
   npm start
   ```

2. Open your web browser and navigate to `http://localhost:3000`.

3. Use the interface to produce messages, visualize partitions, and consume messages.

## Contributing
Contributions are welcome! Please open an issue or submit a pull request for any enhancements or bug fixes.

## License
This project is licensed under the MIT License. See the LICENSE file for details.