// filepath: /kafka-visualizer/kafka-visualizer/public/js/app.js
import { fadeIn, fadeOut } from './animation.js';

// At the beginning of your file after imports
const socket = io();

// Add this logging to debug
socket.on('connect', () => {
  console.log('Connected to server');
});

socket.on('error', (error) => {
  console.error('Socket error:', error);
});

// Fix the setupPartitions function
export function setupPartitions(count, containerEl) {
  containerEl.innerHTML = '';
  
  for (let i = 0; i < count; i++) {
    // Create partition container
    const partitionContainer = document.createElement('div');
    partitionContainer.className = 'partition-container';
    
    // Create partition label
    const label = document.createElement('div');
    label.className = 'partition-label';
    label.textContent = `Partition ${i}`;
    partitionContainer.appendChild(label);
    
    // Create the actual partition div where messages will go
    const partition = document.createElement('div');
    partition.id = `partition-${i}`;
    partition.className = 'partition';
    partition.setAttribute('data-partition', i);
    partitionContainer.appendChild(partition);
    
    // Add the container to the main visualization area
    containerEl.appendChild(partitionContainer);
  }
}

// Add sequence counter for message ordering demonstration
let messageSequence = 0;

// Generate a random message value (for demo)
function generateRandomMessage() {
  messageSequence++;
  const products = ['Apple', 'Banana', 'Orange', 'Mango', 'Pear'];
  const actions = ['ordered', 'shipped', 'delivered', 'returned', 'canceled'];
  const product = products[Math.floor(Math.random() * products.length)];
  const action = actions[Math.floor(Math.random() * actions.length)];
  return `${product} ${action} #${messageSequence}`;
}

// Visualize a message in a partition
function visualizeMessage(partitionId, message, offset, type = 'produced') {
  const partitionEl = document.getElementById(`partition-${partitionId}`);
  if (!partitionEl) return null;
  
  // Create message box (array-cell style)
  const msgEl = document.createElement('div');
  msgEl.className = `message-box ${type}`;
  msgEl.setAttribute('data-offset', offset);
  
  // Message content - simplify to fit in small box
  const key = message.key || 'null';
  const keySpan = document.createElement('span');
  keySpan.className = 'key';
  keySpan.textContent = key;
  
  const valueSpan = document.createElement('span');
  valueSpan.className = 'value';
  valueSpan.textContent = message.value;
  
  msgEl.appendChild(keySpan);
  msgEl.appendChild(document.createTextNode(':'));
  msgEl.appendChild(valueSpan);
  
  // Add to partition container
  partitionEl.appendChild(msgEl);
  
  // Apply fade-in effect
  fadeIn(msgEl);
  
  return msgEl;
}

document.addEventListener('DOMContentLoaded', () => {
  // Initialize partition visualization
  const partitionVis = document.getElementById('partition-visualization');
  const PART_COUNT = 3; // Default partition count
  setupPartitions(PART_COUNT, partitionVis);

  // UI elements
  const msgIn = document.getElementById('message-input');
  const prodBtn = document.getElementById('produce-button');
  const outPro = document.getElementById('producer-output');
  const peekBtn = document.getElementById('peek-button');
  const commitBtn = document.getElementById('commit-button');
  const consumerOut = document.getElementById('consumer-output');
  const commitRadios = document.querySelectorAll('input[name="commitMode"]');
  
  // Initialize message input with random message
  msgIn.value = generateRandomMessage();
  
  // Generate new random message when input is focused while empty
  msgIn.addEventListener('focus', () => {
    if (!msgIn.value.trim()) {
      msgIn.value = generateRandomMessage();
    }
  });
  
  let commitMode = 'single';
  commitRadios.forEach(radio => {
    radio.addEventListener('change', (e) => {
      commitMode = e.target.value;
    });
  });

  // Produce message handler
  prodBtn.addEventListener('click', () => {
    const value = msgIn.value.trim() || generateRandomMessage();
    socket.emit('produce', { key: null, value, topic: 'visualization-topic' });
    outPro.innerHTML += `<p>→ Produced: "${value}"</p>`;
    outPro.scrollTop = outPro.scrollHeight;
    msgIn.value = '';
  });

  // Peek message handler
  peekBtn.addEventListener('click', () => {
    socket.emit('peek', { topic: 'visualization-topic' });
  });

  // Listen for peek results
  socket.on('peekResult', (data) => {
    consumerOut.style.display = 'block';
    
    if (data.error) {
      consumerOut.innerHTML = `<p class="error">Error: ${data.error}</p>`;
      commitBtn.disabled = true;
    } else {
      consumerOut.innerHTML = `<p class="peek-msg">Peeked message from partition ${data.partition}:
        <span class="msg-key">${data.key}</span> → 
        <span class="msg-value">${JSON.stringify(data.value)}</span>
        ${data.value?.corrupted ? '<span class="corrupted-flag">CORRUPTED</span>' : ''}
      </p>`;
      commitBtn.disabled = false;
      
      // Highlight the peeked message in the partition
      const partitionEl = document.getElementById(`partition-${data.partition}`);
      if (partitionEl) {
        // Find message at correct offset
        const messages = partitionEl.querySelectorAll('.message-box');
        const targetMsg = Array.from(messages).find(msg => 
          parseInt(msg.getAttribute('data-offset')) === data.offset
        );
        
        if (targetMsg) {
          targetMsg.classList.add('peeked');
        }
      }
    }
  });

  // Commit message handler  
  commitBtn.addEventListener('click', () => {
    const mode = Array.from(commitRadios).find(r => r.checked)?.value || 'single';
    
    if (mode === 'batch') {
      socket.emit('commitBatch', { topic: 'visualization-topic' });
    } else {
      socket.emit('commitSingle', { topic: 'visualization-topic' });
    }
  });

  // Handle commit results
  socket.on('commitSingleResult', (data) => {
    if (data.error) {
      consumerOut.innerHTML += `<p class="error">Error: ${data.error}</p>`;
    } else {
      consumerOut.innerHTML += `<p class="commit-msg">
        Committed message from partition ${data.partition}
        ${data.deadLetter ? ' (moved to dead-letter queue)' : ''}
      </p>`;
      
      commitBtn.disabled = true;
      
      // Notify if this was moved to dead-letter
      if (data.value && data.value.corrupted) {
        deadLetterPanel.style.display = 'block';
        const dlMsg = document.createElement('div');
        dlMsg.className = 'dead-letter-msg';
        dlMsg.innerHTML = `Key: ${data.key}, Partition: ${data.partition}, 
                          Reason: Corrupted, Value: ${JSON.stringify(data.value)}`;
        deadLetterMessages.appendChild(dlMsg);
      }
    }
    
    // Refresh status
    socket.emit('getStatus', { topic: 'visualization-topic' });
  });

  socket.on('commitBatchResult', (data) => {
    if (data.error) {
      consumerOut.innerHTML += `<p class="error">Error: ${data.error}</p>`;
    } else if (data.records && data.records.length > 0) {
      const goodCount = data.records.filter(r => !r.deadLetter).length;
      const badCount = data.records.filter(r => r.deadLetter).length;
      
      consumerOut.innerHTML += `<p class="commit-msg">
        Batch committed ${data.records.length} messages:
        ${goodCount} successful, ${badCount} to dead-letter queue
      </p>`;
      
      commitBtn.disabled = true;
      
      // Add any dead-letter messages
      if (badCount > 0) {
        deadLetterPanel.style.display = 'block';
        
        data.records.filter(r => r.deadLetter).forEach(record => {
          const dlMsg = document.createElement('div');
          dlMsg.className = 'dead-letter-msg';
          dlMsg.innerHTML = `Key: ${record.key}, Partition: ${record.partition}, 
                            Reason: Corrupted, Value: ${JSON.stringify(record.value)}`;
          deadLetterMessages.appendChild(dlMsg);
        });
      }
    } else {
      consumerOut.innerHTML += `<p class="commit-msg">No messages to commit in batch</p>`;
    }
    
    // Refresh status
    socket.emit('getStatus', { topic: 'visualization-topic' });
  });

  // Demo/auto produce button
  const autoProduce = document.getElementById('auto-produce-25');
  autoProduce.addEventListener('click', () => {
    let count = 25;
    const interval = setInterval(() => {
      const randomKey = Math.floor(Math.random() * 5); // 0-4 for demo key-based routing
      const randomValue = generateRandomMessage();
      socket.emit('produce', { 
        key: randomKey.toString(),
        value: randomValue,
        topic: 'visualization-topic'
      });
      count--;
      if (count === 0) clearInterval(interval);
    }, 100);
  });

  // Show dead-letter messages when emitted
  const deadLetterPanel = document.getElementById('dead-letter-panel');
  const deadLetterMessages = document.getElementById('dead-letter-messages');
  socket.on('deadLetterMessage', (data) => {
    deadLetterPanel.style.display = 'block';
    const el = document.createElement('div');
    el.innerText = `DL p${data.partition}: ${JSON.stringify(data.value)}`;
    deadLetterMessages.appendChild(el);
  });

  // Add scenario button handlers
  const singleScenarioBtn = document.getElementById('single-scenario');
  const batchScenarioBtn = document.getElementById('batch-scenario');
  
  // Single commit scenario (produce 25 messages, then peek + commit one by one)
  singleScenarioBtn.addEventListener('click', () => {
    // Reset sequence counter for demo clarity
    messageSequence = 0;
    
    // Clear previous output
    consumerOut.innerHTML = '<p><strong>Single Commit Scenario Started</strong></p>';
    consumerOut.style.display = 'block';
    
    // Step 1: Produce 25 messages
    let produced = 0;
    const produceInterval = setInterval(() => {
      if (produced >= 25) {
        clearInterval(produceInterval);
        consumerOut.innerHTML += '<p>Production complete. Starting single commits...</p>';
        
        // Step 2: Start consuming one by one with single commits
        consumeSingleMessages();
        return;
      }
      
      const value = generateRandomMessage();
      socket.emit('produce', { 
        key: ['US', 'FR', 'DE'][Math.floor(Math.random() * 3)], // Use country codes
        value, 
        topic: 'visualization-topic' 
      });
      
      produced++;
    }, 100);
  });
  
  // Consume messages one by one with single commits
  function consumeSingleMessages(count = 0) {
    if (count >= 25) {
      consumerOut.innerHTML += '<p><strong>Single Commit Scenario Complete</strong></p>';
      return;
    }
    
    // Peek next message
    socket.emit('peek', { topic: 'visualization-topic' });
    
    // Wait for peek result then commit
    socket.once('peekResult', (data) => {
      if (data.error) {
        consumerOut.innerHTML += `<p>Scenario ended: ${data.error}</p>`;
        return;
      }
      
      // Commit the peeked message
      socket.emit('commitSingle', { topic: 'visualization-topic' });
      
      // Wait for commit result then continue
      socket.once('commitSingleResult', (result) => {
        setTimeout(() => consumeSingleMessages(count + 1), 200);
      });
    });
  }
  
  // Batch commit scenario (produce 25 messages, peek all, then batch commit)
  batchScenarioBtn.addEventListener('click', () => {
    // Reset sequence counter for demo clarity
    messageSequence = 0;
    
    // Clear previous output
    consumerOut.innerHTML = '<p><strong>Batch Commit Scenario Started</strong></p>';
    consumerOut.style.display = 'block';
    
    // Step 1: Produce 25 messages
    let produced = 0;
    const produceInterval = setInterval(() => {
      if (produced >= 25) {
        clearInterval(produceInterval);
        consumerOut.innerHTML += '<p>Production complete. Starting batch peek...</p>';
        
        // Step 2: Peek all messages
        peekAllMessages();
        return;
      }
      
      const value = generateRandomMessage();
      socket.emit('produce', { 
        key: ['US', 'FR', 'DE'][Math.floor(Math.random() * 3)], // Use country codes
        value, 
        topic: 'visualization-topic' 
      });
      
      produced++;
    }, 100);
  });
  
  // Peek all messages before committing in batch
  function peekAllMessages(count = 0) {
    if (count >= 25) {
      consumerOut.innerHTML += '<p>All messages peeked. Committing in batch...</p>';
      
      // Commit all peeked messages in batch
      setTimeout(() => {
        socket.emit('commitBatch', { topic: 'visualization-topic' });
        
        socket.once('commitBatchResult', (result) => {
          if (result.error) {
            consumerOut.innerHTML += `<p>Batch commit error: ${result.error}</p>`;
          } else {
            const goodCount = (result.records || []).filter(r => !r.deadLetter).length;
            const badCount = (result.records || []).filter(r => r.deadLetter).length;
            
            consumerOut.innerHTML += `<p><strong>Batch Commit Complete:</strong> ${goodCount} successful, ${badCount} to dead-letter queue</p>`;
          }
        });
      }, 500);
      
      return;
    }
    
    // Peek next message
    socket.emit('peek', { topic: 'visualization-topic' });
    
    // Wait for peek result then continue peeking
    socket.once('peekResult', (data) => {
      if (data.error) {
        consumerOut.innerHTML += `<p>Peeking ended: ${data.error}</p>`;
        
        // If we can't peek any more messages, commit what we have
        if (count > 0) {
          setTimeout(() => {
            socket.emit('commitBatch', { topic: 'visualization-topic' });
          }, 500);
        }
        
        return;
      }
      
      setTimeout(() => peekAllMessages(count + 1), 100);
    });
  }

  // Handle incoming messages from server
  socket.on('message', ({ partition, key, value, offset }) => {
    console.log(`Received message for partition ${partition}:`, { key, value, offset });
    
    const partitionEl = document.getElementById(`partition-${partition}`);
    if (!partitionEl) {
      console.error(`Partition element not found: partition-${partition}`);
      return;
    }
    
    // Create message element
    const msgEl = document.createElement('div');
    msgEl.className = 'message-box';
    msgEl.setAttribute('data-key', key); // Set key as data attribute for CSS selector
    msgEl.setAttribute('data-offset', offset || partitionEl.children.length);
    
    // Check for corrupted messages
    if (value && value.corrupted) {
      msgEl.classList.add('corrupted');
    }
    
    // Display just the key in the box
    msgEl.textContent = key || 'null';
    
    // Add tooltip with full details
    const tooltip = document.createElement('div');
    tooltip.className = 'tooltip';
    
    // Format payload data for display
    let payloadStr = 'undefined';
    if (value && value.payload) {
      payloadStr = typeof value.payload === 'object' 
        ? JSON.stringify(value.payload) 
        : value.payload;
    }
    
    tooltip.innerHTML = `
      <strong>Key:</strong> ${key || 'null'}<br>
      <strong>Payload:</strong> ${payloadStr}<br>
      <strong>ID:</strong> ${value?.id || 'n/a'}<br>
      <strong>Offset:</strong> ${offset || 'n/a'}<br>
      ${value?.corrupted ? '<strong style="color:#f44336">CORRUPTED</strong>' : ''}
    `;
    
    msgEl.appendChild(tooltip);
    partitionEl.appendChild(msgEl);
  });

  // Auto-refresh consumer group status
  (function() {
    const statusPanel = document.getElementById('consumer-status');
    const statusTableBody = document.querySelector('#status-table tbody');
    statusPanel.style.display = 'block';
    
    // handle status updates
    socket.on('statusResult', (data) => {
      if (data.error) {
        statusTableBody.innerHTML = `<tr><td colspan=\"4\">Error: ${data.error}</td></tr>`;
      } else {
        statusTableBody.innerHTML = data.partitions.map(p => {
          // Determine status text and color
          let status = 'Up to date';
          let statusClass = 'status-ok';
          
          if (p.pendingCount > 0) {
            status = `${p.pendingCount} message(s) peeked`;
            statusClass = 'status-pending';
          }
          
          return `<tr>
            <td>Partition ${p.partition}</td>
            <td>${p.committedOffset} message(s)</td>
            <td>${p.pendingCount > 0 ? `<span class="pending-count">${p.pendingCount}</span>` : '0'}</td>
            <td><span class="${statusClass}">${status}</span></td>
          </tr>`;
        }).join('');
      }
    });
    
    // request status every 2 seconds
    setInterval(() => socket.emit('getStatus', { topic: 'visualization-topic' }), 2000);
    
    // initial status request
    socket.emit('getStatus', { topic: 'visualization-topic' });
  })();
});