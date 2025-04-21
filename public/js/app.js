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

// Generate a random message value (for demo)
function generateRandomMessage() {
  return Math.random().toString(36).substr(2, 8);
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
        statusTableBody.innerHTML = `<tr><td colspan=\"3\">Error: ${data.error}</td></tr>`;
      } else {
        statusTableBody.innerHTML = data.partitions.map(p => 
          `<tr>
            <td>${p.partition}</td>
            <td>${p.committedOffset}</td>
            <td>${p.pendingCount}</td>
          </tr>`
        ).join('');
      }
    });
    // request status every 2 seconds
    setInterval(() => socket.emit('getStatus', { topic: 'visualization-topic' }), 2000);
    // initial status request
    socket.emit('getStatus', { topic: 'visualization-topic' });
  })();
});