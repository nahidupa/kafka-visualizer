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
  const peekBtn = document.getElementById('peek-button');
  const commitBtn = document.getElementById('commit-button');
  const consumerOut = document.getElementById('consumer-output');
  const commitRadios = document.querySelectorAll('input[name="commitMode"]');
  const messageDetailsPopup = document.getElementById('message-details-popup');
  const detailsContent = document.querySelector('#message-details-popup .details-content');
  
  // Initialize message input with random message
  msgIn.value = generateRandomMessage();
  
  // Message details popup functionality
  function showMessageDetails(message) {
    const { key, value, partition, offset } = message;
    
    // Format the message details for display
    let formattedDetails = `Key: ${key || 'null'}\n`;
    formattedDetails += `Partition: ${partition}\n`;
    formattedDetails += `Offset: ${offset}\n\n`;
    
    // Add formatted payload
    if (value) {
      if (value.payload) {
        formattedDetails += `Payload: ${value.payload}\n`;
      }
      if (value.id) {
        formattedDetails += `ID: ${value.id}\n`;
      }
      if (value.ts) {
        const date = new Date(value.ts);
        formattedDetails += `Timestamp: ${date.toLocaleString()}\n`;
      }
      if (value.corrupted) {
        formattedDetails += `\n⚠️ CORRUPTED MESSAGE ⚠️`;
      }
    } else {
      formattedDetails += `Value: ${value}\n`;
    }
    
    // Update popup content and show it
    detailsContent.textContent = formattedDetails;
    messageDetailsPopup.style.display = 'block';
  }
  
  // Close popup when clicking the close button
  document.querySelector('#message-details-popup .close-btn')
    .addEventListener('click', () => {
      messageDetailsPopup.style.display = 'none';
    });
  
  // Close popup when clicking outside (optional)
  window.addEventListener('click', (e) => {
    if (e.target !== messageDetailsPopup && 
        !messageDetailsPopup.contains(e.target) && 
        !e.target.classList.contains('message-box')) {
      messageDetailsPopup.style.display = 'none';
    }
  });
  
  // Generate new random message when input is focused or clicked
  msgIn.addEventListener('focus', () => {
    if (!msgIn.value.trim()) {
      msgIn.value = generateRandomMessage();
    }
  });
  
  // Also generate when clicked, for better UX
  msgIn.addEventListener('click', () => {
    msgIn.value = generateRandomMessage();
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
    msgIn.value = generateRandomMessage();
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
      // Create message display similar to partition visualization
      const msgEl = document.createElement('div');
      msgEl.className = 'message-box peeked';
      msgEl.setAttribute('data-key', data.key);
      msgEl.textContent = data.key || 'null';
      
      // Handle corrupted messages
      if (data.value && data.value.corrupted) {
        msgEl.classList.add('corrupted');
      }
      
      // Add click handler to show details
      msgEl.addEventListener('click', () => showMessageDetails(data));
      
      // Add to consumer output
      const peekMsg = document.createElement('div');
      peekMsg.className = 'peek-msg';
      peekMsg.innerHTML = '<strong>Peeked message:</strong> ';
      peekMsg.appendChild(msgEl);
      
      consumerOut.innerHTML = '';
      consumerOut.appendChild(peekMsg);
      commitBtn.disabled = false;
      
      // Highlight the peeked message in the partition
      const partitionEl = document.getElementById(`partition-${data.partition}`);
      if (partitionEl) {
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
      // Create a container for the commit result
      const commitMsg = document.createElement('div');
      commitMsg.className = 'commit-msg';
      
      // Add message boxes using the same style as partitions
      const msgEl = document.createElement('div');
      msgEl.className = 'message-box committed';
      msgEl.setAttribute('data-key', data.key);
      
      if (data.value && data.value.corrupted) {
        msgEl.classList.add('corrupted');
      }
      
      msgEl.textContent = data.key || 'null';
      
      // Add click handler to show details
      msgEl.addEventListener('click', () => showMessageDetails(data));
      
      // Add message to commit message container
      commitMsg.innerHTML = `<strong>Committed:</strong> `;
      commitMsg.appendChild(msgEl);
      
      // Add to consumer output
      consumerOut.appendChild(commitMsg);
      commitBtn.disabled = true;
      
      // Handle dead letter queue display
      if (data.deadLetter || (data.value && data.value.corrupted)) {
        const deadLetterPanel = document.getElementById('dead-letter-panel');
        const deadLetterMessages = document.getElementById('dead-letter-messages');
        
        deadLetterPanel.style.display = 'block';
        
        // Create a message in the dead letter queue with the same styling
        const dlMsg = document.createElement('div');
        dlMsg.className = 'dead-letter-msg';
        
        const dlEl = document.createElement('div');
        dlEl.className = 'message-box corrupted';
        dlEl.setAttribute('data-key', data.key);
        dlEl.textContent = data.key || 'null';
        
        // Add click handler for details
        dlEl.addEventListener('click', () => showMessageDetails({
          ...data,
          deadLetterReason: 'Corrupted message'
        }));
        
        dlMsg.appendChild(document.createTextNode('Dead Letter: '));
        dlMsg.appendChild(dlEl);
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
      
      // Create batch commit container
      const batchMsg = document.createElement('div');
      batchMsg.className = 'commit-msg batch-commit';
      batchMsg.innerHTML = `<strong>Batch committed ${data.records.length} messages:</strong><br>`;
      
      // Create a container for the message boxes
      const msgContainer = document.createElement('div');
      msgContainer.className = 'message-container';
      
      // Add each message as a message box
      data.records.forEach(record => {
        const msgEl = document.createElement('div');
        msgEl.className = 'message-box committed';
        msgEl.setAttribute('data-key', record.key);
        
        if (record.deadLetter || (record.value && record.value.corrupted)) {
          msgEl.classList.add('corrupted');
        }
        
        msgEl.textContent = record.key || 'null';
        
        // Add click handler to show details
        msgEl.addEventListener('click', () => showMessageDetails(record));
        
        msgContainer.appendChild(msgEl);
        
        // Handle dead letter queue for corrupted records
        if (record.deadLetter || (record.value && record.value.corrupted)) {
          const deadLetterPanel = document.getElementById('dead-letter-panel');
          const deadLetterMessages = document.getElementById('dead-letter-messages');
          
          deadLetterPanel.style.display = 'block';
          
          // Create a message in the dead letter queue with the same styling
          const dlMsg = document.createElement('div');
          dlMsg.className = 'dead-letter-msg';
          
          const dlEl = document.createElement('div');
          dlEl.className = 'message-box corrupted';
          dlEl.setAttribute('data-key', record.key);
          dlEl.textContent = record.key || 'null';
          
          // Add click handler for details
          dlEl.addEventListener('click', () => showMessageDetails({
            ...record,
            deadLetterReason: 'Corrupted message in batch'
          }));
          
          dlMsg.appendChild(document.createTextNode('Dead Letter: '));
          dlMsg.appendChild(dlEl);
          deadLetterMessages.appendChild(dlMsg);
        }
      });
      
      // Add summary message
      const summary = document.createElement('div');
      summary.className = 'batch-summary';
      summary.textContent = `${goodCount} successful, ${badCount} to dead-letter queue`;
      
      batchMsg.appendChild(msgContainer);
      batchMsg.appendChild(summary);
      consumerOut.appendChild(batchMsg);
      
      commitBtn.disabled = true;
    } else {
      consumerOut.innerHTML += `<p class="commit-msg">No messages to commit in batch</p>`;
    }
    
    // Refresh status
    socket.emit('getStatus', { topic: 'visualization-topic' });
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
    
    // Add click handler to show details
    msgEl.addEventListener('click', () => {
      showMessageDetails({ partition, key, value, offset });
    });
    
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

  // Add scenario button handlers
  const autoProduceBtn = document.getElementById('auto-produce-25');
  const singleScenarioBtn = document.getElementById('single-scenario');
  const batchScenarioBtn = document.getElementById('batch-scenario');
  const deadLetterPanel = document.getElementById('dead-letter-panel');
  const deadLetterMessages = document.getElementById('dead-letter-messages');
  
  // Auto-produce 25 messages
  autoProduceBtn?.addEventListener('click', () => {
    console.log('Auto producing 25 messages');
    let count = 25;
    const interval = setInterval(() => {
      if (count <= 0) {
        clearInterval(interval);
        return;
      }
      
      const value = generateRandomMessage();
      const key = ['US', 'FR', 'DE'][Math.floor(Math.random() * 3)];
      socket.emit('produce', { key, value, topic: 'visualization-topic' });
      
      count--;
    }, 100);
  });
  
  // Single commit scenario
  singleScenarioBtn?.addEventListener('click', () => {
    // Reset sequence counter for demo clarity
    messageSequence = 0;
    console.log('Running single commit scenario');
    
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
  
  // Helper function for single commit scenario
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
      setTimeout(() => {
        socket.emit('commitSingle', { topic: 'visualization-topic' });
        
        // Wait for commit result then continue
        socket.once('commitSingleResult', (result) => {
          setTimeout(() => consumeSingleMessages(count + 1), 300);
        });
      }, 200);
    });
  }
  
  // Batch commit scenario
  batchScenarioBtn?.addEventListener('click', () => {
    // Reset sequence counter for demo clarity
    messageSequence = 0;
    console.log('Running batch commit scenario');
    
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
  
  // Helper function for batch commit scenario
  function peekAllMessages(count = 0) {
    if (count >= 25) {
      consumerOut.innerHTML += '<p>All messages peeked. Committing in batch...</p>';
      
      // Commit all peeked messages in batch
      setTimeout(() => {
        socket.emit('commitBatch', { topic: 'visualization-topic' });
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
});