// filepath: /kafka-visualizer/kafka-visualizer/public/js/app.js
import { fadeIn, fadeOut } from './animation.js';

export function setupPartitions(count, containerEl) {
  containerEl.innerHTML = '';
  for (let i = 0; i < count; i++) {
    const d = document.createElement('div');
    d.id = `partition-${i}`;
    d.className = 'partition';
    d.innerText = `Partition ${i}`;
    containerEl.appendChild(d);
  }
}

const socket = io();

// Generate a random message string
function generateRandomMessage() {
  return Math.random().toString(36).substr(2, 8);
}

document.addEventListener('DOMContentLoaded', () => {
  const partitionVis = document.getElementById('partition-visualization');
  const PART_COUNT = 3; // Configurable number of partitions
  setupPartitions(PART_COUNT, partitionVis);

  const msgIn = document.getElementById('message-input');
  const prodBtn = document.getElementById('produce-button');
  const outPro = document.getElementById('producer-output');
  const peekBtn = document.getElementById('peek-button');
  const commitBtn = document.getElementById('commit-button');
  const consumerOut = document.getElementById('consumer-output');
  const commitModeEls = document.getElementsByName('commitMode');

  // initialize input with random message
  msgIn.value = generateRandomMessage();

  prodBtn.addEventListener('click', () => {
    const value = msgIn.value.trim();
    if (!value) return;
    const key = null; // Placeholder for key-based partitioning
    socket.emit('produce', { key, value, topic: 'visualization-topic' });
    outPro.innerHTML += `<p>→ Sent "${value}"</p>`;
    msgIn.value = generateRandomMessage();
  });

  socket.on('message', ({ partition, key, value }) => {
    const partEl = document.getElementById(`partition-${partition}`);
    if (!partEl) return;
    const msgEl = document.createElement('div');
    msgEl.className = 'message fade-in';
    msgEl.innerHTML = `<strong class='key'>Key:</strong><span class='value'>${key}</span> ` +
                      `<strong class='key'>Value:</strong><span class='value'>${value}</span>`;
    partEl.appendChild(msgEl);
  });

  // Hide consumer output until first peek/commit
  consumerOut.style.display = 'none';

  // Peek logic
  peekBtn.addEventListener('click', () => {
    socket.emit('peek', { topic: 'visualization-topic' });
  });

  socket.on('peekResult', (data) => {
    consumerOut.style.display = 'block';
    consumerOut.innerHTML = '';
    if (data.error) {
      consumerOut.innerHTML = `<p>Error: ${data.error}</p>`;
      commitBtn.disabled = true;
    } else {
      consumerOut.innerHTML = `<p>Peeked at "${data.value}" in p${data.partition}</p>`;
      commitBtn.disabled = false;
    }
  });

  // Commit logic
  commitBtn.addEventListener('click', () => {
    const mode = Array.from(commitModeEls).find(el => el.checked).value;
    if (mode === 'single') {
      socket.emit('commitSingle', { topic: 'visualization-topic' });
    } else {
      socket.emit('commitBatch', { topic: 'visualization-topic' });
    }
  });

  socket.on('commitSingleResult', (data) => {
    consumerOut.innerHTML += data.error
      ? `<p>Error: ${data.error}</p>`
      : `<p>Committed single at p${data.partition} offset ${data.offset}</p>`;
    commitBtn.disabled = true;
  });

  socket.on('commitBatchResult', (res) => {
    if (res.error) {
      consumerOut.innerHTML += `<p>Error: ${res.error}</p>`;
    } else {
      res.records.forEach(rec => {
        consumerOut.innerHTML += `<p>Committed p${rec.partition} offset ${rec.offset}</p>`;
      });
    }
    commitBtn.disabled = true;
  });
});