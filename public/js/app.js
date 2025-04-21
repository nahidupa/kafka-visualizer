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

document.addEventListener('DOMContentLoaded', () => {
  const partitionVis = document.getElementById('partition-visualization');
  const PART_COUNT = 3; // Configurable number of partitions
  setupPartitions(PART_COUNT, partitionVis);

  const msgIn = document.getElementById('message-input');
  const prodBtn = document.getElementById('produce-button');
  const outPro = document.getElementById('producer-output');

  prodBtn.addEventListener('click', () => {
    const value = msgIn.value.trim();
    if (!value) return;
    const key = null; // Placeholder for key-based partitioning
    socket.emit('produce', { key, value, topic: 'visualization-topic' });
    outPro.innerHTML += `<p>→ Sent "${value}"</p>`;
    msgIn.value = '';
  });

  socket.on('message', ({ partition, key, value }) => {
    const partEl = document.getElementById(`partition-${partition}`);
    if (!partEl) return;
    const msgEl = document.createElement('div');
    msgEl.className = 'message';
    msgEl.innerText = `Key:${key} Value:${value}`;
    partEl.appendChild(msgEl);
    fadeIn(msgEl);
  });

  const consBtn = document.getElementById('consume-button');
  const outCon = document.getElementById('consumer-output');
  consBtn.addEventListener('click', () => {
    socket.emit('consume', { topic: 'visualization-topic' });
  });
  
  socket.on('consumeResult', (data) => {
    if (data.error) {
      outCon.innerHTML += `<p>Error: ${data.error}</p>`;
    } else {
      outCon.innerHTML += `<p>← Got "${data.value}" from p${data.partition}</p>`;
    }
  });
});