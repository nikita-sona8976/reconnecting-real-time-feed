/**
 * Real-Time Incident Feed Client Application
 * Handles WebSocket lifecycle, linear backoff auto-reconnect,
 * client-side deduplication, and dynamic UI updates.
 */

document.addEventListener('DOMContentLoaded', () => {
  // DOM Elements
  const roomSelect = document.getElementById('roomSelect');
  const connectionStatus = document.getElementById('connectionStatus');
  const btnSeverSocket = document.getElementById('btnSeverSocket');
  const btnRecoverSocket = document.getElementById('btnRecoverSocket');
  const reconnectNotice = document.getElementById('reconnectNotice');
  const reconnectTimerText = document.getElementById('reconnectTimerText');

  const metricLastSeq = document.getElementById('metricLastSeq');
  const metricTotalReceived = document.getElementById('metricTotalReceived');
  const metricDupes = document.getElementById('metricDupes');
  const metricReconnects = document.getElementById('metricReconnects');

  const incidentForm = document.getElementById('incidentForm');
  const incidentMessage = document.getElementById('incidentMessage');
  const severityPicker = document.getElementById('severityPicker');
  const btnPublish = document.getElementById('btnPublish');

  const roomBadge = document.getElementById('roomBadge');
  const btnClearFeed = document.getElementById('btnClearFeed');
  const incidentFeed = document.getElementById('incidentFeed');
  const emptyFeedState = document.getElementById('emptyFeedState');

  // Application State
  let ws = null;
  let currentRoomId = roomSelect.value || 'incident-room-1';
  let lastSeenSeq = 0;
  let selectedSeverity = 'medium';
  
  let totalReceivedCount = 0;
  let dupesDroppedCount = 0;
  let reconnectAttemptCount = 0;

  let isManualDisconnect = false;
  let reconnectTimeoutId = null;
  let countdownIntervalId = null;

  // Initialize Deduplicator
  const deduplicator = new Dedupe(5000);

  // --- Connection State Management ---
  function updateConnectionState(state, message = '') {
    connectionStatus.className = 'status-badge';
    const statusText = connectionStatus.querySelector('.status-text');

    if (state === 'CONNECTED') {
      connectionStatus.classList.add('state-connected');
      statusText.textContent = 'CONNECTED';
      reconnectNotice.classList.add('hidden');
      clearInterval(countdownIntervalId);
    } else if (state === 'RECONNECTING') {
      connectionStatus.classList.add('state-reconnecting');
      statusText.textContent = 'RECONNECTING';
      reconnectNotice.classList.remove('hidden');
    } else { // DISCONNECTED
      connectionStatus.classList.add('state-disconnected');
      statusText.textContent = message || 'DISCONNECTED';
      reconnectNotice.classList.add('hidden');
      clearInterval(countdownIntervalId);
    }
  }

  function updateMetrics() {
    metricLastSeq.textContent = lastSeenSeq;
    metricTotalReceived.textContent = totalReceivedCount;
    metricDupes.textContent = dupesDroppedCount;
    metricReconnects.textContent = reconnectAttemptCount;
  }

  // --- WebSocket Core Logic ---
  function connectWebSocket() {
    if (ws) {
      ws.onopen = null;
      ws.onmessage = null;
      ws.onerror = null;
      ws.onclose = null;
      ws.close();
      ws = null;
    }

    clearTimeout(reconnectTimeoutId);
    clearInterval(countdownIntervalId);

    const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
    const wsUrl = `${protocol}//${window.location.host}`;

    try {
      ws = new WebSocket(wsUrl);
    } catch (e) {
      console.error('WebSocket initialization error:', e);
      scheduleReconnect();
      return;
    }

    ws.onopen = () => {
      console.log(`[WS] Connected to ${wsUrl}. Subscribing to room "${currentRoomId}" with lastSeenSeq=${lastSeenSeq}`);
      updateConnectionState('CONNECTED');
      
      // Send SUBSCRIBE envelope with highest processed sequence
      ws.send(JSON.stringify({
        type: 'SUBSCRIBE',
        roomId: currentRoomId,
        lastSeenSeq: lastSeenSeq
      }));
    };

    ws.onmessage = (event) => {
      try {
        const payload = JSON.parse(event.data);
        handleServerMessage(payload);
      } catch (err) {
        console.error('[WS] Failed to parse message JSON:', err);
      }
    };

    ws.onclose = (event) => {
      console.warn(`[WS] Closed. Code: ${event.code}, Reason: ${event.reason || 'None'}`);
      if (!isManualDisconnect) {
        scheduleReconnect();
      } else {
        updateConnectionState('DISCONNECTED', 'SEVERED BY USER');
      }
    };

    ws.onerror = (err) => {
      console.error('[WS] Socket error encountered:', err);
    };
  }

  // Linear Backoff Reconnection: 2s * attempt, max 10s
  function scheduleReconnect() {
    reconnectAttemptCount++;
    updateConnectionState('RECONNECTING');
    updateMetrics();

    const backoffSeconds = Math.min(10, 2 * reconnectAttemptCount);
    let remainingSeconds = backoffSeconds;

    reconnectTimerText.textContent = `Reconnecting in ${remainingSeconds}s (Attempt ${reconnectAttemptCount})...`;

    countdownIntervalId = setInterval(() => {
      remainingSeconds--;
      if (remainingSeconds > 0) {
        reconnectTimerText.textContent = `Reconnecting in ${remainingSeconds}s (Attempt ${reconnectAttemptCount})...`;
      } else {
        clearInterval(countdownIntervalId);
      }
    }, 1000);

    reconnectTimeoutId = setTimeout(() => {
      if (!isManualDisconnect) {
        connectWebSocket();
      }
    }, backoffSeconds * 1000);
  }

  // --- Server Message Handler ---
  function handleServerMessage(msg) {
    switch (msg.type) {
      case 'SUBSCRIBED':
        console.log(`[WS] Subscribed successfully. Server currentSeq: ${msg.currentSeq}`);
        roomBadge.textContent = `room: ${msg.roomId}`;
        break;

      case 'INCIDENT': {
        const rawEvent = msg.event;
        if (!rawEvent) return;

        totalReceivedCount++;

        // Pass through client deduplicator
        const newEvents = deduplicator.filterNewEvents(rawEvent);

        if (newEvents.length === 0) {
          dupesDroppedCount++;
          console.log(`[Dedupe] Dropped duplicate event ID: ${rawEvent.id} (seq #${rawEvent.seq})`);
        } else {
          const freshEvent = newEvents[0];
          // Advance sequence counter monotonically
          lastSeenSeq = Math.max(lastSeenSeq, freshEvent.seq);
          renderIncidentCard(freshEvent);
        }

        updateMetrics();
        break;
      }

      case 'ERROR':
        console.error('[WS Error]', msg.message);
        break;

      case 'PONG':
        // Heartbeat response
        break;

      default:
        console.log('[WS] Unknown message:', msg);
    }
  }

  // --- Render Incident Cards ---
  function renderIncidentCard(event) {
    if (emptyFeedState) {
      emptyFeedState.style.display = 'none';
    }

    const card = document.createElement('article');
    card.className = `incident-card sev-${event.severity}`;

    const formattedTime = new Date(event.timestamp).toLocaleTimeString([], {
      hour: '2-digit',
      minute: '2-digit',
      second: '2-digit',
      hour12: false
    });

    card.innerHTML = `
      <div class="incident-meta">
        <div class="meta-left">
          <span class="sev-badge sev-${event.severity}">${event.severity}</span>
          <span class="seq-tag">seq #${event.seq}</span>
        </div>
        <span class="timestamp">${formattedTime}</span>
      </div>
      <div class="incident-message">${escapeHtml(event.message)}</div>
      <div class="incident-footer">
        <span>Room: ${escapeHtml(event.roomId)}</span>
        <span class="event-id">id: ${event.id.substring(0, 8)}...</span>
      </div>
    `;

    // Prepend to top of feed
    incidentFeed.insertBefore(card, incidentFeed.firstChild);
  }

  function escapeHtml(str) {
    return String(str)
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;');
  }

  // --- UI Action Event Listeners ---

  // Room Change
  roomSelect.addEventListener('change', (e) => {
    currentRoomId = e.target.value;
    roomBadge.textContent = `room: ${currentRoomId}`;
    
    // Reset deduplicator and feed for new room context
    deduplicator.clear();
    lastSeenSeq = 0;
    totalReceivedCount = 0;
    dupesDroppedCount = 0;
    updateMetrics();

    incidentFeed.innerHTML = '';
    incidentFeed.appendChild(emptyFeedState);
    emptyFeedState.style.display = 'block';

    if (ws && ws.readyState === WebSocket.OPEN) {
      ws.send(JSON.stringify({
        type: 'SUBSCRIBE',
        roomId: currentRoomId,
        lastSeenSeq: 0
      }));
    } else {
      connectWebSocket();
    }
  });

  // Sever Socket Button (Simulate drop)
  btnSeverSocket.addEventListener('click', () => {
    isManualDisconnect = true;
    clearTimeout(reconnectTimeoutId);
    clearInterval(countdownIntervalId);

    if (ws) {
      ws.close(4000, 'User severed connection');
    } else {
      updateConnectionState('DISCONNECTED', 'SEVERED BY USER');
    }
  });

  // Recover Connection Button
  btnRecoverSocket.addEventListener('click', () => {
    isManualDisconnect = false;
    reconnectAttemptCount = 0;
    connectWebSocket();
  });

  // Severity Picker Buttons
  severityPicker.addEventListener('click', (e) => {
    if (e.target.classList.contains('sev-btn')) {
      document.querySelectorAll('.sev-btn').forEach(btn => btn.classList.remove('active'));
      e.target.classList.add('active');
      selectedSeverity = e.target.getAttribute('data-sev');
    }
  });

  // Preset Pills
  document.querySelectorAll('.preset-pill').forEach(pill => {
    pill.addEventListener('click', () => {
      const sev = pill.getAttribute('data-sev');
      const msg = pill.getAttribute('data-msg');
      
      selectedSeverity = sev;
      document.querySelectorAll('.sev-btn').forEach(btn => {
        btn.classList.toggle('active', btn.getAttribute('data-sev') === sev);
      });

      incidentMessage.value = msg;
    });
  });

  // Publish Incident Form Submit
  incidentForm.addEventListener('submit', async (e) => {
    e.preventDefault();
    const msg = incidentMessage.value.trim();
    if (!msg) return;

    btnPublish.disabled = true;

    if (ws && ws.readyState === WebSocket.OPEN) {
      // Send over WebSocket
      ws.send(JSON.stringify({
        type: 'PUBLISH',
        roomId: currentRoomId,
        severity: selectedSeverity,
        message: msg
      }));
      incidentMessage.value = '';
    } else {
      // Fallback to REST API publish if socket is disconnected/severed
      try {
        const response = await fetch(`/api/incidents/${encodeURIComponent(currentRoomId)}/publish`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ severity: selectedSeverity, message: msg })
        });
        if (response.ok) {
          incidentMessage.value = '';
        } else {
          alert('Failed to publish via REST API');
        }
      } catch (err) {
        alert('Error publishing incident: ' + err.message);
      }
    }

    setTimeout(() => {
      btnPublish.disabled = false;
    }, 200);
  });

  // Clear Display
  btnClearFeed.addEventListener('click', () => {
    incidentFeed.innerHTML = '';
    incidentFeed.appendChild(emptyFeedState);
    emptyFeedState.style.display = 'block';
  });

  // Initial Connection
  connectWebSocket();
});
