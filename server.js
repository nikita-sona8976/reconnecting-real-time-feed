const express = require('express');
const http = require('http');
const { WebSocketServer, WebSocket } = require('ws');
const path = require('path');
const crypto = require('crypto');

const PORT = process.env.PORT || 3000;
const MAX_LEDGER_SIZE = 1000;
const DEFAULT_LIMIT = 500;

// In-memory data store for rooms
// Map<roomId, { seq: number, ledger: Array<Object>, subscribers: Set<WebSocket> }>
const rooms = new Map();

/**
 * Get or create a room state object.
 * @param {string} roomId 
 * @returns {Object} { seq, ledger, subscribers }
 */
function getRoom(roomId) {
  if (!roomId) roomId = 'default-room';
  if (!rooms.has(roomId)) {
    rooms.set(roomId, {
      seq: 0,
      ledger: [],
      subscribers: new Set()
    });
  }
  return rooms.get(roomId);
}

/**
 * Filter ledger for missed updates after a given sequence number.
 * @param {string} roomId 
 * @param {number} afterSeq 
 * @param {number} limit 
 * @returns {Array<Object>}
 */
function getMissedUpdates(roomId, afterSeq = 0, limit = DEFAULT_LIMIT) {
  const room = getRoom(roomId);
  const parsedAfterSeq = Math.max(0, parseInt(afterSeq, 10) || 0);
  const clampedLimit = Math.min(Math.max(parseInt(limit, 10) || DEFAULT_LIMIT, 1), 500);

  return room.ledger
    .filter(event => event.seq > parsedAfterSeq)
    .slice(0, clampedLimit);
}

/**
 * Publish an incident to a room, assigning seq and broadcasting to subscribers.
 * @param {string} roomId 
 * @param {string} severity 
 * @param {string} message 
 * @returns {Object} The created incident event object
 */
function publishIncident(roomId, severity, message) {
  const room = getRoom(roomId);
  
  // Assign server-side sequence counter synchronously
  room.seq += 1;

  const validSeverities = ['low', 'medium', 'high', 'critical'];
  const normalizedSeverity = validSeverities.includes(String(severity).toLowerCase())
    ? String(severity).toLowerCase()
    : 'medium';

  const event = {
    id: crypto.randomUUID(),
    seq: room.seq,
    roomId: roomId || 'default-room',
    severity: normalizedSeverity,
    message: String(message || 'No details provided').trim(),
    timestamp: new Date().toISOString()
  };

  // Ring buffer: append event, shift oldest if exceeding MAX_LEDGER_SIZE
  room.ledger.push(event);
  if (room.ledger.length > MAX_LEDGER_SIZE) {
    room.ledger.shift();
  }

  // Broadcast to all active subscribers in room
  const payload = JSON.stringify({ type: 'INCIDENT', event });
  for (const client of room.subscribers) {
    if (client.readyState === WebSocket.OPEN) {
      client.send(payload);
    }
  }

  return event;
}

// Create Express app
const app = express();
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

// REST Endpoints
app.get('/api/health', (req, res) => {
  res.json({
    status: 'ok',
    uptime: process.uptime(),
    activeRooms: rooms.size,
    timestamp: new Date().toISOString()
  });
});

app.get('/api/incidents/:roomId/updates', (req, res) => {
  const { roomId } = req.params;
  const afterSeq = parseInt(req.query.afterSeq, 10) || 0;
  const limit = parseInt(req.query.limit, 10) || DEFAULT_LIMIT;

  const room = getRoom(roomId);
  const events = getMissedUpdates(roomId, afterSeq, limit);

  res.json({
    roomId,
    currentSeq: room.seq,
    afterSeq,
    count: events.length,
    events
  });
});

app.post('/api/incidents/:roomId/publish', (req, res) => {
  const { roomId } = req.params;
  const { severity, message } = req.body || {};

  if (!message || typeof message !== 'string' || !message.trim()) {
    return res.status(400).json({ error: 'Message is required and must be a non-empty string.' });
  }

  const event = publishIncident(roomId, severity, message);
  res.status(201).json({ success: true, event });
});

/**
 * Setup WebSocket server attached to an HTTP server instance.
 * @param {http.Server} httpServer 
 * @returns {WebSocketServer}
 */
function setupWebSocketServer(httpServer) {
  const wss = new WebSocketServer({ server: httpServer });

  wss.on('connection', (ws) => {
    ws.isAlive = true;
    ws.currentRoomId = null;

    ws.on('pong', () => {
      ws.isAlive = true;
    });

    ws.on('message', (rawMessage) => {
      try {
        const data = JSON.parse(rawMessage.toString());
        
        if (!data || typeof data !== 'object') {
          return ws.send(JSON.stringify({ type: 'ERROR', message: 'Invalid message payload' }));
        }

        switch (data.type) {
          case 'SUBSCRIBE': {
            const roomId = data.roomId || 'default-room';
            const lastSeenSeq = Math.max(0, parseInt(data.lastSeenSeq, 10) || 0);

            // Remove from previous room if subscribed elsewhere
            if (ws.currentRoomId && rooms.has(ws.currentRoomId)) {
              rooms.get(ws.currentRoomId).subscribers.delete(ws);
            }

            const room = getRoom(roomId);
            room.subscribers.add(ws);
            ws.currentRoomId = roomId;

            // Confirm subscription
            ws.send(JSON.stringify({
              type: 'SUBSCRIBED',
              roomId,
              lastSeenSeq,
              currentSeq: room.seq
            }));

            // Replay missed events if client missed any
            const missedEvents = getMissedUpdates(roomId, lastSeenSeq, DEFAULT_LIMIT);
            for (const event of missedEvents) {
              ws.send(JSON.stringify({ type: 'INCIDENT', event }));
            }
            break;
          }

          case 'PUBLISH': {
            const roomId = data.roomId || ws.currentRoomId || 'default-room';
            const { severity, message } = data;

            if (!message || typeof message !== 'string' || !message.trim()) {
              return ws.send(JSON.stringify({ type: 'ERROR', message: 'Publish message cannot be empty' }));
            }

            publishIncident(roomId, severity, message);
            break;
          }

          case 'PING': {
            ws.send(JSON.stringify({ type: 'PONG' }));
            break;
          }

          default:
            ws.send(JSON.stringify({ type: 'ERROR', message: `Unknown message type: ${data.type}` }));
        }
      } catch (err) {
        ws.send(JSON.stringify({ type: 'ERROR', message: 'Malformed JSON payload' }));
      }
    });

    ws.on('close', () => {
      if (ws.currentRoomId && rooms.has(ws.currentRoomId)) {
        rooms.get(ws.currentRoomId).subscribers.delete(ws);
      }
    });
  });

  // Heartbeat interval to clean up broken connections
  const interval = setInterval(() => {
    wss.clients.forEach((ws) => {
      if (ws.isAlive === false) {
        if (ws.currentRoomId && rooms.has(ws.currentRoomId)) {
          rooms.get(ws.currentRoomId).subscribers.delete(ws);
        }
        return ws.terminate();
      }
      ws.isAlive = false;
      ws.ping();
    });
  }, 30000);

  wss.on('close', () => {
    clearInterval(interval);
  });

  return wss;
}

// Function to create full server instance (useful for testing)
function createApplicationServer() {
  const server = http.createServer(app);
  const wss = setupWebSocketServer(server);
  return { app, server, wss };
}

// Start standalone server if run directly
if (require.main === module) {
  const { server } = createApplicationServer();
  server.listen(PORT, () => {
    console.log(`⚡ Incident Feed Server running on http://localhost:${PORT}`);
  });
}

module.exports = {
  app,
  rooms,
  getRoom,
  getMissedUpdates,
  publishIncident,
  setupWebSocketServer,
  createApplicationServer,
  MAX_LEDGER_SIZE
};
