const { test, describe, before, after } = require('node:test');
const assert = require('node:assert/strict');
const WebSocket = require('ws');
const http = require('http');

const { createApplicationServer } = require('../server');
const Dedupe = require('../public/dedupe');

describe('Reconnecting Real-Time Incident Feed Integration Tests', () => {
  let app, server, wss;
  let baseUrl;
  let wsUrl;

  before(async () => {
    const inst = createApplicationServer();
    app = inst.app;
    server = inst.server;
    wss = inst.wss;

    await new Promise((resolve) => {
      server.listen(0, '127.0.0.1', () => {
        const port = server.address().port;
        baseUrl = `http://127.0.0.1:${port}`;
        wsUrl = `ws://127.0.0.1:${port}`;
        resolve();
      });
    });
  });

  after(async () => {
    await new Promise((resolve) => {
      wss.close(() => {
        server.close(() => {
          resolve();
        });
      });
    });
  });

  // Helper to open WebSocket and wait for open
  function createWsClient() {
    return new Promise((resolve, reject) => {
      const client = new WebSocket(wsUrl);
      client.on('open', () => resolve(client));
      client.on('error', (err) => reject(err));
    });
  }

  test('1. SUBSCRIBE receives SUBSCRIBED confirmation', async () => {
    const ws = await createWsClient();
    
    const messagePromise = new Promise((resolve) => {
      ws.on('message', (data) => {
        resolve(JSON.parse(data.toString()));
      });
    });

    ws.send(JSON.stringify({
      type: 'SUBSCRIBE',
      roomId: 'room-t1',
      lastSeenSeq: 0
    }));

    const response = await messagePromise;
    assert.equal(response.type, 'SUBSCRIBED');
    assert.equal(response.roomId, 'room-t1');
    assert.equal(response.lastSeenSeq, 0);
    assert.equal(response.currentSeq, 0);

    ws.close();
  });

  test('2. Client PUBLISH assigns server seq and echoes INCIDENT back', async () => {
    const ws = await createWsClient();
    const messages = [];

    ws.on('message', (data) => {
      messages.push(JSON.parse(data.toString()));
    });

    ws.send(JSON.stringify({
      type: 'SUBSCRIBE',
      roomId: 'room-t2',
      lastSeenSeq: 0
    }));

    // Wait for SUBSCRIBED
    await new Promise((r) => setTimeout(r, 100));
    assert.equal(messages[0].type, 'SUBSCRIBED');

    // Publish Incident
    ws.send(JSON.stringify({
      type: 'PUBLISH',
      roomId: 'room-t2',
      severity: 'critical',
      message: 'Database Primary Replica Down'
    }));

    // Wait for INCIDENT payload
    await new Promise((r) => setTimeout(r, 100));

    const incidentMsg = messages.find(m => m.type === 'INCIDENT');
    assert.ok(incidentMsg, 'Should receive INCIDENT event');
    assert.equal(incidentMsg.event.seq, 1, 'Server should assign seq = 1');
    assert.equal(incidentMsg.event.severity, 'critical');
    assert.equal(incidentMsg.event.message, 'Database Primary Replica Down');
    assert.ok(incidentMsg.event.id, 'Should have crypto UUID id');

    ws.close();
  });

  test('3. Multi-client real-time broadcast', async () => {
    const wsA = await createWsClient();
    const wsB = await createWsClient();

    const messagesA = [];
    const messagesB = [];

    wsA.on('message', data => messagesA.push(JSON.parse(data.toString())));
    wsB.on('message', data => messagesB.push(JSON.parse(data.toString())));

    wsA.send(JSON.stringify({ type: 'SUBSCRIBE', roomId: 'room-broadcast', lastSeenSeq: 0 }));
    wsB.send(JSON.stringify({ type: 'SUBSCRIBE', roomId: 'room-broadcast', lastSeenSeq: 0 }));

    await new Promise(r => setTimeout(r, 100));

    // Client A publishes
    wsA.send(JSON.stringify({
      type: 'PUBLISH',
      roomId: 'room-broadcast',
      severity: 'high',
      message: 'High CPU load on API Gateway'
    }));

    await new Promise(r => setTimeout(r, 150));

    const incidentA = messagesA.find(m => m.type === 'INCIDENT');
    const incidentB = messagesB.find(m => m.type === 'INCIDENT');

    assert.ok(incidentA, 'Client A should receive broadcast');
    assert.ok(incidentB, 'Client B should receive broadcast');

    assert.equal(incidentA.event.id, incidentB.event.id, 'Both clients receive identical event ID');
    assert.equal(incidentA.event.seq, incidentB.event.seq, 'Both clients receive identical sequence');

    wsA.close();
    wsB.close();
  });

  test('4. Disconnect and reconnect with lastSeenSeq (Missed-update replay)', async () => {
    const wsA = await createWsClient();
    const messagesA = [];
    wsA.on('message', data => messagesA.push(JSON.parse(data.toString())));

    // Client A subscribes and publishes event #1
    wsA.send(JSON.stringify({ type: 'SUBSCRIBE', roomId: 'room-replay', lastSeenSeq: 0 }));
    await new Promise(r => setTimeout(r, 50));
    wsA.send(JSON.stringify({ type: 'PUBLISH', roomId: 'room-replay', severity: 'low', message: 'Event 1' }));
    await new Promise(r => setTimeout(r, 100));

    const evt1 = messagesA.find(m => m.type === 'INCIDENT');
    assert.equal(evt1.event.seq, 1);

    // Client A disconnects
    wsA.close();

    // Client B publishes events #2 and #3 while Client A is offline
    const wsB = await createWsClient();
    wsB.send(JSON.stringify({ type: 'SUBSCRIBE', roomId: 'room-replay', lastSeenSeq: 1 }));
    await new Promise(r => setTimeout(r, 50));

    wsB.send(JSON.stringify({ type: 'PUBLISH', roomId: 'room-replay', severity: 'medium', message: 'Event 2' }));
    wsB.send(JSON.stringify({ type: 'PUBLISH', roomId: 'room-replay', severity: 'high', message: 'Event 3' }));
    await new Promise(r => setTimeout(r, 100));
    wsB.close();

    // Client A reconnects with lastSeenSeq = 1
    const wsA2 = await createWsClient();
    const messagesReconnected = [];
    wsA2.on('message', data => messagesReconnected.push(JSON.parse(data.toString())));

    wsA2.send(JSON.stringify({ type: 'SUBSCRIBE', roomId: 'room-replay', lastSeenSeq: 1 }));
    await new Promise(r => setTimeout(r, 150));

    const replayedIncidents = messagesReconnected.filter(m => m.type === 'INCIDENT');
    assert.equal(replayedIncidents.length, 2, 'Should receive 2 missed events');
    assert.equal(replayedIncidents[0].event.seq, 2, 'First replayed event is seq #2');
    assert.equal(replayedIncidents[1].event.seq, 3, 'Second replayed event is seq #3');

    wsA2.close();
  });

  test('5. Client-side Dedupe module', () => {
    const dedupe = new Dedupe();
    const event1 = { id: 'evt-100', seq: 1, message: 'Test 1' };
    const event2 = { id: 'evt-101', seq: 2, message: 'Test 2' };

    // Initial filter
    const res1 = dedupe.filterNewEvents([event1, event2]);
    assert.equal(res1.length, 2, 'Should accept unseen events');

    // Duplicate filter
    const res2 = dedupe.filterNewEvents([event1]);
    assert.equal(res2.length, 0, 'Should drop duplicate event1');

    // Mixed filter
    const event3 = { id: 'evt-102', seq: 3, message: 'Test 3' };
    const res3 = dedupe.filterNewEvents([event1, event3]);
    assert.equal(res3.length, 1, 'Should filter out event1 and keep event3');
    assert.equal(res3[0].id, 'evt-102');
  });

  test('6. REST API updates endpoint GET /api/incidents/:roomId/updates', async () => {
    // Publish via REST API
    const postRes = await fetch(`${baseUrl}/api/incidents/room-rest/publish`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ severity: 'low', message: 'REST Incident' })
    });
    const postData = await postRes.json();
    assert.equal(postRes.status, 201);
    assert.equal(postData.success, true);
    assert.equal(postData.event.seq, 1);

    // Query REST updates
    const getRes = await fetch(`${baseUrl}/api/incidents/room-rest/updates?afterSeq=0`);
    const getData = await getRes.json();
    assert.equal(getRes.status, 200);
    assert.equal(getData.roomId, 'room-rest');
    assert.equal(getData.events.length, 1);
    assert.equal(getData.events[0].message, 'REST Incident');
  });

  test('7. Pagination cap enforcement (limit <= 500)', async () => {
    // Attempt limit=999999
    const getRes = await fetch(`${baseUrl}/api/incidents/room-rest/updates?afterSeq=0&limit=999999`);
    const getData = await getRes.json();
    assert.equal(getRes.status, 200);
    assert.ok(getData.count <= 500, 'Limit should be clamped to max 500');
  });

  test('8. Health Check endpoint GET /api/health', async () => {
    const res = await fetch(`${baseUrl}/api/health`);
    const data = await res.json();
    assert.equal(res.status, 200);
    assert.equal(data.status, 'ok');
    assert.ok(typeof data.uptime === 'number');
  });
});
