# Submission — Reconnecting Real-Time Incident Feed

A two-way incident coordination feed for multiple simultaneous clients, with
automatic reconnection, server-assigned ordering, missed-update recovery,
and client-side deduplication.

## 1. Setup and run instructions

Requires Node.js 18+ (for native `fetch`; the app itself only needs 14.17+
for `crypto.randomUUID`).

```bash
npm install         # installs express + ws
npm start            # starts the WebSocket-durable server on :3000
```

Open `http://localhost:3000` in two or more browser tabs. Dispatching an
update from one tab appears in every subscribed tab in real time. Use the
**Sever socket** button to simulate a network failure and **Recover
connection** to demonstrate reconnection with automatic missed-update
replay.

Environment variables:

- `PORT` — port to listen on locally (default `3000`). Unused on Vercel,
  which assigns its own port internally.

Run the test suite:

```bash
npm test              # node test/feed.test.js
```

This starts the real server on an ephemeral port and exercises it exactly
as a client would — no mocks for the WebSocket or HTTP layers.

## 2. Architectural data flow

```
                    ┌─────────────────────────────────────────┐
                    │              server.js                  │
                    │                                          │
 Browser A ── WS ──▶│  SUBSCRIBE {roomId, lastSeenSeq}         │
                    │      │                                   │
                    │      ▼                                   │
                    │  room.subscribers.add(ws)                │
                    │  replay: ledger.filter(seq > lastSeenSeq)│
                    │      │                                   │
 Browser A ◀── WS ──│◀─────┘  (SUBSCRIBED, then INCIDENT x N)  │
                    │                                          │
 Browser B ── WS ──▶│  PUBLISH {roomId, severity, message}     │
                    │      │                                   │
                    │      ▼                                   │
                    │  room.seq += 1                            │
                    │  event = {id, seq, severity, message,…}  │
                    │  INCIDENT_LEDGER[roomId].push(event)     │
                    │  (ring buffer: shift() past 1,000)        │
                    │      │                                   │
                    │      ▼                                   │
                    │  broadcast to room.subscribers            │
                    │      │                                   │
 Browser A ◀── WS ──│◀─────┘  INCIDENT {event}                 │
 Browser B ◀── WS ──│◀────── INCIDENT {event}  (echoed back)   │
                    │                                          │
 Any client ── GET ▶│  /api/incidents/:roomId/updates?afterSeq │
             ◀───── │  { events: ledger.filter(seq>afterSeq) } │
                    └─────────────────────────────────────────┘
```

**On the wire**, every WebSocket message is a small JSON envelope with a
`type`: `SUBSCRIBE` and `PUBLISH` client → server; `SUBSCRIBED`, `INCIDENT`,
`ERROR`, and `PONG` server → client. `seq` is assigned once, server-side, at
publish time — it is the single source of truth for ordering (AC5); clients
never generate or infer ordering themselves.

**On the client**, every incoming `INCIDENT` — whether it arrived as a live
broadcast or as replay — passes through `Dedupe.filterNewEvents()`
(`public/dedupe.js`) keyed on the event's server-assigned `id` before it is
rendered or before `lastSeenSeq` advances. This is what makes overlapping
delivery paths (a broadcast landing at the same moment a reconnect replay is
in flight) safe: duplicates are dropped, not merely visually deduplicated.

**Reconnection** is entirely client-driven: on any unclean close, the UI
moves to `RECONNECTING`, opens a fresh WebSocket after a linear backoff
(2s × attempt number, capped at 10s), and re-sends `SUBSCRIBE` with the
highest `seq` it has already processed. The server treats every `SUBSCRIBE`
identically whether it's the first one on a fresh connection or the tenth
after a flaky network — there is no special "resume" handshake, which keeps
the protocol small.

## 3. What happens if a client disconnects immediately after sending an update

Two cases, both already handled correctly:

- **The update reached the server before the socket dropped.** `PUBLISH` is
  handled synchronously: the server assigns `seq`, appends to
  `INCIDENT_LEDGER`, and broadcasts *before* the WebSocket's `close` event
  could plausibly have been processed for that same message. The event is
  durable in the ledger and in every other subscriber's feed regardless of
  what happens to the publisher's socket a moment later. When that client
  reconnects, it re-subscribes with whatever `lastSeenSeq` it had *before*
  sending — the server's replay will hand its own event straight back to
  it, deduplicated the same as anyone else's. The publisher never loses its
  own message.

- **The update never reached the server** (the socket died mid-frame,
  before the server's `message` handler ran). The server never assigned a
  `seq` for it, so there is nothing to replay — from the server's
  perspective the publish simply never happened. This is intentionally the
  same failure mode as a plain HTTP request that times out before the
  server processes it: the client is the only party that knows it *tried*
  to send something, so it is the client's responsibility to notice the
  socket closed without an acknowledging state change and let the operator
  redispatch. A hardening step not implemented here, but straightforward to
  add: have `PUBLISH` carry a client-generated `clientMessageId` and have
  the server reply with an explicit `ACK {clientMessageId, seq}`; the UI
  would then treat "sent but never acked" as failed and offer a resend,
  rather than relying on the socket's close event as a weaker signal.

## 4. How multiple backend instances would share and order events

The current design keeps `INCIDENT_LEDGER` and `seq` in a single process's
memory, which is correct for one instance but breaks the moment you run two
for availability or horizontal scale — two instances would each hand out
their own competing `seq` sequence, and a client connected to instance A
would never see a publish that landed on instance B.

The fix is to move both the sequence counter and the ledger out of process
memory and into a shared, ordered log, then have every instance treat that
log — not its own memory — as the source of truth:

- **Redis Streams** is the most direct fit. `XADD incidents:<roomId> * ...`
  gives you a server-assigned, monotonically increasing ID per room for
  free (this replaces the in-process `room.seq += 1`), and `XRANGE` /
  `XREAD ... $` map almost one-to-one onto `getMissedUpdates` and live
  broadcast respectively. `XTRIM ... MAXLEN ~ 1000` replaces the manual
  `ledger.shift()` ring buffer. Every app instance subscribes with
  `XREAD BLOCK` and re-broadcasts to its own locally-connected WebSockets —
  the Stream is the durable, ordered ledger; each instance's `subscribers`
  Set stays exactly as it is today, just fed from Redis instead of from a
  local `publishIncident` call.
- **Redis Pub/Sub** alone (without Streams) is simpler but insufficient on
  its own: it has no replay/history, so it can carry the live broadcast fan-
  out between instances but would still need Streams (or Postgres, or
  anything with an ordered, queryable log) behind it for the catch-up path.
- **Kafka** (one partition per room, or a partition key of `roomId`) is the
  better choice at higher throughput or when incidents need to be durable
  and replayable for much longer than an operational ring buffer — the
  tradeoff is materially more operational weight than Redis for what is,
  per room, a modest volume of events.

In all three cases the seq/offset assigned by the shared log — not
anything an individual instance computes — becomes what `getMissedUpdates`
filters on and what the client's `lastSeenSeq` is compared against, so the
client-facing protocol in this submission does not need to change at all;
only `publishIncident` and `getMissedUpdates` move from reading/writing a
`Map` to reading/writing the shared store.

## 5. How to prevent unbounded history replay

Two independent caps, already both present:

- **Retention cap (write side):** `INCIDENT_LEDGER` is a ring buffer capped
  at `MAX_LEDGER_SIZE` (1,000) events per room — `room.ledger.shift()`
  evicts the oldest entry the moment a room would exceed that size. A room
  that has been running for weeks cannot make the server's memory grow
  without bound; its history is always at most 1,000 events deep, mirroring
  `XTRIM ... MAXLEN` if this moves to Redis Streams (§4).
- **Pagination cap (read side):** both the WebSocket replay-on-SUBSCRIBE
  path and the REST catch-up endpoint pass every request through
  `getMissedUpdates`, which clamps the caller-supplied `limit` to
  `Math.min(Math.max(limit, 1), 500)`. A client that reconnects after being
  offline for a long time (or a REST caller with `limit=999999`) cannot
  force the server to serialize and push an unbounded response — it gets
  at most 500 events per request and is expected to page with a rising
  `afterSeq` if it genuinely needs more, which naturally rate-limits how
  much history-of-history a single request can cost the server.

Together these mean the worst case for any single client action — a fresh
`SUBSCRIBE` with `lastSeenSeq: 0` on a room at capacity — is bounded at
exactly 1,000 events (the full ledger), and every subsequent catch-up
request is bounded at 500, regardless of how long the room has existed or
how far behind the client has fallen.

## 6. What metrics to monitor in production

- **Disconnect rate** — WebSocket `close` events per minute, per instance
  and in aggregate, split by close code (clean 1000 vs. abnormal 1006/1011)
  and by whether the heartbeat (`ws.isAlive` check) was the one to
  terminate the socket vs. the client closing it. A rising abnormal-close
  rate localized to one instance points at that instance, not the network.
- **Reconnection attempts per client / time-to-reconnect** — the UI's own
  `reconnectAttempt` counter, aggregated server-side (or via client
  telemetry) as attempts-per-session and seconds-from-first-disconnect-to-
  `SUBSCRIBED`. A shift in the distribution is an early signal of upstream
  network degradation before it shows up as a support ticket.
- **Sequence drift** — for any room, `room.seq` (or the Stream's last ID
  under §4) compared against the highest `seq` each currently-subscribed
  client reports having rendered. A client sitting persistently behind
  `room.seq` despite reporting `CONNECTED` indicates a delivery bug (a
  broadcast silently failing to reach a socket that `readyState` still
  claims is `OPEN`), not a network problem — this is the metric most
  directly tied to AC4/AC5 actually holding in production, not just in
  tests.
- **P99 broadcast latency** — wall-clock time from `publishIncident()`
  assigning a `seq` to the last subscriber's `ws.send()` returning, per
  room size bucket (a room with 3 subscribers vs. 300 has very different
  latency profiles from the same `for (const client of room.subscribers)`
  loop). This is the number that would catch the broadcast loop becoming
  the bottleneck before it becomes visibly slow to users.
- **Ledger memory / ring buffer pressure** — count of rooms at
  `MAX_LEDGER_SIZE` and the age (in seconds) of the oldest retained event
  per room, so a genuine need to raise the retention cap is visible as a
  trend rather than discovered when someone's catch-up request comes back
  truncated.
- **REST catch-up call volume and latency**, split from WebSocket traffic —
  a spike here (especially on the serverless deployment, §7) is the
  earliest signal that clients are falling back to polling because the
  WebSocket path isn't reachable for them.

## 7. Deployment notes (Vercel)

Standard Vercel serverless functions are invoked per-request and torn down
between invocations — they cannot hold a persistent WebSocket connection
open the way `server.js` does locally. `api/index.js` therefore exports
only the Express `app` (REST: `GET`/`POST`
`/api/incidents/:roomId/updates`, plus `/api/health`), routed by
`vercel.json` alongside the static `public/` frontend. The `wss` object
`server.js` constructs still exists when required there, but with no
`upgrade` traffic reaching a serverless function, it never receives a
connection — it's inert rather than broken.

A client running against the Vercel deployment should be configured to
poll `GET /api/incidents/:roomId/updates?afterSeq=<lastSeenSeq>` on an
interval instead of opening a WebSocket (the same dedup engine in
`public/dedupe.js` applies unchanged to polled results — it doesn't care
which transport an event arrived over). For a deployment that needs true
push delivery on Vercel, the realistic options are Vercel's own
WebSocket/Edge-compatible primitives, or moving the WebSocket tier to a
long-running host (a small VM, Fly.io, Render, or similar) while keeping
Vercel for the static frontend — at that point §4's shared-log design is
what lets that WebSocket tier and this REST tier agree on the same `seq`
space.

## 8. Credibility note

*This section is a placeholder for the candidate to complete with their
own, real background — it should not be filled in with invented history.*

> Replace this paragraph with 2–3 sentences about full-stack web
> applications you have personally shipped: the stack, your role, roughly
> what it did in production, and, if relevant, how its scale or reliability
> requirements compare to this exercise (concurrent real-time connections,
> ordering guarantees, reconnection handling, etc.).
