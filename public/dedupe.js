/**
 * Dedupe - Client-side Incident Deduplication Module
 * 
 * Keyed on the event's server-assigned `id`. Ensures overlapping delivery paths
 * (e.g. live WebSocket broadcast landing simultaneously with reconnect replay)
 * do not cause duplicate rendering or duplicate state updates.
 */

class Dedupe {
  constructor(maxCapacity = 5000) {
    this.maxCapacity = maxCapacity;
    this.seenIds = new Set();
    this.seenOrder = []; // For sliding window eviction
  }

  /**
   * Check if an event ID has already been seen.
   * @param {string} id - The server-assigned event ID.
   * @returns {boolean}
   */
  hasSeen(id) {
    if (!id) return false;
    return this.seenIds.has(id);
  }

  /**
   * Register an event ID as seen.
   * @param {string} id 
   */
  add(id) {
    if (!id || this.seenIds.has(id)) return;
    this.seenIds.add(id);
    this.seenOrder.push(id);

    // Evict oldest entries if capacity exceeded
    if (this.seenOrder.length > this.maxCapacity) {
      const oldestId = this.seenOrder.shift();
      this.seenIds.delete(oldestId);
    }
  }

  /**
   * Filter an array of events (or single event object), returning only previously unseen events.
   * Automatically marks returned events as seen.
   * 
   * @param {Array<Object>|Object} events - Array of incident events or single event object.
   * @returns {Array<Object>} Array of strictly new (unseen) events.
   */
  filterNewEvents(events) {
    if (!events) return [];
    const eventList = Array.isArray(events) ? events : [events];
    const newEvents = [];

    for (const event of eventList) {
      if (!event || !event.id) continue;
      if (!this.hasSeen(event.id)) {
        this.add(event.id);
        newEvents.push(event);
      }
    }

    return newEvents;
  }

  /**
   * Get total number of tracked unique event IDs.
   * @returns {number}
   */
  getSeenCount() {
    return this.seenIds.size;
  }

  /**
   * Clear all stored deduplication state.
   */
  clear() {
    this.seenIds.clear();
    this.seenOrder = [];
  }
}

// Export for Node.js / CommonJS and Browser environments
if (typeof module !== 'undefined' && module.exports) {
  module.exports = Dedupe;
} else if (typeof window !== 'undefined') {
  window.Dedupe = Dedupe;
}
