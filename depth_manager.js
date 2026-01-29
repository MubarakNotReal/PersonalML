const { safeNum, sleep, runPool } = require('./utils');

class DepthManager {
  constructor(options) {
    this.restBase = options.restBase;
    this.fetch = options.fetch;
    this.snapshotLimit = options.snapshotLimit;
    this.bufferLimit = options.bufferLimit;
    this.depthLevel = options.depthLevel;
    this.snapshotConcurrency = options.snapshotConcurrency;
    this.resyncCooldownMs = options.resyncCooldownMs;
    this.snapshotSleepMs = options.snapshotSleepMs;
    this.log = options.log || console;

    this.books = new Map();
    this.resyncing = new Set();
    this.nextAllowed = new Map();
  }

  ensure(symbol) {
    if (!this.books.has(symbol)) {
      this.books.set(symbol, createDepthBook());
    }
    return this.books.get(symbol);
  }

  bufferEvent(book, event) {
    book.buffer.push(event);
    if (book.buffer.length > this.bufferLimit) {
      book.buffer = book.buffer.slice(book.buffer.length - this.bufferLimit);
    }
  }

  scheduleResync(symbol, reason) {
    const now = Date.now();
    const nextAllowed = this.nextAllowed.get(symbol) || 0;
    if (now < nextAllowed) {
      return;
    }
    this.nextAllowed.set(symbol, now + this.resyncCooldownMs);
    setTimeout(() => {
      this.syncSymbol(symbol, reason).catch((error) => {
        this.log.warn(`[${symbol}] depth resync failed: ${error.message}`);
      });
    }, 0);
  }

  handleEvent(symbol, data) {
    const now = Date.now();
    const eventTime = safeNum((data || {}).E);
    const eventTimeSafe = Number.isFinite(eventTime) ? eventTime : now;
    const book = this.ensure(symbol);
    const event = {
      U: safeNum(data.U),
      u: safeNum(data.u),
      pu: safeNum(data.pu),
      bids: (data.b || []).map(([p, q]) => [safeNum(p), safeNum(q)]),
      asks: (data.a || []).map(([p, q]) => [safeNum(p), safeNum(q)]),
      time: eventTimeSafe
    };
    book.lastEventTime = eventTimeSafe;

    if (!Number.isFinite(event.U) || !Number.isFinite(event.u)) {
      this.bufferEvent(book, event);
      this.scheduleResync(symbol, 'missing_update_ids');
      return;
    }

    if (!book.ready || !Number.isFinite(book.lastUpdateId)) {
      this.bufferEvent(book, event);
      this.scheduleResync(symbol, 'not_ready');
      return;
    }

    if (Number.isFinite(event.pu) && event.pu !== book.lastUpdateId) {
      this.bufferEvent(book, event);
      this.scheduleResync(symbol, 'pu_mismatch');
      return;
    }

    if (event.u <= book.lastUpdateId) {
      return;
    }

    if (event.U > book.lastUpdateId + 1) {
      this.bufferEvent(book, event);
      this.scheduleResync(symbol, 'gap_detected');
      return;
    }

    applyDepthUpdate(book, event, this.snapshotLimit);
    book.lastUpdateId = event.u;
    book.ready = true;
  }

  async syncAll(symbols) {
    if (!symbols.length) {
      return;
    }
    this.log.log(`Syncing depth snapshots for ${symbols.length} symbols...`);
    await runPool(
      symbols,
      this.snapshotConcurrency,
      async (symbol) => {
        await this.syncSymbol(symbol, 'sync_all');
      },
      (symbol, error) => {
        this.log.warn(`[${symbol}] depth sync failed: ${error.message}`);
      }
    );
  }

  async syncSymbol(symbol, reason) {
    if (this.resyncing.has(symbol)) {
      return;
    }
    this.resyncing.add(symbol);
    try {
      const response = await this.fetch(
        `${this.restBase}/fapi/v1/depth?symbol=${symbol}&limit=${this.snapshotLimit}`
      );
      if (!response.ok) {
        return;
      }
      const payload = await response.json();
      const lastUpdateId = safeNum(payload.lastUpdateId);
      if (!Number.isFinite(lastUpdateId)) {
        return;
      }

      const book = this.ensure(symbol);
      book.bids = new Map();
      book.asks = new Map();
      applySideSnapshot(book.bids, payload.bids);
      applySideSnapshot(book.asks, payload.asks);
      book.lastUpdateId = lastUpdateId;
      book.snapshotTime = Date.now();
      book.ready = false;

      applyBufferedDepthUpdates(this, symbol, book);
      if (book.ready) {
        this.log.log(`[${symbol}] depth synced (${reason}).`);
      }
      await sleep(this.snapshotSleepMs);
    } catch {
      // ignore snapshot errors; stream gaps will trigger resyncs
    } finally {
      this.resyncing.delete(symbol);
    }
  }

  getStats(symbol) {
    const book = this.ensure(symbol);
    const stats = calcDepthStatsFromBook(book, this.depthLevel);
    return {
      ...stats,
      lastEventTime: book.lastEventTime,
      ready: book.ready
    };
  }

  countStale(symbols, now, thresholdMs) {
    let stale = 0;
    for (const symbol of symbols) {
      const book = this.ensure(symbol);
      if (!Number.isFinite(book.lastEventTime) || now - book.lastEventTime > thresholdMs) {
        stale += 1;
      }
    }
    return stale;
  }
}

function createDepthBook() {
  return {
    bids: new Map(),
    asks: new Map(),
    lastUpdateId: null,
    lastEventTime: null,
    snapshotTime: null,
    buffer: [],
    ready: false
  };
}

function applySideSnapshot(sideMap, snapshotSide) {
  if (!Array.isArray(snapshotSide)) {
    return;
  }
  snapshotSide.forEach((row) => {
    const price = safeNum(row[0]);
    const qty = safeNum(row[1]);
    if (!Number.isFinite(price) || !Number.isFinite(qty)) {
      return;
    }
    if (qty === 0) {
      sideMap.delete(price);
    } else {
      sideMap.set(price, qty);
    }
  });
}

function applyBufferedDepthUpdates(manager, symbol, book) {
  if (!book.buffer.length || !Number.isFinite(book.lastUpdateId)) {
    book.ready = true;
    return;
  }
  const lastUpdateId = book.lastUpdateId;
  const nextId = lastUpdateId + 1;
  const usable = book.buffer.filter((event) => Number.isFinite(event.u) && event.u >= nextId);
  if (!usable.length) {
    book.ready = true;
    book.buffer = [];
    return;
  }

  let startIndex = -1;
  for (let i = 0; i < usable.length; i += 1) {
    const event = usable[i];
    if (event.U <= nextId && event.u >= nextId) {
      startIndex = i;
      break;
    }
  }
  if (startIndex === -1) {
    book.ready = false;
    book.buffer = usable.slice(-manager.bufferLimit);
    return;
  }

  book.lastUpdateId = lastUpdateId;
  for (let i = startIndex; i < usable.length; i += 1) {
    const event = usable[i];
    if (Number.isFinite(event.pu) && event.pu !== book.lastUpdateId) {
      book.ready = false;
      book.buffer = usable.slice(i);
      manager.scheduleResync(symbol, 'buffer_pu_mismatch');
      return;
    }
    if (event.U > book.lastUpdateId + 1) {
      book.ready = false;
      book.buffer = usable.slice(i);
      manager.scheduleResync(symbol, 'buffer_gap_detected');
      return;
    }
    applyDepthUpdate(book, event, manager.snapshotLimit);
    book.lastUpdateId = event.u;
  }
  book.ready = true;
  book.buffer = [];
}

function applyDepthUpdate(book, event, snapshotLimit) {
  applySideUpdate(book.bids, event.bids);
  applySideUpdate(book.asks, event.asks);
  pruneDepthBook(book, snapshotLimit);
  book.lastEventTime = event.time;
}

function applySideUpdate(sideMap, updates) {
  if (!Array.isArray(updates)) {
    return;
  }
  updates.forEach((row) => {
    const price = safeNum(row[0]);
    const qty = safeNum(row[1]);
    if (!Number.isFinite(price) || !Number.isFinite(qty)) {
      return;
    }
    if (qty === 0) {
      sideMap.delete(price);
    } else {
      sideMap.set(price, qty);
    }
  });
}

function pruneDepthBook(book, snapshotLimit) {
  const maxSize = snapshotLimit * 3;
  if (book.bids.size > maxSize) {
    book.bids = pruneSide(book.bids, true, snapshotLimit);
  }
  if (book.asks.size > maxSize) {
    book.asks = pruneSide(book.asks, false, snapshotLimit);
  }
}

function pruneSide(sideMap, isBid, keep) {
  const entries = Array.from(sideMap.entries());
  entries.sort((a, b) => (isBid ? b[0] - a[0] : a[0] - b[0]));
  return new Map(entries.slice(0, keep));
}

function calcDepthStatsFromBook(book, depthLevel) {
  if (!book || !book.ready) {
    return { bidQty: null, askQty: null, imbalance: null };
  }
  const bids = topLevels(book.bids, depthLevel, true);
  const asks = topLevels(book.asks, depthLevel, false);
  const bidQty = sumLevels(bids);
  const askQty = sumLevels(asks);
  const imbalance =
    Number.isFinite(bidQty) && Number.isFinite(askQty) && bidQty + askQty > 0
      ? (bidQty - askQty) / (bidQty + askQty)
      : null;
  return { bidQty, askQty, imbalance };
}

function topLevels(sideMap, limit, isBid) {
  if (!sideMap || sideMap.size === 0 || limit <= 0) {
    return [];
  }
  const entries = Array.from(sideMap.entries());
  entries.sort((a, b) => (isBid ? b[0] - a[0] : a[0] - b[0]));
  return entries.slice(0, limit);
}

function sumLevels(levels) {
  if (!levels.length) {
    return 0;
  }
  return levels.reduce((sum, [, qty]) => sum + (Number.isFinite(qty) ? qty : 0), 0);
}

module.exports = {
  DepthManager
};
