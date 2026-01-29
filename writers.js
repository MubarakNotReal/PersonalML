const fs = require('fs');
const path = require('path');

function createBufferedWriter(baseDir, options = {}) {
  const flushIntervalMs = Number(options.flushIntervalMs || 1000);
  const maxBufferLines = Number(options.maxBufferLines || 1000);
  const maxOpenStreams = Number(options.maxOpenStreams || 48);

  fs.mkdirSync(baseDir, { recursive: true });

  const streams = new Map();

  function makeKey(stream, dateKey, hour) {
    return `${stream}:${dateKey}:${hour}`;
  }

  function resolvePath(stream, time) {
    const now = new Date(time || Date.now());
    const dateKey = now.toISOString().slice(0, 10).replace(/-/g, '');
    const hour = String(now.getUTCHours()).padStart(2, '0');
    const dayDir = path.join(baseDir, dateKey);
    fs.mkdirSync(dayDir, { recursive: true });
    const filePath = path.join(dayDir, `${stream}_${hour}.jsonl`);
    return { key: makeKey(stream, dateKey, hour), filePath };
  }

  function openStream(key, filePath) {
    const handle = fs.createWriteStream(filePath, { flags: 'a' });
    const entry = {
      key,
      filePath,
      handle,
      buffer: [],
      lastUsed: Date.now()
    };
    streams.set(key, entry);
    return entry;
  }

  function evictIfNeeded() {
    if (streams.size <= maxOpenStreams) {
      return;
    }
    const victims = [...streams.values()].sort((a, b) => a.lastUsed - b.lastUsed);
    const excess = streams.size - maxOpenStreams;
    for (let i = 0; i < excess; i += 1) {
      const entry = victims[i];
      flushEntry(entry);
      try {
        entry.handle.end();
      } catch {
        // ignore
      }
      streams.delete(entry.key);
    }
  }

  function flushEntry(entry) {
    if (!entry || entry.buffer.length === 0) {
      return;
    }
    const payload = entry.buffer.join('');
    entry.buffer = [];
    entry.handle.write(payload);
  }

  function flushAll() {
    for (const entry of streams.values()) {
      flushEntry(entry);
    }
  }

  const flushTimer = setInterval(flushAll, flushIntervalMs);
  if (flushTimer.unref) {
    flushTimer.unref();
  }

  function getEntry(stream, time) {
    const { key, filePath } = resolvePath(stream, time);
    let entry = streams.get(key);
    if (!entry) {
      entry = openStream(key, filePath);
      evictIfNeeded();
    }
    entry.lastUsed = Date.now();
    return entry;
  }

  function write(stream, payload, time) {
    const entry = getEntry(stream, time);
    entry.buffer.push(`${JSON.stringify(payload)}\n`);
    if (entry.buffer.length >= maxBufferLines) {
      flushEntry(entry);
    }
  }

  function close() {
    clearInterval(flushTimer);
    flushAll();
    for (const entry of streams.values()) {
      try {
        entry.handle.end();
      } catch {
        // ignore
      }
    }
    streams.clear();
  }

  for (const signal of ['SIGINT', 'SIGTERM', 'beforeExit', 'exit']) {
    process.on(signal, () => {
      try {
        close();
      } catch {
        // ignore
      }
    });
  }

  return { write, flushAll, close };
}

module.exports = {
  createBufferedWriter
};

