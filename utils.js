function safeNum(value) {
  const num = Number(value);
  return Number.isFinite(num) ? num : null;
}

function firstFinite(...values) {
  for (const value of values) {
    if (Number.isFinite(value)) {
      return value;
    }
  }
  return null;
}

function ageMs(now, timestamp) {
  if (!Number.isFinite(timestamp)) {
    return null;
  }
  return Math.max(0, now - timestamp);
}

function parseNumberList(input) {
  const out = String(input || '')
    .split(',')
    .map((value) => Number(value.trim()))
    .filter((value) => Number.isFinite(value) && value > 0);
  return [...new Set(out)].sort((a, b) => a - b);
}

function parseSymbolList(input) {
  const out = String(input || '')
    .split(',')
    .map((value) => value.trim().toUpperCase())
    .filter((value) => /^[A-Z0-9]+USDT$/.test(value));
  return [...new Set(out)];
}

function ensureDefaultWindow(windows, defaultValue) {
  const out = windows.slice();
  if (defaultValue > 0 && !out.includes(defaultValue)) {
    out.push(defaultValue);
  }
  return out.sort((a, b) => a - b);
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function runPool(items, concurrency, worker, onError) {
  const queue = items.slice();
  const workers = Array.from({ length: Math.min(concurrency, queue.length) }, () =>
    (async () => {
      while (queue.length) {
        const item = queue.shift();
        if (!item) {
          return;
        }
        try {
          await worker(item);
        } catch (error) {
          if (onError) {
            onError(item, error);
          }
        }
      }
    })()
  );
  await Promise.all(workers);
}

module.exports = {
  safeNum,
  firstFinite,
  ageMs,
  parseNumberList,
  parseSymbolList,
  ensureDefaultWindow,
  sleep,
  runPool
};

