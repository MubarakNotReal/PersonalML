const fs = require('fs');
const path = require('path');
const { createBufferedWriter } = require('./writers');
const { calcCompleteness, COMPLETENESS_KEYS, MICRO_COMPLETENESS_KEYS } = require('./features');

const fetch = global.fetch || require('node-fetch');
let dotenvLoaded = false;
try {
  require('dotenv').config({ path: path.join(__dirname, '.env') });
  dotenvLoaded = true;
} catch {
  // dotenv is optional; fall back to process.env
}

const REST_BASE = process.env.FUTURES_REST_URL || 'https://fapi.binance.com';
const API_KEY = process.env.BINANCE_API_KEY || process.env.API_KEY || '';
const HEADERS = API_KEY ? { 'X-MBX-APIKEY': API_KEY } : {};

const BACKFILL_SYMBOLS_FILE =
  process.env.BACKFILL_SYMBOLS_FILE !== undefined && process.env.BACKFILL_SYMBOLS_FILE !== null
    ? process.env.BACKFILL_SYMBOLS_FILE
    : process.env.SYMBOLS_FILE !== undefined && process.env.SYMBOLS_FILE !== null
      ? process.env.SYMBOLS_FILE
      : '../new_symbols_10pct.txt';
const BACKFILL_TOP_N = Number(process.env.BACKFILL_TOP_N || process.env.TOP_N || 80);
const BACKFILL_USE_ALL_SYMBOLS =
  String(process.env.BACKFILL_USE_ALL_SYMBOLS || process.env.USE_ALL_SYMBOLS || 'false') === 'true';

const BACKFILL_MONTHS = Number(process.env.BACKFILL_MONTHS || 12);
const BACKFILL_START = parseTime(process.env.BACKFILL_START);
const BACKFILL_END = parseTime(process.env.BACKFILL_END) || Date.now();
const BACKFILL_INTERVALS = (process.env.BACKFILL_INTERVALS || '1m')
  .split(',')
  .map((item) => item.trim())
  .filter(Boolean);
const BACKFILL_OPEN_INTEREST_PERIOD = process.env.BACKFILL_OPEN_INTEREST_PERIOD || '5m';
const BACKFILL_MARK_INDEX = String(process.env.BACKFILL_MARK_INDEX || 'true') === 'true';
const BACKFILL_DATA_DIR = process.env.BACKFILL_DATA_DIR || path.join(__dirname, 'data_backfill');
const BACKFILL_SLEEP_MS = Number(process.env.BACKFILL_SLEEP_MS || 250);
const BACKFILL_CONCURRENCY = Number(process.env.BACKFILL_CONCURRENCY || 2);
const BACKFILL_WRITER_FLUSH_INTERVAL_MS = Number(
  process.env.BACKFILL_WRITER_FLUSH_INTERVAL_MS || process.env.WRITER_FLUSH_INTERVAL_MS || 1000
);
const BACKFILL_WRITER_MAX_BUFFER_LINES = Number(
  process.env.BACKFILL_WRITER_MAX_BUFFER_LINES || process.env.WRITER_MAX_BUFFER_LINES || 1000
);
const BACKFILL_WRITER_MAX_OPEN_STREAMS = Number(
  process.env.BACKFILL_WRITER_MAX_OPEN_STREAMS || process.env.WRITER_MAX_OPEN_STREAMS || 64
);

const CONTEXT_ENABLE = String(process.env.CONTEXT_ENABLE || 'true') === 'true';
const CONTEXT_SYMBOLS = CONTEXT_ENABLE
  ? parseSymbolList(process.env.CONTEXT_SYMBOLS || 'BTCUSDT,ETHUSDT')
  : [];

const writer = createBufferedWriter(BACKFILL_DATA_DIR, {
  flushIntervalMs: BACKFILL_WRITER_FLUSH_INTERVAL_MS,
  maxBufferLines: BACKFILL_WRITER_MAX_BUFFER_LINES,
  maxOpenStreams: BACKFILL_WRITER_MAX_OPEN_STREAMS
});

const WINDOW_24H_MS = 24 * 60 * 60 * 1000;
const PRICE_SERIES_WINDOW_MS = 30 * 60 * 1000;

const state = {
  markIndexSupported: BACKFILL_MARK_INDEX
};

let shuttingDown = false;

async function main() {
  const symbols = await resolveSymbols();
  if (!symbols.length) {
    console.error('No symbols found for backfill. Check BACKFILL_SYMBOLS_FILE or BACKFILL_TOP_N.');
    process.exit(1);
  }

  const startTime = BACKFILL_START || BACKFILL_END - BACKFILL_MONTHS * 30 * 24 * 60 * 60 * 1000;
  if (!Number.isFinite(startTime) || startTime <= 0 || startTime >= BACKFILL_END) {
    console.error('Invalid backfill time range. Check BACKFILL_START/BACKFILL_END/BACKFILL_MONTHS.');
    process.exit(1);
  }

  fs.mkdirSync(BACKFILL_DATA_DIR, { recursive: true });

  console.log(
    `Backfill range: ${new Date(startTime).toISOString()} -> ${new Date(BACKFILL_END).toISOString()}`
  );
  console.log(
    `Symbols: ${symbols.length} | intervals: ${BACKFILL_INTERVALS.join(',')} | OI period: ${BACKFILL_OPEN_INTEREST_PERIOD}`
  );

  await runPool(symbols, BACKFILL_CONCURRENCY, async (symbol) => {
    await backfillSymbol(symbol, startTime, BACKFILL_END);
  });

  console.log('Backfill complete.');
  await writer.close();
}

async function backfillSymbol(symbol, startTime, endTime) {
  if (shuttingDown) {
    return;
  }
  console.log(`[${symbol}] fetching klines...`);
  const kline1m = await fetchKlines(symbol, '1m', startTime, endTime, '/fapi/v1/klines');
  if (!kline1m.length) {
    console.warn(`[${symbol}] no 1m klines found.`);
    return;
  }

  let kline5m = [];
  let kline15m = [];
  if (BACKFILL_INTERVALS.includes('5m')) {
    kline5m = await fetchKlines(symbol, '5m', startTime, endTime, '/fapi/v1/klines');
  }
  if (BACKFILL_INTERVALS.includes('15m')) {
    kline15m = await fetchKlines(symbol, '15m', startTime, endTime, '/fapi/v1/klines');
  }

  console.log(`[${symbol}] fetching funding + open interest...`);
  const funding = await fetchFundingHistory(symbol, startTime, endTime);
  const openInterest = await fetchOpenInterestHist(symbol, startTime, endTime, BACKFILL_OPEN_INTEREST_PERIOD);

  let markKlines = [];
  let indexKlines = [];
  if (state.markIndexSupported && BACKFILL_MARK_INDEX) {
    markKlines = await fetchKlines(symbol, '1m', startTime, endTime, '/fapi/v1/markPriceKlines', true);
    indexKlines = await fetchKlines(symbol, '1m', startTime, endTime, '/fapi/v1/indexPriceKlines', true);
    if (!markKlines.length || !indexKlines.length) {
      state.markIndexSupported = false;
      console.warn(`[${symbol}] mark/index klines unavailable. Falling back to close price.`);
    }
  }

  const fundingCursor = createCursor(funding, 'time');
  const oiCursor = createCursor(openInterest, 'time');
  const markCursor = createCursor(markKlines, 'closeTime');
  const indexCursor = createCursor(indexKlines, 'closeTime');
  const k5Cursor = createCursor(kline5m, 'closeTime');
  const k15Cursor = createCursor(kline15m, 'closeTime');

  const priceSeries = [];
  const priceWindow = [];
  const volumeWindow = [];
  const highDeque = [];
  const lowDeque = [];
  const sums = { volumeSum: 0, quoteVolumeSum: 0 };
  let prevSnapshot = null;

  for (const row of kline1m) {
    if (shuttingDown) {
      break;
    }
    const time = row.closeTime;
    const price = row.close;
    if (!Number.isFinite(time) || !Number.isFinite(price)) {
      continue;
    }

    const fundingRow = advanceCursor(fundingCursor, time);
    const oiRow = advanceCursor(oiCursor, time);
    const markRow = advanceCursor(markCursor, time);
    const indexRow = advanceCursor(indexCursor, time);
    const k5Row = advanceCursor(k5Cursor, time);
    const k15Row = advanceCursor(k15Cursor, time);

    const markPrice = markRow ? markRow.close : null;
    const indexPrice = indexRow ? indexRow.close : null;

    pushPrice(priceSeries, time, price);
    const trend5m = calcTrend(priceSeries, time, 5 * 60 * 1000);
    const trend15m = calcTrend(priceSeries, time, 15 * 60 * 1000);
    const vol5m = calcVol(priceSeries, time, 5 * 60 * 1000);
    const vol15m = calcVol(priceSeries, time, 15 * 60 * 1000);

    const { high24h, low24h, volume24h, quoteVolume24h } = updateRolling24h(
      row,
      priceWindow,
      volumeWindow,
      highDeque,
      lowDeque,
      sums
    );

    const changePct24h = calcChangePct24h(priceWindow, price);

    const markAgeMs =
      markRow && Number.isFinite(markRow.closeTime) ? Math.max(0, time - markRow.closeTime) : null;
    const openInterestAgeMs =
      oiRow && Number.isFinite(oiRow.time) ? Math.max(0, time - oiRow.time) : null;
    const fundingAgeMs =
      fundingRow && Number.isFinite(fundingRow.time) ? Math.max(0, time - fundingRow.time) : null;
    const stats24hAgeMs = 0;
    const kline1mAgeMs = 0;
    const kline5mAgeMs =
      k5Row && Number.isFinite(k5Row.closeTime) ? Math.max(0, time - k5Row.closeTime) : null;

    const featureBase = {
      markPrice,
      indexPrice,
      fundingRate: fundingRow ? fundingRow.fundingRate : null,
      nextFundingTime: fundingRow ? fundingRow.nextFundingTime : null,
      openInterest: oiRow ? oiRow.openInterest : null,
      price,
      bestBid: null,
      bestAsk: null,
      spreadPct: null,
      imbalance: null,
      depthBidQty: null,
      depthAskQty: null,
      depthImbalance: null,
      basisPct: calcBasis(markPrice, indexPrice),
      changePct24h,
      high24h,
      low24h,
      volume24h,
      quoteVolume24h,
      aggBuyQty: null,
      aggSellQty: null,
      aggBuyCount: null,
      aggSellCount: null,
      aggVWAP: null,
      aggBuySellRatio: null,
      liqBuyQty: null,
      liqSellQty: null,
      liqBuyCount: null,
      liqSellCount: null,
      kline1mClose: row.close,
      kline1mVol: row.volume,
      kline5mClose: k5Row ? k5Row.close : null,
      kline5mVol: k5Row ? k5Row.volume : null,
      trend5m,
      trend15m,
      vol5m,
      vol15m,
      bookAgeMs: null,
      depthAgeMs: null,
      markAgeMs,
      fundingAgeMs,
      openInterestAgeMs,
      stats24hAgeMs,
      kline1mAgeMs,
      kline5mAgeMs,
      flowAgeMs: null,
      liqAgeMs: null
    };

    featureBase.featureCompleteness = calcCompleteness(featureBase, COMPLETENESS_KEYS);
    featureBase.microCompleteness = calcCompleteness(featureBase, MICRO_COMPLETENESS_KEYS);

    const deltas = buildDeltas(featureBase, prevSnapshot ? prevSnapshot.features : null);
    const snapshot = {
      type: 'snapshot',
      source: 'backfill',
      symbol,
      time,
      price,
      features: {
        ...featureBase,
        ...deltas
      }
    };

    writeSnapshot(snapshot);
    prevSnapshot = snapshot;
  }

  console.log(`[${symbol}] done (${kline1m.length} bars).`);
}

function updateRolling24h(row, priceWindow, volumeWindow, highDeque, lowDeque, sums) {
  const time = row.closeTime;
  const volume = Number.isFinite(row.volume) ? row.volume : 0;
  const quoteVolume = Number.isFinite(row.quoteVolume) ? row.quoteVolume : row.close * volume;

  priceWindow.push({ time, price: row.close });
  volumeWindow.push({ time, volume, quoteVolume });
  sums.volumeSum += volume;
  sums.quoteVolumeSum += quoteVolume;
  while (highDeque.length && highDeque[highDeque.length - 1].value <= row.high) {
    highDeque.pop();
  }
  highDeque.push({ time, value: row.high });
  while (lowDeque.length && lowDeque[lowDeque.length - 1].value >= row.low) {
    lowDeque.pop();
  }
  lowDeque.push({ time, value: row.low });

  const cutoff = time - WINDOW_24H_MS;
  while (volumeWindow.length && volumeWindow[0].time < cutoff) {
    const old = volumeWindow.shift();
    sums.volumeSum -= old.volume;
    sums.quoteVolumeSum -= old.quoteVolume;
  }

  while (priceWindow.length && priceWindow[0].time < cutoff) {
    priceWindow.shift();
  }
  while (highDeque.length && highDeque[0].time < cutoff) {
    highDeque.shift();
  }
  while (lowDeque.length && lowDeque[0].time < cutoff) {
    lowDeque.shift();
  }

  return {
    high24h: highDeque.length ? highDeque[0].value : null,
    low24h: lowDeque.length ? lowDeque[0].value : null,
    volume24h: sums.volumeSum || null,
    quoteVolume24h: sums.quoteVolumeSum || null
  };
}

function calcChangePct24h(priceWindow, currentPrice) {
  if (!priceWindow.length) {
    return null;
  }
  const oldest = priceWindow[0];
  if (!oldest || !Number.isFinite(oldest.price) || oldest.price <= 0) {
    return null;
  }
  return ((currentPrice - oldest.price) / oldest.price) * 100;
}

function writeSnapshot(payload) {
  if (shuttingDown) {
    return;
  }
  const time = payload.time || Date.now();
  writer.write('snapshots', payload, time);
}

function createCursor(series, timeKey) {
  return { series: series || [], timeKey, index: 0, current: null };
}

function advanceCursor(cursor, targetTime) {
  if (!cursor.series.length) {
    return null;
  }
  while (cursor.index < cursor.series.length && cursor.series[cursor.index][cursor.timeKey] <= targetTime) {
    cursor.current = cursor.series[cursor.index];
    cursor.index += 1;
  }
  return cursor.current;
}

function buildDeltas(current, prev) {
  const out = {};
  if (!prev) {
    Object.keys(current).forEach((key) => {
      out[`${key}Delta`] = null;
    });
    return out;
  }
  Object.keys(current).forEach((key) => {
    const a = current[key];
    const b = prev[key];
    if (Number.isFinite(a) && Number.isFinite(b)) {
      out[`${key}Delta`] = a - b;
    } else {
      out[`${key}Delta`] = null;
    }
  });
  return out;
}

function pushPrice(series, time, price) {
  series.push({ t: time, p: price });
  const cutoff = time - PRICE_SERIES_WINDOW_MS;
  while (series.length && series[0].t < cutoff) {
    series.shift();
  }
}

function calcTrend(series, now, windowMs) {
  if (!series || !series.length) {
    return null;
  }
  const startTime = now - windowMs;
  let startPrice = null;
  for (let i = series.length - 1; i >= 0; i -= 1) {
    if (series[i].t < startTime) {
      startPrice = series[i + 1] ? series[i + 1].p : series[0].p;
      break;
    }
  }
  if (startPrice === null) {
    startPrice = series[0] ? series[0].p : undefined;
  }
  const last = series[series.length - 1];
  const endPrice = last ? last.p : undefined;
  if (!Number.isFinite(startPrice) || !Number.isFinite(endPrice) || startPrice <= 0) {
    return null;
  }
  return ((endPrice - startPrice) / startPrice) * 100;
}

function calcVol(series, now, windowMs) {
  if (!series || series.length < 3) {
    return null;
  }
  const startTime = now - windowMs;
  const windowed = [];
  for (let i = series.length - 1; i >= 0; i -= 1) {
    if (series[i].t < startTime) {
      break;
    }
    windowed.push(series[i]);
  }
  windowed.reverse();
  if (windowed.length < 3) {
    return null;
  }
  const returns = [];
  for (let i = 1; i < windowed.length; i += 1) {
    const prev = windowed[i - 1].p;
    const next = windowed[i].p;
    if (!(prev > 0 && next > 0)) {
      continue;
    }
    returns.push(((next - prev) / prev) * 100);
  }
  if (returns.length < 3) {
    return null;
  }
  const mean = returns.reduce((sum, value) => sum + value, 0) / returns.length;
  const variance =
    returns.reduce((sum, value) => sum + (value - mean) ** 2, 0) / (returns.length - 1);
  return Math.sqrt(variance);
}

function calcBasis(mark, index) {
  if (!Number.isFinite(mark) || !Number.isFinite(index) || !index) {
    return null;
  }
  return ((mark - index) / index) * 100;
}

function parseTime(input) {
  if (!input) {
    return null;
  }
  const num = Number(input);
  if (Number.isFinite(num)) {
    return num;
  }
  const parsed = Date.parse(input);
  return Number.isFinite(parsed) ? parsed : null;
}

function parseSymbolList(input) {
  return [...new Set(String(input || '')
    .split(',')
    .map((value) => value.trim().toUpperCase())
    .filter((value) => /^[A-Z0-9]+USDT$/.test(value)))];
}

function parseKline(row) {
  return {
    openTime: safeNum(row[0]),
    open: safeNum(row[1]),
    high: safeNum(row[2]),
    low: safeNum(row[3]),
    close: safeNum(row[4]),
    volume: safeNum(row[5]),
    closeTime: safeNum(row[6]),
    quoteVolume: safeNum(row[7]),
    trades: safeNum(row[8]),
    takerBuyBase: safeNum(row[9]),
    takerBuyQuote: safeNum(row[10])
  };
}

async function fetchKlines(symbol, interval, startTime, endTime, endpoint, tolerateFailure = false) {
  const limit = 1000;
  const out = [];
  let cursor = startTime;
  while (cursor < endTime) {
    if (shuttingDown) {
      break;
    }
    const url = `${REST_BASE}${endpoint}?symbol=${symbol}&interval=${interval}&startTime=${cursor}&endTime=${endTime}&limit=${limit}`;
    const response = await fetchJson(url);
    if (!Array.isArray(response) || response.length === 0) {
      if (tolerateFailure && response && response.__error) {
        return [];
      }
      break;
    }
    for (const row of response) {
      const parsed = parseKline(row);
      if (Number.isFinite(parsed.closeTime)) {
        out.push(parsed);
      }
    }
    const last = response[response.length - 1];
    const lastClose = safeNum(last[6]);
    if (!Number.isFinite(lastClose) || lastClose <= cursor) {
      break;
    }
    cursor = lastClose + 1;
    await sleep(BACKFILL_SLEEP_MS);
  }
  return out;
}

async function fetchFundingHistory(symbol, startTime, endTime) {
  const limit = 1000;
  const out = [];
  let cursor = startTime;
  while (cursor < endTime) {
    if (shuttingDown) {
      break;
    }
    const url = `${REST_BASE}/fapi/v1/fundingRate?symbol=${symbol}&startTime=${cursor}&endTime=${endTime}&limit=${limit}`;
    const response = await fetchJson(url);
    if (!Array.isArray(response) || response.length === 0) {
      break;
    }
    for (const row of response) {
      out.push({
        time: safeNum(row.fundingTime),
        fundingRate: safeNum(row.fundingRate),
        nextFundingTime: safeNum(row.fundingTime) ? safeNum(row.fundingTime) + 8 * 60 * 60 * 1000 : null
      });
    }
    const last = response[response.length - 1];
    const lastTime = safeNum(last.fundingTime);
    if (!Number.isFinite(lastTime) || lastTime <= cursor) {
      break;
    }
    cursor = lastTime + 1;
    await sleep(BACKFILL_SLEEP_MS);
  }
  return out.filter((row) => Number.isFinite(row.time));
}

async function fetchOpenInterestHist(symbol, startTime, endTime, period) {
  const limit = 500;
  const out = [];
  let cursor = startTime;
  while (cursor < endTime) {
    if (shuttingDown) {
      break;
    }
    const url = `${REST_BASE}/fapi/v1/openInterestHist?symbol=${symbol}&period=${period}&startTime=${cursor}&endTime=${endTime}&limit=${limit}`;
    const response = await fetchJson(url);
    if (!Array.isArray(response) || response.length === 0) {
      break;
    }
    for (const row of response) {
      out.push({
        time: safeNum(row.timestamp),
        openInterest:
          safeNum(row.sumOpenInterest) !== null
            ? safeNum(row.sumOpenInterest)
            : safeNum(row.openInterest)
      });
    }
    const last = response[response.length - 1];
    const lastTime = safeNum(last.timestamp);
    if (!Number.isFinite(lastTime) || lastTime <= cursor) {
      break;
    }
    cursor = lastTime + 1;
    await sleep(BACKFILL_SLEEP_MS);
  }
  return out.filter((row) => Number.isFinite(row.time));
}

async function fetchJson(url) {
  const maxAttempts = 5;
  for (let attempt = 0; attempt < maxAttempts; attempt += 1) {
    if (shuttingDown) {
      return { __error: true, status: 0, text: 'shutdown' };
    }
    try {
      const response = await fetch(url, { headers: HEADERS });
      if (response.status === 418 || response.status === 429) {
        await sleep(BACKFILL_SLEEP_MS * (attempt + 2));
        continue;
      }
      if (!response.ok) {
        const text = await response.text();
        return { __error: true, status: response.status, text };
      }
      return await response.json();
    } catch {
      await sleep(BACKFILL_SLEEP_MS * (attempt + 2));
    }
  }
  return { __error: true, status: 0, text: 'fetch failed' };
}

async function resolveSymbols() {
  const sources = [];
  if (BACKFILL_TOP_N > 0) {
    const top = await fetchTopSymbols(BACKFILL_TOP_N);
    if (top.length) {
      sources.push(top);
    }
  }
  const fromFile = loadSymbolsFromFile();
  if (fromFile.length) {
    sources.push(fromFile);
  }
  if (BACKFILL_USE_ALL_SYMBOLS) {
    const all = await fetchAllSymbols();
    if (all.length) {
      sources.push(all);
    }
  }
  if (CONTEXT_SYMBOLS.length) {
    sources.push(CONTEXT_SYMBOLS);
  }
  if (!sources.length) {
    return [];
  }
  return [...new Set(sources.flat().map((value) => String(value || '').toUpperCase()))].filter(
    (symbol) => /^[A-Z0-9]+USDT$/.test(symbol)
  );
}

function loadSymbolsFromFile() {
  if (!BACKFILL_SYMBOLS_FILE || !String(BACKFILL_SYMBOLS_FILE).trim()) {
    return [];
  }
  const filePath = path.join(__dirname, BACKFILL_SYMBOLS_FILE);
  if (!fs.existsSync(filePath)) {
    return [];
  }
  try {
    const stat = fs.statSync(filePath);
    if (!stat.isFile()) {
      return [];
    }
  } catch {
    return [];
  }
  let raw = '';
  try {
    raw = fs.readFileSync(filePath, 'utf8');
  } catch {
    return [];
  }
  return raw
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter(Boolean)
    .map((line) => line.split(/\s+/)[0])
    .filter((symbol) => /^[A-Z0-9]+USDT$/.test(symbol));
}

async function fetchAllSymbols() {
  const response = await fetchJson(`${REST_BASE}/fapi/v1/exchangeInfo`);
  if (!response || response.__error) {
    return [];
  }
  return (response.symbols || [])
    .map((row) => row.symbol)
    .filter((symbol) => symbol && symbol.endsWith('USDT'));
}

async function fetchTopSymbols(limit) {
  const response = await fetchJson(`${REST_BASE}/fapi/v1/ticker/24hr`);
  if (!Array.isArray(response)) {
    return [];
  }
  return response
    .filter((row) => row.symbol && row.symbol.endsWith('USDT'))
    .sort((a, b) => Number(b.quoteVolume || 0) - Number(a.quoteVolume || 0))
    .slice(0, limit)
    .map((row) => row.symbol);
}

function safeNum(value) {
  const num = Number(value);
  return Number.isFinite(num) ? num : null;
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function runPool(items, concurrency, worker) {
  const queue = items.slice();
  const workers = Array.from({ length: Math.min(concurrency, queue.length) }, () =>
    (async () => {
      while (queue.length && !shuttingDown) {
        const item = queue.shift();
        if (!item) {
          return;
        }
        await worker(item);
      }
    })()
  );
  await Promise.all(workers);
}

async function requestShutdown(signal) {
  if (shuttingDown) {
    return;
  }
  shuttingDown = true;
  console.log(`Backfill shutting down (${signal})...`);
  try {
    await writer.close();
  } catch {
    // ignore
  }
  process.exit(0);
}

process.on('SIGINT', () => requestShutdown('SIGINT'));
process.on('SIGTERM', () => requestShutdown('SIGTERM'));

main().catch((error) => {
  console.error(`Backfill failed: ${error.message}`);
  process.exit(1);
});
