const fs = require('fs');
const path = require('path');
const WebSocket = require('ws');

const fetch = global.fetch || require('node-fetch');
try {
  require('dotenv').config({ path: path.join(__dirname, '.env') });
} catch {
  // dotenv is optional; fall back to process.env
}

const { createBufferedWriter } = require('./writers');
const { DepthManager } = require('./depth_manager');
const { RollingManager, emptyMetrics } = require('./rolling_windows');
const {
  safeNum,
  firstFinite,
  ageMs,
  parseNumberList,
  parseSymbolList,
  ensureDefaultWindow,
  runPool
} = require('./utils');
const {
  calcTrend,
  calcVol,
  calcSpreadPct,
  calcImbalance,
  calcBasis,
  calcCompleteness,
  buildDeltas,
  COMPLETENESS_KEYS,
  MICRO_COMPLETENESS_KEYS
} = require('./features');

const WS_BASE = process.env.FUTURES_WS_URL || 'wss://fstream.binance.com';
const REST_BASE = process.env.FUTURES_REST_URL || 'https://fapi.binance.com';

const SYMBOLS_FILE =
  process.env.SYMBOLS_FILE !== undefined && process.env.SYMBOLS_FILE !== null
    ? process.env.SYMBOLS_FILE
    : '../new_symbols_10pct.txt';
const TOP_N = Number(process.env.TOP_N || 0);
const USE_ALL_SYMBOLS = String(process.env.USE_ALL_SYMBOLS || 'false') === 'true';
const SYMBOL_REFRESH_MS = Number(process.env.SYMBOL_REFRESH_MS || 5 * 60 * 1000);
const FREEZE_SYMBOLS = String(process.env.FREEZE_SYMBOLS || 'false') === 'true';

const SNAPSHOT_INTERVAL_MS = Number(process.env.SNAPSHOT_INTERVAL_MS || 1000);

const MODE = String(process.env.MODE || 'all').toLowerCase();
let ENABLE_BOOK_TICKER = String(process.env.ENABLE_BOOK_TICKER || 'true') === 'true';
const BOOK_TICKER_ALL_STREAM = String(process.env.BOOK_TICKER_ALL_STREAM || 'false') === 'true';
let ENABLE_DEPTH = String(process.env.ENABLE_DEPTH || 'true') === 'true';
const DEPTH_LEVEL = Number(process.env.DEPTH_LEVEL || 5);
const DEPTH_SPEED = process.env.DEPTH_SPEED || '1000ms';
const DEPTH_STREAM_MODE = String(process.env.DEPTH_STREAM_MODE || 'diff').toLowerCase();
const DEPTH_SNAPSHOT_LIMIT = Number(process.env.DEPTH_SNAPSHOT_LIMIT || 100);
const DEPTH_BUFFER_LIMIT = Number(process.env.DEPTH_BUFFER_LIMIT || 2000);
const DEPTH_SNAPSHOT_CONCURRENCY = Number(process.env.DEPTH_SNAPSHOT_CONCURRENCY || 4);
const DEPTH_RESYNC_COOLDOWN_MS = Number(process.env.DEPTH_RESYNC_COOLDOWN_MS || 10000);
const DEPTH_SNAPSHOT_SLEEP_MS = Number(process.env.DEPTH_SNAPSHOT_SLEEP_MS || 50);

let ENABLE_AGG_TRADES = String(process.env.ENABLE_AGG_TRADES || 'true') === 'true';
let ENABLE_FORCE_ORDERS = String(process.env.ENABLE_FORCE_ORDERS || 'true') === 'true';
const FORCE_ORDER_ALL_STREAM = String(process.env.FORCE_ORDER_ALL_STREAM || 'true') === 'true';
let ENABLE_MARK_PRICE = String(process.env.ENABLE_MARK_PRICE || 'true') === 'true';
const MARK_PRICE_SPEED = process.env.MARK_PRICE_SPEED || '1s';
let ENABLE_TICKER_STREAM = String(process.env.ENABLE_TICKER_STREAM || 'true') === 'true';
let ENABLE_KLINE_1M = String(process.env.ENABLE_KLINE_1M || 'true') === 'true';
let ENABLE_KLINE_5M = String(process.env.ENABLE_KLINE_5M || 'true') === 'true';

if (MODE === 'depth') {
  ENABLE_DEPTH = true;
  ENABLE_BOOK_TICKER = false;
  ENABLE_AGG_TRADES = false;
  ENABLE_FORCE_ORDERS = false;
  ENABLE_MARK_PRICE = false;
  ENABLE_TICKER_STREAM = false;
  ENABLE_KLINE_1M = false;
  ENABLE_KLINE_5M = false;
}

if (MODE === 'flow') {
  ENABLE_DEPTH = false;
}

const OPEN_INTEREST_POLL_MS = Number(process.env.OPEN_INTEREST_POLL_MS || 30 * 1000);
const FUNDING_POLL_MS = Number(process.env.FUNDING_POLL_MS || 30 * 1000);
const STATS24H_POLL_MS = Number(process.env.STATS24H_POLL_MS || 60 * 1000);

const FLOW_WINDOWS_SEC = parseNumberList(process.env.FLOW_WINDOWS_SEC || '1,5,30,300');
const FLOW_DEFAULT_WINDOW_SEC = Number(process.env.FLOW_DEFAULT_WINDOW_SEC || 5);
const FLOW_WINDOWS_MS = ensureDefaultWindow(FLOW_WINDOWS_SEC, FLOW_DEFAULT_WINDOW_SEC).map(
  (value) => value * 1000
);
const FLOW_DEFAULT_WINDOW_MS = FLOW_DEFAULT_WINDOW_SEC * 1000;

const LIQ_WINDOWS_SEC = parseNumberList(process.env.LIQ_WINDOWS_SEC || '1,5,30,300');
const LIQ_DEFAULT_WINDOW_SEC = Number(process.env.LIQ_DEFAULT_WINDOW_SEC || 5);
const LIQ_WINDOWS_MS = ensureDefaultWindow(LIQ_WINDOWS_SEC, LIQ_DEFAULT_WINDOW_SEC).map(
  (value) => value * 1000
);
const LIQ_DEFAULT_WINDOW_MS = LIQ_DEFAULT_WINDOW_SEC * 1000;

const CONTEXT_ENABLE = String(process.env.CONTEXT_ENABLE || 'true') === 'true';
const CONTEXT_SYMBOLS = CONTEXT_ENABLE
  ? parseSymbolList(process.env.CONTEXT_SYMBOLS || 'BTCUSDT,ETHUSDT')
  : [];
const CONTEXT_WINDOWS_MIN = parseNumberList(process.env.CONTEXT_WINDOWS_MIN || '1,5,15');
const CONTEXT_WINDOWS_MS = CONTEXT_WINDOWS_MIN.map((value) => value * 60 * 1000);

const DATA_DIR = process.env.DATA_DIR || path.join(__dirname, 'data');
const LOG_RAW_EVENTS = String(process.env.LOG_RAW_EVENTS || 'true') === 'true';
const LOG_RAW_EVENTS_TYPES = new Set(
  (process.env.LOG_RAW_EVENTS_TYPES || 'aggTrade,bookTicker,depthUpdate,markPriceUpdate,forceOrder')
    .split(',')
    .map((item) => item.trim())
    .filter(Boolean)
);
const MAX_STREAMS_PER_WS = Number(process.env.MAX_STREAMS_PER_WS || 120);

const WRITER_FLUSH_INTERVAL_MS = Number(process.env.WRITER_FLUSH_INTERVAL_MS || 1000);
const WRITER_MAX_BUFFER_LINES = Number(process.env.WRITER_MAX_BUFFER_LINES || 1000);
const WRITER_MAX_OPEN_STREAMS = Number(process.env.WRITER_MAX_OPEN_STREAMS || 64);

const METRICS_LOG_MS = Number(process.env.METRICS_LOG_MS || 60 * 1000);
const STALE_THRESHOLD_MS = Number(process.env.STALE_THRESHOLD_MS || 15 * 1000);

const state = {
  symbols: [],
  contextSymbols: CONTEXT_SYMBOLS,
  lastSymbolRefresh: 0,
  book: new Map(),
  mark: new Map(),
  openInterest: new Map(),
  stats24h: new Map(),
  kline1m: new Map(),
  kline5m: new Map(),
  priceSeries: new Map(),
  lastSnapshot: new Map(),
  metrics: {
    snapshotsWritten: 0,
    lastLogTime: Date.now(),
    lastLogCount: 0,
    depthEvents: 0,
    lastDepthLog: Date.now(),
    lastDepthCount: 0
  }
};

const intervals = [];
let shuttingDown = false;

const writers = createBufferedWriter(DATA_DIR, {
  flushIntervalMs: WRITER_FLUSH_INTERVAL_MS,
  maxBufferLines: WRITER_MAX_BUFFER_LINES,
  maxOpenStreams: WRITER_MAX_OPEN_STREAMS
});

const depthManager = new DepthManager({
  restBase: REST_BASE,
  fetch,
  snapshotLimit: DEPTH_SNAPSHOT_LIMIT,
  bufferLimit: DEPTH_BUFFER_LIMIT,
  depthLevel: DEPTH_LEVEL,
  snapshotConcurrency: DEPTH_SNAPSHOT_CONCURRENCY,
  resyncCooldownMs: DEPTH_RESYNC_COOLDOWN_MS,
  snapshotSleepMs: DEPTH_SNAPSHOT_SLEEP_MS,
  streamMode: DEPTH_STREAM_MODE,
  log: console
});

const flowManager = new RollingManager(FLOW_WINDOWS_MS);
const liqManager = new RollingManager(LIQ_WINDOWS_MS);

async function init() {
  state.symbols = await resolveSymbols();
  if (!state.symbols.length) {
    console.error('No symbols found. Check SYMBOLS_FILE or TOP_N.');
    process.exit(1);
  }
  state.lastSymbolRefresh = Date.now();
  console.log(`Ultimate collector tracking ${state.symbols.length} symbols.`);

  await refreshRestState();
  buildStreams();

  if (ENABLE_DEPTH) {
    setTimeout(() => {
      depthManager.syncAll(state.symbols).catch((error) => {
        console.warn(`Depth sync failed: ${error.message}`);
      });
    }, 1500);
  }

  intervals.push(setInterval(snapshotTick, SNAPSHOT_INTERVAL_MS));
  if (!FREEZE_SYMBOLS && SYMBOL_REFRESH_MS > 0) {
    intervals.push(setInterval(refreshSymbolsIfNeeded, SYMBOL_REFRESH_MS));
  } else {
    console.log('Symbol refresh disabled (FREEZE_SYMBOLS=true or SYMBOL_REFRESH_MS<=0).');
  }
  intervals.push(setInterval(refreshOpenInterest, OPEN_INTEREST_POLL_MS));
  intervals.push(setInterval(refreshFunding, FUNDING_POLL_MS));
  intervals.push(setInterval(refresh24hStats, STATS24H_POLL_MS));
  intervals.push(setInterval(logMetrics, METRICS_LOG_MS));
}

async function resolveSymbols() {
  const sources = [];
  if (TOP_N > 0) {
    const top = await fetchTopSymbols(TOP_N);
    if (top.length) {
      sources.push(top);
    }
  }
  const fromFile = loadSymbolsFromFile();
  if (fromFile.length) {
    sources.push(fromFile);
  }
  if (USE_ALL_SYMBOLS) {
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
  const merged = sources.reduce((acc, list) => acc.concat(list), []);
  return [...new Set(merged.map((value) => String(value || '').toUpperCase()))].filter(
    (symbol) => /^[A-Z0-9]+USDT$/.test(symbol)
  );
}

function loadSymbolsFromFile() {
  if (!SYMBOLS_FILE || !String(SYMBOLS_FILE).trim()) {
    return [];
  }
  const filePath = path.join(__dirname, SYMBOLS_FILE);
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
  try {
    const response = await fetch(`${REST_BASE}/fapi/v1/exchangeInfo`);
    if (!response.ok) {
      return [];
    }
    const payload = await response.json();
    return (payload.symbols || [])
      .map((row) => row.symbol)
      .filter((symbol) => symbol && symbol.endsWith('USDT'));
  } catch {
    return [];
  }
}

async function fetchTopSymbols(limit) {
  try {
    const response = await fetch(`${REST_BASE}/fapi/v1/ticker/24hr`);
    if (!response.ok) {
      return [];
    }
    const payload = await response.json();
    if (!Array.isArray(payload)) {
      return [];
    }
    return payload
      .filter((row) => row.symbol && row.symbol.endsWith('USDT'))
      .sort((a, b) => Number(b.quoteVolume || 0) - Number(a.quoteVolume || 0))
      .slice(0, limit)
      .map((row) => row.symbol);
  } catch {
    return [];
  }
}

async function refreshSymbolsIfNeeded() {
  if (FREEZE_SYMBOLS) {
    return;
  }
  const now = Date.now();
  if (state.lastSymbolRefresh && now - state.lastSymbolRefresh < SYMBOL_REFRESH_MS) {
    return;
  }
  const next = await resolveSymbols();
  if (!next.length) {
    return;
  }
  const key = next.join(',');
  const prevKey = state.symbols.join(',');
  state.symbols = next;
  state.lastSymbolRefresh = now;
  if (key !== prevKey) {
    console.log(`Symbols refreshed (${state.symbols.length}). Rebuilding streams.`);
    buildStreams(true);
    if (ENABLE_DEPTH) {
      depthManager.syncAll(state.symbols).catch((error) => {
        console.warn(`Depth sync failed after refresh: ${error.message}`);
      });
    }
  }
}

function buildStreams(rebuild = false) {
  if (rebuild && state._streamManager) {
    state._streamManager.closeAll();
  }
  const streams = [];
  const symbolsLower = state.symbols.map((sym) => sym.toLowerCase());

  if (ENABLE_BOOK_TICKER) {
    if (BOOK_TICKER_ALL_STREAM) {
      streams.push('!bookTicker@arr');
    } else {
      streams.push(...symbolsLower.map((sym) => `${sym}@bookTicker`));
    }
  }
  if (ENABLE_DEPTH) {
    const depthStream =
      DEPTH_STREAM_MODE === 'partial'
        ? (sym) => `${sym}@depth${DEPTH_LEVEL}@${DEPTH_SPEED}`
        : (sym) => `${sym}@depth@${DEPTH_SPEED}`;
    streams.push(...symbolsLower.map(depthStream));
  }
  if (ENABLE_AGG_TRADES) {
    streams.push(...symbolsLower.map((sym) => `${sym}@aggTrade`));
  }
  if (ENABLE_FORCE_ORDERS) {
    if (FORCE_ORDER_ALL_STREAM) {
      streams.push('!forceOrder@arr');
    } else {
      streams.push(...symbolsLower.map((sym) => `${sym}@forceOrder`));
    }
  }
  if (ENABLE_MARK_PRICE) {
    streams.push(...symbolsLower.map((sym) => `${sym}@markPrice@${MARK_PRICE_SPEED}`));
  }
  if (ENABLE_TICKER_STREAM) {
    streams.push('!ticker@arr');
  }
  if (ENABLE_KLINE_1M) {
    streams.push(...symbolsLower.map((sym) => `${sym}@kline_1m`));
  }
  if (ENABLE_KLINE_5M) {
    streams.push(...symbolsLower.map((sym) => `${sym}@kline_5m`));
  }

  state._streamManager = new StreamManager(WS_BASE, streams, handleStreamMessage, MAX_STREAMS_PER_WS);
  state._streamManager.connectAll();
}

class StreamManager {
  constructor(base, streams, onMessage, maxPerConn) {
    this.base = base.replace(/\/$/, '');
    this.streams = streams;
    this.onMessage = onMessage;
    this.maxPerConn = maxPerConn;
    this.sockets = [];
    this.closed = false;
  }

  connectAll() {
    const chunks = chunkArray(this.streams, this.maxPerConn);
    chunks.forEach((chunk) => this.openConnection(chunk));
  }

  openConnection(streams) {
    if (!streams.length) {
      return;
    }
    const url = `${this.base}/stream?streams=${streams.join('/')}`;
    const ws = new WebSocket(url);
    ws.on('open', () => console.log(`WS connected (${streams.length} streams)`));
    ws.on('message', (data) => {
      try {
        const payload = JSON.parse(data.toString());
        this.onMessage(payload);
      } catch {
        // ignore malformed payloads
      }
    });
    ws.on('close', () => {
      if (this.closed || shuttingDown) {
        return;
      }
      console.warn('WS closed, reconnecting...');
      setTimeout(() => this.openConnection(streams), 1500);
    });
    ws.on('error', () => {
      if (this.closed || shuttingDown) {
        return;
      }
      ws.close();
    });
    this.sockets.push(ws);
  }

  closeAll() {
    this.closed = true;
    this.sockets.forEach((ws) => {
      try {
        ws.close();
      } catch {
        // ignore
      }
    });
    this.sockets = [];
  }
}

function chunkArray(list, size) {
  const out = [];
  for (let i = 0; i < list.length; i += size) {
    out.push(list.slice(i, i + size));
  }
  return out;
}

function handleStreamMessage(msg) {
  if (!msg) {
    return;
  }
  const data = msg.data || msg;
  if (!data || !data.e) {
    if (Array.isArray(data)) {
      handleTickerArray(data);
    }
    return;
  }
  if (LOG_RAW_EVENTS && LOG_RAW_EVENTS_TYPES.has(data.e)) {
    const recvTime = Date.now();
    const eventTime = resolveEventTime(data);
    writers.write(
      `events_${data.e}`,
      { type: data.e, time: eventTime, recvTime, data },
      eventTime
    );
  }
  switch (data.e) {
    case 'bookTicker':
      handleBookTicker(data);
      break;
    case 'depthUpdate':
      handleDepth(data);
      break;
    case 'aggTrade':
      handleAggTrade(data);
      break;
    case 'forceOrder':
      handleForceOrder(data);
      break;
    case 'markPriceUpdate':
      handleMarkPrice(data);
      break;
    case 'kline':
      handleKline(data);
      break;
    case '24hrTicker':
      handleTicker(data);
      break;
    default:
      break;
  }
}

function handleTickerArray(payload) {
  payload.forEach((row) => {
    if (!row || !row.s) {
      return;
    }
    handleTicker(row);
  });
}

function handleTicker(row) {
  const symbol = row.s;
  if (!symbol || !state.symbols.includes(symbol)) {
    return;
  }
  const eventTime = resolveEventTime(row);
  const recvTime = Date.now();
  const lastPrice = safeNum(row.c);
  state.stats24h.set(symbol, {
    priceChangePercent: safeNum(row.P),
    highPrice: safeNum(row.h),
    lowPrice: safeNum(row.l),
    volume: safeNum(row.v),
    quoteVolume: safeNum(row.q),
    lastPrice,
    time: eventTime,
    recvTime
  });
  if (Number.isFinite(lastPrice)) {
    pushPrice(symbol, lastPrice, eventTime);
  }
}

function handleBookTicker(data) {
  const symbol = data.s;
  if (!symbol) {
    return;
  }
  const eventTime = resolveEventTime(data);
  const recvTime = Date.now();
  if (LOG_RAW_EVENTS) {
    writers.write(
      'events_bookTicker',
      { type: 'bookTicker', time: eventTime, recvTime, data },
      eventTime
    );
  }
  const bid = safeNum(data.b);
  const ask = safeNum(data.a);
  state.book.set(symbol, {
    bid,
    bidQty: safeNum(data.B),
    ask,
    askQty: safeNum(data.A),
    time: eventTime,
    recvTime
  });
  const mid = Number.isFinite(bid) && Number.isFinite(ask) ? (bid + ask) / 2 : bid;
  if (Number.isFinite(mid)) {
    pushPrice(symbol, mid, eventTime);
  }
}

function handleDepth(data) {
  const symbol = data.s;
  if (!symbol) {
    return;
  }
  state.metrics.depthEvents += 1;
  if (LOG_RAW_EVENTS) {
    const eventTime = resolveEventTime(data);
    const recvTime = Date.now();
    writers.write(
      'events_depthUpdate',
      { type: 'depthUpdate', time: eventTime, recvTime, data },
      eventTime
    );
  }
  depthManager.handleEvent(symbol, data);
}

function handleAggTrade(data) {
  const symbol = data.s;
  if (!symbol) {
    return;
  }
  const eventTime = safeNum(data.T);
  const eventTimeSafe = Number.isFinite(eventTime) ? eventTime : resolveEventTime(data);
  const price = safeNum(data.p);
  const qty = safeNum(data.q);
  const isBuyerMaker = data.m === true;
  const side = isBuyerMaker ? 'SELL' : 'BUY';
  if (!Number.isFinite(qty) || qty <= 0) {
    return;
  }
  flowManager.add(symbol, price, qty, side, eventTimeSafe);
  if (Number.isFinite(price)) {
    pushPrice(symbol, price, eventTimeSafe);
  }
}

function handleForceOrder(data) {
  const order = data.o || data;
  const symbol = order.s;
  if (!symbol) {
    return;
  }
  const eventTime = safeNum(order.T);
  const eventTimeSafe = Number.isFinite(eventTime) ? eventTime : resolveEventTime(data);
  const side = order.S || order.side;
  const qty = safeNum(order.q || order.l || order.Q);
  const price = safeNum(order.p || order.ap || order.P);
  if (!Number.isFinite(qty) || qty <= 0) {
    return;
  }
  liqManager.add(symbol, price, qty, side === 'BUY' ? 'BUY' : 'SELL', eventTimeSafe);
}

function handleMarkPrice(data) {
  const symbol = data.s;
  if (!symbol) {
    return;
  }
  const eventTime = resolveEventTime(data);
  const recvTime = Date.now();
  const markPrice = safeNum(data.p);
  state.mark.set(symbol, {
    markPrice,
    indexPrice: safeNum(data.i),
    fundingRate: safeNum(data.r),
    nextFundingTime: safeNum(data.T),
    time: eventTime,
    recvTime
  });
  if (Number.isFinite(markPrice)) {
    pushPrice(symbol, markPrice, eventTime);
  }
}

function handleKline(data) {
  const k = data.k;
  if (!k) {
    return;
  }
  const symbol = k.s;
  const interval = k.i;
  const eventTime = safeNum(k.T);
  const eventTimeSafe = Number.isFinite(eventTime) ? eventTime : resolveEventTime(data);
  const payload = {
    open: safeNum(k.o),
    high: safeNum(k.h),
    low: safeNum(k.l),
    close: safeNum(k.c),
    volume: safeNum(k.v),
    tradeCount: safeNum(k.n),
    start: safeNum(k.t),
    end: safeNum(k.T),
    isFinal: k.x === true,
    time: eventTimeSafe,
    recvTime: Date.now()
  };
  if (interval === '1m') {
    state.kline1m.set(symbol, payload);
  }
  if (interval === '5m') {
    state.kline5m.set(symbol, payload);
  }
  if (payload.isFinal && Number.isFinite(payload.close) && Number.isFinite(payload.end)) {
    pushPrice(symbol, payload.close, payload.end);
  }
}

async function refreshRestState() {
  await Promise.all([refreshOpenInterest(), refreshFunding(), refresh24hStats()]);
}

async function refreshOpenInterest() {
  const symbols = state.symbols.slice();
  await runPool(
    symbols,
    6,
    async (symbol) => {
      try {
        const response = await fetch(`${REST_BASE}/fapi/v1/openInterest?symbol=${symbol}`);
        if (!response.ok) {
          return;
        }
        const payload = await response.json();
        const now = Date.now();
        state.openInterest.set(symbol, {
          openInterest: safeNum(payload.openInterest),
          time: now,
          recvTime: now
        });
      } catch {
        // ignore polling errors
      }
    },
    (symbol, error) => {
      console.warn(`[${symbol}] open interest poll failed: ${error.message}`);
    }
  );
}

async function refreshFunding() {
  const symbols = state.symbols.slice();
  await runPool(
    symbols,
    6,
    async (symbol) => {
      try {
        const response = await fetch(`${REST_BASE}/fapi/v1/premiumIndex?symbol=${symbol}`);
        if (!response.ok) {
          return;
        }
        const payload = await response.json();
        const now = Date.now();
        state.mark.set(symbol, {
          markPrice: safeNum(payload.markPrice),
          indexPrice: safeNum(payload.indexPrice),
          fundingRate: safeNum(payload.lastFundingRate),
          nextFundingTime: safeNum(payload.nextFundingTime),
          time: now,
          recvTime: now
        });
      } catch {
        // ignore polling errors
      }
    },
    (symbol, error) => {
      console.warn(`[${symbol}] funding poll failed: ${error.message}`);
    }
  );
}

async function refresh24hStats() {
  if (ENABLE_TICKER_STREAM) {
    return;
  }
  const symbols = state.symbols.slice();
  await runPool(
    symbols,
    6,
    async (symbol) => {
      try {
        const response = await fetch(`${REST_BASE}/fapi/v1/ticker/24hr?symbol=${symbol}`);
        if (!response.ok) {
          return;
        }
        const payload = await response.json();
        const now = Date.now();
        state.stats24h.set(symbol, {
          priceChangePercent: safeNum(payload.priceChangePercent),
          highPrice: safeNum(payload.highPrice),
          lowPrice: safeNum(payload.lowPrice),
          volume: safeNum(payload.volume),
          quoteVolume: safeNum(payload.quoteVolume),
          lastPrice: safeNum(payload.lastPrice),
          time: now,
          recvTime: now
        });
      } catch {
        // ignore polling errors
      }
    },
    (symbol, error) => {
      console.warn(`[${symbol}] 24h stats poll failed: ${error.message}`);
    }
  );
}

function snapshotTick() {
  const now = Date.now();
  const contextFeatures = buildContextFeatures(now);
  for (const symbol of state.symbols) {
    const snap = buildSnapshot(symbol, now, contextFeatures);
    if (!snap) {
      continue;
    }
    writers.write('snapshots', snap, now);
    state.lastSnapshot.set(symbol, snap);
    state.metrics.snapshotsWritten += 1;
  }
}

function buildContextFeatures(now) {
  if (!CONTEXT_ENABLE || !state.contextSymbols.length) {
    return {};
  }
  const out = {};
  for (const ctxSymbol of state.contextSymbols) {
    const series = state.priceSeries.get(ctxSymbol) || [];
    const ctxPrice = series.length ? series[series.length - 1].p : null;
    out[`ctx_${ctxSymbol}_price`] = Number.isFinite(ctxPrice) ? ctxPrice : null;
    for (const windowMs of CONTEXT_WINDOWS_MS) {
      const label = `${Math.round(windowMs / 60000)}m`;
      out[`ctx_${ctxSymbol}_trend_${label}`] = calcTrend(series, now, windowMs);
      out[`ctx_${ctxSymbol}_vol_${label}`] = calcVol(series, now, windowMs);
    }
  }
  return out;
}

function snapshotRolling(manager, symbol, now, defaultWindowMs, prefix) {
  const { metricsByWindow, lastEventTime } = manager.snapshot(symbol, now);
  const defaultMetrics = metricsByWindow.get(defaultWindowMs) || emptyMetrics();
  const expanded = manager.expandFeatures(prefix, metricsByWindow);
  return { metricsByWindow, defaultMetrics, lastEventTime, expanded };
}

function buildSnapshot(symbol, now, contextFeatures) {
  const book = state.book.get(symbol) || {};
  const mark = state.mark.get(symbol) || {};
  const oi = state.openInterest.get(symbol) || {};
  const stats = state.stats24h.get(symbol) || {};
  const k1 = state.kline1m.get(symbol) || {};
  const k5 = state.kline5m.get(symbol) || {};
  const depthStats = depthManager.getStats(symbol);

  const price = firstFinite(mark.markPrice, book.bid, book.ask, stats.lastPrice, k1.close, k5.close);
  if (!Number.isFinite(price)) {
    return null;
  }

  const spread = calcSpreadPct(book.bid, book.ask);
  const imbalance = calcImbalance(book.bidQty, book.askQty);
  const basis = calcBasis(mark.markPrice, mark.indexPrice);

  const series = state.priceSeries.get(symbol) || [];
  const trend5m = calcTrend(series, now, 5 * 60 * 1000);
  const trend15m = calcTrend(series, now, 15 * 60 * 1000);
  const vol5m = calcVol(series, now, 5 * 60 * 1000);
  const vol15m = calcVol(series, now, 15 * 60 * 1000);

  const flowSnap = snapshotRolling(flowManager, symbol, now, FLOW_DEFAULT_WINDOW_MS, 'flow');
  const liqSnap = snapshotRolling(liqManager, symbol, now, LIQ_DEFAULT_WINDOW_MS, 'liq');
  const flowDefault = flowSnap.defaultMetrics;
  const liqDefault = liqSnap.defaultMetrics;

  const ageFeatures = {
    bookAgeMs: ageMs(now, book.time),
    depthAgeMs: ageMs(now, depthStats.lastEventTime),
    markAgeMs: ageMs(now, mark.time),
    openInterestAgeMs: ageMs(now, oi.time),
    stats24hAgeMs: ageMs(now, stats.time),
    kline1mAgeMs: ageMs(now, k1.time),
    kline5mAgeMs: ageMs(now, k5.time),
    flowAgeMs: ageMs(now, flowSnap.lastEventTime),
    liqAgeMs: ageMs(now, liqSnap.lastEventTime)
  };

  const featureBase = {
    markPrice: mark.markPrice !== undefined && mark.markPrice !== null ? mark.markPrice : null,
    indexPrice: mark.indexPrice !== undefined && mark.indexPrice !== null ? mark.indexPrice : null,
    fundingRate: mark.fundingRate !== undefined && mark.fundingRate !== null ? mark.fundingRate : null,
    nextFundingTime:
      mark.nextFundingTime !== undefined && mark.nextFundingTime !== null ? mark.nextFundingTime : null,
    openInterest: oi.openInterest !== undefined && oi.openInterest !== null ? oi.openInterest : null,
    price,
    bestBid: book.bid !== undefined && book.bid !== null ? book.bid : null,
    bestAsk: book.ask !== undefined && book.ask !== null ? book.ask : null,
    spreadPct: spread,
    imbalance,
    depthBidQty: depthStats.bidQty,
    depthAskQty: depthStats.askQty,
    depthImbalance: depthStats.imbalance,
    basisPct: basis,
    changePct24h:
      stats.priceChangePercent !== undefined && stats.priceChangePercent !== null
        ? stats.priceChangePercent
        : null,
    high24h: stats.highPrice !== undefined && stats.highPrice !== null ? stats.highPrice : null,
    low24h: stats.lowPrice !== undefined && stats.lowPrice !== null ? stats.lowPrice : null,
    volume24h: stats.volume !== undefined && stats.volume !== null ? stats.volume : null,
    quoteVolume24h:
      stats.quoteVolume !== undefined && stats.quoteVolume !== null ? stats.quoteVolume : null,
    aggBuyQty: flowDefault.buyQty,
    aggSellQty: flowDefault.sellQty,
    aggBuyCount: flowDefault.buyCount,
    aggSellCount: flowDefault.sellCount,
    aggVWAP: flowDefault.vwap,
    aggBuySellRatio: flowDefault.buySellRatio,
    aggNetQty: flowDefault.netQty,
    aggNetNotional: flowDefault.netNotional,
    liqBuyQty: liqDefault.buyQty,
    liqSellQty: liqDefault.sellQty,
    liqBuyCount: liqDefault.buyCount,
    liqSellCount: liqDefault.sellCount,
    liqVWAP: liqDefault.vwap,
    liqBuySellRatio: liqDefault.buySellRatio,
    liqNetQty: liqDefault.netQty,
    liqNetNotional: liqDefault.netNotional,
    kline1mClose: k1.close !== undefined && k1.close !== null ? k1.close : null,
    kline1mVol: k1.volume !== undefined && k1.volume !== null ? k1.volume : null,
    kline5mClose: k5.close !== undefined && k5.close !== null ? k5.close : null,
    kline5mVol: k5.volume !== undefined && k5.volume !== null ? k5.volume : null,
    trend5m,
    trend15m,
    vol5m,
    vol15m,
    ...ageFeatures,
    ...flowSnap.expanded,
    ...liqSnap.expanded,
    ...contextFeatures
  };

  featureBase.featureCompleteness = calcCompleteness(featureBase, COMPLETENESS_KEYS);
  featureBase.microCompleteness = calcCompleteness(featureBase, MICRO_COMPLETENESS_KEYS);

  const prev = state.lastSnapshot.get(symbol);
  const deltas = buildDeltas(featureBase, prev ? prev.features : null);

  return {
    type: 'snapshot',
    symbol,
    time: now,
    price,
    features: {
      ...featureBase,
      ...deltas
    }
  };
}

function pushPrice(symbol, price, time = Date.now()) {
  if (!Number.isFinite(price)) {
    return;
  }
  if (!state.priceSeries.has(symbol)) {
    state.priceSeries.set(symbol, []);
  }
  const series = state.priceSeries.get(symbol);
  series.push({ t: time, p: price });
  const cutoff = time - 30 * 60 * 1000;
  while (series.length && series[0].t < cutoff) {
    series.shift();
  }
}

function logMetrics() {
  const now = Date.now();
  const elapsedMs = Math.max(1, now - state.metrics.lastLogTime);
  const deltaSnapshots = state.metrics.snapshotsWritten - state.metrics.lastLogCount;
  const snapshotsPerSec = (deltaSnapshots / elapsedMs) * 1000;

  const staleBook = countStale(state.book, now, STALE_THRESHOLD_MS);
  const staleMark = countStale(state.mark, now, STALE_THRESHOLD_MS);
  const staleStats = countStale(state.stats24h, now, STALE_THRESHOLD_MS * 4);
  const staleDepth = depthManager.countStale(state.symbols, now, STALE_THRESHOLD_MS * 2);

  const depthElapsedMs = Math.max(1, now - state.metrics.lastDepthLog);
  const depthDelta = state.metrics.depthEvents - state.metrics.lastDepthCount;
  const depthPerSec = (depthDelta / depthElapsedMs) * 1000;

  console.log(
    `Metrics | snaps/sec=${snapshotsPerSec.toFixed(2)} | depthEvents/sec=${depthPerSec.toFixed(
      2
    )} | stale(book/mark/depth/stats)=${staleBook}/${staleMark}/${staleDepth}/${staleStats}`
  );

  state.metrics.lastLogTime = now;
  state.metrics.lastLogCount = state.metrics.snapshotsWritten;
  state.metrics.lastDepthLog = now;
  state.metrics.lastDepthCount = state.metrics.depthEvents;
}

function countStale(map, now, thresholdMs) {
  let stale = 0;
  for (const symbol of state.symbols) {
    const row = map.get(symbol);
    if (!row || !Number.isFinite(row.time) || now - row.time > thresholdMs) {
      stale += 1;
    }
  }
  return stale;
}


function resolveEventTime(payload) {
  const source = payload || {};
  const eventTime = safeNum(source.E || source.T || source.time);
  return Number.isFinite(eventTime) ? eventTime : Date.now();
}

function shutdown(reason) {
  if (shuttingDown) {
    return;
  }
  shuttingDown = true;
  console.log(`Shutting down collector (${reason})...`);
  intervals.forEach((timer) => clearInterval(timer));
  if (state._streamManager) {
    state._streamManager.closeAll();
  }
  try {
    writers.close();
  } catch {
    // ignore
  }
  process.exit(0);
}

process.on('SIGINT', () => shutdown('SIGINT'));
process.on('SIGTERM', () => shutdown('SIGTERM'));

init().catch((error) => {
  console.error(`Collector failed: ${error.message}`);
  process.exit(1);
});
