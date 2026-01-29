const fs = require('fs');
const path = require('path');

const fetch = global.fetch || require('node-fetch');
try {
  require('dotenv').config({ path: path.join(__dirname, '.env') });
} catch {
  // dotenv is optional
}

const REST_BASE = process.env.FUTURES_REST_URL || 'https://fapi.binance.com';
const TOP_N = Number(process.env.TOP_N || 80);
const OUT_FILE = (process.env.SYMBOLS_FILE || './symbols_topn.txt').trim() || './symbols_topn.txt';
const CONTEXT_SYMBOLS = String(process.env.CONTEXT_SYMBOLS || 'BTCUSDT,ETHUSDT')
  .split(',')
  .map((s) => s.trim().toUpperCase())
  .filter((s) => /^[A-Z0-9]+USDT$/.test(s));

async function fetchTopSymbols(limit) {
  const response = await fetch(`${REST_BASE}/fapi/v1/ticker/24hr`);
  if (!response.ok) {
    throw new Error(`Failed to fetch top symbols: HTTP ${response.status}`);
  }
  const payload = await response.json();
  if (!Array.isArray(payload)) {
    throw new Error('Unexpected ticker payload shape.');
  }
  return payload
    .filter((row) => row.symbol && row.symbol.endsWith('USDT'))
    .sort((a, b) => Number(b.quoteVolume || 0) - Number(a.quoteVolume || 0))
    .slice(0, limit)
    .map((row) => row.symbol.toUpperCase());
}

async function main() {
  if (!(TOP_N > 0)) {
    throw new Error(`TOP_N must be > 0. Got: ${TOP_N}`);
  }
  const top = await fetchTopSymbols(TOP_N);
  const merged = [...new Set([...CONTEXT_SYMBOLS, ...top])];

  const outPath = path.join(__dirname, OUT_FILE);
  const dir = path.dirname(outPath);
  fs.mkdirSync(dir, { recursive: true });
  fs.writeFileSync(outPath, `${merged.join('\n')}\n`, 'utf8');

  console.log(`Wrote ${merged.length} symbols to ${OUT_FILE}`);
  console.log(`First 10: ${merged.slice(0, 10).join(', ')}`);
}

main().catch((error) => {
  console.error(`freeze_symbols failed: ${error.message}`);
  process.exit(1);
});

