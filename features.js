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
    startPrice = series[0]?.p;
  }
  const endPrice = series[series.length - 1]?.p;
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

function calcSpreadPct(bid, ask) {
  if (!Number.isFinite(bid) || !Number.isFinite(ask)) {
    return null;
  }
  const mid = (bid + ask) / 2;
  if (!mid) {
    return null;
  }
  return ((ask - bid) / mid) * 100;
}

function calcImbalance(bidQty, askQty) {
  if (!Number.isFinite(bidQty) || !Number.isFinite(askQty)) {
    return null;
  }
  const total = bidQty + askQty;
  if (!total) {
    return null;
  }
  return (bidQty - askQty) / total;
}

function calcBasis(mark, index) {
  if (!Number.isFinite(mark) || !Number.isFinite(index) || !index) {
    return null;
  }
  return ((mark - index) / index) * 100;
}

function calcCompleteness(obj, keys) {
  if (!keys.length) {
    return null;
  }
  let present = 0;
  for (const key of keys) {
    const value = obj[key];
    if (typeof value === 'boolean' || Number.isFinite(value)) {
      present += 1;
    }
  }
  return present / keys.length;
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
    if (key.endsWith('AgeMs') || key.includes('Completeness')) {
      out[`${key}Delta`] = null;
      return;
    }
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

const COMPLETENESS_KEYS = [
  'markPrice',
  'indexPrice',
  'fundingRate',
  'openInterest',
  'bestBid',
  'bestAsk',
  'spreadPct',
  'imbalance',
  'depthBidQty',
  'depthAskQty',
  'depthImbalance',
  'basisPct',
  'changePct24h',
  'volume24h',
  'quoteVolume24h',
  'kline1mClose',
  'kline1mVol',
  'kline5mClose',
  'kline5mVol',
  'trend5m',
  'trend15m',
  'vol5m',
  'vol15m',
  'aggBuyQty',
  'aggSellQty',
  'liqBuyQty',
  'liqSellQty'
];

const MICRO_COMPLETENESS_KEYS = [
  'bestBid',
  'bestAsk',
  'spreadPct',
  'imbalance',
  'depthBidQty',
  'depthAskQty',
  'depthImbalance',
  'aggBuyQty',
  'aggSellQty',
  'aggBuyCount',
  'aggSellCount',
  'liqBuyQty',
  'liqSellQty',
  'liqBuyCount',
  'liqSellCount'
];

module.exports = {
  calcTrend,
  calcVol,
  calcSpreadPct,
  calcImbalance,
  calcBasis,
  calcCompleteness,
  buildDeltas,
  COMPLETENESS_KEYS,
  MICRO_COMPLETENESS_KEYS
};

