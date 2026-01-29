class RollingManager {
  constructor(windowsMs) {
    this.windowsMs = [...new Set(windowsMs)].sort((a, b) => a - b);
    this.maxWindowMs = this.windowsMs.length ? this.windowsMs[this.windowsMs.length - 1] : 0;
    this.bySymbol = new Map();
  }

  ensure(symbol) {
    if (!this.bySymbol.has(symbol)) {
      this.bySymbol.set(symbol, createRollingState(this.windowsMs));
    }
    return this.bySymbol.get(symbol);
  }

  add(symbol, price, qty, side, time) {
    if (!Number.isFinite(qty) || qty <= 0) {
      return;
    }
    const state = this.ensure(symbol);
    const event = createEvent(price, qty, side, time);
    state.lastEventTime = time;
    for (const window of state.windows) {
      window.queue.push(event);
      applyMetrics(window.metrics, event, 1);
      expireWindow(window, time);
    }
  }

  snapshot(symbol, now) {
    const state = this.ensure(symbol);
    const metricsByWindow = new Map();
    for (const window of state.windows) {
      expireWindow(window, now);
      metricsByWindow.set(window.ms, finalizeMetrics(window.metrics));
    }
    return { metricsByWindow, lastEventTime: state.lastEventTime };
  }

  expandFeatures(prefix, metricsByWindow) {
    const out = {};
    for (const [windowMs, metrics] of metricsByWindow.entries()) {
      const suffix = `${Math.round(windowMs / 1000)}s`;
      out[`${prefix}BuyQty_${suffix}`] = metrics.buyQty;
      out[`${prefix}SellQty_${suffix}`] = metrics.sellQty;
      out[`${prefix}BuyCount_${suffix}`] = metrics.buyCount;
      out[`${prefix}SellCount_${suffix}`] = metrics.sellCount;
      out[`${prefix}VWAP_${suffix}`] = metrics.vwap;
      out[`${prefix}BuySellRatio_${suffix}`] = metrics.buySellRatio;
      out[`${prefix}NetQty_${suffix}`] = metrics.netQty;
      out[`${prefix}NetNotional_${suffix}`] = metrics.netNotional;
    }
    return out;
  }
}

function emptyMetrics() {
  return {
    buyQty: 0,
    sellQty: 0,
    buyCount: 0,
    sellCount: 0,
    vwap: null,
    buySellRatio: null,
    netQty: 0,
    netNotional: 0
  };
}

function createRollingState(windowsMs) {
  return {
    lastEventTime: null,
    windows: windowsMs.map((ms) => ({
      ms,
      queue: [],
      head: 0,
      metrics: {
        buyQty: 0,
        sellQty: 0,
        buyCount: 0,
        sellCount: 0,
        buyNotional: 0,
        sellNotional: 0,
        vwapQty: 0,
        vwapNotional: 0
      }
    }))
  };
}

function createEvent(price, qty, side, time) {
  const notional = Number.isFinite(price) ? price * qty : null;
  return { price, qty, side, notional, t: time };
}

function applyMetrics(metrics, event, sign) {
  const qtyDelta = sign * event.qty;
  const countDelta = sign;
  if (event.side === 'BUY') {
    metrics.buyQty += qtyDelta;
    metrics.buyCount += countDelta;
  } else {
    metrics.sellQty += qtyDelta;
    metrics.sellCount += countDelta;
  }
  if (Number.isFinite(event.notional)) {
    const notionalDelta = sign * event.notional;
    if (event.side === 'BUY') {
      metrics.buyNotional += notionalDelta;
    } else {
      metrics.sellNotional += notionalDelta;
    }
    metrics.vwapQty += qtyDelta;
    metrics.vwapNotional += notionalDelta;
  }
}

function expireWindow(window, now) {
  const cutoff = now - window.ms;
  while (window.head < window.queue.length && window.queue[window.head].t < cutoff) {
    const old = window.queue[window.head];
    window.head += 1;
    applyMetrics(window.metrics, old, -1);
  }
  if (window.head > 1024 && window.head > window.queue.length / 2) {
    window.queue = window.queue.slice(window.head);
    window.head = 0;
  }
}

function finalizeMetrics(metrics) {
  const vwap = metrics.vwapQty > 0 ? metrics.vwapNotional / metrics.vwapQty : null;
  const buySellRatio = metrics.sellQty > 0 ? metrics.buyQty / metrics.sellQty : null;
  const netQty = metrics.buyQty - metrics.sellQty;
  const netNotional = metrics.buyNotional - metrics.sellNotional;
  return {
    buyQty: metrics.buyQty,
    sellQty: metrics.sellQty,
    buyCount: metrics.buyCount,
    sellCount: metrics.sellCount,
    vwap: Number.isFinite(vwap) ? vwap : null,
    buySellRatio: Number.isFinite(buySellRatio) ? buySellRatio : null,
    netQty,
    netNotional: Number.isFinite(netNotional) ? netNotional : 0
  };
}

module.exports = {
  RollingManager,
  emptyMetrics
};

