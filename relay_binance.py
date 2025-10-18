from flask import Flask, request, jsonify
import os, time, requests, math

app = Flask(__name__)

# ============== CONFIGURAÇÕES GERAIS ==============
BINANCE_BASE = os.getenv("BINANCE_BASE_URL", "https://fapi.binance.com/fapi/v1")
DEFAULT_INTERVAL = "5m"
INTERVAL_MS = 5 * 60 * 1000
DEFAULT_N = int(os.getenv("SNAPSHOT_N", "50"))
HTTP_TIMEOUT = float(os.getenv("HTTP_TIMEOUT", "12.0"))
AGG_LIMIT = int(os.getenv("AGG_LIMIT", "1000"))

# Guard-rails
DIST_MIN = float(os.getenv("DIST_MIN", "0.25"))
SPREAD_MAX_BPS = float(os.getenv("SPREAD_MAX_BPS", "3.0"))
MIN_DEPTH_QTY = float(os.getenv("MIN_DEPTH_QTY", "50"))
ATR_M5_PCTL_MIN = float(os.getenv("ATR_M5_PCTL_MIN", "0.20"))

_snapshot_cache = {}

def now_ms() -> int:
    return int(time.time() * 1000)


# ============== FUNÇÃO HTTP ROBUSTA ==============
def _binance_get(path: str, params: dict):
    """Chamada GET limpa (sem headers de API) + retry automático."""
    url = f"{BINANCE_BASE}{path}"
    for attempt in range(3):
        try:
            print(f"[DEBUG] GET {url} (tentativa {attempt+1}) params={params}")
            # headers explícitos e limpos (sem X-MBX-APIKEY)
            r = requests.get(url, params=params, headers={}, timeout=HTTP_TIMEOUT)
            if r.status_code == 418:
                print("[ERROR] Binance 418: bloqueio leve detectado (API key em rota pública?)")
            if r.status_code == 200:
                return r.json(), r.status_code
            print(f"[WARN] Status {r.status_code} da Binance -> retry em 1.5s")
        except requests.exceptions.Timeout:
            print(f"[TIMEOUT] {url}")
        except requests.exceptions.RequestException as e:
            print(f"[ERROR] _binance_get Exception: {e}")
        time.sleep(1.5)
    return {"code": "UPSTREAM_ERROR", "message": f"Falha ao conectar {url}"}, 503


# ============== FUNÇÕES AUXILIARES DE DADOS ==============
def fetch_klines_closed(symbol: str, n: int) -> list:
    body, status = _binance_get("/klines", {"symbol": symbol, "interval": DEFAULT_INTERVAL, "limit": n + 2})
    if status != 200:
        raise RuntimeError(f"klines upstream {status}")
    raw, t_now = body, now_ms()
    closed = []
    for row in raw:
        open_time = int(row[0])
        close_time = open_time + INTERVAL_MS
        if close_time <= t_now:
            closed.append({
                "open_time": open_time, "close_time": close_time,
                "open": str(row[1]), "high": str(row[2]), "low": str(row[3]),
                "close": str(row[4]), "volume": str(row[5]),
            })
    if not closed:
        raise RuntimeError("no closed candles yet")
    return closed[-n:]


def fetch_klines_closed_interval(symbol: str, interval: str, n: int) -> list:
    body, status = _binance_get("/klines", {"symbol": symbol, "interval": interval, "limit": n + 2})
    if status != 200:
        raise RuntimeError(f"klines upstream {status}")
    raw, t_now = body, now_ms()
    closed = []
    for i, row in enumerate(raw):
        open_time = int(row[0])
        if i + 1 < len(raw):
            close_time = int(raw[i + 1][0])
        else:
            dt = int(row[0]) - int(raw[i - 1][0]) if i > 0 else INTERVAL_MS
            close_time = open_time + dt
        if close_time <= t_now:
            closed.append({
                "open_time": open_time, "close_time": close_time,
                "open": str(row[1]), "high": str(row[2]), "low": str(row[3]),
                "close": str(row[4]), "volume": str(row[5]),
            })
    if not closed:
        raise RuntimeError("no closed candles yet")
    return closed[-n:]


def ema(values: list, period: int) -> float:
    if not values or len(values) < period:
        return 0.0
    k = 2.0 / (period + 1)
    e = values[0]
    for v in values[1:]:
        e = v * k + e * (1 - k)
    return e


def slope_dir(series: list, lookback: int = 5) -> int:
    if len(series) <= lookback:
        return 0
    a, b = series[-1], series[-1 - lookback]
    tol = max(1e-9, abs(a) * 1e-4)
    if a > b + tol: return +1
    if a < b - tol: return -1
    return 0


def depth_snapshot(symbol: str, limit: int = 5):
    body, status = _binance_get("/depth", {"symbol": symbol, "limit": limit})
    if status != 200:
        return None
    bids, asks = body.get("bids", []), body.get("asks", [])
    bid_sum = sum(float(q) for _, q in bids)
    ask_sum = sum(float(q) for _, q in asks)
    spread_bps = 1e9
    if bids and asks:
        best_bid, best_ask = float(bids[0][0]), float(asks[0][0])
        mid = (best_bid + best_ask) / 2.0
        spread_bps = (best_ask - best_bid) / mid * 1e4 if mid else 1e9
    return {"IB": 0.0, "spread_bps": spread_bps, "depth_top5_qty": bid_sum + ask_sum}


def atr14_from_candles(candles: list) -> float:
    if len(candles) < 15:
        return 0.0
    trs, prev_close = [], None
    for c in candles:
        h, l, cl = float(c["high"]), float(c["low"]), float(c["close"])
        tr = max(h - l, abs(h - prev_close), abs(l - prev_close)) if prev_close else h - l
        trs.append(tr)
        prev_close = cl
    return sum(trs[-14:]) / 14.0


def fetch_agg_trades_window(symbol: str, start_ms: int, end_ms: int, per_req: int = AGG_LIMIT) -> list:
    out, params, safety = [], {"symbol": symbol, "startTime": start_ms, "endTime": end_ms, "limit": per_req}, 0
    while True:
        safety += 1
        if safety > 50:
            break
        r = requests.get(f"{BINANCE_BASE}/aggTrades", params=params, headers={}, timeout=HTTP_TIMEOUT)
        if r.status_code != 200:
            break
        batch = r.json()
        if not batch:
            break
        out.extend(batch)
        next_start = int(batch[-1].get("T", params["startTime"])) + 1
        if next_start >= end_ms:
            break
        params["startTime"] = next_start
    return out


def agg_stats_for_window(symbol: str, start_ms: int, end_ms: int):
    agg = fetch_agg_trades_window(symbol, start_ms, end_ms, per_req=AGG_LIMIT)
    buy_vol = sell_vol = 0.0
    for t in agg:
        qty = float(t["q"])
        if t.get("m", False):
            sell_vol += qty
        else:
            buy_vol += qty
    total = buy_vol + sell_vol
    pct_aggressive_buy = (buy_vol / total * 100.0) if total > 0 else 0.0
    return {"buy_vol": buy_vol, "sell_vol": sell_vol, "pct_agg_buy": pct_aggressive_buy, "delta": buy_vol - sell_vol}


def zscore(value: float, series: list) -> float:
    if not series or len(series) < 2:
        return 0.0
    m = sum(series) / len(series)
    sd = math.sqrt(sum((x - m) ** 2 for x in series) / (len(series) - 1))
    return (value - m) / (sd if sd > 1e-12 else 1.0)


def deltas_last_k_m5(symbol: str, candles_m5: list, k: int = 12) -> list:
    out = []
    for c in candles_m5[-(min(k, len(candles_m5) - 1) + 1):-1]:
        stats = agg_stats_for_window(symbol, c["open_time"], c["close_time"])
        out.append(stats["delta"])
    return out


def compute_regime_h1(symbol: str):
    candles_h1 = fetch_klines_closed_interval(symbol, "1h", 250)
    closes = [float(c["close"]) for c in candles_h1]
    ema200 = ema(closes, 200)
    ema_series, e, k = [], None, 2.0 / (200 + 1)
    for v in closes:
        e = v if e is None else v * k + e * (1 - k)
        ema_series.append(e)
    slope = slope_dir(ema_series, 5)
    atr_h1 = atr14_from_candles(candles_h1)
    last_close = closes[-1]
    dist_norm = abs(last_close - ema200) / atr_h1 if atr_h1 > 0 else 0.0
    return {
        "ema200_h1": ema200,
        "ema200_h1_slope": slope,
        "close_h1": last_close,
        "atr14_h1": atr_h1,
        "distance_to_ema200_h1": dist_norm,
        "regime_above": last_close > ema200,
        "regime_below": last_close < ema200
    }


# ============== ROTAS FLASK ==============
@app.route("/")
def home():
    return {"status": "ok", "message": "Relay Binance ativo"}


@app.route("/m5-snapshot")
def m5_snapshot():
    symbol = request.args.get("symbol", "BTCUSDT").upper()
    n = max(20, min(100, int(request.args.get("n", DEFAULT_N))))
    ck, t_now = (symbol, n), now_ms()
    cached = _snapshot_cache.get(ck)
    if cached and t_now < cached["expires"]:
        return jsonify(cached["payload"])

    try:
        candles = fetch_klines_closed(symbol, n)
    except Exception as e:
        return jsonify({"code": "SNAPSHOT_NOT_READY", "message": str(e)}), 503

    last = candles[-1]
    last_open, last_close = last["open_time"], last["close_time"]

    try:
        regime = compute_regime_h1(symbol)
    except Exception as e:
        regime = {"error": str(e), "ema200_h1": 0, "ema200_h1_slope": 0,
                  "atr14_h1": 0, "distance_to_ema200_h1": 0,
                  "regime_above": False, "regime_below": False}

    try:
        flow_last = agg_stats_for_window(symbol, last_open, last_close)
        deltas_hist = deltas_last_k_m5(symbol, candles, k=12)
        ofi_z_proxy = zscore(flow_last["delta"], deltas_hist) if deltas_hist else 0.0
        coverage = "full"
    except Exception:
        flow_last = {"buy_vol": 0, "sell_vol": 0, "pct_agg_buy": 0, "delta": 0}
        ofi_z_proxy, coverage = 0.0, "unknown"

    atr_m5 = atr14_from_candles(candles)
    atr_series = [atr14_from_candles(candles[:i+1]) for i in range(1, len(candles))]
    atr_m5_pct = sum(1 for x in atr_series[-50:] if x <= atr_m5) / max(1, len(atr_series[-50:]))

    depth = depth_snapshot(symbol, limit=5) or {"IB": 0.0, "spread_bps": 1e9, "depth_top5_qty": 0.0}

    guards = {
        "distance_ok": regime["distance_to_ema200_h1"] >= DIST_MIN,
        "spread_ok": depth["spread_bps"] <= SPREAD_MAX_BPS,
        "depth_ok": depth["depth_top5_qty"] >= MIN_DEPTH_QTY,
        "market_ok": atr_m5_pct >= ATR_M5_PCTL_MIN,
        "distance_to_EMA200_H1": regime["distance_to_ema200_h1"],
        "atr_m5_percentile": atr_m5_pct,
        "spread_bps": depth["spread_bps"],
        "depth_top5_qty": depth["depth_top5_qty"]
    }

    payload = {
        "symbol": symbol,
        "interval": DEFAULT_INTERVAL,
        "server_time": t_now,
        "candles": candles,
        "flow_last_candle": {
            "open_time": last_open, "close_time": last_close,
            "buy_volume": flow_last["buy_vol"], "sell_volume": flow_last["sell_vol"],
            "pct_aggressive_buy": flow_last["pct_agg_buy"], "delta": flow_last["delta"],
            "ofi_z_proxy": ofi_z_proxy, "coverage": coverage
        },
        "indicators": {"atr14_m5": atr_m5},
        "regime_h1": {
            "ema200": regime.get("ema200_h1", 0),
            "slope_dir": regime.get("ema200_h1_slope", 0),
            "atr14_h1": regime.get("atr14_h1", 0),
            "distance_to_ema200": regime.get("distance_to_ema200_h1", 0),
            "regime_above": regime.get("regime_above", False),
            "regime_below": regime.get("regime_below", False)
        },
        "depth_snapshot": depth,
        "guards": guards,
        "meta": {"data_version": "snap_1.1.4", "notes": "fix 418 + retry + headers limpos"}
    }

    next_close = last_close + INTERVAL_MS
    _snapshot_cache[ck] = {"expires": t_now + max(5000, next_close - t_now), "payload": payload}
    return jsonify(payload), 200


@app.route("/__health")
def health():
    return {"ok": True, "time": now_ms()}


if __name__ == "__main__":
    port = int(os.getenv("PORT", "10000"))
    app.run(host="0.0.0.0", port=port)
