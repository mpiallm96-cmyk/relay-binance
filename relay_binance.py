from flask import Flask, request, jsonify
import os, time
import requests

app = Flask(__name__)

# ============== Config ==============
BINANCE_BASE = os.getenv("BINANCE_BASE_URL", "https://fapi.binance.com/fapi/v1")
DEFAULT_INTERVAL = "5m"
INTERVAL_MS = 5 * 60 * 1000  # 5 minutos
DEFAULT_N = int(os.getenv("SNAPSHOT_N", "50"))        # velas fechadas no snapshot
HTTP_TIMEOUT = float(os.getenv("HTTP_TIMEOUT", "8.0"))
AGG_LIMIT = int(os.getenv("AGG_LIMIT", "1000"))       # máx por chamada do /aggTrades

# Thresholds configuráveis (guard-rails)
DIST_MIN = float(os.getenv("DIST_MIN", "0.25"))           # distância mínima normalizada à EMA200(H1)
SPREAD_MAX_BPS = float(os.getenv("SPREAD_MAX_BPS", "3.0"))
MIN_DEPTH_QTY = float(os.getenv("MIN_DEPTH_QTY", "50"))   # heurístico - ajuste por par
ATR_M5_PCTL_MIN = float(os.getenv("ATR_M5_PCTL_MIN", "0.20"))

# Sessão HTTP reusável
session = requests.Session()

def now_ms() -> int:
    return int(time.time() * 1000)

# Cache simples até o próximo close_time
_snapshot_cache = {}  # key: (symbol, n) -> { "expires": ms, "payload": dict }

# ============== Helpers seguros (evita 502) ==============
def _binance_get(path: str, params: dict):
    """Wrapper com timeout/erros tratados (retorna (json, status_code))."""
    try:
        r = session.get(f"{BINANCE_BASE}{path}", params=params, timeout=HTTP_TIMEOUT)
        r.raise_for_status()
        return r.json(), r.status_code
    except requests.exceptions.Timeout:
        return {"code": "UPSTREAM_TIMEOUT", "message": "Binance timeout"}, 503
    except requests.exceptions.RequestException as e:
        return {"code": "UPSTREAM_ERROR", "message": str(e)}, 503

def fetch_klines_closed(symbol: str, n: int) -> list:
    """
    Busca n velas M5 **fechadas**.
    Estratégia: pede n+2, calcula close_time e descarta qualquer vela com close_time > agora.
    """
    body, status = _binance_get("/klines", {"symbol": symbol, "interval": DEFAULT_INTERVAL, "limit": n + 2})
    if status != 200:
        raise RuntimeError(f"klines upstream {status}")
    raw = body  # [[openTime, open, high, low, close, volume, ...], ...]
    t_now = now_ms()
    closed = []
    for row in raw:
        open_time = int(row[0])
        close_time = open_time + INTERVAL_MS
        if close_time <= t_now:
            closed.append({
                "open_time": open_time,
                "close_time": close_time,
                "open":  str(row[1]),
                "high":  str(row[2]),
                "low":   str(row[3]),
                "close": str(row[4]),
                "volume": str(row[5]),
            })
    if not closed:
        raise RuntimeError("no closed candles yet")
    return closed[-n:]  # últimas n fechadas

# ======== Novos Helpers de série / indicadores ========
def ema(values: list, period: int) -> float:
    """EMA simples: retorna o último valor da EMA(period) de uma lista de closes (floats)."""
    if not values or period <= 1 or len(values) < period:
        return 0.0
    k = 2.0 / (period + 1)
    e = values[0]
    for v in values[1:]:
        e = v * k + e * (1 - k)
    return e

def fetch_klines_closed_interval(symbol: str, interval: str, n: int) -> list:
    """Versão com intervalo parametrizável (ex.: '1h' para H1)."""
    body, status = _binance_get("/klines", {"symbol": symbol, "interval": interval, "limit": n + 2})
    if status != 200:
        raise RuntimeError(f"klines upstream {status}")
    raw = body
    t_now = now_ms()
    closed = []
    for i, row in enumerate(raw):
        open_time = int(row[0])
        # close_time = próximo open_time quando disponível; senão, estima pelo delta das últimas
        if i + 1 < len(raw):
            close_time = int(raw[i + 1][0])
        else:
            if i > 0:
                dt = int(row[0]) - int(raw[i - 1][0])
            else:
                dt = INTERVAL_MS
            close_time = open_time + dt
        if close_time <= t_now:
            closed.append({
                "open_time": open_time,
                "close_time": close_time,
                "open":  str(row[1]),
                "high":  str(row[2]),
                "low":   str(row[3]),
                "close": str(row[4]),
                "volume": str(row[5]),
            })
    if not closed:
        raise RuntimeError("no closed candles yet")
    return closed[-n:]

def slope_dir(series: list, lookback: int = 5) -> int:
    """+1 se último > valor de lookback atrás, -1 se menor, 0 se ~igual (tolerância pequena)."""
    if len(series) <= lookback:
        return 0
    a = series[-1]
    b = series[-1 - lookback]
    tol = max(1e-9, abs(a) * 1e-4)
    if a > b + tol: return +1
    if a < b - tol: return -1
    return 0

def depth_snapshot(symbol: str, limit: int = 5):
    """Pega /depth e calcula IB e spread bps."""
    body, status = _binance_get("/depth", {"symbol": symbol, "limit": limit})
    if status != 200:
        return None
    bids = body.get("bids", [])
    asks = body.get("asks", [])
    def _sum(levels):
        s = 0.0
        for p, q in levels:
            s += float(q)
        return s
    bid_sum = _sum(bids)
    ask_sum = _sum(asks)
    ib = 0.0
    denom = bid_sum + ask_sum
    if denom > 0:
        ib = (bid_sum - ask_sum) / denom
    # spread bps:
    if bids and asks:
        best_bid = float(bids[0][0])
        best_ask = float(asks[0][0])
        mid = (best_bid + best_ask) / 2.0
        spread_bps = 0.0 if mid == 0 else (best_ask - best_bid) / mid * 1e4
    else:
        spread_bps = 1e9
    # profundidade top-5 em "quantidade" (soma de qty; serve como heurística)
    depth_top5_qty = bid_sum + ask_sum
    return {"IB": ib, "spread_bps": spread_bps, "depth_top5_qty": depth_top5_qty}

def atr14_from_candles(candles: list) -> float:
    """ATR(14) simples usando TR padrão sobre os candles fornecidos (list de dict)."""
    if len(candles) < 15:
        return 0.0
    trs = []
    prev_close = None
    for c in candles:
        h = float(c["high"])
        l = float(c["low"])
        cl = float(c["close"])
        if prev_close is None:
            tr = h - l
        else:
            tr = max(h - l, abs(h - prev_close), abs(l - prev_close))
        trs.append(tr)
        prev_close = cl
    return sum(trs[-14:]) / 14.0

def agg_stats_for_window(symbol: str, start_ms: int, end_ms: int):
    """Computa buy_vol, sell_vol, %Agg e delta na janela [start, end)."""
    agg = fetch_agg_trades_window(symbol, start_ms, end_ms, per_req=AGG_LIMIT)
    buy_vol = sell_vol = 0.0
    if agg:
        for t in agg:
            qty = float(t["q"])
            is_buyer_maker = bool(t.get("m", False))  # m=True => agressor foi vendedor
            if is_buyer_maker:
                sell_vol += qty
            else:
                buy_vol += qty
    total = buy_vol + sell_vol
    pct_aggressive_buy = (buy_vol / total * 100.0) if total > 0 else 0.0
    return {
        "buy_vol": buy_vol,
        "sell_vol": sell_vol,
        "pct_agg_buy": pct_aggressive_buy,
        "delta": buy_vol - sell_vol
    }

def zscore(value: float, series: list) -> float:
    """z-score simples do value contra series (média e desvio da série)."""
    import math
    if not series or len(series) < 2:
        return 0.0
    m = sum(series) / len(series)
    var = sum((x - m) ** 2 for x in series) / (len(series) - 1)
    sd = math.sqrt(max(var, 1e-12))
    return (value - m) / sd

def deltas_last_k_m5(symbol: str, candles_m5: list, k: int = 12) -> list:
    """
    Retorna lista de deltas das últimas k velas M5 (anteriores à última).
    Usa aggTrades por vela fechada.
    """
    out = []
    take = min(k, max(0, len(candles_m5) - 1))
    if take == 0:
        return out
    window = candles_m5[-(take+1):-1]
    for c in window:
        stats = agg_stats_for_window(symbol, c["open_time"], c["close_time"])
        out.append(stats["delta"])
    return out

def fetch_agg_trades_window(symbol: str, start_ms: int, end_ms: int, per_req: int = AGG_LIMIT) -> list:
    """
    Busca aggTrades cobrindo a janela [start_ms, end_ms).
    Paginado por startTime (e timestamp do último item) até cobrir a janela.
    """
    out = []
    params = {"symbol": symbol, "startTime": start_ms, "endTime": end_ms, "limit": per_req}
    safety = 0
    while True:
        safety += 1
        if safety > 50:  # proteção contra loop infinito
            break
        r = session.get(f"{BINANCE_BASE}/aggTrades", params=params, timeout=HTTP_TIMEOUT)
        if r.status_code != 200:
            # Se der erro upstream, retorna o que já temos (caller decide coverage)
            break
        batch = r.json()
        if not batch:
            break
        out.extend(batch)
        # Move início para após o último timestamp/aggId retornado
        last = batch[-1]
        last_T = int(last.get("T", params["startTime"]))
        next_start = last_T + 1
        if next_start >= end_ms:
            break
        params["startTime"] = next_start
    return out

def compute_regime_h1(symbol: str):
    """
    Calcula EMA200(H1), slope (±1/0), ATR14(H1), distância normalizada e flags de regime.
    """
    candles_h1 = fetch_klines_closed_interval(symbol, "1h", 250)
    closes = [float(c["close"]) for c in candles_h1]
    ema200 = ema(closes, 200)
    # série de EMA200 ao longo do tempo para medir direção (com lookback de 5 candles H1)
    ema200_series = []
    e = None
    k = 2.0 / (200 + 1)
    for i, v in enumerate(closes):
        if i == 0:
            e = v
        else:
            e = v * k + e * (1 - k)
        ema200_series.append(e if i + 1 >= 1 else 0.0)
    slope = slope_dir(ema200_series, lookback=5)
    atr_h1 = atr14_from_candles(candles_h1)
    last_close = closes[-1]
    dist_norm = 0.0 if atr_h1 <= 0 else abs(last_close - ema200) / atr_h1
    regime_above = last_close > ema200
    regime_below = last_close < ema200
    return {
        "ema200_h1": ema200,
        "ema200_h1_slope": slope,   # +1 / 0 / -1
        "close_h1": last_close,
        "atr14_h1": atr_h1,
        "distance_to_ema200_h1": dist_norm,
        "regime_above": regime_above,
        "regime_below": regime_below
    }

# ============== Rotas originais (proxy) ==============
@app.route("/")
def home():
    return {"status": "ok", "message": "Binance relay ativo no Render"}

@app.route("/depth")
def depth():
    symbol = request.args.get("symbol", "BTCUSDT")
    limit = int(request.args.get("limit", 5))
    body, status = _binance_get("/depth", {"symbol": symbol, "limit": limit})
    return jsonify(body), status

@app.route("/klines")
def klines():
    symbol = request.args.get("symbol", "BTCUSDT")
    interval = request.args.get("interval", "1m")
    limit = int(request.args.get("limit", 100))
    body, status = _binance_get("/klines", {"symbol": symbol, "interval": interval, "limit": limit})
    return jsonify(body), status

@app.route("/trades")
def trades():
    symbol = request.args.get("symbol", "BTCUSDT")
    limit = int(request.args.get("limit", 10))
    body, status = _binance_get("/trades", {"symbol": symbol, "limit": limit})
    return jsonify(body), status

# ============== Nova rota: SNAPSHOT M5 ==============
@app.route("/m5-snapshot")
def m5_snapshot():
    symbol = request.args.get("symbol", "BTCUSDT").upper()
    try:
        n = int(request.args.get("n", DEFAULT_N))
    except ValueError:
        n = DEFAULT_N
    n = max(20, min(100, n))  # bound 20..100

    # cache: até o próximo close_time
    ck = (symbol, n)
    t_now = now_ms()
    cached = _snapshot_cache.get(ck)
    if cached and t_now < cached.get("expires", 0):
        return jsonify(cached["payload"])

    # candles M5 fechadas
    try:
        candles = fetch_klines_closed(symbol, n)
    except Exception as e:
        return jsonify({"code": "SNAPSHOT_NOT_READY", "message": str(e)}), 503

    last = candles[-1]
    last_open = int(last["open_time"])
    last_close = int(last["close_time"])

    # ===== 4.1 Regime H1 (EMA200, slope, distância) =====
    try:
        regime = compute_regime_h1(symbol)
    except Exception as e:
        regime = {
            "ema200_h1": 0.0, "ema200_h1_slope": 0, "close_h1": 0.0,
            "atr14_h1": 0.0, "distance_to_ema200_h1": 0.0,
            "regime_above": False, "regime_below": False, "error": str(e)
        }

    # ===== 4.2 Fluxo da última M5 (aggTrades) =====
    try:
        flow_last = agg_stats_for_window(symbol, last_open, last_close)  # buy/sell/pctAgg/delta
        # série de deltas das últimas k velas para z-score (proxy de OFI_z)
        deltas_hist = deltas_last_k_m5(symbol, candles, k=12)
        ofi_z_proxy = zscore(flow_last["delta"], deltas_hist) if deltas_hist else 0.0
        coverage = "full"
    except Exception:
        flow_last = {"buy_vol": 0.0, "sell_vol": 0.0, "pct_agg_buy": 0.0, "delta": 0.0}
        ofi_z_proxy = 0.0
        coverage = "unknown"

    # ===== 4.3 ATRs e "mercado morto" =====
    atr_m5 = atr14_from_candles(candles)
    # série de ATRs progressivos para rank (proxy de percentil)
    atr_series = []
    for i in range(1, len(candles)):
        atr_series.append(atr14_from_candles(candles[:i+1]))
    def _percentile_rank(series, value):
        if not series:
            return 1.0
        below = sum(1 for x in series if x <= value)
        return below / len(series)
    atr_m5_pct = _percentile_rank(atr_series[-50:], atr_m5)  # usa ~50 últimas leituras

    # ===== 4.4 Depth snapshot -> IB e spread =====
    depth = depth_snapshot(symbol, limit=5) or {"IB": 0.0, "spread_bps": 1e9, "depth_top5_qty": 0.0}

    # ===== 4.5 Guards (regras duras) =====
    distance_ok = (regime.get("atr14_h1", 0) > 0) and (regime["distance_to_ema200_h1"] >= DIST_MIN)
    spread_ok = (depth["spread_bps"] <= SPREAD_MAX_BPS)
    depth_ok = (depth["depth_top5_qty"] >= MIN_DEPTH_QTY)
    market_ok = (atr_m5_pct >= ATR_M5_PCTL_MIN)

    guards = {
        "distance_ok": distance_ok,
        "spread_ok": spread_ok,
        "depth_ok": depth_ok,
        "market_ok": market_ok,
        "distance_to_EMA200_H1": regime["distance_to_ema200_h1"],
        "atr_m5_percentile": atr_m5_pct,
        "spread_bps": depth["spread_bps"],
        "depth_top5_qty": depth["depth_top5_qty"]
    }

    # ===== 4.6 Montagem do payload =====
    payload = {
        "symbol": symbol,
        "interval": DEFAULT_INTERVAL,
        "server_time": t_now,
        "candles": candles,  # exatamente n velas fechadas (M5)
        "flow_last_candle": {
            "open_time": last_open,
            "close_time": last_close,
            "buy_volume": flow_last["buy_vol"],
            "sell_volume": flow_last["sell_vol"],
            "pct_aggressive_buy": flow_last["pct_agg_buy"],
            "delta": flow_last["delta"],
            "ofi_z_proxy": ofi_z_proxy,
            "coverage": coverage
        },
        "indicators": {
            "atr14_m5": atr_m5
        },
        "regime_h1": {
            "ema200": regime["ema200_h1"],
            "slope_dir": regime["ema200_h1_slope"],   # +1/0/-1
            "close": regime["close_h1"],
            "atr14_h1": regime["atr14_h1"],
            "distance_to_ema200": regime["distance_to_ema200_h1"],
            "regime_above": regime["regime_above"],
            "regime_below": regime["regime_below"]
        },
        "depth_snapshot": {
            "IB": depth["IB"],
            "spread_bps": depth["spread_bps"],
            "depth_top5_qty": depth["depth_top5_qty"]
        },
        "guards": guards,
        "meta": {
            "data_version": "snap_1.1",
            "notes": "inclui regime H1, proxies de fluxo e guard-rails"
        }
    }

    # define validade do cache: até a próxima vela fechar
    next_close = last_close + INTERVAL_MS
    ttl_ms = max(5_000, next_close - t_now)  # no mínimo 5s
    _snapshot_cache[ck] = {"expires": t_now + ttl_ms, "payload": payload}

    return jsonify(payload), 200

# ============== Diagnóstico / Health ==============
@app.route("/__routes")
def __routes():
    return jsonify(sorted([str(r) for r in app.url_map.iter_rules()]))

@app.route("/__health")
def __health():
    return {"ok": True, "time": now_ms()}

# ============== Main ==============
if __name__ == "__main__":
    port = int(os.getenv("PORT", "10000"))
    app.run(host="0.0.0.0", port=port)
