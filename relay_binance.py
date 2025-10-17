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

    # fluxo na janela da última M5 com aggTrades (startTime/endTime)
    try:
        agg = fetch_agg_trades_window(symbol, last_open, last_close, per_req=AGG_LIMIT)
        buy_vol = sell_vol = 0.0
        coverage = "unknown"
        if agg:
            tmin = min(int(t.get("T", last_open)) for t in agg)
            tmax = max(int(t.get("T", last_open)) for t in agg)
            for t in agg:
                qty = float(t["q"])
                is_buyer_maker = bool(t.get("m", False))  # m=True => agressor foi vendedor
                if is_buyer_maker:
                    sell_vol += qty
                else:
                    buy_vol += qty
            # heurística de cobertura: vimos início e fim da janela?
            coverage = "full" if (tmin <= last_open + 30_000 and tmax >= last_close - 30_000) else "partial"
        else:
            buy_vol = sell_vol = 0.0
            coverage = "unknown"
    except Exception:
        buy_vol = sell_vol = 0.0
        coverage = "unknown"

    payload = {
        "symbol": symbol,
        "interval": DEFAULT_INTERVAL,
        "server_time": t_now,
        "candles": candles,  # exatamente n velas fechadas
        "flow_last_candle": {
            "open_time": last_open,
            "close_time": last_close,
            "buy_volume": buy_vol,
            "sell_volume": sell_vol,
            "coverage": coverage
        },
        "indicators": {
            "atr14_m5": atr14_from_candles(candles)
        },
        "meta": {
            "data_version": "snap_1.0"
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
