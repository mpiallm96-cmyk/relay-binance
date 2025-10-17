from flask import Flask, request, jsonify
import os, time
import requests

app = Flask(__name__)

# ------------ Config ------------
BINANCE_BASE = os.getenv("BINANCE_BASE_URL", "https://fapi.binance.com/fapi/v1")
DEFAULT_INTERVAL = "5m"
INTERVAL_MS = 5 * 60 * 1000  # 5 minutos
DEFAULT_N = int(os.getenv("SNAPSHOT_N", "50"))  # número padrão de velas fechadas
TRADES_LIMIT = int(os.getenv("TRADES_LIMIT", "600"))  # trades para cobrir a janela
HTTP_TIMEOUT = float(os.getenv("HTTP_TIMEOUT", "8.0"))

# Sessão HTTP reusável
session = requests.Session()

def now_ms() -> int:
    return int(time.time() * 1000)

# Cache simples até o próximo close_time
_snapshot_cache = {}  # key: (symbol, n) -> { "expires": ms, "payload": dict }

# ------------ Rotas originais (proxy) ------------
@app.route("/")
def home():
    return {"status": "ok", "message": "Binance relay ativo no Render"}

@app.route("/depth")
def depth():
    symbol = request.args.get("symbol", "BTCUSDT")
    limit = int(request.args.get("limit", 5))
    r = session.get(f"{BINANCE_BASE}/depth", params={"symbol": symbol, "limit": limit}, timeout=HTTP_TIMEOUT)
    return jsonify(r.json()), r.status_code

@app.route("/klines")
def klines():
    symbol = request.args.get("symbol", "BTCUSDT")
    interval = request.args.get("interval", "1m")
    limit = int(request.args.get("limit", 100))
    r = session.get(
        f"{BINANCE_BASE}/klines",
        params={"symbol": symbol, "interval": interval, "limit": limit},
        timeout=HTTP_TIMEOUT,
    )
    return jsonify(r.json()), r.status_code

@app.route("/trades")
def trades():
    symbol = request.args.get("symbol", "BTCUSDT")
    limit = int(request.args.get("limit", 10))
    r = session.get(f"{BINANCE_BASE}/trades", params={"symbol": symbol, "limit": limit}, timeout=HTTP_TIMEOUT)
    return jsonify(r.json()), r.status_code

# ------------ Helper funcs ------------
def fetch_klines_closed(symbol: str, n: int) -> list:
    """
    Busca n velas M5 **fechadas**.
    Estratégia: pede n+2, calcula close_time e descarta qualquer vela com close_time > agora.
    """
    # pedimos um pouco a mais para garantir descarte da aberta
    limit = n + 2
    resp = session.get(
        f"{BINANCE_BASE}/klines",
        params={"symbol": symbol, "interval": DEFAULT_INTERVAL, "limit": limit},
        timeout=HTTP_TIMEOUT,
    )
    if resp.status_code != 200:
        raise RuntimeError(f"klines upstream {resp.status_code}")
    raw = resp.json()  # lista de arrays: [openTime, open, high, low, close, volume, ...]
    t_now = now_ms()
    closed = []
    for row in raw:
        open_time = int(row[0])
        close_time = open_time + INTERVAL_MS
        if close_time <= t_now:
            # guardamos como dict já no formato do snapshot
            closed.append({
                "open_time": open_time,
                "close_time": close_time,
                "open": str(row[1]),
                "high": str(row[2]),
                "low":  str(row[3]),
                "close":str(row[4]),
                "volume": str(row[5]),
            })
    if not closed:
        raise RuntimeError("no closed candles yet")
    return closed[-n:]  # últimas n fechadas

def fetch_trades(symbol: str, limit: int) -> list:
    resp = session.get(
        f"{BINANCE_BASE}/trades",
        params={"symbol": symbol, "limit": limit},
        timeout=HTTP_TIMEOUT,
    )
    if resp.status_code != 200:
        raise RuntimeError(f"trades upstream {resp.status_code}")
    return resp.json()  # [{id, price, qty, isBuyerMaker, time}, ...]

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

# ------------ Novo endpoint: /m5-snapshot ------------
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

    try:
        candles = fetch_klines_closed(symbol, n)
    except Exception as e:
        return jsonify({"code": "SNAPSHOT_NOT_READY", "message": str(e)}), 503

    last = candles[-1]
    last_open = int(last["open_time"])
    last_close = int(last["close_time"])

    # pega trades e filtra pela janela da última M5 fechada
    try:
        trades = fetch_trades(symbol, TRADES_LIMIT)
    except Exception as e:
        # devolve snapshot mesmo assim, mas com coverage unknown
        trades = []
        coverage = "unknown"
        buy_vol = 0.0
        sell_vol = 0.0
    else:
        in_window = [t for t in trades if last_open <= int(t.get("time", 0)) < last_close]
        buy_vol = sum(float(t["qty"]) for t in in_window if not t.get("isBuyerMaker", True))
        sell_vol = sum(float(t["qty"]) for t in in_window if t.get("isBuyerMaker", False))
        if in_window:
            tmin = min(int(t["time"]) for t in in_window)
            tmax = max(int(t["time"]) for t in in_window)
            # heurística simples de cobertura: vimos início e fim da janela?
            coverage = "full" if (tmin <= last_open + 30_000 and tmax >= last_close - 30_000) else "partial"
        else:
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
            # futuros: "rv_1m", "vwap" se você quiser adicionar
        },
        "meta": {
            "data_version": "snap_1.0"
        }
    }

    # define validade do cache: até a próxima vela fechar
    next_close = last_close + INTERVAL_MS
    ttl_ms = max(5_000, next_close - t_now)  # no mínimo 5s
    _snapshot_cache[ck] = {"expires": t_now + ttl_ms, "payload": payload}

    return jsonify(payload)

# ------------ Main ------------
if __name__ == "__main__":
    # Render/Heroku costumam setar PORT; local roda 10000
    port = int(os.getenv("PORT", "10000"))
    app.run(host="0.0.0.0", port=port)

