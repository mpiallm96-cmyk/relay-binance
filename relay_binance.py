from flask import Flask, request, jsonify
import requests

app = Flask(__name__)
BASE_URL = "https://fapi.binance.com/fapi/v1"

@app.route("/")
def home():
    return {"status": "ok", "message": "Binance relay ativo no Render"}

@app.route("/depth")
def depth():
    symbol = request.args.get("symbol", "BTCUSDT")
    limit = request.args.get("limit", 5)
    r = requests.get(f"{BASE_URL}/depth", params={"symbol": symbol, "limit": limit})
    return jsonify(r.json())

@app.route("/klines")
def klines():
    symbol = request.args.get("symbol", "BTCUSDT")
    interval = request.args.get("interval", "1m")
    limit = request.args.get("limit", 100)
    r = requests.get(f"{BASE_URL}/klines", params={"symbol": symbol, "interval": interval, "limit": limit})
    return jsonify(r.json())

@app.route("/trades")
def trades():
    symbol = request.args.get("symbol", "BTCUSDT")
    limit = request.args.get("limit", 10)
    r = requests.get(f"{BASE_URL}/trades", params={"symbol": symbol, "limit": limit})
    return jsonify(r.json())

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10000)
