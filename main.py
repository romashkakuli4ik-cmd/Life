import asyncio
import json
import os
import time
from collections import deque
import aiohttp
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class ConnectionManager:
    def __init__(self):
        self.active_connections: list[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)

    def disconnect(self, websocket: WebSocket):
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)

    async def broadcast(self, message: dict):
        dead = []
        for connection in self.active_connections:
            try:
                await connection.send_json(message)
            except Exception:
                dead.append(connection)
        for d in dead:
            self.disconnect(d)

manager = ConnectionManager()

# ЕДИНЫЙ ДВИЖОК СКОРИНГА (0-100)
class LiveScorer:
    def __init__(self, baseline=350.0, cheap_limit=120.0, exp_limit=1800.0):
        self.window = deque(maxlen=35)
        self.baseline = baseline
        self.cheap_limit = cheap_limit
        self.exp_limit = exp_limit

    def add_drop(self, price: float):
        self.window.append({"price": price, "time": time.time()})

    def get_metrics(self):
        n = len(self.window)
        if n == 0:
            return 50.0, "WARM", "Штатный темп, стандартное распределение", 0, 0, 0

        prices = [d["price"] for d in self.window]
        cheap_count = sum(1 for p in prices if p <= self.cheap_limit)
        cheap_ratio = cheap_count / n
        upset_count = sum(1 for p in prices if p >= self.exp_limit)

        now = time.time()
        weighted_val = 0.0
        weight_sum = 0.0
        for d in self.window:
            age = max(1.0, now - d["time"])
            w = 1.0 / (age ** 0.5)
            rel = d["price"] / self.baseline
            weighted_val += min(rel, 8.0) * w
            weight_sum += w

        momentum = (weighted_val / weight_sum) if weight_sum > 0 else 1.0
        raw_score = 50.0 - (cheap_ratio - 0.5) * 40.0 + (upset_count / n) * 85.0 + (momentum - 1.0) * 20.0
        score = max(0.0, min(100.0, raw_score))

        if score < 28:
            lvl, desc = "COLD", "Преобладание дешёвых исходов, фон ниже нормы"
        elif score < 58:
            lvl, desc = "WARM", "Штатный темп, стандартное распределение"
        elif score < 82:
            lvl, desc = "HOT", "Повышенная плотность ценных дропов и апсетов"
        else:
            lvl, desc = "FIRE", "Аномальный кластер редких и дорогих исходов"

        dpm = n
        if n > 1:
            span = self.window[-1]["time"] - self.window[0]["time"]
            if span > 1.5:
                dpm = (n / span) * 60.0

        return round(score, 1), lvl, desc, round(dpm), round(cheap_ratio * 100), upset_count

scorer = LiveScorer()

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(websocket)

# ПАРСИНГ РЕАЛЬНОЙ ЛЕНТЫ ДРОПОВ ЧЕРЕЗ СЕТЕВУЮ СЕССИЮ
async def real_live_parser():
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
    }
    
    seen_ids = set()

    async with aiohttp.ClientSession(headers=headers) as session:
        while True:
            try:
                # Опрос открытого эндпоинта последней активности live-выигрышей
                url = "https://case-battle.vip/api/live-drops"
                
                async with session.get(url, timeout=5) as response:
                    if response.status == 200:
                        data = await response.json()
                        drops = data if isinstance(data, list) else data.get("drops", [])
                        
                        for item in reversed(drops):
                            item_id = item.get("id") or (item.get("user") + item.get("name"))
                            if item_id not in seen_ids:
                                seen_ids.add(item_id)
                                if len(seen_ids) > 200:
                                    seen_ids.pop()

                                user_name = item.get("user", "Игрок")
                                skin_name = item.get("name", "Скин")
                                skin_price = float(item.get("price", 100.0))

                                scorer.add_drop(skin_price)
                                score, lvl, desc, dpm, cheap_pct, upsets = scorer.get_metrics()

                                payload = {
                                    "drop": {"user": user_name, "item": skin_name, "price": skin_price},
                                    "score": score,
                                    "level": lvl,
                                    "desc": desc,
                                    "dpm": dpm,
                                    "cheap_pct": cheap_pct,
                                    "upsets": upsets,
                                }
                                await manager.broadcast(payload)
                    else:
                        # Если сервер под Cloudflare или задержка, опрашиваем через интервал
                        await asyncio.sleep(2)

            except Exception:
                await asyncio.sleep(2)

            await asyncio.sleep(1.2)

@app.on_event("startup")
async def startup_event():
    asyncio.create_task(real_live_parser())

if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run("main:app", host="0.0.0.0", port=port)
  
