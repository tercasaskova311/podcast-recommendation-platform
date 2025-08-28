# api.py
import os, asyncio, json
from typing import List, Set
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from pymongo import MongoClient
from bson.json_util import dumps

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB = os.getenv("MONGO_DB", "podcasts")
COL_RECS = os.getenv("MONGO_COLLECTION_FINAL_RECS", "final_recommendations")

app = FastAPI()
client = MongoClient(MONGO_URI)
db = client[MONGO_DB]

class Hub:
    def __init__(self):
        self.clients: Set[WebSocket] = set()
    async def connect(self, ws: WebSocket):
        await ws.accept()
        self.clients.add(ws)
    def disconnect(self, ws: WebSocket):
        self.clients.discard(ws)
    async def broadcast(self, msg: str):
        dead = []
        for ws in list(self.clients):
            try:
                await ws.send_text(msg)
            except WebSocketDisconnect:
                dead.append(ws)
        for d in dead:
            self.disconnect(d)

hub = Hub()

@app.websocket("/ws/recs")
async def ws_recs(ws: WebSocket):
    await hub.connect(ws)
    try:
        while True:
            await asyncio.sleep(60)  # keepalive; we only push server-side
    finally:
        hub.disconnect(ws)

async def watch_recs():
    with db[COL_RECS].watch(full_document="updateLookup") as stream:
        for change in stream:
            payload = json.dumps({
                "op": change["operationType"],
                "doc": change.get("fullDocument", {})
            }, default=str)
            await hub.broadcast(payload)

@app.on_event("startup")
async def startup():
    asyncio.create_task(watch_recs())

