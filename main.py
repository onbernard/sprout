import asyncio
import time
from uuid import UUID, uuid4
from concurrent.futures.process import ProcessPoolExecutor
from typing import Optional, Dict, List
from http import HTTPStatus

from fastapi import Body, FastAPI, Form, Request, WebSocket
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from sse_starlette.sse import EventSourceResponse

from pydantic import BaseModel


from test_multi import app, afunc, anotherfunc, fails
from sprout.future import FutureModel

app = FastAPI()


app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

html = """
<!DOCTYPE html>
<html>
    <head>
        <title>Chat</title>
    </head>
    <body>
        <h1>WebSocket Chat</h1>
        <form action="" onsubmit="sendMessage(event)">
            <input type="text" id="messageText" autocomplete="off"/>
            <button>Send</button>
        </form>
        <ul id='messages'>
        </ul>
        <script>
            var ws = new WebSocket("ws://localhost:8000/ws");
            ws.onmessage = function(event) {
                var messages = document.getElementById('messages')
                var message = document.createElement('li')
                var content = document.createTextNode(event.data)
                message.appendChild(content)
                messages.appendChild(message)
            };
            function sendMessage(event) {
                var input = document.getElementById("messageText")
                ws.send(input.value)
                input.value = ''
                event.preventDefault()
            }
        </script>
    </body>
</html>
"""


@app.get("/")
async def get():
    return HTMLResponse(html)


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    async for stream_key, item in afunc.monitor():
        await websocket.send_text(f"{stream_key} {item.json()}")
    # while True:
    #     data = await websocket.receive_text()
    #     await websocket.send_text(f"Message text was: {data}")


@app.get("/stream")
async def message_stream(request: Request):
    async def event_gen():
        yield 1
        async for stream_key, item in afunc.monitor():
            if await request.is_disconnected():
                break
            yield item
        
    return EventSourceResponse(event_gen())