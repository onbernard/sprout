import asyncio
import time
from uuid import UUID, uuid4
from concurrent.futures.process import ProcessPoolExecutor
from typing import Optional, Dict, List
from http import HTTPStatus

from fastapi import Body, FastAPI, Form, Request, BackgroundTasks
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from pydantic import BaseModel

from src.sprout import SproutRecord

class StreamA(SproutRecord):
    a: str = "uwu"
    b: str = "lol"


class StreamB(SproutRecord):
    c: str


@StreamA.processor
async def printy_boi(stream):
    async for stuff in stream:
        print(stuff)

# class Job(BaseModel):
#     uuid: UUID = uuid4()
#     status: str = "in_progress"
#     result: Optional[int] = None

# jobs: Dict[UUID, Job] = {}

# async def run_in_process(fn, *args):
#     loop = asyncio.get_event_loop()
#     return await loop.run_in_executor(app.state.executor, fn, *args)

# async def start_cpu_bound_task(uuid: UUID, param: int) -> None:
#     jobs[uuid].result = await run_in_process(lambda t: time.sleep(t), param)
#     jobs[uuid].status = "complete"

# app = FastAPI()

# @app.on_event("startup")
# async def startup():
#     app.state.executor = ProcessPoolExecutor()

# @app.on_event("shutdown")
# async def shutdown():
#     app.state.executor.shutdown()

# @app.post("/new_task/{param}", status_code=HTTPStatus.ACCEPTED)
# async def task_handler(param: int, background_task: BackgroundTasks):
#     new_task = Job()
#     jobs[new_task.uuid] = new_task
#     background_task.add_task(start_cpu_bound_task, new_task.uuid, param)
#     return new_task

app = FastAPI()

@app.on_event("startup")
async def startup():
    app.state.executor = ProcessPoolExecutor()

@app.on_event("shutdown")
async def shutdown():
    app.state.executor.shutdown()

@app.get("/")
async def root():
    return "hello"


@app.get("/a")
async def get_a() -> List[StreamA]:
    return [ await StreamA.get(pk) async for pk in await StreamA.all_pks() ]


@app.get("/b")
async def get_b() -> List[StreamB]:
    return [ await StreamB.get(pk) async for pk in await StreamB.all_pks() ]

@app.post("/add_a")
async def add_a(a: StreamA) -> StreamA:
    return await a.save()