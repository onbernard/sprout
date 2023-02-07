from typing import Callable, Optional, Any, Union, List, Type
from uuid import UUID, uuid4
import contextlib

import redis.asyncio as aioredis
from redis.asyncio.client import Pipeline

from pydantic import BaseModel

from .future import future, StreamItem
from .utils.stream import AsyncRedisStream, watch_streams


class Task:
    def __init__(self, db: aioredis.Redis, func: Callable, prefix: str) -> None:
        self.db = db
        self.func = func
        self.prefix = prefix
        self.futureW = future(db, func, prefix)
        self.pending_stream = AsyncRedisStream(
            db = self.db,
            model = StreamItem,
            key = f"{prefix}:pending"
        )
        self.inprogress_stream = AsyncRedisStream(
            db = self.db,
            model = StreamItem,
            key = f"{prefix}:inprogress"
        )
        self.failed_stream = AsyncRedisStream(
            db = self.db,
            model = StreamItem,
            key = f"{prefix}:failed"
        )
        self.completed_stream = AsyncRedisStream(
            db = self.db,
            model = StreamItem,
            key = f"{prefix}:completed"
        )

    async def __call__(self, *args, **kwargs):
        future = self.futureW(args, kwargs)
        if await future.setnx():
            await future.push(self.pending_stream)
        return await future.get()

    async def poke(self, *args, **kwargs):
        return await self.futureW(args, kwargs).get()

    async def consumer(self, consumername: Optional[str] = None):
        groupname = "_worker"
        consumername = consumername or f"_worker:{uuid4().hex}"
        await self.pending_stream.create_group(groupname)
        await self.pending_stream.create_consumer(groupname,consumername)
        async for event in self.pending_stream.readgroup(groupname, consumername):
            for idx, item in event:
                future = await self.futureW.from_stream(item)
                async with future.update(self.inprogress_stream) as model:
                    model.status = "inprogress"
                    print(f"{self.prefix}-{consumername} GOT {model.arguments}")
                try:
                    res = future()
                except Exception as exc:
                    print(f"{self.prefix}-{consumername} ENCOUNTERED EXCEPTION\n{exc}")
                    async with future.update(self.failed_stream) as model:
                        model.status = "failed"
                else:
                    print(f"{self.prefix}-{consumername} COMPLETED TASK")
                    async with future.update(self.completed_stream) as model:
                        model.status = "completed"
                        model.result = res
                await self.pending_stream.ack("_worker", idx)
                yield future

    async def run(self):
        async for _ in self.consumer():
            ...

    @property
    def streams(self) -> List[AsyncRedisStream]:
        return (self.pending_stream, self.inprogress_stream, self.completed_stream, self.failed_stream)

    async def watch(self):
        async for _ in watch_streams(*self.streams):
            yield _

    async def all(self):
        for idx, item in await self.pending_stream.range():
            yield (await self.futureW.from_stream(item)).future



class GeneratorTask(Task):
    async def consumer(self, consumername: Optional[str] = None):
        groupname = "_worker"
        consumername = consumername or f"_worker:{uuid4().hex}"
        await self.pending_stream.create_group(groupname)
        await self.pending_stream.create_consumer(groupname,consumername)
        async for event in self.pending_stream.readgroup(groupname, consumername):
            for idx, item in event:
                future = await self.futureW.from_stream(item)
                async with future.update(self.inprogress_stream) as model:
                    model.status = "inprogress"
                    print(f"{self.prefix}-{consumername} GOT {model.arguments}")
                try:
                    for res in future():
                        async with future.update(self.inprogress_stream) as model:
                            model.result = res
                except Exception as exc:
                    print(f"{self.prefix}-{consumername} ENCOUNTERED EXCEPTION\n{exc}")
                    async with future.update(self.failed_stream) as model:
                        model.status = "failed"
                else:
                    async with future.update(self.completed_stream) as model:
                        model.status = "completed"
                await self.pending_stream.ack("_worker", idx)
                yield future
