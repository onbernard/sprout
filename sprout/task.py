from typing import Callable, Optional, Any, Union, List, Type
from uuid import uuid4

import redis.asyncio as aioredis

from .future import StreamModel, Future, make_future
from .utils.stream import AsyncRedisStream, watch_streams


class Task:
    def __init__(self, db: aioredis.Redis, func: Callable, prefix: str) -> None:
        self.db = db
        self.func = func
        self.prefix = prefix
        self.future_T = make_future(db, func, prefix)
        self.pending_stream = AsyncRedisStream(
            db = self.db,
            model = StreamModel,
            key = f"{prefix}:pending"
        )
        self.inprogress_stream = AsyncRedisStream(
            db = self.db,
            model = StreamModel,
            key = f"{prefix}:inprogress"
        )
        self.failed_stream = AsyncRedisStream(
            db = self.db,
            model = StreamModel,
            key = f"{prefix}:failed"
        )
        self.completed_stream = AsyncRedisStream(
            db = self.db,
            model = StreamModel,
            key = f"{prefix}:completed"
        )

    async def __call__(self, *args, **kwargs) -> Future:
        future = self.future_T(args, kwargs)
        if await future.setnx():
            await future.push(self.pending_stream)
        return await future.pull()

    async def consumer(self, consumername: Optional[str] = None):
        groupname = "_worker"
        consumername = consumername or f"_worker:{uuid4().hex}"
        await self.pending_stream.create_group(groupname)
        await self.pending_stream.create_consumer(groupname,consumername)
        async for event in self.pending_stream.readgroup(groupname, consumername):
            for idx, item in event:
                future = await self.future_T.from_stream(item)
                print(f"{self.prefix}-{consumername} GOT TASK {future.arguments}")
                await future.publish_progress(None, self.inprogress_stream)
                try:
                    res = future()
                except Exception as exc:
                    print(f"{self.prefix}-{consumername} ENCOUNTERED EXCEPTION\n{exc}")
                    await future.publish_failure(self.failed_stream)
                else:
                    print(f"{self.prefix}-{consumername} COMPLETED TASK")
                    await future.publish_completion(res, self.completed_stream)
                await self.pending_stream.ack("_worker", idx)
                yield future

    async def run(self):
        async for turfu in self.consumer():
            ...

    @property
    def streams(self) -> List[AsyncRedisStream]:
        return [self.pending_stream, self.inprogress_stream, self.completed_stream, self.failed_stream]

    async def watch(self):
        async for stream, stream_model in watch_streams(*self.streams):
            yield self.future_T.from_stream(stream_model)

    async def all(self):
        for idx, item in await self.pending_stream.range():
            yield await self.future_T.from_stream(item)



class GeneratorTask(Task):
    async def consumer(self, consumername: Optional[str] = None):
        groupname = "_worker"
        consumername = consumername or f"_worker:{uuid4().hex}"
        await self.pending_stream.create_group(groupname)
        await self.pending_stream.create_consumer(groupname,consumername)
        async for event in self.pending_stream.readgroup(groupname, consumername):
            for idx, item in event:
                future = await self.future_T.from_stream(item)
                await future.publish_progress(self.inprogress_stream)
                print(f"{self.prefix}-{consumername} GOT TASK {future.arguments}")
                try:
                    for res in future():
                        await future.publish_progress(res, self.inprogress_stream)
                except Exception as exc:
                    print(f"{self.prefix}-{consumername} ENCOUNTERED EXCEPTION\n{exc}")
                    await future.publish_failure(self.failed_stream)
                else:
                    await future.publish_completion(self.completed_stream)
                await self.pending_stream.ack("_worker", idx)
                yield future
