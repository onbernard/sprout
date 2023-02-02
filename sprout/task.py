from typing import Callable, Optional, Any
from uuid import UUID, uuid4

import redis.asyncio as aioredis
from pydantic import BaseModel, validator, ValidationError
from pydantic.decorator import ValidatedFunction

from .future import Arguments, FutureModel, create_model_from_signature
from .utils.stream import AsyncRedisStream, watch_stream


class StreamItem(BaseModel):
    key: str


class InvalidArguments(Exception):
    ...


class Task:
    def __init__(self, db: aioredis.Redis, func: Callable, name: str, prefix: str) -> None:
        self.db = db
        self.func = func
        self.name = name
        self.prefix = prefix
        self.validated_func = ValidatedFunction(func, None)
        self.model = create_model_from_signature(func, prefix)
        self.pending_stream = AsyncRedisStream(
            db = self.db,
            model = StreamItem,
            prefix = prefix,
            suffix = f"{name}:pending"
        )
        self.inprogress_stream = AsyncRedisStream(
            db = self.db,
            model = StreamItem,
            prefix = prefix,
            suffix = f"{name}:inprogress"
        )
        self.failed_stream = AsyncRedisStream(
            db = self.db,
            model = StreamItem,
            prefix = prefix,
            suffix = f"{name}:failed"
        )
        self.completed_stream = AsyncRedisStream(
            db = self.db,
            model = StreamItem,
            prefix = prefix,
            suffix = f"{name}:completed"
        )

    def validate_arguments(self, args, kwargs):
        try:
            self.validated_func.init_model_instance(*args, **kwargs)
        except ValidationError as exc:
            raise InvalidArguments from exc

    async def __call__(self, *args, **kwargs) -> FutureModel:
        self.validate_arguments(args, kwargs)
        arguments = Arguments(args=(), kwargs=self.validated_func.build_values(args, kwargs))
        future = FutureModel(arguments=arguments)
        if await self.db.setnx(future.key(self.name), future.json()):
            await self.pending_stream.push(StreamItem(key=future.key(self.name)))
        else:
            future = FutureModel.parse_raw(await self.db.get(future.key(self.name)))
        return future

    async def poke(self, *args, **kwargs) -> Optional[FutureModel]:
        self.validate_arguments(args, kwargs)
        arguments = Arguments(args=(), kwargs=self.validated_func.build_values(args, kwargs))
        future = FutureModel(arguments=arguments)
        cached = await self.db.get(future.key(self.name))
        return FutureModel.parse_raw(cached) if cached else None

    async def consumer(self, consumername: Optional[str] = None):
        consumername = consumername or f"_consumer:{uuid4().hex}"
        groupname = "_worker"
        await self.pending_stream.create_group(groupname)
        await self.pending_stream.create_consumer(groupname,consumername)
        async for event in self.pending_stream.iter_group(groupname, consumername):
            for idx, item in event:
                future = FutureModel.parse_raw(await self.db.get(item.key))
                future.status = "inprogress"
                await self.db.set(future.key(self.name), future.json())
                await self.inprogress_stream.push(item)
                m = self.validated_func.init_model_instance(**future.arguments.kwargs)
                print(f"Consumer {self.name}-{consumername} got {future.arguments}")
                try:
                    res = self.validated_func.execute(m)
                except Exception as exc:
                    print(str(exc))
                    future.status = "failed"
                    await self.failed_stream.push(item)
                else:
                    print(f"Consumer {self.name}-{consumername} with {future.arguments} completed task")
                    future.status = "completed"
                    future.result = res
                    await self.completed_stream.push(item)
                await self.pending_stream.ack("_worker", idx)
                await self.db.set(future.key(self.name), future.json())
                yield future

    async def run(self):
        async for future in self.consumer():
            ...

    async def resolve(self, future: FutureModel) -> FutureModel:
        return FutureModel.parse_raw(await self.db.get(future.key(self.name)))

    async def unwrap(self, future: FutureModel):
        return await self.resolve(future).result

    async def all(self):
        for idx, item in await self.pending_stream.range():
            yield FutureModel.parse_raw(await self.db.get(item.key))

    async def completed(self):
        for idx, item in await self.completed_stream.range():
            yield FutureModel.parse_raw(await self.db.get(item.key))

    async def failed(self):
        for idx, item in await self.failed_stream.range():
            yield FutureModel.parse_raw(await self.db.get(item.key))

    async def monitor(self):
        async for stream, item in watch_stream(self.pending_stream, self.inprogress_stream, self.completed_stream, self.failed_stream):
            yield (stream.key, item)


class GeneratorTask(Task):
    async def consumer(self, consumername: Optional[str] = None):
        consumername = consumername or f"_consumer:{uuid4().hex}"
        groupname = "_worker"
        await self.pending_stream.create_group(groupname)
        await self.pending_stream.create_consumer(groupname,consumername)
        async for event in self.pending_stream.iter_group(groupname, consumername):
            for idx, item in event:
                future = FutureModel.parse_raw(await self.db.get(item.key))
                m = self.validated_func.init_model_instance(**future.arguments.kwargs)
                future.status = "inprogress"
                await self.db.set(future.key(self.name), future.json())
                await self.inprogress_stream.push(item)
                try:
                    for res in self.validated_func.execute(m):
                        future.result = res
                        await self.db.set(future.key(self.name), future.json())
                        await self.inprogress_stream.push(item)
                except Exception as exc:
                    print(str(exc.with_traceback()))
                    future.status = "failed"
                    await self.failed_stream.push(item)
                else:
                    future.status = "completed"
                    await self.completed_stream.push(item)
                await self.pending_stream.ack("_worker", idx)
                yield future
