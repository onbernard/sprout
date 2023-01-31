from typing import Callable, Optional, Any
from uuid import UUID, uuid4
from functools import wraps

import redis.asyncio as aioredis
from pydantic import BaseModel, validator, ValidationError
from pydantic.decorator import ValidatedFunction

from sprout.future import Arguments, FutureModel, create_model_from_signature
from sprout.utils.stream import AsyncRedisStream


class StreamItem(BaseModel):
    key: str


class InvalidArguments(Exception):
    ...


class Task:
    def __init__(self, db: aioredis.Redis, func: Callable, name: str) -> None:
        self.db = db
        self.func = func
        self.name = name
        self.validated_func = ValidatedFunction(func, None)
        self.model = create_model_from_signature(func)
        self.pending_stream = AsyncRedisStream(
            db = self.db,
            model = StreamItem,
            suffix = f"{name}:pending"
        )
        self.failed_stream = AsyncRedisStream(
            db = self.db,
            model = StreamItem,
            suffix = f"{name}:failed"
        )
        self.completed_stream = AsyncRedisStream(
            db = self.db,
            model = StreamItem,
            suffix = f"{name}:completed"
        )

    def validate_arguments(self, args, kwargs):
        try:
            self.validated_func.init_model_instance(*args, **kwargs)
        except ValidationError as exc:
            raise InvalidArguments from exc

    async def __call__(self, *args, **kwargs) -> Any:
        self.validate_arguments(args, kwargs)
        # future = self.model(arguments=self.validated_func.init_model_instance(*args, **kwargs))
        arguments = Arguments(args=(), kwargs=self.validated_func.build_values(args, kwargs))
        future = FutureModel(arguments=arguments)
        if await self.db.setnx(future.key(self.name), future.json()):
            await self.pending_stream.push(StreamItem(key=future.key(self.name)))
        else:
            future = FutureModel.parse_raw(await self.db.get(future.key(self.name)))
            # future = FutureModel.parse_raw(await self.db.get(future.key(self.name)))
        return future

    async def consumer(self, consumername: Optional[str] = None):
        consumername = consumername or f"_consumer:{uuid4().hex}"
        groupname = "_worker"
        await self.pending_stream.create_group(groupname)
        await self.pending_stream.create_consumer(groupname,consumername)
        async for event in self.pending_stream.iter_group(groupname, consumername):
            for idx, item in event:
                # future = self.model.parse_raw(await self.db.get(item.key))
                future = FutureModel.parse_raw(await self.db.get(item.key))
                m = self.validated_func.init_model_instance(**future.arguments.kwargs)
                try:
                    res = self.validated_func.execute(m)
                except Exception as exc:
                    print(str(exc))
                    future.status = "failed"
                    await self.failed_stream.push(item)
                else:
                    future.status = "completed"
                    future.result = res
                    await self.completed_stream.push(item)
                await self.pending_stream.ack("_worker", idx)
                await self.db.set(future.key(self.name), future.json())
                yield future

    async def run(self):
        async for future in self.consumer():
            print(future)

    async def resolve(self, future: FutureModel):
        # return self.model.parse_raw(await self.db.get(future.key(self.name)))
        return FutureModel.parse_raw(await self.db.get(future.key(self.name)))

    async def all(self):
        for idx, item in await self.pending_stream.range():
            # yield self.model.parse_raw(await self.db.get(item.key))
            yield FutureModel.parse_raw(await self.db.get(item.key))

    async def completed(self):
        for idx, item in await self.completed_stream.range():
            # yield self.model.parse_raw(await self.db.get(item.key))
            yield FutureModel.parse_raw(await self.db.get(item.key))

    async def failed(self):
        for idx, item in await self.failed_stream.range():
            # yield self.model.parse_raw(await self.db.get(item.key))
            yield FutureModel.parse_raw(await self.db.get(item.key))