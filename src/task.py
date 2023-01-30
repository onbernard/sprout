from typing import Callable, Optional, Any
from uuid import UUID, uuid4
from functools import wraps

import redis.asyncio as aioredis
from pydantic import BaseModel, validator, ValidationError
from pydantic.decorator import ValidatedFunction

from src.future import Arguments, FutureModel
from src.utils.stream import AsyncRedisStream


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
        arguments = Arguments(args=args, kwargs=kwargs)
        future = FutureModel(arguments=arguments)
        await self.db.set(future.key(self.name), future.json())
        return await self.pending_stream.push(StreamItem(key=future.key(self.name)))

    async def consumer(self, consumername: Optional[str] = None):
        consumername = consumername or f"_consumer:{uuid4().hex}"
        groupname = "_worker"
        await self.pending_stream.create_group(groupname)
        await self.pending_stream.create_consumer(groupname,consumername)
        async for event in self.pending_stream.iter_group(groupname, consumername):
            for idx, item in event:
                future = FutureModel.parse_raw(await self.db.get(item.key))
                try:
                    res = self.func(*future.arguments.args, **future.arguments.kwargs)
                except Exception as exc:
                    future.status = "failed"
                    await self.failed_stream.push(item)
                else:
                    future.status = "completed"
                    future.result = res
                    await self.completed_stream.push(item)
                await self.pending_stream.ack("_worker", idx)
                await self.db.set(future.key(self.name), future.json())
                yield future

    async def resolve(self, future: FutureModel):
        try:
            return await FutureModel.parse_raw(self.db.get(future.key(self.name)))
        except:
            return None