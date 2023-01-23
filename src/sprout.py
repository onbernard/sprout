import inspect
import os
import uuid
from typing import Callable, Optional, Dict, ClassVar
from functools import wraps, cached_property
from concurrent.futures.process import ProcessPoolExecutor

import redis.asyncio as redis
from fastapi import FastAPI, BackgroundTasks
from pydantic import BaseModel
from aredis_om import JsonModel



class SproutRecord(JsonModel):
    class Config:
        frozen = True
    class Meta:
        global_key_prefix = "_sprout"
        model_key_prefix = "record"
        database: redis.Redis

    async def save(self, pipeline: Optional[redis.client.Pipeline] = None) -> "JsonModel":
        outp = await super().save(pipeline)
        await self.db().xadd(name=self._stream_key(), fields={"key": self.pk})
        return outp

    @classmethod
    def _stream_key(cls):
        return f"_sprout:stream:{cls.__name__}"

    @classmethod
    async def _stream(cls, count: int = 1, block: int=0, last_seen: Optional[str] = None):
        last_seen = last_seen if last_seen else "$"
        while True:
            items = await cls.Meta.database.xread(streams={cls._stream_key():last_seen}, count=count, block=block)
            if items:
                last_seen = items[0][1][-1][0]
                new_count = yield [await cls.get(msg_dict["key"]) for index, msg_dict in items[0][1]]
                count = new_count if new_count else count

    @classmethod
    def processor(cls, func: Callable):
        assert inspect.iscoroutinefunction(func)
        @wraps(func)
        def wrapper():
            return func(cls._stream())
        return wrapper
