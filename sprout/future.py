from typing import Any, Literal, Callable, Optional, Type, Generic, TypeVar, AsyncGenerator, Dict, get_type_hints
from hashlib import md5
import inspect
import contextlib

import redis.asyncio as aioredis
from redis.asyncio.client import Pipeline

from pydantic import BaseModel, create_model
from pydantic.generics import GenericModel
from pydantic.decorator import ValidatedFunction

from .utils.stream import AsyncRedisStream

# TODO : always check consistency between FutureT and inner class of mk_future_model
# same for Future and FutureWrapper

_ArgsT = TypeVar("_ArgsT", bound=BaseModel)
_ResT = TypeVar("_ResT")


class FutureModel(GenericModel, Generic[_ArgsT, _ResT]):
    arguments: _ArgsT
    result: Optional[_ResT] = None
    status: Literal["pending", "inprogress", "completed", "failed"] = "pending"

    @property
    def key(self) -> str:
        raise NotImplementedError

    @property
    def kwargs(self) -> Dict:
        raise NotImplementedError

    @classmethod
    def build_model(cls, args, kwargs) -> "FutureModel":
        raise NotImplementedError


def mk_args_model(func: Callable) -> Type[BaseModel]:
    type_index = {}
    for name, param in inspect.signature(func).parameters.items():
        annotation = param.annotation if param.annotation != inspect._empty else Any
        default = param.default if param.default != inspect._empty else ...
        type_index[name] = (annotation, default)
    return create_model("Args", **type_index)


def mk_future_model(func: Callable, prefix: str) -> FutureModel:
    args_model = mk_args_model(func)
    result_type = get_type_hints(func).get("return") or Any
    validated_function = ValidatedFunction(func, None)
    class Future(FutureModel[args_model, result_type], Generic[_ArgsT, _ResT]):
        @property
        def key(self) -> str:
            return f"{prefix}:{md5(self.arguments.json().encode()).hexdigest()}"
        @property
        def kwargs(self) -> Dict:
            return dict(self.arguments)
        @classmethod
        def build_model(cls, args, kwargs) -> "Future":
            return cls(arguments=args_model(**validated_function.build_values(args, kwargs)))
    return Future


class StreamItem(BaseModel):
    key: str

class Future:
    def __init__(self, args, kwargs, future: Optional[FutureModel] = None):
        self.future: FutureModel
        self.stream_item: StreamItem
        self.stream: AsyncRedisStream
    @classmethod
    async def from_stream(cls, item: StreamItem) -> "Future":
        raise NotImplementedError
    @classmethod
    async def from_key(cls, key: str) -> "Future":
        raise NotImplementedError
    @property
    def key(self) -> str:
        raise NotImplementedError
    async def get(self) -> Optional[FutureModel]:
        raise NotImplementedError
    async def set(self, pipe: Optional[Pipeline] = None):
        raise NotImplementedError
    async def setnx(self) -> bool:
        raise NotImplementedError
    async def pull(self) -> FutureModel:
        raise NotImplementedError
    async def push(self, *streams: AsyncRedisStream):
        raise NotImplementedError
    async def update(self, *streams: AsyncRedisStream):
        raise NotImplementedError
    def __call__(self):
        raise NotImplementedError

def future(db: aioredis.Redis, func: Callable, prefix: str) -> Type[Future]:
    future_model = mk_future_model(func, prefix)
    class FutureWrapper:
        def __init__(self, args, kwargs, future: Optional[future_model] = None) -> None:
            nonlocal db, future_model
            self.future = future or future_model.build_model(args, kwargs)
            self.stream_item = StreamItem(key=self.future.key)
            self.stream = AsyncRedisStream(db, StreamItem, f"{self.future.key}:stream")

        @classmethod
        async def from_stream(cls, item: StreamItem) -> "FutureWrapper":
            nonlocal db
            return cls(None, None, future_model.parse_raw(await db.get(item.key)))

        @classmethod
        async def from_key(cls, key: str) -> "FutureWrapper":
            nonlocal db
            return cls(None, None, future_model.parse_raw(await db.get(key)))

        @property
        def key(self) -> str:
            return self.future.key

        async def get(self) -> Optional[future_model]:
            nonlocal db
            cached = await db.get(self.key)
            return future_model.parse_raw(cached) if cached else None

        async def set(self, pipe: Optional[Pipeline] = None):
            nonlocal db
            if pipe:
                return pipe.set(self.key, self.future.json())
            return await db.set(self.key, self.future.json())

        async def setnx(self) -> bool:
            nonlocal db
            return await db.setnx(self.key, self.future.json())

        async def pull(self) -> future_model:
            nonlocal db
            cached = future_model.parse_raw(await db.get(self.key))
            self.future = cached
            return cached

        async def push(self, *streams: AsyncRedisStream):
            nonlocal db
            pipe = db.pipeline()
            await self.set(pipe)
            await self.stream.push(self.stream_item, pipe)
            for stream in streams:
                await stream.push(self.stream_item, pipe)
            await pipe.execute()

        @contextlib.asynccontextmanager
        async def update(self, *streams: AsyncRedisStream) -> AsyncGenerator[future_model, None]:
            nonlocal db
            try:
                yield self.future
            finally:
                await self.push(*streams)

        def __call__(self):
            nonlocal func
            return func(**self.future.kwargs)
    return FutureWrapper