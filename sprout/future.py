from typing import Any, Literal, Callable, Optional, Type, Generic, TypeVar, AsyncGenerator, Sequence, Dict, AsyncContextManager, get_type_hints
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


class GenericFutureModel(GenericModel, Generic[_ArgsT, _ResT]):
    arguments: _ArgsT
    result: Optional[_ResT] = None
    status: Literal["pending", "inprogress", "completed", "failed"] = "pending"
    @property
    def key(self) -> str:
        raise NotImplementedError
    @property
    def kwargs(self) -> Dict:
        return dict(self.arguments)
    @classmethod
    def build_model(cls, args, kwargs) -> "GenericFutureModel":
        raise NotImplementedError


def mk_args_model(func: Callable) -> Type[BaseModel]:
    type_index = {}
    for name, param in inspect.signature(func).parameters.items():
        annotation = param.annotation if param.annotation != inspect._empty else Any
        default = param.default if param.default != inspect._empty else ...
        type_index[name] = (annotation, default)
    return create_model("Args", **type_index)


def mk_future_model(func: Callable, prefix: str) -> Type[GenericFutureModel]:
    args_model = mk_args_model(func)
    result_type = get_type_hints(func).get("return") or Any
    validated_function = ValidatedFunction(func, None)
    class FutureModel(GenericFutureModel[args_model, result_type], Generic[_ArgsT, _ResT]):
        @property
        def key(self) -> str:
            return f"{prefix}:{md5(self.arguments.json().encode()).hexdigest()}"
        @classmethod
        def build_model(cls, args, kwargs) -> "Future":
            return cls(arguments=args_model(**validated_function.build_values(args, kwargs)))
    return FutureModel


class StreamModel(BaseModel):
    key: str

_FutureModelT = TypeVar("_FutureModelT", bound=GenericFutureModel)

class Future(Generic[_FutureModelT]):
    def __init__(self, args: Sequence, kwargs: Dict, future_model: Optional[GenericFutureModel[_ArgsT, _ResT]] = None):
        self.future_model: GenericFutureModel[_ArgsT, _ResT]
        self.stream_model: StreamModel
        self.stream: AsyncRedisStream
    @classmethod
    async def from_stream(cls, stream_model: StreamModel) -> "Future[_ArgsT, _ResT]":
        raise NotImplementedError
    @classmethod
    async def from_key(cls, key: str) -> "Future[_ArgsT, _ResT]":
        raise NotImplementedError
    @property
    def arguments(self):
        return self.future_model.arguments
    @property
    def result(self):
        return self.future_model.result
    @property
    def status(self):
        return self.future_model.status
    @property
    def kwargs(self):
        return self.future_model.kwargs
    @property
    def key(self) -> str:
        return self.future_model.key
    async def get(self) -> Optional[_FutureModelT]:
        raise NotImplementedError
    async def set(self, pipe: Optional[Pipeline] = None):
        raise NotImplementedError
    async def setnx(self) -> bool:
        raise NotImplementedError
    async def pull(self) -> _FutureModelT:
        raise NotImplementedError
    async def push(self, *streams: AsyncRedisStream) -> None:
        raise NotImplementedError
    async def update(self, *streams: AsyncRedisStream) -> AsyncContextManager[_FutureModelT]:
        raise NotImplementedError
    async def publish_progress(self, result, *streams: AsyncRedisStream) -> None:
        raise NotImplementedError
    async def publish_failure(self, *streams: AsyncRedisStream) -> None:
        raise NotImplementedError
    async def publish_completion(self, result, *streams: AsyncRedisStream) -> None:
        raise NotImplementedError
    def __call__(self) -> _ResT:
        raise NotImplementedError

def make_future(db: aioredis.Redis, func: Callable, prefix: str) -> Type[Future]:
    future_model_T = mk_future_model(func, prefix)
    class FutureWrapper(Future):
        def __init__(self, args, kwargs, future_model: Optional[future_model_T] = None) -> None:
            nonlocal db, func, prefix, future_model_T
            self.future_model = future_model or future_model_T.build_model(args, kwargs)
            self.stream_model = StreamModel(key=self.future_model.key)
            self.stream = AsyncRedisStream(db, StreamModel, f"{self.future_model.key}:stream")

        @classmethod
        async def from_stream(cls, stream_model: StreamModel) -> "FutureWrapper[future_model_T]":
            nonlocal db
            return cls(None, None, future_model_T.parse_raw(await db.get(stream_model.key)))

        @classmethod
        async def from_key(cls, key: str) -> "FutureWrapper[future_model_T]":
            nonlocal db
            return cls(None, None, future_model_T.parse_raw(await db.get(key)))

        async def get(self) -> Optional[future_model_T]:
            nonlocal db
            cached = await db.get(self.key)
            return future_model_T.parse_raw(cached) if cached else None

        async def set(self, pipe: Optional[Pipeline] = None):
            nonlocal db
            if pipe:
                return pipe.set(self.key, self.future_model.json())
            return await db.set(self.key, self.future_model.json())

        async def setnx(self) -> bool:
            nonlocal db
            return await db.setnx(self.key, self.future_model.json())

        async def pull(self) -> future_model_T:
            nonlocal db
            cached = future_model_T.parse_raw(await db.get(self.key))
            self.future_model = cached
            return cached

        async def push(self, *streams: AsyncRedisStream) -> None:
            nonlocal db
            pipe = db.pipeline()
            await self.set(pipe)
            await self.stream.push(self.stream_model, pipe)
            for stream in streams:
                await stream.push(self.stream_model, pipe)
            await pipe.execute()

        @contextlib.asynccontextmanager
        async def update(self, *streams: AsyncRedisStream):
            nonlocal db
            try:
                yield self.future_model
            finally:
                await self.push(*streams)

        def __call__(self):
            nonlocal func
            return func(**self.kwargs)

        async def publish_progress(self, result, *streams: AsyncRedisStream) -> None:
            async with self.update(*streams) as model:
                model.status = "inprogress"
                if result:
                    model.result = result

        async def publish_failure(self, *streams: AsyncRedisStream) -> None:
            async with self.update(*streams) as model:
                model.status = "failed"

        async def publish_completion(self, result, *streams: AsyncRedisStream) -> None:
            async with self.update(*streams) as model:
                model.status = "completed"
                if result:
                    model.result = result
    return FutureWrapper
