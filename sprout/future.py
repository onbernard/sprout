from typing import (
    Callable,
    Optional,
    Type,
    Generic,
    TypeVar,
    Sequence,
    AsyncGenerator,
    Any,
    Dict,
    AsyncContextManager,
    get_type_hints,
)
from abc import ABC, abstractmethod, abstractclassmethod
import contextlib

from redis.asyncio.client import Pipeline
import redis.asyncio as aioredis

from .utils.stream import AsyncRedisStream
from .models import GenericFutureModel, StreamModel, make_future_model, make_args_model, _ArgsT, _ResT


# TODO : always check consistency between Future and inner class of mk_future


_FutureModelT = TypeVar("_FutureModelT", bound=GenericFutureModel)


class Future(ABC, Generic[_FutureModelT,_ArgsT,_ResT]):
    """Represent a task call"""
    def __init__(self, args: Optional[Sequence], kwargs: Optional[Dict], future_model: Optional[GenericFutureModel] = None):
        self.future_model: _FutureModelT
        self.stream_model: StreamModel
        self.stream: AsyncRedisStream

    @property
    def arguments(self) -> _ArgsT:
        """Pydantic model containing the arguments of the function call"""
        return self.future_model.arguments

    @property
    def result(self) -> Optional[_ResT]:
        """Result of the function call"""
        return self.future_model.result

    @result.setter # type: ignore
    def result(self, result):
        self.future_model.result = result

    @property
    def status(self):
        """Status of the function call"""
        return self.future_model.status

    @status.setter # type: ignore
    def status(self, status):
        self.future_model.status = status

    def kwargs(self) -> Dict:
        """Return dictionary with the function arguments

        Returns:
            Dict: Dictionary with the function arguments
        """
        return self.future_model.kwargs()

    @property
    def key(self) -> str:
        """Value used as the redis key"""
        return self.future_model.key

    async def watch(self) -> AsyncGenerator["Future", None]:
        """Yield and update self every time an event is publised on the future stream

        Yields:
            Future: Self
        """
        async for _ in self.stream.read():
            await self.pull()
            yield self

    @abstractclassmethod
    async def from_stream(cls, stream_model: StreamModel) -> "Future[_FutureModelT, _ArgsT, _ResT]":
        """Construct a class instance from a stream item

        Args:
            stream_model (StreamModel): A stream item

        Returns:
            Future[_FutureModelT, _ArgsT, _ResT]: The class instance referenced by the stream item
        """
        raise NotImplementedError
    @abstractclassmethod
    async def from_key(cls, key: str) -> "Future[_FutureModelT, _ArgsT, _ResT]":
        """Construct a class instance from a key

        Args:
            key (str): A redis key

        Returns:
            Future[_FutureModelT, _ArgsT, _ResT]: The class instace referenced by the stream item
        """
        raise NotImplementedError
    @abstractmethod
    def __call__(self):
        """Call the function with the arguments
        """
        raise NotImplementedError
    @abstractmethod
    async def get(self) -> Optional[_FutureModelT]:
        """Get the future in the redis db if it exists, None otherwise

        Returns:
            Optional[_FutureModelT[_ArgsT, _ResT]]: The future model if it is the db, None otherwise
        """
        raise NotImplementedError
    @abstractmethod
    async def set(self, pipe: Optional[Pipeline] = None) -> None:
        """Set the future in the redis db

        Args:
            pipe (Optional[Pipeline], optional): An optional pipe. Defaults to None.
        """
        raise NotImplementedError
    @abstractmethod
    async def setnx(self) -> bool:
        """Set the future in the redis db if it does not exists.

        Returns:
            bool: True if the key was set, false otherwise
        """
        raise NotImplementedError
    @abstractmethod
    async def pull(self) -> "Future[_FutureModelT, _ArgsT, _ResT]":
        """Get the future in the redis db and update the instance attributes accordingly

        Returns:
            Future[_FutureModelT, _ArgsT, _ResT]: Self
        """
        raise NotImplementedError
    @abstractmethod
    async def push(self, *streams: AsyncRedisStream) -> None:
        """Set the future in the redis db, publish to self and provided streams
        """
        raise NotImplementedError
    @abstractmethod
    async def update(self, *streams: AsyncRedisStream) -> AsyncContextManager["Future[_FutureModelT, _ArgsT, _ResT]"]:
        """Asynchronous context manager that returns itself and push to the db afterward

        Args:
            streams (AsyncRedisStream): Streams where to push the changes
        """
        raise NotImplementedError
    @abstractmethod
    async def publish_progress(self, result: _ResT, *streams: AsyncRedisStream) -> None:
        """Set result and status to inprogress in the db

        Args:
            result (_ResT): Result to set
            streams (AsyncRedisStream): Streams where to push the changes
        """
        raise NotImplementedError
    @abstractmethod
    async def publish_failure(self, *streams: AsyncRedisStream) -> None:
        """Set status to failed in the db

        Args:
            streams (AsyncRedisStream): Streams where to push the changes
        """
        raise NotImplementedError
    @abstractclassmethod
    async def publish_completion(self, result: _ResT, *streams: AsyncRedisStream) -> None:
        """Set result and status to completed in the db

        Args:
            result (_ResT): Result to set
            streams (AsyncRedisStream): Streams where to push the changes
        """
        raise NotImplementedError



def make_future(db: aioredis.Redis, func: Callable, prefix: str) -> Type[Future]:
    future_model_T = make_future_model(func, prefix)
    args_T = make_args_model(func)
    res_T = get_type_hints(func).get("return") or Any

    class FutureWrapper(Future[_FutureModelT, _ArgsT, _ResT]):
        def __init__(self, args: Optional[Sequence], kwargs: Optional[Dict], future_model: Optional[_FutureModelT] = None):
            nonlocal db, func, prefix, future_model_T
            self.future_model = future_model or future_model_T.build_model(args, kwargs) # type: ignore
            self.stream_model = StreamModel(key=self.future_model.key)
            self.stream = AsyncRedisStream(db, StreamModel, f"{self.future_model.key}:stream")

        @classmethod
        async def from_stream(cls, stream_model: StreamModel) -> "FutureWrapper[_FutureModelT, _ArgsT, _ResT]": # type: ignore
            nonlocal db
            return cls(None, None, future_model_T.parse_raw(await db.get(stream_model.key))) # type: ignore

        @classmethod
        async def from_key(cls, key: str) -> "FutureWrapper[_FutureModelT, _ArgsT, _ResT]": # type: ignore
            nonlocal db
            return cls(None, None, future_model_T.parse_raw(await db.get(key))) # type: ignore

        def __call__(self):
            nonlocal func
            return func(**self.kwargs())

        async def get(self) -> Optional[_FutureModelT]:
            nonlocal db
            cached = await db.get(self.key)
            return future_model_T.parse_raw(cached) if cached else None # type: ignore

        async def set(self, pipe: Optional[Pipeline] = None) -> None:
            nonlocal db
            if pipe:
                return pipe.set(self.key, self.future_model.json())
            await db.set(self.key, self.future_model.json())

        async def setnx(self) -> bool:
            nonlocal db
            return await db.setnx(self.key, self.future_model.json())

        async def pull(self) -> "Future[_FutureModelT, _ArgsT, _ResT]":
            nonlocal db
            cached = future_model_T.parse_raw(await db.get(self.key)) # type: ignore
            self.future_model = cached # type: ignore
            return self

        async def push(self, *streams: AsyncRedisStream) -> None:
            nonlocal db
            pipe = db.pipeline()
            await self.set(pipe)
            await self.stream.push(self.stream_model, pipe)
            for stream in streams:
                await stream.push(self.stream_model, pipe)
            await pipe.execute()

        @contextlib.asynccontextmanager
        async def update(self, *streams: AsyncRedisStream): # type: ignore
            nonlocal db
            try:
                yield self
            finally:
                await self.push(*streams)

        async def publish_progress(self, result: _ResT, *streams: AsyncRedisStream) -> None:
            async with self.update(*streams) as future:
                future.status = "inprogress"
                if result:
                    future.result = result

        async def publish_failure(self, *streams: AsyncRedisStream) -> None:
            async with self.update(*streams) as future:
                future.status = "failed"

        async def publish_completion(self, result: _ResT, *streams: AsyncRedisStream) -> None: # type: ignore
            async with self.update(*streams) as future:
                future.status = "completed"
                if result:
                    future.result = result

    return FutureWrapper[future_model_T, args_T, res_T] # type: ignore
