from uuid import UUID, uuid4
from typing import Optional, Type, List, Tuple, AsyncGenerator

import redis.asyncio as aioredis
from redis.asyncio.client import Pipeline

from pydantic import BaseModel


class AsyncRedisStream:
    def __init__(self, db: aioredis.Redis, model: Type[BaseModel], key: str):
        self.db = db
        self.model = model
        self.key = key

    async def read(self, count: int = 1, block: int = 0, last_seen: Optional[str] = None):
        last_seen = last_seen or "0"
        while True:
            items = await self.db.xread(
                streams = {self.key: last_seen},
                count = count,
                block = block
            )
            if items:
                last_seen = items[0][1][-1][0]
                new_count = yield [(idx, self.model.parse_raw(msg["val"])) for idx, msg in items[0][1]]
                count = new_count or count
            else:
                break

    async def push(self, item: BaseModel, pipe: Optional[Pipeline]) -> bytes:
        if pipe:
            return pipe.xadd(name=self.key, fields={"val":item.json()})
        return await self.db.xadd(name=self.key, fields={"val":item.json()})

    async def create_group(self, groupname: str, last_seen: Optional[str] = None, mkstream: bool = True, existok: bool = True):
        try:
            return await self.db.xgroup_create(
                name = self.key,
                groupname = groupname,
                id = last_seen or "0",
                mkstream = mkstream
            )
        except aioredis.ResponseError as exc:
            if existok:
                return None
            else:
                raise exc

    async def create_consumer(self, groupname: str, consumername: str):
        return await self.db.xgroup_createconsumer(
            name = self.key,
            groupname = groupname,
            consumername = consumername,
        )

    async def readgroup(self, groupname: str, consumername: str, count: int = 1, block: int = 0, noack: bool = False, last_seen: Optional[str] = None):
        last_seen = last_seen or ">"
        while True:
            items = await self.db.xreadgroup(
                groupname = groupname,
                consumername = consumername,
                streams = {self.key: last_seen},
                count = count,
                block = block,
                noack = noack
            )
            if items:
                new_count = yield [(idx, self.model.parse_raw(msg["val"])) for idx, msg in items[0][1]]
                count = new_count or count
            else:
                break

    async def ack(self, groupname: str, *ids: bytes):
        await self.db.xack(self.key, groupname, *ids)

    async def range(self, min: str = "-", max: str = "+") -> List[Tuple[bytes, BaseModel]]:
        return [(index, self.model.parse_raw(msg["val"])) for index, msg in await self.db.xrange(
            name = self.key,
            min = min,
            max = max
        )]


async def watch_streams(*stream_list: AsyncRedisStream) -> AsyncGenerator[Tuple[AsyncRedisStream,BaseModel], None]:
    index = {
        stream.key: "$"
    for stream in stream_list}
    rev_index = {
        stream.key: stream
    for stream in stream_list}
    while True:
        items = await stream_list[0].db.xread(
            streams = index,
            count = 1,
            block = 0
        )
        for it in items:
            key = it[0]
            index[key] = it[1][-1][0]
            yield (rev_index[key], stream_list[0].model.parse_raw(it[1][-1][1]["val"]))
