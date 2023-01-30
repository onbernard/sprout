from uuid import UUID, uuid4
from typing import Optional, Type, List, Tuple

import redis
import redis.asyncio as aioredis
from pydantic import BaseModel

class RedisStream:
    def __init__(self, db: redis.Redis, model: Type[BaseModel], suffix: Optional[str] = None):
        self.db = db
        self.model = model
        suffix = suffix if suffix else uuid4().hex
        self.key = f"_stream:{suffix}"

    def iter(self, count: int = 1, block: int = 0, last_seen: Optional[str] = None):
        last_seen = last_seen if last_seen else "0"
        while True:
            items = self.db.xread(
                streams = {self.key: last_seen},
                count = count,
                block = block
            )
            if items:
                last_seen = items[0][1][-1][0]
                new_count = yield [(idx, self.model.parse_raw(msg[b"val"])) for idx, msg in items[0][1]]
                count = new_count if new_count else count

    def get(self, index: str = "-") -> Tuple[bytes,BaseModel]:
        return [(index, self.model.parse_raw(msg[b"val"])) for index, msg in self.db.xrange(
            name = self.key,
            min = index,
            count = 1
        )][0]

    def range(self, min: str = "-", max: str = "+") -> List[Tuple[bytes, BaseModel]]:
        return [(index, self.model.parse_raw(msg[b"val"])) for index, msg in self.db.xrange(
            name = self.key,
            min = min,
            max = max
        )]

    def pop(self, index: Optional[bytes] = None) -> BaseModel:
        index = index or "-"
        index,val = self.get(index)
        self.db.xdel(self.key, index)
        return val

    def push(self, msg: BaseModel) -> bytes:
        return self.db.xadd(name=self.key, fields={"val":msg.json()})



class AsyncRedisStream:
    def __init__(self, db: aioredis.Redis, model: Type[BaseModel], suffix: Optional[str] = None):
        self.db = db
        self.model = model
        suffix = suffix or uuid4().hex
        self.key = f"_stream:{suffix}"

    async def iter(self, count: int = 1, block: int = 0, last_seen: Optional[str] = None):
        last_seen = last_seen or "0"
        while True:
            items = await self.db.xread(
                streams = {self.key: last_seen},
                count = count,
                block = block
            )
            if items:
                last_seen = items[0][1][-1][0]
                new_count = yield [(idx, self.model.parse_raw(msg[b"val"])) for idx, msg in items[0][1]]
                count = new_count or count
            else:
                break

    async def push(self, item: BaseModel) -> bytes:
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

    async def iter_group(self, groupname: str, consumername: str, count: int = 1, block: int = 0, noack: bool = False, last_seen: Optional[str] = None):
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
                new_count = yield [(idx, self.model.parse_raw(msg[b"val"])) for idx, msg in items[0][1]]
                count = new_count or count
            else:
                break

    async def ack(self, groupname: str, *ids: bytes):
        await self.db.xack(self.key, groupname, *ids)