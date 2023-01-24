from uuid import UUID, uuid4
from typing import Optional, Type, List, Tuple

import redis
from pydantic import BaseModel

class RedisStream:
    def __init__(self, db: redis.Redis, model: Type[BaseModel], suffix: Optional[str] = None):
        self.db = db
        self.model = model
        suffix = suffix if suffix else uuid4().hex
        self.key = f"_sprout:stream:{suffix}"

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
                new_count = yield [(index, self.model.parse_raw(msg[b"val"])) for index, msg in items[0][1]]
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