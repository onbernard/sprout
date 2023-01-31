from typing import List, Optional, Callable, Any
import asyncio

import redis.asyncio as redis

from src.task import Task

class Sprout:
    def __init__(self, host: str = "localhost", port: int = 6379) -> None:
        self.db = redis.Redis(host=host, port=port)
        self.task_index: List[Task] = []

    def task(self, name: Optional[str] = None):
        def inner(func: Callable):
            nonlocal name
            name = name or func.__name__
            task = Task(self.db, func, name)
            self.task_index.append(task)
            return task
        return inner

    async def run(self):
        res = await asyncio.gather(
            *(task.run() for task in self.task_index)
        )
        print(res)

    def __call__(self, *args: Any, **kwds: Any) -> Any:
        asyncio.run(self.run())