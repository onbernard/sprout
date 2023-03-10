from typing import List, Optional, Callable, Any, Union
from pathlib import Path
import inspect
import asyncio

import redis.asyncio as redis

from .task import Task, GeneratorTask

class Sprout:
    def __init__(self, name: Optional[str] = None, host: str = "localhost", port: int = 6379) -> None:
        self.name = name or "sprout"
        self.db: redis.Redis = redis.Redis(host=host, port=port, decode_responses=True)
        self.task_index: List[Task] = []

    def task(self, name: Optional[str] = None) -> Callable[[Callable], Union[Task, GeneratorTask]]:
        def inner(func: Callable) -> Union[Task, GeneratorTask]:
            nonlocal name
            name = name or func.__name__
            task: Union[Task, GeneratorTask]
            if inspect.isgeneratorfunction(func):
                task = GeneratorTask(db=self.db, func=func, prefix=f"{self.name}:{name}")
            elif inspect.isfunction(func):
                task = Task(db=self.db, func=func, prefix=f"{self.name}:{name}")
            else:
                raise TypeError("function or generator function required")
            self.task_index.append(task)
            return task
        return inner

    async def run(self):
        await asyncio.gather(
            *(task.run() for task in self.task_index)
        )

    def __call__(self) -> Any:
        asyncio.run(self.run())