import inspect
import sys
import os
from uuid import UUID, uuid4
from typing import Callable, Optional, Dict, ClassVar, Any, List, Literal, get_type_hints, Type, Tuple, Union
from functools import wraps, cached_property
from concurrent.futures.process import ProcessPoolExecutor
from multiprocessing import Process, Queue

import redis
from fastapi import FastAPI, BackgroundTasks
from pydantic import BaseModel, validator, create_model
from pydantic.decorator import ValidatedFunction

from src.utils.stream import RedisStream



class Sprout:
    def __init__(self, host: str = "localhost", port: int = 6379):
        self.db = redis.Redis(host=host, port=port)
        self.task_index: List[Task] = []

    def task(self, n_worker: int = 1, name: Optional[str] = None):
        def inner(func: Callable):
            task = Task(func, n_worker, self.db, name)
            self.task_index.append(task)
            return task
        return inner

    def start(self):
        for task in self.task_index:
            task.start()


class Task:
    def __init__(self, func: Callable, n_worker: int, db: redis.Redis, name: Optional[str] = None) -> None:
        self.func = func
        self.n_worker = n_worker
        self.db = db
        self.name = name or func.__name__
        self.validated_function = ValidatedFunction(func, None)
        self.future_model = self.create_future_model(func)
        self.input_stream = RedisStream(self.db, self.future_model, suffix=f"in{self.name}")
        self.input_queue = Queue()
        self.output_stream = RedisStream(self.db, self.future_model, suffix=f"out{self.name}")
        self.worker_list: List[Process] = []
        self.master: Optional[Process] = None

    def create_future_model(self,func: Callable):
        # class CustomFuture(BaseModel):
        #     func_val: ValidatedFunction(func, None).model
        #     status: Literal["pending", "completed"] = "pending"
        #     result: Optional[Any] = None
        #     key: Optional[str] = None

        @validator("key", pre=True, always=True)
        def set_key(cls, v):
            return v or f"_sprout:future:{uuid4().hex}"
        
        outp = create_model(
            __model_name="CustomFuture",
            __validators__={"set_key":set_key},
            func_val = (ValidatedFunction(func,None).model,...),
            status = (Literal["pending", "completed"], "pending"),
            result = (Optional[Any], None),
            key = (Optional[str], None)
        )
        return outp

    def func_validation_model(self, func: Callable):
        return ValidatedFunction(func, None).model


    def start(self):
        self.worker_list = [Process(target=self.consumer, args=(i,)) for i in range(self.n_worker)]
        for worker in self.worker_list:
            worker.start()
        self.master = Process(target=self.producer)
        self.master.start()
        

    def stop(self):
        for _ in self.worker_list:
            self.input_queue.put(None)
        for worker in self.worker_list:
            worker.join()

    def producer(self):
        for items in self.input_stream.iter():
            for index, item in items:
                self.input_queue.put((index, item))

    def consumer(self, id):
        print(f"Worker {self.name} {id} starting...")
        while True:
            index, item = self.input_queue.get()
            self.input_stream.pop(index)
            if not item:
                print(f"Worker {self.name} {id} stopping...", flush=True)
                break
            print(f"Worker {self.name} {id} got {item}", flush=True)
            res = self.validated_function.execute(item.func_val)
            print(f"Worker {self.name} {id} computed {res}", flush=True)
            self.publish(item, res)

    def publish(self, item: "Future", res: Any):
        item.result = res
        item.status = "completed"
        self.output_stream.push(item)
        self.db.set(item.key, item.json())

    def exe(self, *args, **kwargs):
        return self.future_model(
            func_val = self.validated_function.model(*args, **kwargs)
        )

    def __getstate__(self):
        state = self.__dict__.copy()
        del state["create_future_model"]
        return state

    def __setstate__(self, state):
        self.__dict__.update(state)

    def __call__(self, *args, **kwargs: Any) -> Any:
        future = self.future_model(
            func_val = self.validated_function.model(*args, **kwargs)
        )
        self.db.set(future.key, future.json())
        self.input_stream.push(future)
        return future

    def pending(self) -> List[Tuple[bytes, "Future"]]:
        return self.input_stream.range()

    def completed(self) -> List[Tuple[bytes, "Future"]]:
        return self.output_stream.range()

    def resolve(self, future: "Future") -> Optional[Any]:
        try:
            return future.parse_raw(self.db.get(future.key))
        except:
            return None


class Future(BaseModel):
    status: Literal["pending", "completed"] = "pending"
    kwargs: Dict[str, Union[BaseModel, Any]]
    result: Optional[Any] = None
    key: Optional[str] = None

    @validator("key", pre=True, always=True)
    def set_key(cls, v):
        return v or f"_sprout:future:{uuid4().hex}"


