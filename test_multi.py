from src.future import FutureModel, Arguments
from src.task import Task
from pydantic import BaseModel
from redis.asyncio import Redis

def afunc(a:int):
    return 1

task = Task(Redis(), afunc, "uwu")
