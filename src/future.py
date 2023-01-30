from typing import Any, Literal, Hashable, Dict, Tuple, Callable, Optional, Type
from uuid import UUID, uuid4
from collections import namedtuple
from hashlib import md5
import inspect

from pydantic import BaseModel, validator
from pydantic.decorator import ValidatedFunction

class Kwargs(dict):
    @classmethod
    def __get_validators__(cls):
        yield cls.validate

    @classmethod
    def validate(cls, v):
        return cls(v)


class Args(tuple):
    @classmethod
    def __get_validators__(cls):
        yield cls.validate

    @classmethod
    def validate(cls, v):
        return cls(v)


class Arguments(BaseModel):
    args: Args
    kwargs: Kwargs

    def hash(self) -> str:
        return md5(self.json().encode()).hexdigest()


class FutureModel(BaseModel):
    arguments: Arguments
    result: Any = None
    status: Literal["pending", "completed", "failed"] = "pending"

    def key(self, mixin: str) -> str:
        return f"_future:{mixin}:{self.arguments.hash()}"
