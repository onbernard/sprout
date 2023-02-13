from typing import (
    Generic,
    Optional,
    Literal,
    Sequence,
    TypeVar,
    Callable,
    Type,
    Any,
    Dict,
    get_type_hints,
)
from hashlib import md5
import inspect

from pydantic import BaseModel, create_model
from pydantic.generics import GenericModel
from pydantic.decorator import ValidatedFunction

# TODO : always check consistency between GenericFutureModel and inner class of mk_future_model

_ArgsT = TypeVar("_ArgsT", bound=BaseModel)
_ResT = TypeVar("_ResT")


class GenericFutureModel(GenericModel, Generic[_ArgsT, _ResT]):
    """Pydantic model representing the state of a function call"""
    arguments: _ArgsT
    result: Optional[_ResT] = None
    status: Literal["pending", "inprogress", "completed", "failed"] = "pending"

    def kwargs(self) -> Dict:
        """Return dictionary with the function arguments

        Returns:
            Dict: Dictionary with the function arguments
        """
        return dict(self.arguments)

    @property
    def key(self) -> str:
        """Return the value used as the redis key

        Returns:
            str: The value used a the redis key
        """
        raise NotImplementedError

    @classmethod
    def build_model(cls, args: Sequence, kwargs: Dict) -> "GenericFutureModel[_ArgsT, _ResT]":
        """Instanciate the class with the args and kwargs of the function call

        Args:
            args (_type_): Positional arguments of the function call
            kwargs (_type_): Keyword arguments of the function call

        Returns:
            GenericFutureModel[_ArgsT, _ResT]: Instance representing the function call
        """
        raise NotImplementedError


class StreamModel(BaseModel):
    key: str


def make_args_model(func: Callable) -> Type[BaseModel]:
    """Generate a pydantic model based on the type hints of the parameters in the signarue

    Args:
        func (Callable): Any function

    Returns:
        Type[BaseModel]: Pydantic model whose keys, values pairs are the name and type hints of the function
    """
    type_index = {}
    for name, param in inspect.signature(func).parameters.items():
        annotation = param.annotation if param.annotation != inspect._empty else Any
        default = param.default if param.default != inspect._empty else ...
        type_index[name] = (annotation, default)
    return create_model("Args", **type_index) # type: ignore


def make_future_model(func: Callable, prefix: str) -> Type[GenericFutureModel]:
    """Generate a pydantic model based on the type hints of the parameters and return value of a function

    Args:
        func (Callable): Any function
        prefix (str): string that will be prepended to the redis keys

    Returns:
        Type[GenericFutureModel]: A pydantic model representing the state of a function call
    """
    args_model = make_args_model(func)
    result_type = get_type_hints(func).get("return") or Any
    validated_function = ValidatedFunction(func, None)
    class FutureModel(GenericFutureModel, Generic[_ArgsT, _ResT]):
        @property
        def key(self) -> str:
            return f"{prefix}:{md5(self.arguments.json().encode()).hexdigest()}"
        @classmethod
        def build_model(cls, args, kwargs) -> "FutureModel[_ArgsT, _ResT]":
            return cls(arguments=args_model(**validated_function.build_values(args, kwargs)))
    return FutureModel[args_model, result_type] # type: ignore