from inspect import Parameter
from inspect import signature
from typing import Any
from typing import Callable
from typing import TypedDict
from typing import Union
from typing import get_args
from typing import get_origin

from pydantic import TypeAdapter

from flive.types import JSON
from flive.types import Params
from flive.types import Result


class SerializedParams(TypedDict):
    args: list[JSON]
    kwargs: dict[str, JSON]


def serialize_parameters(
    method: Callable[Params, Any], *args: Params.args, **kwargs: Params.kwargs
) -> SerializedParams:
    # Step 1: Inspect the method signature
    sig = signature(method)
    bound_args = sig.bind(*args, **kwargs)
    bound_args.apply_defaults()

    # Step 2: Get serializers using Pydantic's TypeAdapter
    serializers = {}
    for name, param in sig.parameters.items():
        if param.annotation is not Parameter.empty:
            serializers[name] = TypeAdapter(param.annotation)
        else:
            raise ValueError(f"Annotation for {name} missing.")

    # Step 3: Serialize the arguments
    serialized_args = []
    serialized_kwargs = {}

    # Iterate over parameters and bound arguments
    for name, param in sig.parameters.items():
        value = bound_args.arguments.get(name, None)
        serialized = serializers[name].dump_python(value, mode="json")
        match param.kind:
            case Parameter.POSITIONAL_ONLY:
                if value is not None:
                    serialized_args.append(serialized)
            case Parameter.POSITIONAL_OR_KEYWORD | Parameter.KEYWORD_ONLY:
                serialized_kwargs[name] = serialized
            case Parameter.VAR_POSITIONAL | Parameter.VAR_KEYWORD:
                raise ValueError("Can't handle variadic arguments.")

    return {"args": serialized_args, "kwargs": serialized_kwargs}


def deserialize_parameters(
    method: Callable[Params, Any], serialized_params: SerializedParams
) -> tuple[list[Any], dict[str, Any]]:
    # Currently there's no way to type the return type correctly.
    # Step 1: Inspect the method signature
    sig = signature(method)

    # Step 2: Get deserializers using Pydantic's TypeAdapter
    deserializers: dict[str, TypeAdapter] = {}
    for name, param in sig.parameters.items():
        if param.annotation is not Parameter.empty:
            deserializers[name] = TypeAdapter(param.annotation)
        else:
            raise ValueError(f"Annotation for {name} missing.")

    args = serialized_params["args"]
    kwargs = serialized_params["kwargs"]

    # Step 3: Deserialize the arguments
    deserialized_args = []
    deserialized_kwargs = {}

    arg_index = 0
    for name, param in sig.parameters.items():
        deserializer = deserializers[name]

        if param.kind == Parameter.POSITIONAL_ONLY:
            serialized_value = args[arg_index]
            deserialized_args.append(deserializer.validate_python(serialized_value))
            arg_index += 1
        elif param.kind in (Parameter.POSITIONAL_OR_KEYWORD, Parameter.KEYWORD_ONLY):
            if name in kwargs:
                value = kwargs[name]
                deserialized_kwargs[name] = deserializer.validate_python(value)

            elif arg_index < len(args):
                value = args[arg_index]
                deserialized_args.append(deserializer.validate_python(value))
                arg_index += 1

        elif param.kind in (Parameter.VAR_POSITIONAL, Parameter.VAR_KEYWORD):
            raise ValueError("Can't handle variadic arguments yet.")

    return deserialized_args, deserialized_kwargs


def serialize_result(method: Callable[..., Result], value: Result) -> JSON:
    # Step 1: Inspect the method signature
    sig = signature(method)
    return_annotation = sig.return_annotation

    # Step 2: Ensure the return type is annotated
    if return_annotation is Parameter.empty:
        if value is None:
            return None
        else:
            raise ValueError(
                f"The return type of the method '{method.__name__}' is not annotated."
            )

    # Step 3: Use Pydantic's TypeAdapter to serialize the return value
    type_adapter = TypeAdapter(return_annotation)
    serialized_value = type_adapter.dump_python(value, mode="json")

    return serialized_value


def deserialize_result(method: Callable[..., Result], value: JSON) -> Result:
    # Step 1: Inspect the method signature
    sig = signature(method)
    return_annotation = sig.return_annotation

    # Step 2: Handle methods that don't return a value
    if return_annotation is Parameter.empty or return_annotation is None:
        if value is None:
            return None
        else:
            raise ValueError(
                f"The return type of the method '{method.__name__}' is not annotated."
            )

    # Handle Optional types (e.g., Optional[int] which is Union[int, None])
    origin = get_origin(return_annotation)
    args = get_args(return_annotation)
    if origin is Union and type(None) in args:
        if value is None:
            return None

    # Step 3: Use Pydantic's TypeAdapter to deserialize the return value
    type_adapter = TypeAdapter(return_annotation)
    deserialized_value = type_adapter.validate_python(value)

    return deserialized_value
