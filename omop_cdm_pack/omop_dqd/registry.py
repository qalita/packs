"""Mapping from OHDSI check names to their Polars implementation."""

from typing import Callable, Dict

_REGISTRY: Dict[str, Callable] = {}


def register(check_name: str) -> Callable:
    def decorator(function: Callable) -> Callable:
        if check_name in _REGISTRY:
            raise ValueError(f"Check {check_name!r} is already registered")
        _REGISTRY[check_name] = function
        return function

    return decorator


def get_check(check_name: str) -> Callable:
    if check_name not in _REGISTRY:
        raise KeyError(f"No implementation for check {check_name!r}")
    return _REGISTRY[check_name]


def is_registered(check_name: str) -> bool:
    return check_name in _REGISTRY


def registered_names():
    return sorted(_REGISTRY)
