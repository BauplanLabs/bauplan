"""Bauplan decorators that specify runtime options."""

from typing import Optional, Callable


def python(
    version: Optional[str] = None,
    pip: Optional[dict[str, str]] = None,
) -> Callable:
    """
    Decorator that defines a Python environment for a Bauplan function (e.g. a model or
    expectation). It is used to specify which Python version a model or expectation
    should run on and which Python packages should be available.

    Parameters:
        version: The python interpreter version (e.g. `'3.11'`).
        pip: A dictionary containing python packages and their versions required by the
             function, for example: `{'requests': '2.26.0'}`.
    """

    def decorator(wrapped_fn: Callable) -> Callable:
        return wrapped_fn

    return decorator
