from typing import Optional, Callable, TypedDict, Unpack


class PythonKeywordArgs(TypedDict):
    """Accepted keyword arguments for the `python` decorator."""

    ...


def python(
    version: Optional[str] = None,
    pip: Optional[dict[str, str]] = None,
    **kwargs: Unpack[PythonKeywordArgs],
) -> Callable:
    """
    Decorator that defines a Python environment for a Bauplan function (e.g. a model or
    expectation). It is used to specify directly in code the configuration of the Python
    environment required to run the function, i.e. the Python version and the Python
    packages required.

    Parameters:
        version: The python version for the interpreter (e.g. `'3.11'`).
        pip: A dictionary of dependencies (and versions) required by the function, for example:
             `{'requests': '2.26.0'}`.
    """

    def decorator(wrapped_fn: Callable) -> Callable:
        return wrapped_fn

    return decorator
