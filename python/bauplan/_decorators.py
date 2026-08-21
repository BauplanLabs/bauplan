"""

Bauplan functions are normal Python functions enriched by a few key decorators.
This module contains the decorators used to define Bauplan models, expectations and
Python environments, with examples of how to use them.

"""

import functools
from typing import Any, Callable, Dict, List, Literal, Optional, Tuple, Union


def expectation(
    **kwargs: Any,
) -> Callable:
    """
    Decorator that defines a Bauplan expectation.

    An expectation is a function from one (or more) dataframe-like object(s) to a boolean: it
    is commonly used to perform data validation and data quality checks when running a pipeline.
    Expectations take as input the table(s) they are validating and return a boolean indicating
    whether the expectation is met or not. A Python expectation needs a Python environment to run,
    which is defined using the `python` decorator, e.g.:

    ```python
    import bauplan
    from bauplan.standard_expectations import expect_column_no_nulls

    @bauplan.expectation()
    @bauplan.python('3.11')
    def test_joined_dataset(
        data=bauplan.Model(
            'join_dataset',
            columns=['anomaly']
        )
    ):
        # your data validation code here
        return expect_column_no_nulls(data, 'anomaly')
    ```
    """

    def decorator(f: Callable) -> Callable:
        return f

    return decorator


def resources(
    cpus: Optional[Union[int, float]] = None,
    memory: Optional[Union[int, str]] = None,
    memory_swap: Optional[Union[int, str]] = None,
    timeout: Optional[int] = None,
    **kwargs: Any,
) -> Callable:
    """
    Decorator that defines the resources required by a Bauplan function (e.g. a model or expectation). It is used to
    specify directly in code the configuration of the resources required to run the function.

    Parameters:
        cpus: The number of CPUs required by the function (e.g: ``0.5``)
        memory: The amount of memory required by the function (e.g: ``1G``, ``1000``)
        memory_swap: The amount of swap memory required by the function (e.g: ``1G``, ``1000``)
        timeout: The maximum time the function is allowed to run (e.g: ``60``)
    """

    def decorator(f: Callable) -> Callable:
        return f

    return decorator


def extras(*args) -> Callable:
    """
    Decorator that defines the `bauplan` package extras to install.

    This decorator allows specifying which optional feature sets (extras)
    of the `bauplan` package are required by the decorated function.

    For example, using ``@bauplan.extras('ai')`` will request the installation of ai specific functionalities,
    ensuring that the right dependencies are installed.

    Parameters:
        args: A variable list of strings, where each string is the name of an extra to install (e.g., ``'ai'``, ``'prefect'``).
    """

    def decorator(f: Callable) -> Callable:
        return f

    return decorator
