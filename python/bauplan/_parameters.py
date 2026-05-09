import logging

from typing import ClassVar

from bauplan._internal import __bpln_feature_typecontracts__

if __bpln_feature_typecontracts__:
    from bauplan._contracts import ValRegistryMeta


module_logger = logging.getLogger("bauplan")


if __bpln_feature_typecontracts__:

    class ParameterBaseType(metaclass=ValRegistryMeta):  # type: ignore[reportRedeclaration]
        """A base type for Parameter if type contracts are enabled."""

        ...

else:

    class ParameterBaseType:
        """A base type for Parameter if type contracts are disabled."""

        ...


class Parameter(ParameterBaseType):  # type: ignore[reportGeneralTypeIssues]
    """
    Represents a parameter that can be used to "template" values
    passed to a model during a run or query with, e.g.,
    ``bauplan run --parameter interest_rate=2.0``.

    Parameters must be defined with default value under the top level
    `parameters` key in the `bauplan.yml` project file.

    e.g.

    ```yaml
    project:
        id: xyzxyz
        name: eggs

    parameters:
        interest_rate:
            default: 5.5
        loan_amount:
            default: 100000
        customer_name:
            default: "John MacDonald"
    ```

    Then, to use them in a model, use `bauplan.Parameter`:

    ```python
    #! import pyarrow
    def a_model_using_params(
        # parent models are passed as inputs, using bauplan.Model
        interest_rate=bauplan.Parameter('interest_rate'),
        loan_amount=bauplan.Parameter('loan_amount'),
        customer_name=bauplan.Parameter('customer_name'),
    ):
        print(f"Calculating interest for {customer_name}")
        return pyarrow.Table.from_pydict({'interest': [float(loan_amount) * float(interest_rate)]})
    ```
    """

    _requested: ClassVar[set] = set()

    def __init__(self, param_name: str) -> None:
        if __bpln_feature_typecontracts__:
            module_logger.warning(
                f'Using deprecated syntax: `Parameter("{param_name}")`'
            )

        Parameter._requested.add(param_name)

    def __float__(self) -> float:
        raise NotImplementedError

    def __int__(self) -> int:
        raise NotImplementedError

    def __str__(self) -> str:
        raise NotImplementedError

    def __index__(self) -> int:
        raise NotImplementedError
