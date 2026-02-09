from typing import Annotated, ClassVar

from typing_extensions import TypeAlias

# The below enables us to document Parameter, which is actually
# an instance of a callable object, usually docstrings only work
# for classes and functions, but we can use Annotated to add it
# here.
ParameterType: TypeAlias = Annotated[
    'Parameter',
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
def a_model_using_params(
    # parent models are passed as inputs, using bauplan.Model
    interest_rate=bauplan.Parameter('interest_rate'),
    loan_amount=bauplan.Parameter('loan_amount'),
    customer_name=bauplan.Parameter('customer_name'),
):
    print(f"Calculating interest for {customer_name}")
    return pyarrow.Table.from_pydict({'interest': [loan_amount * interest_rate]})
```
""",
]


class _ParameterKwargTracker:
    """
    _ParameterKwargTracker is a callable object that is used to track
    the parameters that are used in a model. Because default function
    args are evaluated once and only once, we can build a set of the
    args for kwargs.
    """

    requested: ClassVar[set] = set()

    def __init__(self) -> None:
        pass

    def __call__(self, param_name: str) -> None:
        _ParameterKwargTracker.requested.add(param_name)


Parameter: ParameterType = _ParameterKwargTracker()
