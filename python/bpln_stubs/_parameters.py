"""Bauplan pySDK stubs for project parameters."""

from dataclasses import dataclass


@dataclass
class Parameter:
    """
    Represents a parameter that can be used to "template" values passed to a model during a run or
    query with, e.g., ``bauplan run --parameter interest_rate=2.0``.

    Legacy syntax for accessing a parameter uses the init method in place of a default value:
        ``proj_param=Parameter('interest_rate')``

    Which is deprecated in favor of using the init method as an `Annotation`:
        ``proj_param: Annotated[Float64, Parameter('interest_rate')]``

    In both cases, the instantiated object is ignored at runtime; the syntax communicates
    the proper behavior to the Bauplan control plane.
    """

    param_name: str
