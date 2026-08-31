"""Bauplan pySDK stubs for project parameters."""

from dataclasses import dataclass


@dataclass
class Parameter:
    """
    Represents a parameter that can be used to "template" values passed to a model during a run or
    query with, e.g., ``bauplan run --parameter interest_rate=2.0``.

    Syntax for accessing a parameter uses the init method as an `Annotation` to
    communicate the proper behavior to the Bauplan control plane:
        ``proj_param: Annotated[float, Parameter('interest_rate')]``
    """

    param_name: str
