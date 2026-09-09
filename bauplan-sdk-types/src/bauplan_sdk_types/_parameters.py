"""Bauplan pySDK stubs for project parameters."""

from dataclasses import dataclass


@dataclass
class Parameter:
    """
    Represents a parameter that can be used to "template" values passed to a model during a run or
    query with, e.g., ``bauplan run --parameter interest_rate=2.0``.

    To bind a project parameter to a model argument, attach a Parameter instance as metadata using typing.Annotated. 
    The type before the comma is the parameter's expected type; the Parameter(name) after it tells Bauplan which 
    named parameter to inject:
        ``proj_param: Annotated[float, Parameter('interest_rate')]``
    """

    param_name: str
