from bauplan._internal import (
    __version__,
    __bpln_feature_typecontracts__,
)

# Re-export everything from the extension module.
from bauplan._internal import (
    Client,
    InfoState,
    OrganizationInfo,
    RunnerNodeInfo,
    UserInfo,
)

# Submodules.
from bauplan import exceptions, schema, state, standard_expectations
from bauplan.schema import JobKind, JobState, RefType
from bauplan._classes import Model
from bauplan._decorators import (
    ModelCacheStrategy,
    ModelMaterializationStrategy,
    expectation,
    extras,
    model,
    pyspark,
    python,
    resources,
    synthetic_model,
)
from bauplan._parameters import Parameter


__all__ = [
    "__version__",
    # Submodules.
    "exceptions",
    "schema",
    "standard_expectations",
    "state",
    # From _internal.
    "Client",
    "InfoState",
    "JobKind",
    "JobState",
    "OrganizationInfo",
    "RefType",
    "RunnerNodeInfo",
    "UserInfo",
    # Decorators and model definitions.
    "Model",
    "ModelCacheStrategy",
    "ModelMaterializationStrategy",
    "Parameter",
    "expectation",
    "extras",
    "model",
    "pyspark",
    "python",
    "resources",
    "synthetic_model",
]

if __bpln_feature_typecontracts__:
    from bauplan._contracts import Artifact, Catalog, ModelTask

    __all__.extend(
        [
            "Artifact",
            "Catalog",
            "ModelTask",
        ]
    )
