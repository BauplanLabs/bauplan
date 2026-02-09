"""Key-value object store for passing objects between DAG nodes."""

from typing import Any


def load_obj(key: str) -> Any:
    """Return the Python object previously stored at the given key."""
    raise NotImplementedError("Only available at runtime in Bauplan")


def save_obj(key: str, obj: Any) -> None:
    """Store a Python object with a key for later retrieval."""
    raise NotImplementedError("Only available at runtime in Bauplan")
