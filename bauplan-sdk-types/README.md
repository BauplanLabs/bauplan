# Overview

This package, `bauplan_sdk_types`, contains the types required to define Pipelines in Bauplan. These types are
just a facade, with no functionality attached.

Definitions in `bauplan_sdk_types` are re-exported in the `bauplan` package, so these two
statements are identical:

```python
from bauplan import Model
from bauplan_sdk_types import Model
```
