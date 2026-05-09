from typing import Any

import pytest

from bauplan._internal import __bpln_feature_typecontracts__
from bauplan._parameters import Parameter


@pytest.mark.skipif(
    not __bpln_feature_typecontracts__,
    reason='requires BPLN_ENABLE_TYPE_CONTRACT',
)
class TestTypedParameterValueEntry:
    def test_unregistered_defaults_to_none(self) -> None:
        assert Parameter.interest_rate is None

    def test_registered_defaults_to_entry_value(self) -> None:
        Parameter.register('golden_ratio', 1.618)
        Parameter.register('max_iterations', 100)

        def my_model(
            golden_ratio: Any = Parameter.golden_ratio,
            max_iterations: Any = Parameter.max_iterations,
        ) -> tuple[Any, Any]:
            return golden_ratio, max_iterations

        assert my_model() == (1.618, 100)

    def test_kwarg_overrides_default(self) -> None:
        Parameter.register('interest_rate', 5.5)

        def my_model(
            interest_rate: Any = Parameter.interest_rate,
            loan_amount: Any = Parameter.loan_amount,
        ) -> tuple[Any, Any]:
            return interest_rate, loan_amount

        assert my_model(interest_rate=9.9, loan_amount=200000) == (9.9, 200000)
