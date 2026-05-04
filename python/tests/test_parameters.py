from bauplan._parameters import Parameter


class TestParameterValueEntry:
    def test_unregistered_defaults_to_none(self):
        assert Parameter.interest_rate is None

    def test_registered_defaults_to_entry_value(self):
        Parameter.register('golden_ratio', 1.618)
        Parameter.register('max_iterations', 100)

        def my_model(
            golden_ratio=Parameter.golden_ratio,
            max_iterations=Parameter.max_iterations,
        ):
            return golden_ratio, max_iterations

        assert my_model() == (1.618, 100)

    def test_kwarg_overrides_default(self):
        Parameter.register('interest_rate', 5.5)

        def my_model(
            interest_rate=Parameter.interest_rate,
            loan_amount=Parameter.loan_amount,
        ):
            return interest_rate, loan_amount

        assert my_model(interest_rate=9.9, loan_amount=200000) == (9.9, 200000)
