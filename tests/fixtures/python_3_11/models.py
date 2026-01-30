import bauplan
import platform

@bauplan.model(
    materialization_strategy='NONE',
)
@bauplan.python()
def normalize_data(
    data=bauplan.Model('query_model'),
):
    print(f'Running on python {platform.python_version()}')
    return data
