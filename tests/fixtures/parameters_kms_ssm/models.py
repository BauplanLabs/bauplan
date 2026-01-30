import bauplan
import pyarrow as pa


@bauplan.model(
    columns=['y'],
    materialization_strategy='NONE',
)
@bauplan.python('3.11')
def params_are_cool_model(
    yayparams=bauplan.Model(
        'taxi_fhvhv',
        columns=[
            'trip_miles',
        ],
        filter="pickup_datetime >= '2023-01-01T00:00:00+00:00' AND pickup_datetime < '2023-01-01T01:00:00+00:00'",
    ),
    my_secret_value_1=bauplan.Parameter('my_secret_key_1'),
    my_secret_value_2=bauplan.Parameter('my_secret_key_2'),
    my_vault_string_us_value=bauplan.Parameter('my_vault_string_us'),
    my_vault_string_list_us_value=bauplan.Parameter('my_vault_string_list_us'),
    my_vault_secure_string_us_value=bauplan.Parameter('my_vault_secure_string_us'),
    my_vault_override_us_with_eu_value=bauplan.Parameter('my_vault_override_us_with_eu'),
    my_vault_string_eu_value=bauplan.Parameter('my_vault_string_eu'),
    my_vault_string_list_eu_value=bauplan.Parameter('my_vault_string_list_eu'),
    my_vault_secure_string_eu_value=bauplan.Parameter('my_vault_secure_string_eu'),
    my_vault_override_eu_with_us_value=bauplan.Parameter('my_vault_override_eu_with_us'),
) -> pa.Table:
    print(f'my_secret_key_1={my_secret_value_1}')
    print(f'my_secret_key_1_reversed={my_secret_value_1[::-1]}')
    print(f'my_secret_key_2={my_secret_value_2}')
    print(f'my_secret_key_2_reversed={my_secret_value_2[::-1]}')

    print()
    print(f'my_vault_string_us={my_vault_string_us_value}')
    print(f'my_vault_string_list_us={my_vault_string_list_us_value}')
    print(f'my_vault_secure_string_us={my_vault_secure_string_us_value}')
    print(f'my_vault_secure_string_us_reversed={my_vault_secure_string_us_value[::-1]}')
    print(f'my_vault_override_us_with_eu={my_vault_override_us_with_eu_value}')
    print(f'my_vault_override_us_with_eu_reversed={my_vault_override_us_with_eu_value[::-1]}')

    print()
    print(f'my_vault_string_eu={my_vault_string_eu_value}')
    print(f'my_vault_string_list_eu={my_vault_string_list_eu_value}')
    print(f'my_vault_secure_string_eu={my_vault_secure_string_eu_value}')
    print(f'my_vault_secure_string_eu_reversed={my_vault_secure_string_eu_value[::-1]}')
    print(f'my_vault_override_eu_with_us={my_vault_override_eu_with_us_value}')
    print(f'my_vault_override_eu_with_us_reversed={my_vault_override_eu_with_us_value[::-1]}')

    print()
    print(yayparams)
    print(f'yayparams.num_rows={yayparams.num_rows}')
    print(f'yayparams.num_columns={yayparams.num_columns}')

    return pa.Table.from_pydict({'y': [1, 2, 3]})
