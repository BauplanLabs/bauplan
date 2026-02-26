import bauplan

@bauplan.model(
    columns=['col_2'],
    materialization_strategy='NONE',
)
@bauplan.python('3.11', pip={'numpy': '2.4.2'})
def model_2(
    model_0=bauplan.Model('model_0', columns=['col_0']),
    model_1=bauplan.Model('model_1', columns=['col_1'])
):
    import pyarrow as pa
    val_0 = model_0['col_0'].to_numpy()[0]
    val_1 = model_1['col_1'].to_numpy()[0]
    assert val_0 == 'val_0'
    assert val_1 == 'val_1'
    return pa.Table.from_pydict({'col_2': pa.array([val_0, val_1])})

@bauplan.model(
    columns=['col_4'],
    materialization_strategy='NONE',
)
@bauplan.python('3.11', pip={'numpy': '2.4.2'})
def model_4(
    model_0=bauplan.Model('model_0', columns=['col_0']),
    model_1=bauplan.Model('model_1', columns=['col_1']),
    model_2=bauplan.Model('model_2', columns=['col_2']),
    model_3=bauplan.Model('model_3', columns=['col_3'])
):
    import pyarrow as pa
    val_0 = model_0['col_0'].to_numpy()[0]
    val_1 = model_1['col_1'].to_numpy()[0]
    val_2_0 = model_2['col_2'].to_numpy()[0]
    val_2_1 = model_2['col_2'].to_numpy()[1]
    val_3_0 = model_3['col_3'].to_numpy()[0]
    val_3_1 = model_3['col_3'].to_numpy()[1]
    assert val_0 == 'val_0'
    assert val_1 == 'val_1'

    assert val_2_0 == 'val_0'
    assert val_2_1 == 'val_1'

    # the same cause we select col_2 AS col_3 in the SQL model, model_3
    assert val_3_0 == 'val_0'
    assert val_3_1 == 'val_1'

    all_vals = [
        val_0,
        val_1,
        val_2_0,
        val_2_1,
        val_3_0,
        val_3_1,
    ]
    print(','.join(all_vals))

    return pa.Table.from_pydict({'col_4': pa.array(all_vals)})
