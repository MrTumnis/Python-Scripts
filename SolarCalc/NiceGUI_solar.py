from nicegui import ui, events

# ui.html('<strong>SOLAR CALCULATOR</strong>')
# with ui.row():
#     ui.label('CSS').style('color: #888; font-weight: bold')
#     ui.label('Tailwind').classes('font-serif')


col = []
with ui.table(columns=[{"name": "col1", "label": "col1", "field": "col1"}], rows=[1,2,3,4,5]) as table:
    for val in col:
        with table.add_slot("body"):
            ui.input(label='Solar Items')

ui.button('Select all', on_click=lambda: grid.run_grid_method('selectAll'))


grid = ui.aggrid({
    'columnDefs': [
        {'headerName': 'Solar Items', 'field': 'Solar', 'checkboxSelection':True},
        {'headerName': 'Amps', 'field': 'Amps'},
        {'headerName': 'Watts', 'field': 'Watts'},
    ],
    'rowData': [
        {'Solar': '', 'Amps': '' , 'Watts': ''},
        {'Solar': '', 'Amps': '' , 'Watts': ''},
        {'Solar': '', 'Amps': '' , 'Watts': ''},
    ],
    'rowSelection': 'multiple',
}).classes('max-h-40')


async def output_selected_rows():
    rows = await grid.get_selected_rows()
    if rows:
        for row in rows:
            ui.notify(f"{row['Solar']}, {row['Amps']}, {row['Watts']}")
    else:
        ui.notify('No rows selected.')

# def update():
#     grid.options['rowData'][0]['age'] += 1
#     grid.auto_size_columns = True
#     grid.update()
# ui.button('Output selected rows', on_click=output_selected_rows)
# # ui.button('Select all', on_click=lambda: grid.run_grid_method('selectAll'))
# create_table()


ui.run()
