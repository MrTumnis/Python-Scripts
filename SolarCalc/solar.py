import streamlit as st
import numpy as np
import polars as pl
import io
import os
import json
import streamlit.components.v1 as components
from fpdf import FPDF

st.set_page_config(
    page_title="Solar Calculator",
    page_icon=":sunny:",
    layout="wide",
    initial_sidebar_state="expanded",
    menu_items={
        'About': "# Solar Calculator by Thomas Hefferman"
    }
)
    
advm2_logo = './advm2logo.png'
st.logo(advm2_logo)
st.title("Solar Calculator")
site = st.text_input(label='Site Name')
st.header(f"{site}")

#Sidebar Items
sys_vol = pl.DataFrame({
    'voltage column': [6 ,12 , 24 ,36 ,48],
    })
sys_voltage = st.sidebar.selectbox( 
    'Choose The System Voltage',
     sys_vol['voltage column'], key='voltage')

batterys = pl.DataFrame({
    'batt_voltage': [6, 12, 24, 48],
    })
batt_voltage = st.sidebar.selectbox( 
    'Choose The Battery Voltage',
     batterys['batt_voltage'], key='batt_voltage')

batt_amps = st.sidebar.number_input("Battery Amperage", value = 0)

discharge = st.sidebar.slider("Maximum Discharge of Batterys", 0, 100, (80), step=5)
batt_dis = (discharge * .01)

sun_day = pl.DataFrame({
    'Days': [3, 4, 5, 6, 7]
    })
sun_days = st.sidebar.selectbox( 
    'Average Sun Hours Per Day',
     sun_day['Days'], key='sun_days')

cloudy_day = pl.DataFrame({
    'days': [1 , 2 , 3 , 4 , 5 , 6 , 7]
    })
cloudy_days = st.sidebar.selectbox( 
    'Continuos Cloudy Days',
     cloudy_day['days'], key='cloudy days')

panel_amps = st.sidebar.number_input("Panel Amperage", value = 0)


#Main Datatable
col_list = ['Solar Items', 'Amps', 'Watts']
df = pl.DataFrame(
    [
        {"Solar Items": "Insert Solar Items Here", "Amps": 0.00, "Watts": 0.00},
        {"Solar Items": "Insert Solar Items Here", "Amps": 0.00, "Watts": 0.00},
        {"Solar Items": "Insert Solar Items Here", "Amps": 0.00, "Watts": 0.00},
    ]
)

def save_user_input():
    condition = (pl.col("Solar Items").str.starts_with("Insert")).not_() & ((pl.col("Amps") != 0.00) | (pl.col("Watts") != 0.00))
    new_data = col_df.with_columns(col_list).filter(condition)
    new_df = st.data_editor(new_data, key='new_df', column_order= col_list)
    if 'new_df' not in st.session_state:
        st.session_state.new_df = new_df
    return new_df
    
# col_df = st.data_editor(df, key='datatable', num_rows='dynamic', column_order= col_list, use_container_width=True)
col_df = st.data_editor(
    df,
    column_config={
        "Solar Items": st.column_config.TextColumn(
            width="medium",
            default="Insert Solar Items Here",
            ),
        "Amps": st.column_config.NumberColumn(
            min_value=0,
            default=0,
        ),
        "Watts": st.column_config.NumberColumn(
            min_value=0,
            default=0,
        ),
    },
    hide_index=True,
    num_rows='dynamic',
    column_order=col_list,
    use_container_width=True,
    disabled=False,
)


if 'datatable' not in st.session_state:
    st.session_state.datatable = pl.DataFrame(col_df)

if st.button("Save Items", key='save_button', help='Save the Solar Items for use at a later time'):
    df = save_user_input()
    if os.path.exists('./datatable.json'):
        if os.path.getsize('./datatable.json') > 0:
            df_json = pl.read_json('./datatable.json')
            new_df = df.join(df_json, on=col_list, how='full', coalesce=True)
            new_df.write_json('./datatable.json')
        else: 
            df.write_json('./datatable.json')
    else:
        df.write_json('./datatable.json')
    st.success("Data saved!")

on = st.toggle("Show Saved Items")


if on:
    if os.path.exists('./datatable.json'):
        df = pl.read_json('./datatable.json')
        # del_items = [row[0] for row in df.iter_rows()]
        del_items = df.rows()

        saved_items = st.data_editor(
            df,
            key='json',
            column_config={
                "Solar Items": st.column_config.TextColumn(width="medium"),
                "Amps": st.column_config.NumberColumn(),
                "Watts": st.column_config.NumberColumn(),
            },
            hide_index=True,
            use_container_width=True,
            disabled=False,
        ),

        del_opt = st.multiselect('Select Items to Delete from Saved File', del_items, placeholder='Items to Delete'),
        if del_opt is not 'Items to Delete':
            for item in del_opt:
                for row in item:
                   col = row[0] 
        if st.button('Delete Items'):#, on_click=)
            if del_opt is not 'Items to Delete':
                df_col = df.filter(~pl.col('Solar Items').str.contains(f'^{col}$', literal=False))
                st.write(df_col)
                df_col.write_json('./datatable.json')
    else:
        st.write('No Saved Items Exist')


#'''calculations'''
amps_tot_np = col_df.select(pl.col("Amps").sum()).to_numpy()[0]
watt_tot_np = col_df.select(pl.col("Watts").sum() + (pl.col("Amps") * sys_voltage)).to_numpy()[0]

watt_hour_np = col_df.select(pl.when(pl.col("Amps").sum() > 0)
                .then((pl.col("Watts").sum() + ((pl.col('Amps').sum()) * sys_voltage)))
                .otherwise(col_df.select(pl.col("Watts").sum()))).to_numpy()
watt_hour = watt_hour_np[0]

amps_tot = (amps_tot_np + (watt_tot_np / sys_voltage))

watt_day_np = col_df.select(pl.when(pl.col("Amps").sum() > 0)
                .then((pl.col("Watts").sum() + ((pl.col('Amps').sum()) * sys_voltage)) * 24)
                .otherwise(col_df.select(pl.col("Watts").sum() * 24))).to_numpy()
watt_day = watt_day_np[0]

watt_week_np = col_df.select(pl.when(pl.col("Amps").sum() > 0)
                .then((pl.col("Watts").sum() + ((pl.col('Amps').sum()) * sys_voltage)) * 168)
                .otherwise(col_df.select(pl.col("Watts").sum() * 168))).to_numpy()
watt_week = watt_week_np[0]

#'''Write a session state change for watts to amps, or amps to watts'''
amp_week = (watt_week / sys_voltage) 
amp_day = (amp_week / 7) * 1.2
amps_day = (amp_week / 7) 
amp_hour = (amp_day / 24)
inverter_min = (watt_hour/ sys_voltage)
solar_amps = (amp_day / sun_days)
solar_panels = (solar_amps / panel_amps)
batt_tot = (((amp_day / batt_dis) * cloudy_days) / batt_amps)
amps_res = ((cloudy_days * amps_day) / batt_dis)

#had to place 'sidebar item' after calculations
df_result = st.sidebar.dataframe({
    "Total Amps in Reserve":f'{amps_res}',
    "Inverter Size":f'{np.round(inverter_min)}Watts',
    "Solar Amps Required": f'{solar_amps} Amps',
    "Total Solar Panels": f'{solar_panels}', 
   f"{batt_voltage}-Volt Batteries Required": f'{np.round(batt_tot)}' 
})

#total energy required
df_res = st.dataframe({
    "Total Amps": f'{amps_tot}',
    "Total Amps Per Day": f'{amps_day}',
    "Total Amps Per Week": f'{amp_week}',
    "- - - - - - - - - - - - - - - - - - -": '< -------- >',
    "Total Watt Hours": f'{watt_hour}',
    "Total Watt Hours Per Day": f'{watt_day}', 
    "Total Watt Hours Per Week": f'{watt_week}' 
})

@st.fragment()
def create_pdf(col_df):
    DF = pl.DataFrame(col_df) 
    DF_RESULT = pl.DataFrame({
    "Total Amps in Reserve":f'{amps_res}',
    "Inverter Size":f'{np.round(inverter_min)}Watts',
    "Solar Amps Required": f'{solar_amps} Amps',
    "Total Solar Panels": f'{solar_panels}', 
   f"{batt_voltage}-Volt Batteries Required": f'{np.round(batt_tot)}' 
}) 
    DF_RES = pl.DataFrame({
    "Total Amps": f'{amps_tot}',
    "Total Amps Per Day": f'{amps_day}',
    "Total Amps Per Week": f'{amp_week}',
    "Total Watt Hours": f'{watt_hour}',
    "Total Watt Hours Per Day": f'{watt_day}', 
    "Total Watt Hours Per Week": f'{watt_week}'
    })

    COLUMNS = DF.columns  # Get list of dataframe columns
    ROWS = DF.rows()  # Get list of dataframe rows
    # DATA = COLUMNS + ROWS  # Combine columns and rows in one list
    # f'data{DATA}'

    pdf = FPDF()
    pdf.add_page()
    pdf.set_top_margin(225.5)
    pdf.set_font("Times", size=30)
    pdf.cell(20, 20, f'{site} Solar Calculations')
    pdf.ln()
    pdf.set_font("Times", size=10)
    # Column headers main items
    pdf.cell(60, 10, 'Solar Items', border=3)
    pdf.cell(60, 10, 'Amps', border=3)
    pdf.cell(60, 10, 'Watts', border=3)
    pdf.ln()  # Newline for row separation

    # Data rows
    for row in ROWS:
        for datum in row:
            pdf.cell(60, 10, str(datum), border=3)
        pdf.ln()  # Newline after each row
    
    pdf.ln()
    pdf.cell(32.5,10, "Total AH", border= 3)
    pdf.cell(32.5,10, "Total AH Per Day", border = 3)
    pdf.cell(32.5,10, "Total AH Per Week", border = 3)
    pdf.cell(32.5,10, "Total WH", border = 3) 
    pdf.cell(32.5,10, "Total WH Per Day", border = 3)
    pdf.cell(32.5,10, "Total WH Per Week", border = 3)
    pdf.ln()
    RES_ROWS = DF_RES.rows()  # Get list of dataframe rows

    for row in RES_ROWS:
        for datum in row:
            pdf.cell(w=32.5, h=10, txt = str(datum), border=3)
        pdf.ln() 
   
    pdf.ln()
    pdf.cell(39,10, "Total Amps in Reserve", border= 3)
    pdf.cell(39,10, "Inverter Size", border = 3)
    pdf.cell(39,10, "Solar Amps Required", border = 3)
    pdf.cell(39,10, "Total Solar Panels", border = 3) 
    pdf.cell(39,10, f"{batt_voltage}-Volt Batteries Required", border = 3)
    pdf.ln()
    RESULT_ROWS = DF_RESULT.rows()  # Get list of dataframe rows

    for row in RESULT_ROWS:
        for datum in row:
            pdf.cell(39, 10, str(datum), border=3)
        pdf.ln()  # Newline after each row

    pdf.output(f"{site}SolarCalcs.pdf")
    
st.title("Create PDF")
if st.button(label="Print", key="generate", help='Generate a PDF of the Solar Calculations'):
    pdf_data = create_pdf(col_df)
    st.text('Success')

