# /// script
# requires-python = ">=3.10"
# dependencies = [
#     "fpdf",
#     "numpy",
#     "polars",
#     "streamlit",
# ]
# ///
import streamlit as st
import numpy as np
import polars as pl
import os
import math
import time
import base64
import json
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
     sys_vol['voltage column'], key='voltage', index=1)

batterys = pl.DataFrame({
    'batt_voltage': [6, 12, 24, 48],
    })
batt_voltage = st.sidebar.selectbox( 
    'Choose The Battery Voltage',
     batterys['batt_voltage'], key='batt_voltage')

batt_amps = st.sidebar.number_input("Battery Amperage", value = 0)

discharge = st.sidebar.slider("Maximum Discharge of Batterys", 0, 100, (80), step=5)
batt_discharge = (discharge * .01)

sun_day = pl.DataFrame({
    'Days': [3, 4, 5, 6, 7]
    })
sun_days = st.sidebar.selectbox( 
    'Average Sun Hours Per Day',
     sun_day['Days'], key='sun_days', index=1)

cloudy_day = pl.DataFrame({
    'days': [1 , 2 , 3 , 4 , 5 , 6 , 7]
    })
cloudy_days = st.sidebar.selectbox( 
    'Continuos Cloudy Days',
     cloudy_day['days'], key='cloudy days', index=1)

panel_amps = st.sidebar.number_input("Panel Amperage", value = 0)

#Main Datatable
col_list = ['Solar Items', 'Amps', 'Watts']

if 'datatable' not in st.session_state:
    st.session_state.datatable = pl.DataFrame(
    [
        {"Solar Items": "Insert Solar Items Here", "Amps": 0.00, "Watts": 0.00},
        {"Solar Items": "Insert Solar Items Here", "Amps": 0.00, "Watts": 0.00},
        {"Solar Items": "Insert Solar Items Here", "Amps": 0.00, "Watts": 0.00},
    ]
)


def save_user_input():
    condition = (pl.col("Solar Items").str.starts_with("Insert")).not_() & ((pl.col("Amps") != 0.00) | (pl.col("Watts") != 0.00))
    new_data = col_df.with_columns(col_list).filter(condition)
    new_df = st.data_editor(new_data, key='newdf', column_order= col_list)
    if 'newdf' not in st.session_state:
        st.session_state.new_df = new_df
    return new_df
    

col_df = st.data_editor(
    st.session_state.datatable,
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
    key = 'data_table',
    column_order=col_list,
    use_container_width=True,
    disabled=False,
)


if 'data_table' not in st.session_state:
    st.session_state.data_table = data_table

if 'datatable' in st.session_state:
    df_temp = st.session_state.datatable 
    df_temp2 = st.session_state.data_table
    columns = df_temp2.get('edited_rows', {})
    json_list = []
    for key, val in columns.items():
        new_col = columns.get(key) 
        json_list.append(new_col)
        
    with open("./.temp.json", "w") as outfile:
        json.dump(json_list, outfile)

    json_df = pl.read_json('./.temp.json', schema= {'Solar Items':pl.String, 'Amps':pl.Float64, 'Watts':pl.Float64})

    new_df = pl.concat([json_df,df_temp])
    st.write(new_df)

    # st.session_state.datatable




if st.button("Save Items", key='save_button', help='Save the Solar Items to File'):
    df = save_user_input()
    if os.path.exists('./datatable.json'):
        if os.path.getsize('./datatable.json') > 0:
            df_json = pl.read_json('./datatable.json')
            new_df = df.join(df_json, on=col_list, how='full', coalesce=True)
            if df.is_empty():
                st.error('Input a Name and Value for the Solar Item before saving')
            else:
                new_df.write_json('./datatable.json')
                st.success("Data saved!")
                time.sleep(2)
                st.rerun()
        else: 
            df.write_json('./datatable.json')
            st.success("Data saved!")
            time.sleep(2)
            st.rerun()
    else:
        if df.is_empty():
            st.error('Input a Name and Value for the Solar Item before saving')
        else:
            df.write_json('./datatable.json')
            st.success("Data saved!")
            time.sleep(2)
            st.rerun()



on = st.toggle("View Saved Items", help='Copy/Paste and Delete Items', key='toggle', value=st.session_state.get('toggle', False))

if 'toggle' not in st.session_state:
    st.session_state.toggle = False 

if on:
    if os.path.exists('./datatable.json'):
        df = pl.read_json('./datatable.json')
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

        del_opt = st.selectbox('Delete from Saved File', del_items, placeholder='Item to Delete'),
        if del_opt is not None:
            for item in del_opt:
                for i, row in enumerate(item):
                    if i == 0:
                        col = row 
                    elif i == 1:
                        col2 = row 
                    else:
                        col3 = row 


            but1, but2 = st.columns(2)
            with but1:
                if st.button('Add Items to DataTable'):
                    if del_opt is not None: 
                        condition = ((pl.col('Solar Items').str.contains(f'^{col}$')) & ((pl.col('Amps') == col2) & (pl.col('Watts') == col3)))
                        df_col = df.select(pl.col(['Solar Items', 'Amps', 'Watts']).filter(condition))
                        df1 = st.session_state.datatable
                        df_new = pl.concat([df1,df_col])
                        if not st.session_state.datatable.equals(df_new):
                            st.session_state.datatable = df_new.clone()
                        st.rerun()

            with but2:
                if st.button('Delete Items From Save File'):
                    if del_opt is not None: 
                        condition = ((pl.col('Solar Items').str.contains(f'^{col}$')) & ((pl.col('Amps') == col2) & (pl.col('Watts') == col3)))
                        df_col = df.select(pl.col(['Solar Items', 'Amps', 'Watts']).filter(condition))
                        df_new = df.join(df_col,on=df.columns, how='anti')

                        if df_new.is_empty(): 
                            os.remove('./datatable.json')
                            st.write('Item Deleted')
                            if 'toggle' in st.session_state:
                                del st.session_state.toggle 
                            time.sleep(1)
                            st.rerun()
                        else:
                            df_new.write_json('./datatable.json')
                            st.write('Item Deleted')
                            time.sleep(1)
                            st.rerun()

    else:
        st.write('No Saved Items Exist')



#'''calculations'''
watt_tot_np = col_df.select(pl.col("Watts").sum() + (pl.col("Amps") * sys_voltage)).item(0,0)

watt_hour_df = col_df.select(pl.when(pl.col("Amps").sum() > 0)
                .then((pl.col("Watts").sum() + ((pl.col('Amps') * sys_voltage).sum())))
                .otherwise(col_df.select(pl.col("Watts").sum())).alias('watt_hour'))
watt_hour = watt_hour_df.select(pl.col('watt_hour')).item(0,0)

watt_day_df = col_df.select(pl.when(pl.col("Amps").sum() > 0)
                .then((pl.col("Watts").sum() + ((pl.col('Amps') * sys_voltage).sum())) * 24)
                .otherwise(col_df.select(pl.col("Watts").sum() * 24)).alias('watt_day'))
watt_day = watt_day_df.select(pl.col('watt_day')).item(0,0)

watt_week_df = col_df.select(pl.when(pl.col("Amps").sum() > 0)
                .then((pl.col("Watts").sum() + ((pl.col('Amps') * sys_voltage).sum())) * 168)
                .otherwise(col_df.select(pl.col("Watts").sum() * 168)).alias('watt_week'))
watt_week = watt_week_df.select(pl.col('watt_week')).item(0,0)

#'''Write a session state change for watts to amps, or amps to watts'''
amp_tot_df= col_df.with_columns(pl.when(pl.col("Watts").sum() > 0)
                .then((pl.col("Amps").sum() + (pl.col('Watts').sum()) / sys_voltage))
                .otherwise(col_df.select(pl.col("Amps").sum())).alias('amp_tot'))
amps_tot = amp_tot_df.select(pl.col('amp_tot').round(2)).item(0,0)

amp_week_df = watt_week_df.with_columns(pl.when(pl.col("watt_week").sum() > 0)
                .then(watt_week_df.select(pl.col("watt_week") / sys_voltage) * 1.2).alias('amps_week'))

amp_per_day = watt_week_df.with_columns(pl.when(pl.col("watt_week").sum() > 0)
                .then(watt_week_df.select(pl.col("watt_week") / sys_voltage) / 7).alias('amps_per_day'))

amps_per_day = amp_per_day.select(pl.when(pl.col('amps_per_day') > 0).then(pl.col('amps_per_day').round(2)).otherwise(0)).item(0,0)

amps_per_week = np.round((amps_per_day * 7), 2)

amps_week = amp_week_df.select(pl.col('amps_week').round(2)).item(0,0)

amps_day = amp_week_df.select(pl.when(pl.col('amps_week') > 0).then((pl.col('amps_week') / 7).round(2)).otherwise(0)).item(0,0)

amp_hours = amp_week_df.select(((pl.col('amps_week') / 7) / 24).round(2)).item(0,0)

inverter_min = watt_hour_df.select((pl.col('watt_hour') * sys_voltage).round(2)).item(0,0) 

solar_amps = round((amps_day / sun_days), 1)

amps_res = ((cloudy_days * amps_per_day) / batt_discharge)

if panel_amps > 0:
    solar_panels = math.ceil(solar_amps / panel_amps)
else: 
    solar_panels = 0

if batt_amps > 0:
    if sys_voltage < batt_voltage:
        batt_tot = 0
        st.error('Cannot have a battery voltage higher than the system voltage')

    elif sys_voltage == batt_voltage:
        batt_tot = math.ceil(amps_res / batt_amps) 

    elif sys_voltage  == batt_voltage * 2:
        batt_tot = math.ceil(amps_res / batt_amps) * 2

    elif sys_voltage == batt_voltage * 4:
        batt_tot = math.ceil(amps_res / batt_amps) * 4

    elif sys_voltage == batt_voltage * 6:
        batt = math.ceil(amps_res / batt_amps) 
        if (batt * batt_voltage) < sys_voltage:  
            batt_tot = 6
        elif (batt * batt_voltage) > sys_voltage:
            multiplier = math.ceil((batt * batt_voltage) / sys_voltage)
            batt_tot = (batt_voltage * multiplier)
        else:
            batt_tot = st.error('check battery multiplier')

    elif sys_voltage == batt_voltage * 8:
        batt_tot = math.ceil(amps_res / batt_amps) * 8
    else:
        batt_tot = 0
        st.error('Cannot create a proper voltage with this combination ')

else: 
    batt_tot = 0


#had to place 'sidebar item' after calculations
df_result = st.sidebar.dataframe({
    "Total Amps in Reserve":f'{amps_res}',
    "Inverter Size":f'{inverter_min} Watts',
    "Solar Amps Required": f'{solar_amps} Amps',
    "Total Solar Panels": f'{solar_panels}', 
   f"{batt_voltage}-Volt Batteries Required": f'{batt_tot}' 
})

#total energy required
df_res = st.dataframe({
    "Total Amps Per Day": f'{amps_per_day}',
    "Total Amps/Day (x1.2)": f'{amps_day}',
    "Total Amps Per Week": f'{amps_per_week}',
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
    "Inverter Size":f'{inverter_min} Watts',
    "Solar Amps Required": f'{solar_amps} Amps',
    "Total Solar Panels": f'{solar_panels}', 
   f"{batt_voltage}-Volt Batteries Required": f'{batt_tot}' 
}) 
    DF_RES = pl.DataFrame({
    "Total Amps": f'{amps_tot}',
    "Total Amps Per Day": f'{amps_day}',
    "Total Amps Per Week": f'{amps_week}',
    "Total Watt Hours": f'{watt_hour}',
    "Total Watt Hours Per Day": f'{watt_day}', 
    "Total Watt Hours Per Week": f'{watt_week}'
    })

    ROWS = DF.rows()  

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
    pdf.ln()   
    # Data rows
    for row in ROWS:
        for datum in row:
            pdf.cell(60, 10, str(datum), border=3)
        pdf.ln()  
    
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
    RESULT_ROWS = DF_RESULT.rows()  

    for row in RESULT_ROWS:
        for datum in row:
            pdf.cell(39, 10, str(datum), border=3)
        pdf.ln()   

    pdf.output(f"{site}SolarCalcs.pdf")
    
st.title("Create PDF")
if st.button(label="Print", key="generate", help='Generate a PDF of the Solar Calculations'):
    pdf_data = create_pdf(col_df)
    st.text('Success')
    time.sleep(1)

