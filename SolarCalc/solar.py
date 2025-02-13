import streamlit as st
import numpy as np
import polars as pl
import pandas as pd
import streamlit.components.v1 as components

# def store_value(key):
#     st.session_state[voltage] = st.session_state["_"+voltage]
# def load_value(key):
#     st.session_state["_"+voltage] = st.session_state[voltage]
# load_value("voltage")
# st.number_input("Number of filters", key="_voltage", on_change=store_value, args=["voltage"])
    
advm2_logo = './advm2logo.png'
st.logo(advm2_logo)
st.title("Solar Calculator")

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
    'Choose The Battery Size',
     batterys['batt_voltage'], key='batt_voltage')

discharge = st.sidebar.slider("Maximum Discharge of Batterys", 0, 100, (80), step=5)
batt_dis = (discharge * .01)

sun_day = pl.DataFrame({
    'Days': [3, 4, 5, 6, 7]
    })
sun_days = st.sidebar.selectbox( 
    'Average Sun Hours Per Day',
     sun_day['Days'], key='sun days')

cloudy_day = pl.DataFrame({
    'days': [1 , 2 , 3 , 4 , 5 , 6 , 7]
    })
cloudy_days = st.sidebar.selectbox( 
    'Average Cloudy Days',
     cloudy_day['days'], key='cloudy days')

panel_amps = st.sidebar.number_input("Panel Amps", value = 0, help= 'Amps of the chosen Solar Panels')
# st.write("Panel Amps", number)


#Main Datatable
df = pl.DataFrame(
    [
        {"Solar Items": "Insert Solar Items Here", "Amps": 0.00, "Watts": 0.00},
        {"Solar Items": "Insert Solar Items Here", "Amps": 0.00, "Watts": 0.00},
        {"Solar Items": "Insert Solar Items Here", "Amps": 0.00, "Watts": 0.00},
    ]
)


col_list = ['Solar Items', 'Amps', 'Watts']
col_df = st.data_editor(df, key='datatable', num_rows='dynamic', column_order= col_list, use_container_width=True)



amps_tot_np = col_df.select(pl.col("Amps").sum()).to_numpy()
amps_tot = amps_tot_np[0]
# watt_tot = col_df.select(pl.col("Watts").sum() + (pl.col("Amps") * voltage)).to_numpy()[0]
# watt_hours = col_df.select((pl.col("Watts").sum() * 24)).to_numpy()[0]
# watt_week = col_df.select((pl.col("Watts").sum() * 168 )).to_numpy()[0]

watt_hour_np = col_df.select(pl.when(pl.col("Amps").sum() > 0)
                .then((pl.col("Watts").sum() + ((pl.col('Amps').sum() * 1.2) * sys_voltage)))
                .otherwise(col_df.select(pl.col("Watts").sum() * 1.2))).to_numpy()
watt_hour = watt_hour_np[0]

watt_day_np = col_df.select(pl.when(pl.col("Amps").sum() > 0)
                .then((pl.col("Watts").sum() + ((pl.col('Amps').sum() * 1.2) * sys_voltage)) * 24)
                .otherwise(col_df.select(pl.col("Watts").sum() * 24) * 1.2)).to_numpy()
watt_day = watt_day_np[0]

watt_week_np = col_df.select(pl.when(pl.col("Amps").sum() > 0)
                .then((pl.col("Watts").sum() + ((pl.col('Amps').sum() * 1.2) * sys_voltage)) * 168)
                .otherwise(col_df.select(pl.col("Watts").sum() * 168) * 1.2)).to_numpy()
watt_week = watt_week_np[0]

amp_week = (watt_week / sys_voltage) 
amp_day = (amp_week / 7)
amp_hour = (amp_day / 24)
inverter_min = (watt_hour/ sys_voltage)
solar_amps = (amp_hour/ sun_days)
solar_panels = (solar_amps / panel_amps)
batt_tot = ((amp_day / batt_dis) * cloudy_days) / batt_amps
# 'Total Amps Hours', amps_tot
# 'Total Amps Per Day', amp_day
# 'Total Amps Per Week', amp_week
# 'Total Watt Hours', watt_hour
# 'Total Watt Hours Per Day', watt_day 
# 'Total Watt Hours Per Week', watt_week

#total energy required
df_res = st.dataframe({
    "Total Amps Hours": f'{amps_tot}',
    "Total Amps Per Day": f'{amp_day}',
    "Total Amps Per Week": f'{amp_week}',
    "- - - - - - - - - - - - - - - - - - -": '< -------- >',
    "Total Watt Hours": f'{watt_hour}',
    "Total Watt Hours Per Day": f'{watt_day}', 
    "Total Watt Hours Per Week": f'{watt_week}' 
})

df_res = st.sidebar.dataframe({
    "Total Amps in Reserve": '{amp_tot}',
    "Inverter Size":f'{np.round(inverter_min)}Watts',
    "Solar Amps Required": f'{np.round(solar_amps)} Amps',
    "Total Solar Panels": f'{solar_panels}', 
   f"Total {batt_voltage}-Volt Batteries Required": f'{batt_tot}' 
})



# # bootstrap 4 collapse example
# components.html(
#     """


#used to 'run' the solar calcs if I choose not to automatically update them
# if st.button:
#     run code...
