import streamlit as st
import numpy as np
import polars as pl
import pandas as pd
import io
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
     sun_day['Days'], key='sun days')

cloudy_day = pl.DataFrame({
    'days': [1 , 2 , 3 , 4 , 5 , 6 , 7]
    })
cloudy_days = st.sidebar.selectbox( 
    'Continuos Cloudy Days',
     cloudy_day['days'], key='cloudy days')

panel_amps = st.sidebar.number_input("Panel Amperage", value = 0)


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


#'''calculations'''
amps_tot_np = col_df.select(pl.col("Amps").sum()).to_numpy()[0]
watt_tot_np = col_df.select(pl.col("Watts").sum() + (pl.col("Amps") * sys_voltage)).to_numpy()[0]
# watt_hours = col_df.select((pl.col("Watts").sum() * 24)).to_numpy()[0]
# watt_week = col_df.select((pl.col("Watts").sum() * 168 )).to_numpy()[0]

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

amp_week = (watt_week / sys_voltage) 
amp_day = (amp_week / 7) * 1.2
amps_day = (amp_week / 7) 
amp_hour = (amp_day / 24)
inverter_min = (watt_hour/ sys_voltage)
solar_amps = (amp_day/ sun_days)
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


print(col_df)


#may need to @st.cache_data instead to prevent computation on every rerun
@st.fragment()
def create_pdf(col_df):
    # f'{df_list[0]}'
    # DF = pd.DataFrame(
    #     {
    #         "First name": ["Jules", "Mary", "Carlson", "Lucas"],
    #         "Last name": ["Smith", "Ramos", "Banks", "Cimon"],
    #         "Age": [34, 45, 19, 31],
    #         "City": ["San Juan", "Orlando", "Los Angeles", "Saint-Mahturin-sur-Loire"],
    #     }
    #     # Convert all data inside dataframe into string type:
    # ).map(str)

    DF = pl.DataFrame(col_df) 
    f'df{DF}'
    COLUMNS = DF.columns  # Get list of dataframe columns
    f'columns{COLUMNS}'
    ROWS = DF.rows()  # Get list of dataframe rows
    f'rows{ROWS}'
    DATA = COLUMNS + ROWS  # Combine columns and rows in one list
    f'data{DATA}'

    pdf = FPDF()
    pdf.add_page()
    pdf.set_font("Times", size=10)
    # Column headers main items
    pdf.cell(60, 10, 'Solar Items', border=1)
    pdf.cell(60, 10, 'Amps', border=1)
    pdf.cell(60, 10, 'Watts', border=1)
    pdf.ln()  # Newline for row separation

    # Data rows
    for row in ROWS:
        for datum in row:
            pdf.cell(60, 10, str(datum), border=1)
        pdf.ln()  # Newline after each row
    # with pdf.table(
    #     borders_layout="MINIMAL",
    #     cell_fill_color=200,  # grey
    #     cell_fill_mode="ROWS",
    #     line_height=pdf.font_size * 2.5,
    #     text_align="CENTER",
    #     width=160,
    # ) as table:
    #     for data_row in DATA:
    #         row = table.rows()
    #         for datum in data_row:
    #             row.cell(datum)
    pdf.output("table_from_polars.pdf")
    
st.title("Create PDF")
if st.button(label="Generate PDF", key="generate"):
    pdf_data = create_pdf(col_df)
    st.text('Success')

       # pdf = FPDF()
        # df_table =(("Solar Items", "Amps", "Watts"),
        #         ("Insert Columns Here", 0.00,0.00)
        # )

        # pdf.set_auto_page_break(auto=True, margin=15)
        # # Add a page to the PDF
        # pdf.add_page()
        # # Set font for the content
        # pdf.set_font("Arial", size=12)
        # with pdf.table() as table:
        #     for data_row in df_table:
        #         row = table.row()
        #         for datum in data_row:
        #             row.cell(datum)
        # pdf_output = io.BytesIO()
        # pdf.output(pdf_output)
        # pdf_output.seek(0)  # Reset the buffer's position to the start
    
        # return pdf_output

# st.title("Create PDF")
# if st.button(label="Generate PDF", key="generate"):
#     pdf_data = create_pdf()

    # Provide a download button for the PDF
    # st.download_button(
    #     label="Download PDF",
    #     key="download",
    #     data=pdf_data,
    #     file_name=f"{site}SolarCalcs.pdf",
    #     mime="application/pdf"
    # )

    
# # bootstrap 4 collapse example
# components.html(
#     """

