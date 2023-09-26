from msilib.schema import Icon
import streamlit as st
from re import S
import psycopg2
import pandas as pd
import numpy as np
import matplotlib as mlt
import plotly.express as px

st.set_page_config(page_title="Phonepe Data plus Harvesting and Analysis", layout='wide', page_icon="📊")
with st.container():
    col1, col2 = st.columns([5, 1])
    with col1:
        html = "<p style='color:#032456; font-size:250%; text-align:left'><b>PhonePe Pulse - The Beat of Progress</b></p>"
        st.markdown(html, unsafe_allow_html=True)
    with col2:
        type = st.selectbox("Select your type", ["Transactions", "Users", "Data Insights"])
html = "<p style='color:#021B42; text-align:justify;font-family:courier'>Pulse is India's pioneering interactive geospatial platform, providing comprehensive insights, engaging discussions, and intriguing data on the evolving payment landscape in the country.</p>"
st.markdown(html, unsafe_allow_html=True)
db = psycopg2.connect(host='localhost', user='postgres', password='SivA@879789', port=5432, database='Phonepe')
exe = db.cursor()
exe.execute("select distinct(state) from Aggregated_transaction")
f = exe.fetchall()
State_data = ["All States",]
for i in f:
    State_data.append(i[0])

if type == "Transactions":
    col1,col2,col3 = st.columns(3)
    with col1:
        state = st.selectbox("State", State_data)
    
    with col2:
        year = st.selectbox("Year", [2018, 2019, 2020, 2021, 2022, 2023])
    
    with col3:
        if year == 2023:
            quarter = st.selectbox("Quarter", [1, 2])
        else:
            quarter = st.selectbox("Quarter", [1, 2, 3, 4])
    
    with st.container():
        c1, c2, c3 = st.columns([1, 1, 1])
        
        with c2:
            opt = st.selectbox("Analysis of Top 10", ["States", "Districts", "Pincodes"])
        
    with st.container():
        col11, col12, col13 = st.columns([3, 6, 3])
        
        with col11:
            with st.container():
                html = "<p style='color:#002147; font-size:100%; text-align:left'><b>All PhonePe Transactions</b></p>"
                head = st.markdown(html, unsafe_allow_html=True)
                
                if state != "All States":
                    exe.execute("SELECT sum(no_of_transaction) FROM Aggregated_transaction WHERE State = %s AND Year = %s AND Quarter = %s", (state, year, quarter))
                    h = exe.fetchone()
                else:
                    exe.execute("SELECT sum(no_of_transaction) FROM Aggregated_transaction WHERE Year = %s AND Quarter = %s", (year, quarter))
                    h = exe.fetchone()
                st.markdown(h[0])
            
            with st.container():
                html = "<p style='color:#002147; font-size:100%; text-align:left'><b>Total Payment Value</b></p>"
                head = st.markdown(html, unsafe_allow_html=True)
                
                if state != "All States":
                    exe.execute("SELECT sum(amount) FROM Aggregated_transaction WHERE State = %s AND Year = %s AND Quarter = %s", (state, year, quarter))
                    h = exe.fetchone()
                else:
                    exe.execute("SELECT sum(amount) FROM Aggregated_transaction WHERE Year = %s AND Quarter = %s", (year, quarter))
                    h = exe.fetchone()
                currency = "₹ {:,.2f}".format(h[0])
                st.markdown(currency)
                
            with st.container():
                html = "<p style='color:#002147; font-size:100%; text-align:left'><b>Average Transaction value</b></p>"
                head = st.markdown(html, unsafe_allow_html=True)
                
                if state != "All States":
                    exe.execute("SELECT sum(amount)/sum(no_of_transaction) FROM Aggregated_transaction WHERE State = %s AND Year = %s AND Quarter = %s", (state, year, quarter))
                    h = exe.fetchone()
                else:
                    exe.execute("SELECT sum(amount)/sum(no_of_transaction) FROM Aggregated_transaction WHERE Year = %s AND Quarter = %s", (year, quarter))
                    h = exe.fetchone()
                currency = "₹ {:,.2f}".format(h[0])
                st.markdown(currency)
        with col12:
            with st.container():
                if opt == "States":
                    output = []
                    exe.execute("SELECT state, sum(no_of_transaction), sum(amount) FROM Aggregated_transaction WHERE  Year = %s AND Quarter = %s group by state order by sum(amount) desc limit 10", ( year, quarter))
                    h = exe.fetchall()
                    for i in h:
                        output.append(i)
                    data = pd.DataFrame(output,columns=["State", "No of Txn", "Txn Value"])
                    st.dataframe(data, height = 240, width = 750)
                if opt == "Districts":
                    output = []
                    if state != "All States":
                        exe.execute("SELECT district, sum(count), sum(amount) FROM map_transaction WHERE State = %s AND Year = %s AND Quarter = %s group by district order by sum(amount) desc limit 10", (state, year, quarter))
                        h = exe.fetchall()
                    else:
                        exe.execute("SELECT district, sum(count), sum(amount) FROM map_transaction WHERE Year = %s AND Quarter = %s group by district order by sum(amount) desc limit 10", (year, quarter))
                        h = exe.fetchall()
                    for i in h:
                        output.append(i)
                    data = pd.DataFrame(output,columns=["District", "No of Txn", "Txn Value"])
                    st.dataframe(data, height = 250, width = 750)
                if opt == "Pincodes":
                    output = []
                    if state != "All States":
                        exe.execute("SELECT pincode, sum(transaction_count), sum(transaction_amount) FROM top_transaction WHERE state = %s AND Year = %s AND Quarter = %s group by pincode order by sum(transaction_amount) desc limit 10", ( state,year, quarter))
                        h = exe.fetchall()
                    else:
                        exe.execute("SELECT pincode, sum(transaction_count), sum(transaction_amount) FROM top_transaction WHERE  Year = %s AND Quarter = %s group by pincode order by sum(transaction_amount) desc limit 10", (year, quarter))
                        h = exe.fetchall()
                    for i in h:
                        output.append(i)
                    data = pd.DataFrame(output,columns=["Pincode", "No of Txn", "Txn Value"])
                    print(data)
                    st.dataframe(data, height = 250, width = 750)
        with col13:
            with st.container():
                col1,col2 = st.columns([3,2])
                with col1:
                    html =html_code = "<p style='color:#002147; font-size:100%; text-align:left'><b>Merchant Payments</b></p>"
                    head = st.markdown(html,unsafe_allow_html=True)
                with col2:
                    if state != "All States":
                        exe.execute("SELECT sum(no_of_transaction) FROM Aggregated_transaction WHERE State = %s AND Year = %s AND Quarter = %s AND transaction_type = 'Merchant payments' ", (state, year, quarter))
                        h = exe.fetchone()
                    else:
                        exe.execute("SELECT sum(no_of_transaction) FROM Aggregated_transaction WHERE Year = %s AND Quarter = %s AND transaction_type = 'Merchant payments' ", (year, quarter))
                        h = exe.fetchone()
                    st.markdown(h[0])
            with st.container():
                col1,col2 = st.columns([3,2])
                with col1:
                    html =html_code = "<p style='color:#002147; font-size:100%; text-align:left'><b>Peer to Peer Payments</b></p>"
                    head = st.markdown(html,unsafe_allow_html=True)
                with col2:
                    if state != "All States":
                        exe.execute("SELECT sum(no_of_transaction) FROM Aggregated_transaction WHERE State = %s AND Year = %s AND Quarter = %s AND transaction_type = 'Peer-to-peer payments' ", (state, year, quarter))
                        h = exe.fetchone()
                    else:
                        exe.execute("SELECT sum(no_of_transaction) FROM Aggregated_transaction WHERE Year = %s AND Quarter = %s AND transaction_type = 'Peer-to-peer payments' ", (year, quarter))
                        h = exe.fetchone()
                    st.markdown(h[0])
            with st.container():
                col1,col2 = st.columns([3,2])
                with col1:
                    html =html_code = "<p style='color:#002147; font-size:100%; text-align:left'><b>Recharge Bills</b></p>"
                    head = st.markdown(html,unsafe_allow_html=True)
                with col2:
                    if state != "All States":
                        exe.execute("SELECT sum(no_of_transaction) FROM Aggregated_transaction WHERE State = %s AND Year = %s AND Quarter = %s AND transaction_type = 'Recharge & bill payments' ", (state, year, quarter))
                        h = exe.fetchone()
                    else:
                        exe.execute("SELECT sum(no_of_transaction) FROM Aggregated_transaction WHERE Year = %s AND Quarter = %s AND transaction_type = 'Recharge & bill payments' ", (year, quarter))
                        h = exe.fetchone()
                    st.markdown(h[0])
            with st.container():
                col1,col2 = st.columns([3,2])
                with col1:
                    html =html_code = "<p style='color:#002147; font-size:100%; text-align:left'><b>Financial Services</b></p>"
                    head = st.markdown(html,unsafe_allow_html=True)
                with col2:
                    if state != "All States":
                        exe.execute("SELECT sum(no_of_transaction) FROM Aggregated_transaction WHERE State = %s AND Year = %s AND Quarter = %s AND transaction_type = 'Financial Services' ", (state, year, quarter))
                        h = exe.fetchone()
                    else:
                        exe.execute("SELECT sum(no_of_transaction) FROM Aggregated_transaction WHERE  Year = %s AND Quarter = %s AND transaction_type = 'Financial Services' ", ( year, quarter))
                        h = exe.fetchone()
                    st.markdown(h[0])
            with st.container():
                col1,col2 = st.columns([3,2])
                with col1:
                    html =html_code = "<p style='color:#002147; font-size:100%; text-align:left'><b>Others</b></p>"
                    head = st.markdown(html,unsafe_allow_html=True)
                with col2:
                    if state != "All States":
                        exe.execute("SELECT sum(no_of_transaction) FROM Aggregated_transaction WHERE State = %s AND Year = %s AND Quarter = %s AND transaction_type = 'Others' ", (state, year, quarter))
                        h = exe.fetchone()
                    else:
                        exe.execute("SELECT sum(no_of_transaction) FROM Aggregated_transaction WHERE Year = %s AND Quarter = %s AND transaction_type = 'Others' ", ( year, quarter))
                        h = exe.fetchone()
                    st.markdown(h[0])

    with st.container():
        
        map = []
        exe.execute("SELECT state , sum(count), sum(amount), sum(amount)/sum(count) as Average FROM map_transaction WHERE Year = %s AND Quarter = %s group by state",(year, quarter))
        h = exe.fetchall()
        for i in h:
            map.append(i)
        map_df = pd.DataFrame (map,columns = ["State","No of txn","Txn Value","Average"])
        map_df.index = map_df.index + 1

        fig = px.choropleth(map_df,
                            geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
                            featureidkey="properties.ST_NM",
                            locations="State",
                            color="Txn Value",
                            hover_data=["State","No of txn","Txn Value","Average"],
                            projection="equal earth",
                            color_continuous_scale="Aggrnyl"
                            )
        fig.update_geos(fitbounds="locations", visible=False)
        fig.update_layout(width=500, height=500, title ="Dynamic Data Visualisation of Phonepe Pulse")
        st.plotly_chart(fig, theme=None, use_container_width=True )


if type =="Users":
    col1, col2, col3 = st.columns([1,1,1])
    db = psycopg2.connect(host='localhost', user='postgres', password='SivA@879789', port=5432, database='Phonepe')
    exe = db.cursor()
    with col1:
        state = st.selectbox("State", State_data)
    with col2:
        year  = st.selectbox("Year", [2018,2019,2020,2021,2022,2023])
    with col3:
        if year == 2023:
            quarter = st.selectbox("Quarter", [1,2])
        else:
            quarter = st.selectbox("Quarter", [1,2,3,4])
    with st.container():
        c1,c2,c3 = st.columns([1,1,1])
        with c1:
            with st.container():
                a,b = st.columns(2)
                with a:
                    html = "<p style='color:#002147; font-size:100%; text-align:left'><b>Registered Phonepe Users</b></p>"
                    head = st.markdown(html,unsafe_allow_html=True)
                with b:
                    if state != "All States":
                        exe.execute("select sum(count) from aggregated_users WHERE State = %s AND Year = %s AND Quarter = %s", (state, year, quarter))
                        h = exe.fetchone()
                    else:
                        exe.execute("select sum(count) from aggregated_users WHERE Year = %s AND Quarter = %s", (year, quarter))
                        h = exe.fetchone()
                    st.markdown(h[0])
        with c3:
            with st.container():
                a,b = st.columns(2)
                with a:
                    html = "<p style='color:#002147; font-size:100%; text-align:left'><b>Phonepe App Opens</b></p>"
                    head = st.markdown(html,unsafe_allow_html=True)
                with b:
                    if state != 'All States':
                        exe.execute("select sum(appopens) from map_users WHERE State = %s AND Year = %s AND Quarter = %s", (state, year, quarter))
                        h = exe.fetchone()
                    else:
                        exe.execute("select sum(appopens) from map_users WHERE Year = %s AND Quarter = %s", (year, quarter))
                        h = exe.fetchone()
                    st.markdown(h[0])
    with st.container():
        c1,c2,c3,c4= st.columns([1,1,1,1])
        with c1:
            with st.container():
                a,b = st.columns(2)
                with a:
                    html =html_code = "<p style='color:#002147; font-size:100%; text-align:left'><b>Tecno</b></p>"
                    head = st.markdown(html,unsafe_allow_html=True)
                with b:
                    if state != "All States":
                        exe.execute("select sum(count) from aggregated_users WHERE State = %s AND Year = %s AND Quarter = %s And brand = 'Tecno'", (state, year, quarter))
                        h = exe.fetchone()
                    else:
                        exe.execute("select sum(count) from aggregated_users WHERE Year = %s AND Quarter = %s And brand = 'Tecno'", (year, quarter))
                        h = exe.fetchone()
                    st.markdown(h[0])
            with st.container():
                a,b = st.columns (2)
                with a:
                    html = "<p style='color:#002147; font-size:100%; text-align:left'><b>Realme</b></p>"
                    head = st.markdown(html,unsafe_allow_html=True)
                with b:
                    if state != "All States":
                        exe.execute("select sum(count) from aggregated_users WHERE State = %s AND Year = %s AND Quarter = %s And brand = 'Realme'", (state, year, quarter))
                        h = exe.fetchone()
                    else:
                        exe.execute("select sum(count) from aggregated_users WHERE Year = %s AND Quarter = %s And brand = 'Realme'", (year, quarter))
                        h = exe.fetchone()
                    st.markdown(h[0])
            with st.container():
                a,b = st.columns (2)
                with a:
                    html =html_code = "<p style='color:#002147; font-size:100%; text-align:left'><b>Vivo</b></p>"
                    head = st.markdown(html,unsafe_allow_html=True)
                with b:
                    if state != "All States":
                        exe.execute("select sum(count) from aggregated_users WHERE State = %s AND Year = %s AND Quarter = %s And brand = 'Vivo'", (state, year, quarter))
                        h = exe.fetchone()
                    else:
                        exe.execute("select sum(count) from aggregated_users WHERE Year = %s AND Quarter = %s And brand = 'Vivo'", (year, quarter))
                        h = exe.fetchone()
                    st.markdown(h[0])
            with st.container():
                a,b = st.columns (2)
                with a:
                    html =html_code = "<p style='color:#002147; font-size:100%; text-align:left'><b>Asus</b></p>"
                    head = st.markdown(html,unsafe_allow_html=True)
                with b:
                    if state != "All States":
                        exe.execute("select sum(count) from aggregated_users WHERE State = %s AND Year = %s AND Quarter = %s And brand = 'Asus'", (state, year, quarter))
                        h = exe.fetchone()
                    else:
                        exe.execute("select sum(count) from aggregated_users WHERE Year = %s AND Quarter = %s And brand = 'Asus'", (year, quarter))
                        h = exe.fetchone()
                    st.markdown(h[0])
            with st.container():
                a,b = st.columns (2)
                with a:
                    html =html_code = "<p style='color:#002147; font-size:100%; text-align:left'><b>OnePlus</b></p>"
                    head = st.markdown(html,unsafe_allow_html=True)
                with b:
                    if state != "All States":
                        exe.execute("select sum(count) from aggregated_users WHERE State = %s AND Year = %s AND Quarter = %s And brand = 'OnePlus'", (state, year, quarter))
                        h = exe.fetchone()
                    else:
                        exe.execute("select sum(count) from aggregated_users WHERE Year = %s AND Quarter = %s And brand = 'OnePlus'", (year, quarter))
                        h = exe.fetchone()
                    st.markdown(h[0])
        with c2:
            with st.container():
                a,b = st.columns (2)
                with a:
                    html =html_code = "<p style='color:#002147; font-size:100%; text-align:left'><b>Huawei</b></p>"
                    head = st.markdown(html,unsafe_allow_html=True)
                with b:
                    if state != "All States":
                        exe.execute("select sum(count) from aggregated_users WHERE State = %s AND Year = %s AND Quarter = %s And brand = 'Huawei'", (state, year, quarter))
                        h = exe.fetchone()
                    else:
                        exe.execute("select sum(count) from aggregated_users WHERE Year = %s AND Quarter = %s And brand = 'Huawei'", (year, quarter))
                        h = exe.fetchone()
                    st.markdown(h[0])
            with st.container():
                a,b = st.columns (2)
                with a:
                    html =html_code = "<p style='color:#002147; font-size:100%; text-align:left'><b>Motorola</b></p>"
                    head = st.markdown(html,unsafe_allow_html=True)
                with b:
                    if state != "All States":
                        exe.execute("select sum(count) from aggregated_users WHERE State = %s AND Year = %s AND Quarter = %s And brand = 'Motorola'", (state, year, quarter))
                        h = exe.fetchone()
                    else:
                        exe.execute("select sum(count) from aggregated_users WHERE Year = %s AND Quarter = %s And brand = 'Motorola'", (year, quarter))
                        h = exe.fetchone()
                    st.markdown(h[0])
            with st.container():
                a,b = st.columns (2)
                with a:
                    html =html_code = "<p style='color:#002147; font-size:100%; text-align:left'><b>HMD Global</b></p>"
                    head = st.markdown(html,unsafe_allow_html=True)
                with b:
                    if state != "All States":
                        exe.execute("select sum(count) from aggregated_users WHERE State = %s AND Year = %s AND Quarter = %s And brand = 'HMD Global'", (state, year, quarter))
                        h = exe.fetchone()
                    else:
                        exe.execute("select sum(count) from aggregated_users WHERE Year = %s AND Quarter = %s And brand = 'HMD Global'", (year, quarter))
                        h = exe.fetchone()
                    st.markdown(h[0])
            with st.container():
                a,b = st.columns (2)
                with a:
                    html =html_code = "<p style='color:#002147; font-size:100%; text-align:left'><b>Samsung</b></p>"
                    head = st.markdown(html,unsafe_allow_html=True)
                with b:
                    if state != "All States":
                        exe.execute("select sum(count) from aggregated_users WHERE State = %s AND Year = %s AND Quarter = %s And brand = 'Samsung'", (state, year, quarter))
                        h = exe.fetchone()
                    else:
                        exe.execute("select sum(count) from aggregated_users WHERE Year = %s AND Quarter = %s And brand = 'Samsung'", (year, quarter))
                        h = exe.fetchone()
                    st.markdown(h[0])
            with st.container():
                a,b = st.columns (2)
                with a:
                    html =html_code = "<p style='color:#002147; font-size:100%; text-align:left'><b>Lenovo</b></p>"
                    head = st.markdown(html,unsafe_allow_html=True)
                with b:
                    if state != "All States":
                        exe.execute("select sum(count) from aggregated_users WHERE State = %s AND Year = %s AND Quarter = %s And brand = 'Lenovo'", (state, year, quarter))
                        h = exe.fetchone()
                    else:
                        exe.execute("select sum(count) from aggregated_users WHERE Year = %s AND Quarter = %s And brand = 'Lenovo'", (year, quarter))
                        h = exe.fetchone()
                    st.markdown(h[0])
        with c3:
            with st.container():
                a,b = st.columns (2)
                with a:
                    html =html_code = "<p style='color:#002147; font-size:100%; text-align:left'><b>Xiaomi</b></p>"
                    head = st.markdown(html,unsafe_allow_html=True)
                with b:
                    if state != "All States":
                        exe.execute("select sum(count) from aggregated_users WHERE State = %s AND Year = %s AND Quarter = %s And brand = 'Xiaomi'", (state, year, quarter))
                        h = exe.fetchone()
                    else:
                        exe.execute("select sum(count) from aggregated_users WHERE Year = %s AND Quarter = %s And brand = 'Xiaomi'", (year, quarter))
                        h = exe.fetchone()
                    st.markdown(h[0])
            with st.container():
                a,b = st.columns (2)
                with a:
                    html =html_code = "<p style='color:#002147; font-size:100%; text-align:left'><b>COOLPAD</b></p>"
                    head = st.markdown(html,unsafe_allow_html=True)
                with b:
                    if state != "All States":
                        exe.execute("select sum(count) from aggregated_users WHERE State = %s AND Year = %s AND Quarter = %s And brand = 'COOLPAD'", (state, year, quarter))
                        h = exe.fetchone()
                    else:
                        exe.execute("select sum(count) from aggregated_users WHERE Year = %s AND Quarter = %s And brand = 'COOLPAD'", (year, quarter))
                        h = exe.fetchone()
                    st.markdown(h[0])
            with st.container():
                a,b = st.columns (2)
                with a:
                    html =html_code = "<p style='color:#002147; font-size:100%; text-align:left'><b>Oppo</b></p>"
                    head = st.markdown(html,unsafe_allow_html=True)
                with b:
                    if state != "All States":
                        exe.execute("select sum(count) from aggregated_users WHERE State = %s AND Year = %s AND Quarter = %s And brand = 'Oppo'", (state, year, quarter))
                        h = exe.fetchone()
                    else:
                        exe.execute("select sum(count) from aggregated_users WHERE Year = %s AND Quarter = %s And brand = 'Oppo'", (year, quarter))
                        h = exe.fetchone()
                    st.markdown(h[0])
            with st.container():
                a,b = st.columns (2)
                with a:
                    html =html_code = "<p style='color:#002147; font-size:100%; text-align:left'><b>Infinix</b></p>"
                    head = st.markdown(html,unsafe_allow_html=True)
                with b:
                    if state != "All States":
                        exe.execute("select sum(count) from aggregated_users WHERE State = %s AND Year = %s AND Quarter = %s And brand = 'Infinix'", (state, year, quarter))
                        h = exe.fetchone()
                    else:
                        exe.execute("select sum(count) from aggregated_users WHERE Year = %s AND Quarter = %s And brand = 'Infinix'", (year, quarter))
                        h = exe.fetchone()
                    st.markdown(h[0])
            with st.container():
                a,b = st.columns (2)
                with a:
                    html =html_code = "<p style='color:#002147; font-size:100%; text-align:left'><b>Apple</b></p>"
                    head = st.markdown(html,unsafe_allow_html=True)
                with b:
                    if state != "All States":
                        exe.execute("select sum(count) from aggregated_users WHERE State = %s AND Year = %s AND Quarter = %s And brand = 'Apple'", (state, year, quarter))
                        h = exe.fetchone()
                    else:
                        exe.execute("select sum(count) from aggregated_users WHERE Year = %s AND Quarter = %s And brand = 'Apple'", (year, quarter))
                        h = exe.fetchone()
                    st.markdown(h[0])
        with c4:
            with st.container():
                a,b = st.columns (2)
                with a:
                    html =html_code = "<p style='color:#002147; font-size:100%; text-align:left'><b>Lyf</b></p>"
                    head = st.markdown(html,unsafe_allow_html=True)
                with b:
                    if state != "All States":
                        exe.execute("select sum(count) from aggregated_users WHERE State = %s AND Year = %s AND Quarter = %s And brand = 'Lyf'", (state, year, quarter))
                        h = exe.fetchone()
                    else:
                        exe.execute("select sum(count) from aggregated_users WHERE Year = %s AND Quarter = %s And brand = 'Lyf'", (year, quarter))
                        h = exe.fetchone()
                    st.markdown(h[0])
            with st.container():
                a,b = st.columns (2)
                with a:
                    html =html_code = "<p style='color:#002147; font-size:100%; text-align:left'><b>Lava</b></p>"
                    head = st.markdown(html,unsafe_allow_html=True)
                with b:
                    if state != "All States":
                        exe.execute("select sum(count) from aggregated_users WHERE State = %s AND Year = %s AND Quarter = %s And brand = 'Lava'", (state, year, quarter))
                        h = exe.fetchone()
                    else:
                        exe.execute("select sum(count) from aggregated_users WHERE Year = %s AND Quarter = %s And brand = 'Lava'", (year, quarter))
                        h = exe.fetchone()
                    st.markdown(h[0])
            with st.container():
                a,b = st.columns (2)
                with a:
                    html =html_code = "<p style='color:#002147; font-size:100%; text-align:left'><b>Micromax</b></p>"
                    head = st.markdown(html,unsafe_allow_html=True)
                with b:
                    if state != "All States":
                        exe.execute("select sum(count) from aggregated_users WHERE State = %s AND Year = %s AND Quarter = %s And brand = 'Micromax'", (state, year, quarter))
                        h = exe.fetchone()
                    else:
                        exe.execute("select sum(count) from aggregated_users WHERE Year = %s AND Quarter = %s And brand = 'Micromax'", (year, quarter))
                        h = exe.fetchone()
                    st.markdown(h[0])
            with st.container():
                a,b = st.columns (2)
                with a:
                    html =html_code = "<p style='color:#002147; font-size:100%; text-align:left'><b>Gionee</b></p>"
                    head = st.markdown(html,unsafe_allow_html=True)
                with b:
                    if state != "All States":
                        exe.execute("select sum(count) from aggregated_users WHERE State = %s AND Year = %s AND Quarter = %s And brand = 'Gionee'", (state, year, quarter))
                        h = exe.fetchone()
                    else:
                        exe.execute("select sum(count) from aggregated_users WHERE Year = %s AND Quarter = %s And brand = 'Gionee'", (year, quarter))
                        h = exe.fetchone()
                    st.markdown(h[0])
            with st.container():
                a,b = st.columns (2)
                with a:
                    html =html_code = "<p style='color:#002147; font-size:100%; text-align:left'><b>Others</b></p>"
                    head = st.markdown(html,unsafe_allow_html=True)
                with b:
                    if state != "All States":
                        exe.execute("select sum(count) from aggregated_users WHERE State = %s AND Year = %s AND Quarter = %s And brand = 'Others'", (state, year, quarter))
                        h = exe.fetchone()
                    else:
                        exe.execute("select sum(count) from aggregated_users WHERE Year = %s AND Quarter = %s And brand = 'Others'", (year, quarter))
                        h = exe.fetchone()
                    st.markdown(h[0])
            with st.container():
                a,b = st.columns (2)
                with a:
                    html =html_code = "<p style='color:#002147; font-size:100%; text-align:left'><b>Undefined</b></p>"
                    head = st.markdown(html,unsafe_allow_html=True)
                with b:
                    if state != "All States":
                        exe.execute("select sum(count) from aggregated_users WHERE State = %s AND Year = %s AND Quarter = %s And brand = 'Undefined'", (state, year, quarter))
                        h = exe.fetchone()
                    else:
                        exe.execute("select sum(count) from aggregated_users WHERE Year = %s AND Quarter = %s And brand = 'Undefined'", (year, quarter))
                        h = exe.fetchone()
                    st.markdown(h[0])
    with st.container():
        map = []
        exe.execute("SELECT state , sum(registereduser), sum(appopens) FROM map_users WHERE Year = %s AND Quarter = %s group by state",(year, quarter))
        h = exe.fetchall()
        for i in h:
            map.append(i)
        map_df = pd.DataFrame (map,columns = ["State","Registered users","App Opens"])
        map_df.index = map_df.index + 1

        fig = px.choropleth(map_df,
                            geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
                            featureidkey="properties.ST_NM",
                            locations="State",
                            color="Registered users",
                            hover_data=["State","Registered users","App Opens"],
                            projection="equal earth",
                            color_continuous_scale="Aggrnyl"
                            )
        fig.update_geos(fitbounds="locations", visible=False)
        fig.update_layout(width=500, height=500, title ="Dynamic Data Visualisation of Phonepe Pulse")
        st.plotly_chart(fig, theme=None, use_container_width=True )

if type =="Data Insights":
    col1,col2,col3 = st.columns([1,2,1])
    with col2 :
        Question = st.selectbox("Select the dropdown to visualise",["High profit yielding states","High profit yielding districts",
        "high profit yielding pincodes","Top Mobile brands users of Phonepe","Top 10 mobile brands with high percentage of transction"])
    with col1:
        year = st.selectbox("Year", [2018, 2019, 2020, 2021, 2022, 2023])
    with col3:
        if year == 2023:
            quarter = st.selectbox("Quarter", [1, 2])
        else:
            quarter = st.selectbox("Quarter", [1, 2, 3, 4])
    with st.container():
        result = []
        db = psycopg2.connect(host='localhost', user='postgres', password='SivA@879789', port=5432, database='Phonepe')
        exe = db.cursor()
        if Question == "High profit yielding states":
                exe.execute("SELECT state, SUM(amount) / SUM(no_of_transaction) AS avg FROM Aggregated_transaction WHERE Year = %s AND Quarter = %s GROUP BY state ORDER BY avg DESC limit 10", (year, quarter))
                h = exe.fetchall()
                for i in h:
                    result.append(i)
                br = px.bar(result,x = 0, y= 1,color = 1,color_continuous_scale='thermal',title='High Profit Yielding States')
                br.update_layout(title_font=dict(size=25), title_font_color='#002147')
                st.plotly_chart(br)
        if Question == "High profit yielding districts":
                exe.execute("SELECT district, SUM(amount)/SUM(count) AS avg FROM map_transaction WHERE Year = %s AND Quarter = %s GROUP BY district ORDER BY avg DESC limit 10", (year, quarter))
                h = exe.fetchall()
                for i in h:
                    result.append(i)
                br = px.bar(result,x = 0, y= 1,color = 1,color_continuous_scale='thermal',title='High Profit Yielding Districts')
                br.update_layout(title_font=dict(size=25), title_font_color='#002147')
                st.plotly_chart(br)
        if Question == "high profit yielding pincodes":
                exe.execute("SELECT pincode, SUM(transaction_amount)/SUM(transaction_count) AS avg FROM top_transaction WHERE Year = %s AND Quarter = %s GROUP BY pincode ORDER BY avg DESC limit 10", (year, quarter))
                h = exe.fetchall()
                for i in h:
                    result.append(i)
                df = pd.DataFrame(result)
                df[0] =df[0].astype(str)
                br = px.scatter(df,x = 0, y= 1,color = 0,color_continuous_scale='thermal',title='High Profit Yielding Pincodes')
                br.update_layout(title_font=dict(size=25), title_font_color='#002147')
                st.plotly_chart(br)
        if Question == "Top Mobile brands users of Phonepe":
                exe.execute("SELECT  brand, sum(count) FROM Aggregated_users WHERE Year = %s AND Quarter = %s GROUP BY brand order by sum(count) DESC limit 10", (year, quarter))
                h = exe.fetchall()
                for i in h:
                    result.append(i)
                br = px.bar(result,x = 0, y= 1,color = 1,color_continuous_scale='thermal',title='Top Mobile brands users of Phonepe')
                br.update_layout(title_font=dict(size=25), title_font_color='#002147')
                st.plotly_chart(br)
        if Question == "Top 10 mobile brands with high percentage of transction":
                exe.execute("SELECT  brand, sum(percentage) FROM Aggregated_users WHERE Year = %s AND Quarter = %s GROUP BY brand order by sum(percentage) DESC limit 10", (year, quarter))
                h = exe.fetchall()
                for i in h:
                    result.append(i)
                br = px.bar(result,x = 0, y= 1,color = 1,color_continuous_scale='thermal',title='Top 10 mobile brands with high percentage of transction')
                br.update_layout(title_font=dict(size=25), title_font_color='#002147')
                st.plotly_chart(br)

