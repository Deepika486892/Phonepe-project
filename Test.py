import streamlit as st
import pandas as pd
import pymysql
import plotly.express as px
import requests
import json
from typing import Optional

# ------------------------------
# Page config
# ------------------------------
st.set_page_config(page_title="PhonePe - India Dashboard", layout="wide")
# ------------------------------
# DB connection
# ------------------------------
DB_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "root",
    "database": "phone_pe",
    "port": 3306,
    "cursorclass": pymysql.cursors.DictCursor
}

@st.cache_resource
def get_db_connection():
    try:
        return pymysql.connect(**DB_CONFIG)
    except Exception as e:
        st.error(f"Could not connect to DB: {e}")
        return None

conn = get_db_connection()
if conn is None:
    st.stop()
cursor = conn.cursor()

# ------------------------------
# Utility function
# ------------------------------
def run_query(sql: str) -> pd.DataFrame:
    try:
        cursor.execute(sql)
        rows = cursor.fetchall()
        return pd.DataFrame(rows)
    except Exception as e:
        st.error(f"SQL error: {e}")
        return pd.DataFrame()

@st.cache_data(show_spinner=False)
def load_geojson(url: str, local_fallback: Optional[str] = "india_states.geojson"):
    try:
        r = requests.get(url, timeout=10)
        r.raise_for_status()
        return r.json()
    except Exception:
        try:
            with open(local_fallback, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            st.error("Could not load India states geojson.")
            st.stop()

st.sidebar.markdown("## PhonePe Project")

data_type = st.sidebar.radio(
    "View Type",
    ["Transactions", "Users", "Insurance", "Brands"],
    index=0
)

year_list = list(range(2018, 2025))
selected_year = st.sidebar.selectbox("Select Year", year_list, index=len(year_list)-3)

quarter_list = ["Q1 (Jan-Mar)", "Q2 (Apr-Jun)", "Q3 (Jul-Sep)", "Q4 (Oct-Dec)"]
selected_quarter = st.sidebar.selectbox("Select Quarter", quarter_list, index=0)
selected_quarter_num = quarter_list.index(selected_quarter) + 1

# ------------------------------
# Queries
# ------------------------------
# Transactions Leaderboards
top_transaction = f"""
    SELECT State, SUM(Transaction_amount) AS Total_Amount
    FROM agg_transaction
    WHERE `Year` = {selected_year} AND Quarter = {selected_quarter_num}
    GROUP BY State
    ORDER BY Total_Amount DESC
    LIMIT 10;
"""

low_txn_state = f"""
    SELECT State, SUM(Transaction_amount) AS Total_Amount
    FROM agg_transaction
    WHERE `Year` = {selected_year} AND Quarter = {selected_quarter_num}
    GROUP BY State
    ORDER BY Total_Amount ASC
    LIMIT 10;
"""

top_district = f"""
    SELECT District, SUM(Transaction_amount) AS Total_Amount
    FROM map_transaction
    WHERE `Year` = {selected_year} AND Quarter = {selected_quarter_num}
    GROUP BY District
    ORDER BY Total_Amount DESC
    LIMIT 10;
"""

top_pincode = f"""
    SELECT Pincode, SUM(Transaction_amount) AS Total_Amount
    FROM top_transaction
    WHERE `Year` = {selected_year} AND Quarter = {selected_quarter_num}
    GROUP BY Pincode
    ORDER BY Total_Amount DESC
    LIMIT 10;
"""

# Insurance Leaderboards
top_insurance_states = f"""
    SELECT State, SUM(Insurance_amount) AS Total_Insurance
    FROM agg_insurance
    WHERE `Year` = {selected_year} AND Quarter = {selected_quarter_num}
    GROUP BY State
    ORDER BY Total_Insurance DESC
    LIMIT 10;
"""

low_insurance_state = f"""
    SELECT State, SUM(Insurance_amount) AS Total_Insurance
    FROM agg_insurance
    WHERE `Year` = {selected_year} AND Quarter = {selected_quarter_num}
    GROUP BY State
    ORDER BY Total_Insurance ASC
    LIMIT 10;
"""

top_insurance_districts = f"""
    SELECT District, SUM(transaction_amount) AS trans_amount
    FROM map_insurance
    WHERE `Year` = {selected_year} AND Quarter = {selected_quarter_num}
    GROUP BY District
    ORDER BY trans_amount DESC
    LIMIT 10;
"""

low_insurance_districts = f"""
    SELECT District, SUM(transaction_amount) AS Total_Insurance
    FROM map_insurance
    WHERE `Year` = {selected_year} AND Quarter = {selected_quarter_num}
    GROUP BY District
    ORDER BY Total_Insurance ASC
    LIMIT 10;
"""

top_insurance_pincode_q = f"""
    SELECT Pincode, SUM(Transaction_count) AS Transaction_count
    FROM top_insurance
    WHERE `Year` = {selected_year} AND Quarter = {selected_quarter_num}
    GROUP BY Pincode
    ORDER BY Transaction_count DESC
    LIMIT 10;
"""

# Users Leaderboards
top_users_states_q = f"""
    SELECT State, SUM(RegisteredUser) AS Total_Users
    FROM map_user
    WHERE `Year` = {selected_year} AND Quarter = {selected_quarter_num}
    GROUP BY State
    ORDER BY Total_Users DESC
    LIMIT 10;
"""

low_users_states_q = f"""
    SELECT State, SUM(RegisteredUser) AS Total_Users
    FROM map_user
    WHERE `Year` = {selected_year} AND Quarter = {selected_quarter_num}
    GROUP BY State
    ORDER BY Total_Users ASC
    LIMIT 10;
"""

top_users_districts_q = f"""
    SELECT District, SUM(RegisteredUser) AS Total_Users
    FROM map_user
    WHERE `Year` = {selected_year} AND Quarter = {selected_quarter_num}
    GROUP BY District
    ORDER BY Total_Users DESC
    LIMIT 10;
"""

low_users_districts_q = f"""
    SELECT District, SUM(RegisteredUser) AS Total_Users
    FROM map_user
    WHERE `Year` = {selected_year} AND Quarter = {selected_quarter_num}
    GROUP BY District
    ORDER BY Total_Users ASC
    LIMIT 10;
"""
#  Brands Leaderboard

top_district_app_opens_q = f"""
    SELECT State, SUM(AppOpens) AS Total_AppOpens
    FROM map_user
    WHERE `Year` = {selected_year} AND Quarter = {selected_quarter_num}
    GROUP BY State
    ORDER BY Total_AppOpens DESC
    LIMIT 10;
"""

top_brand_user_count = f"""
    SELECT State, SUM(Transaction_count) AS Transaction_count 
    FROM agg_user
    WHERE `Year` = {selected_year} AND Quarter = {selected_quarter_num}
    GROUP BY State
    ORDER BY Transaction_count DESC LIMIT 10;
"""



# ------------------------------
# Main query 
# ------------------------------
if data_type == "Transactions":
    st.markdown("## 📊 PhonePe Transactions")  # This will show above the chart
    main_query = f"""
        SELECT State, 
               SUM(Transaction_amount) AS Total_Transaction_amount,
               SUM(Transaction_count) AS Total_Transactions_count
        FROM agg_transaction
        WHERE `Year` = {selected_year} AND Quarter = {selected_quarter_num}
        GROUP BY State;
    """
    color_column = "Total_Transaction_amount"
    hover_data = {
        "Total_Transaction_amount": ":,.0f",
        "Total_Transactions_count": ":,",
        "State": False
    }

elif data_type == "Users":
    st.markdown("## 📊 PhonePe User")
    main_query = f"""
        SELECT State, 
               SUM(RegisteredUser) AS Total_Users
        FROM map_user
        WHERE `Year` = {selected_year} AND Quarter = {selected_quarter_num}
        GROUP BY State;
    """
    color_column = "Total_Users"
    hover_data = {
        "Total_Users": ":,",
        "State": False
    }

elif data_type == "Insurance":
    st.markdown("## 📊 PhonePe Insurance")
    main_query = f"""
        SELECT State, 
               SUM(insurance_amount) AS Total_Insurance
        FROM agg_insurance
        WHERE `Year` = {selected_year} AND Quarter = {selected_quarter_num}
        GROUP BY State;
    """
    color_column = "Total_Insurance"
    hover_data = {
        "Total_Insurance": ":,.0f",
        "State": False
    }
else:  # Brands
    st.markdown("## 📊 Brand User")
    main_query = f"""
        SELECT Brand, 
               SUM(Transaction_count) AS Total_Users
        FROM agg_user
        WHERE `Year` = {selected_year} AND Quarter = {selected_quarter_num}
        GROUP BY Brand;
    """
    color_column = "Total_Users"
    hover_data = {
        "Total_Users": ":,",
        "Brand": False
    }


df = run_query(main_query)

if df.empty:
    st.warning("No data for the selected Year/Quarter.")
    st.stop()

if data_type != "Brands":  # Only format State if it exists
    df["State"] = df["State"].astype(str).str.strip().str.title()
df = run_query(main_query)

if df.empty:
    st.warning("No data for the selected Year/Quarter.")
    st.stop()

if data_type == "Brands":
    # Bar chart for Brands
    fig = px.bar(
        df,
        x="Brand",
        y="Total_Users",
        text="Total_Users",
        title="Top Brands by Users",
        color="Total_Users"
    )
    fig.update_traces(texttemplate='%{text:,}', textposition='outside')
    st.plotly_chart(fig, use_container_width=True)

else:
    # Map for others
    df["State"] = df["State"].astype(str).str.strip().str.title()
    geojson_url = "https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"
    india_states = load_geojson(geojson_url)

    fig = px.choropleth(
        df,
        geojson=india_states,  # Assuming you have geojson loaded
        featureidkey="properties.ST_NM",
        locations="State",
        color=color_column,
        hover_name="State",
        hover_data=hover_data,
        color_continuous_scale="Viridis"
    )
    fig.update_geos(fitbounds="locations", visible=False)
    st.plotly_chart(fig, use_container_width=True)

# --- BLUE BACKGROUND STYLE ---
st.markdown(
    """
    <style>
    .stApp {
        background-color: #add8e6;  /* Light blue */
    }
    section[data-testid="stSidebar"] {
        background-color: #ffffff; 
    }
    h1, h2, h3, h4, h5, h6 {
        color: #003366;
    }
    </style>
    """,
    unsafe_allow_html=True
)

# --- Inside your main logic ---
if data_type == "Transactions":

    col_main, col_options = st.columns([3, 1])

    with col_options:
        st.markdown("### Transaction Insights")
        txn_option = st.radio(
            "Choose an analysis:",
            ["Top Transaction States", "Low Transaction States", "Top Districts", "Top Pincodes"]
        )

    # Map the radio option to SQL query
    query_map = {
        "Top Transaction States": top_transaction,
        "Low Transaction States": low_txn_state,
        "Top Districts": top_district,
        "Top Pincodes": top_pincode
    }

    selected_query = query_map[txn_option]
    df_txn = run_query(selected_query)

    with col_main:
        st.subheader(txn_option)
        if not df_txn.empty:
            st.dataframe(df_txn)
        else:
            st.warning("No data available for this selection.")

if data_type == "Users":
    # Simulated right sidebar
    col_main, col_options = st.columns([3, 1])

    with col_options:
        st.markdown("### User Insights")
        txn_option = st.radio(
            "Choose an analysis:",
            ["Top Users States", "Low Users States", "Top Users Districts", "Low Users Districts"]
        )

    # Match radio options to queries
    query_map = {
        "Top Users States": top_users_states_q,
        "Low Users States": low_users_states_q,
        "Top Users Districts": top_users_districts_q,
        "Low Users Districts": low_users_districts_q
    }

    # Get and run the selected query
    selected_query = query_map[txn_option]
    df_txn = run_query(selected_query)

    with col_main:
        st.subheader(txn_option)
        if not df_txn.empty:
            st.dataframe(df_txn)
        else:
            st.warning("No data available for this selection.")

if data_type == "Insurance":
    # Simulated right sidebar
    col_main, col_options = st.columns([3, 1])

    with col_options:
        st.markdown("### Insurance Insights")
        txn_option = st.radio(
            "Choose an analysis:",
            [
                "Top Insurance States",
                "Low Insurance States",
                "Top Insurance Districts",
                "Low Insurance Districts",
                "Top Insurance Pincodes"
            ]
        )

    # Match radio options to queries
    query_map = {
        "Top Insurance States": top_insurance_states,
        "Low Insurance States": low_insurance_state,
        "Top Insurance Districts": top_insurance_districts,
        "Low Insurance Districts": low_insurance_districts,
        "Top Insurance Pincodes": top_insurance_pincode_q
    }

    # Get and run the selected query
    selected_query = query_map[txn_option]
    df_txn = run_query(selected_query)

    with col_main:
        st.subheader(txn_option)
        if not df_txn.empty:
            st.dataframe(df_txn)
        else:
            st.warning("No data available for this selection.")
# ****************************************************************************************
if data_type == "Brand":
    # Simulated right sidebar
    col_main, col_options = st.columns([3, 1])

    with col_options:
        st.markdown("### Brand Insights")
        txn_option = st.radio(
            "Choose an analysis:",
            [
                "Top Brand User Count"
            ]
        )

    # Match radio options to queries
    query_map = {
        "Top Brand User Count": top_brand_user_count
    }

    # Get and run the selected query
    selected_query = query_map[txn_option]
    df_txn = run_query(selected_query)

    with col_main:
        st.subheader(txn_option)
        if not df_txn.empty:
            st.dataframe(df_txn)
        else:
            st.warning("No data available for this selection.")

