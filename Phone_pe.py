import os
import json
import pandas as pd
import pymysql # type: ignore

# MySQL connection configuration
config = {
    'host': 'localhost',
    'user': 'root',
    'password': 'root'
}

# Connect to MySQL and create database + tables
try:
    conn = pymysql.connect(**config)
    cursor = conn.cursor()

    cursor.execute("CREATE DATABASE IF NOT EXISTS phone_pe")
    print("Database 'phone_pe' exists or created.")

    cursor.execute("USE phone_pe")
    print("Switched to database 'phone_pe'")

    create_table_query = [
        """CREATE TABLE IF NOT EXISTS agg_transaction (
            State VARCHAR(255),
            Year INT,
            Quarter INT,
            Transaction_type VARCHAR(100),
            Transaction_count BIGINT,
            Transaction_amount BIGINT
        )""",

        """CREATE TABLE IF NOT EXISTS agg_insurance (
            State VARCHAR(255),
            Year INT,
            Quarter INT,
            Insurance_type VARCHAR(100),
            Insurance_count BIGINT,
            Insurance_amount BIGINT
        )""",

        """CREATE TABLE IF NOT EXISTS agg_user (
            State VARCHAR(255),
            Year INT,
            Quarter INT,
            Brand VARCHAR(100),
            Transaction_count BIGINT,
            Percentage FLOAT )""",
        
        """CREATE TABLE IF NOT EXISTS map_transaction (
            State VARCHAR(255),
            Year INT,
            Quarter INT,
            District VARCHAR(100),
            Transaction_count BIGINT,
            Transaction_amount BIGINT)""",
            
        """CREATE TABLE IF NOT EXISTS map_user (
            State VARCHAR(100),
            Year INT,
            Quarter INT,
            District VARCHAR(100),
            RegisteredUser BIGINT,
            AppOpens BIGINT)""",
            
        """CREATE TABLE IF NOT EXISTS map_insurance (
            State VARCHAR(255),
            Year INT,
            Quarter INT,
            District VARCHAR(100),
            Transaction_count BIGINT,
            transaction_amount BIGINT)""",
            
        """CREATE TABLE IF NOT EXISTS Top_transaction (
            State VARCHAR(255),
            Year INT,
            Quarter INT,
            Pincode INT(100),
            Transaction_count BIGINT,
            transaction_amount BIGINT)""",
            
        """CREATE TABLE IF NOT EXISTS Top_user (
            State VARCHAR(255),
            Year INT,
            Quarter INT,
            Pincode INT,
            RegisteredByUsers INT)""",
            
        """CREATE TABLE IF NOT EXISTS Top_insurance (
            State VARCHAR(255),
            Year INT,
            Quarter INT,
            Pincode INT(100),
            Transaction_count BIGINT,
            transaction_amount BIGINT)"""        
    ]
     
    for query in create_table_query:
        cursor.execute(query)

    conn.commit()
    print("All tables created or already exist.")

except pymysql.err.OperationalError as err:
    print("Connection or SQL error:", err)

# ******************************************************************************************

# Load aggregated transaction data
base_path_1 = r"C:\Users\user\Desktop\DS SETS\Phone-pe\data\aggregated\transaction\country\india\state"
agg_transaction_list = os.listdir(base_path_1)

clm = {
    'State': [], 'Year': [], 'Quarter': [],
    'Transaction_type': [], 'Transaction_count': [], 'Transaction_amount': []
}

for state in agg_transaction_list:
    state_path = os.path.join(base_path_1, state)
    year_list = os.listdir(state_path)

    for year in year_list:
        year_path = os.path.join(state_path, year)
        quarter_files = os.listdir(year_path)

        for quarter_file in quarter_files:
            file_path = os.path.join(year_path, quarter_file)
            with open(file_path, 'r') as f:
                data = json.load(f)

            try:
                transaction_data = data['data']['transactionData']
                for transaction in transaction_data:
                    name = transaction['name']
                    count = transaction['paymentInstruments'][0]['count']
                    amount = transaction['paymentInstruments'][0]['amount']

                    clm['State'].append(state)
                    clm['Year'].append(int(year))
                    clm['Quarter'].append(int(quarter_file.strip('.json')))
                    clm['Transaction_type'].append(name)
                    clm['Transaction_count'].append(count)
                    clm['Transaction_amount'].append(amount)
            except (KeyError, TypeError) as e:
                print(f"Skipping file (Transaction) due to error: {file_path} - {e}")

#create dataframe
Agg_Transaction_data = pd.DataFrame(clm)
#display dataframe
print("\nAggregated Transaction Data:")
print(Agg_Transaction_data)

insert_query = """
    INSERT INTO agg_transaction (State, Year, Quarter, Transaction_type, Transaction_count, Transaction_amount)
    VALUES (%s, %s, %s, %s, %s, %s)
"""
for agg_transaction, row in Agg_Transaction_data.iterrows():#iterrows used for rows insert by loop
    cursor.execute(insert_query, tuple(row))
conn.commit()
print ("Data inserted into agg_transaction")


# ******************************************************************************************

# Load aggregated insurance data
base_path_2 = r"C:\Users\user\Desktop\DS SETS\Phone-pe\data\aggregated\insurance\country\india\state"
agg_insurance_list = os.listdir(base_path_2)

clm2 = {
    "State": [], "Year": [], "Quarter": [],
    "Insurance_type": [], "Insurance_count": [], "Insurance_amount": []
}

for state in agg_insurance_list:
    state_path = os.path.join(base_path_2, state)
    year_list = os.listdir(state_path)

    for year in year_list:
        year_path = os.path.join(state_path, year)
        quarter_files = os.listdir(year_path)

        for quarter_file in quarter_files:
            file_path = os.path.join(year_path, quarter_file)
            with open(file_path, 'r') as f:
                data = json.load(f)

            try:
                insurance_data = data['data']['transactionData']
                for transactionData in insurance_data:
                    name = transactionData['name']
                    count = transactionData['paymentInstruments'][0]['count']
                    amount = transactionData['paymentInstruments'][0]['amount']

                    clm2['State'].append(state)
                    clm2['Year'].append(int(year))
                    clm2['Quarter'].append(int(quarter_file.strip('.json')))
                    clm2['Insurance_type'].append(name)
                    clm2['Insurance_count'].append(count)
                    clm2['Insurance_amount'].append(amount)
            except (KeyError, TypeError) as e:
                print(f"Skipping file (Insurance) due to error: {file_path} - {e}")

#create dataframe
agg_insurance_data = pd.DataFrame(clm2)

#display the dataframe
print("\nAggregated Insurance Data:")
print(agg_insurance_data)

insert_query = """
    INSERT INTO agg_insurance (State, Year, Quarter, Insurance_type, Insurance_count, Insurance_amount)
    VALUES (%s, %s, %s, %s, %s, %s)
"""
for agg_insurance, row in agg_insurance_data.iterrows():
    cursor.execute(insert_query, tuple(row))
conn.commit()
print(" Data inserted into agg_insurance")


# ******************************************************************************************

# Load aggregated user data
base_path_3 = r"C:\Users\user\Desktop\DS SETS\Phone-pe\data\aggregated\user\country\india\state"
print(base_path_3)

#get the list of state folder
agg_user_list = os.listdir(base_path_3)
print(agg_user_list)

clm3 = {
    'State': [], 'Year': [], 'Quarter': [],
    'Brand': [], 'Transaction_count': [], 'Percentage': []
}

for state in agg_user_list:
    state_path = os.path.join(base_path_3, state)
    year_list = os.listdir(state_path)

    for year in year_list:
        year_path = os.path.join(state_path, year)
        quarter_files = os.listdir(year_path)

        for quarter_file in quarter_files:
            file_path = os.path.join(year_path, quarter_file)
            with open(file_path, 'r') as f:
                data = json.load(f)

            try:
                if data.get('data') and data['data'].get('usersByDevice'):
                    user_data = data['data']['usersByDevice']
                    for usersByDevice in user_data:
                        brand = usersByDevice['brand']
                        count = usersByDevice['count']
                        percentage = usersByDevice['percentage']

                        clm3['State'].append(state)
                        clm3['Year'].append(int(year))
                        clm3['Quarter'].append(int(quarter_file.strip('.json')))
                        clm3['Brand'].append(brand)
                        clm3['Transaction_count'].append(count)
                        clm3['Percentage'].append(percentage)
            except (KeyError, TypeError) as e:
                print(f"Skipping file (User) due to error: {file_path} - {e}")
#create dataframe
agg_users_data = pd.DataFrame(clm3)
#Display dataframe
print("\nAggregated User Data:")
print(agg_users_data)

insert_query = """
    INSERT INTO agg_user (State, Year, Quarter, Brand, Transaction_count, Percentage)
    VALUES (%s, %s, %s, %s, %s, %s)
"""
for agg_user, row in agg_users_data.iterrows():
    cursor.execute(insert_query, tuple(row))
conn.commit()
print("Data inserted into agg_user")


#*************************************************************************************************************

# Define the path
base_path_4 = r"C:\Users\user\Desktop\DS SETS\Phone-pe\data\map\transaction\hover\country\india\state"
print(base_path_4)

# Get the list of state folders
map_transaction_list = os.listdir(base_path_4)
print(map_transaction_list)

clm4 = {
    'State': [],'Year': [],'Quarter': [],
    'District': [],'Transaction_count': [],'Transaction_amount': []
}

# Loop through each state
for state in map_transaction_list:
    state_path_4 = os.path.join(base_path_4, state)
    year_list_4 = os.listdir(state_path_4)

    for year in year_list_4:
        year_path_4 = os.path.join(state_path_4, year)
        quarter_files = os.listdir(year_path_4)

        for quarter_file in quarter_files:
            file_path_4 = os.path.join(year_path_4, quarter_file)
            with open(file_path_4, 'r') as f:
                data = json.load(f)

            try:
                hoverdata_list = data.get('data', {}).get('hoverDataList', [])
                for item in hoverdata_list:
                    district = item.get('name', 'Unknown')
                    metrics = item.get('metric', [{}])
                    count = metrics[0].get('count', 0)
                    amount = metrics[0].get('amount', 0)

                    clm4['State'].append(state)
                    clm4['Year'].append(int(year))
                    clm4['Quarter'].append(int(quarter_file.strip('.json')))
                    clm4['District'].append(district)
                    clm4['Transaction_count'].append(count)
                    clm4['Transaction_amount'].append(amount)

            except Exception as e:
                print(f"Skipping file due to error: {file_path_4} - {e}")

# Convert to DataFrame
map_transaction_data = pd.DataFrame(clm4)

# Define DataFrame
print("\n Map Transaction Data:")
print(map_transaction_data)

insert_query = """
    INSERT INTO map_transaction (State, Year, Quarter, District, Transaction_count, Transaction_amount)
    VALUES (%s, %s, %s, %s, %s, %s)
"""
for map_transaction, row in map_transaction_data.iterrows():
    cursor.execute(insert_query, tuple(row))
conn.commit()
print("Data inserted into map_transaction")


#*******************************************************************************************
base_path_state = r"C:\Users\user\Desktop\DS SETS\Phone-pe\data\map\user\hover\country\india\state"
base_path_hover = r"C:\Users\user\Desktop\DS SETS\Phone-pe\data\map\user\hover"

clm5 = {
    'State': [], 'Year': [], 'Quarter': [],
    'District': [], 'RegisteredUser': [], 'AppOpens': []
}

# ================== 1. STATE LEVEL DATA ==================
print("Fetching STATE Level Data...")
for state in os.listdir(base_path_state):
    state_path = os.path.join(base_path_state, state)
    if os.path.isdir(state_path):
        for year in os.listdir(state_path):
            year_path = os.path.join(state_path, year)
            if os.path.isdir(year_path):
                for file in os.listdir(year_path):
                    if file.endswith(".json"):
                        quarter = file.replace(".json", "")
                        file_path = os.path.join(year_path, file)
                        try:
                            with open(file_path, 'r', encoding='utf-8') as f:
                                data = json.load(f)

                            if not data["data"]:
                                continue

                            if "hoverData" in data["data"]:
                                for dist_name, dist_info in data["data"]["hoverData"].items():
                                    clm5['State'].append(state)
                                    clm5['Year'].append(int(year))
                                    clm5['Quarter'].append(int(quarter))
                                    clm5['District'].append(dist_name)
                                    clm5['RegisteredUser'].append(dist_info.get("registeredUsers"))
                                    clm5['AppOpens'].append(dist_info.get("appOpens"))

                        except Exception as e:
                            print(f"Error in {file_path}: {e}")

# ================== 2. HOVER LEVEL DATA ==================
print("Fetching HOVER District Data...")
for state in os.listdir(base_path_hover):
    state_path = os.path.join(base_path_hover, state)
    if os.path.isdir(state_path):
        for year in os.listdir(state_path):
            year_path = os.path.join(state_path, year)
            if os.path.isdir(year_path):
                for file in os.listdir(year_path):
                    if file.endswith(".json"):
                        quarter = file.replace(".json", "")
                        file_path = os.path.join(year_path, file)
                        try:
                            with open(file_path, 'r', encoding='utf-8') as f:
                                data = json.load(f)

                            if not data["data"]:
                                continue

                            if "districts" in data["data"]:
                                for dist_name, dist_info in data["data"]["districts"].items():
                                    clm5['State'].append(state)
                                    clm5['Year'].append(int(year))
                                    clm5['Quarter'].append(int(quarter))
                                    clm5['District'].append(dist_name)
                                    clm5['RegisteredUser'].append(dist_info.get("registeredUsers"))
                                    clm5['AppOpens'].append(dist_info.get("appOpens"))

                            elif "hoverData" in data["data"]:
                                for dist_name, dist_info in data["data"]["hoverData"].items():
                                    clm5['State'].append(state)
                                    clm5['Year'].append(int(year))
                                    clm5['Quarter'].append(int(quarter))
                                    clm5['District'].append(dist_name)
                                    clm5['RegisteredUser'].append(dist_info.get("registeredUsers"))
                                    clm5['AppOpens'].append(dist_info.get("appOpens"))

                            elif "hoverDataList" in data["data"]:
                                for dist in data["data"]["hoverDataList"]:
                                    clm5['State'].append(state)
                                    clm5['Year'].append(int(year))
                                    clm5['Quarter'].append(int(quarter))
                                    clm5['District'].append(dist.get("name"))
                                    clm5['RegisteredUser'].append(dist.get("registeredUsers"))
                                    clm5['AppOpens'].append(dist.get("appOpens"))

                        except Exception as e:
                            print(f"Error in {file_path}: {e}")

# ---------- DataFrame ----------
map_user_data = pd.DataFrame(clm5)
print(f"Total records fetched: {len(map_user_data)}")
# Display the DataFrame
print("\n Map user Data:")
print(map_user_data)
# ---------- Insert into SQL ----------
insert_query = """
INSERT INTO map_user (State, Year, Quarter, District, RegisteredUser, AppOpens)
VALUES (%s, %s, %s, %s, %s, %s)
"""

for map_user, row in map_user_data.iterrows():
    cursor.execute(insert_query, tuple(row))
conn.commit()
print("Data inserted into map_user table successfully!")

#****************************************************************************************************************

# Define the path
base_path_6 = r"C:\Users\user\Desktop\DS SETS\Phone-pe\data\map\insurance\hover\country\india\state"
print(base_path_6)

# Get the list of state folders
map_insurance_list = os.listdir(base_path_6)
print("States found:", map_insurance_list)

clm6 = {
    'State': [], 'Year': [], 'Quarter': [],
    'District': [], 'Transaction_count': [], 'Transaction_amount': []
}

# Loop through each state
for state in map_insurance_list:
    state_path_6 = os.path.join(base_path_6, state)
    year_list_6 = os.listdir(state_path_6)

    for year in year_list_6:
        year_path_6 = os.path.join(state_path_6, year)
        quarter_files = os.listdir(year_path_6)

        for quarter_file in quarter_files:
            file_path_6 = os.path.join(year_path_6, quarter_file)
            with open(file_path_6, 'r') as f:
                data = json.load(f)

            try:
                insurance_data = data['data'].get('hoverDataList', [])
                for item in insurance_data:
                    district = item.get('name')
                    metric = item.get('metric', [{}])[0]  # assumes one metric per district

                    count = metric.get('count', 0)
                    amount = metric.get('amount', 0.0)

                    clm6['State'].append(state)
                    clm6['Year'].append(int(year))
                    clm6['Quarter'].append(int(quarter_file.strip('.json')))
                    clm6['District'].append(district)
                    clm6['Transaction_count'].append(count)
                    clm6['Transaction_amount'].append(amount)

            except Exception as e:
                print(f"Skipping file due to error: {file_path_6} - {e}")

# Convert to DataFrame
map_insurance_data = pd.DataFrame(clm6)

# Display the DataFrame
print("\n Map Insurance Data:")
print(map_insurance_data)

insert_query = """
    INSERT INTO map_insurance (State, Year, Quarter, District, Transaction_count, transaction_amount)
    VALUES (%s, %s, %s, %s, %s, %s)
"""
for map_insurance, row in map_insurance_data.iterrows():
    cursor.execute(insert_query, tuple(row))
conn.commit()
print("Data inserted into map_insurance")


#*********************************************************************************************************

# Define base path
base_path_7 = r"C:\Users\user\Desktop\DS SETS\Phone-pe\data\top\transaction\country\india\state"
print("Reading from:", base_path_7)

# Get list of states
Top_state_list = os.listdir(base_path_7)
print("States found:", Top_state_list)

clm7 = {
    'State': [],'Year': [],'Quarter': [],
    'Pincode': [],'Transaction_Count': [],'Transaction_Amount': []
}

# Loop through each state
for state in Top_state_list:
    state_path_7 = os.path.join(base_path_7, state)
    year_list_7 = os.listdir(state_path_7)

    for year in year_list_7:
        year_path_7 = os.path.join(state_path_7, year)
        quarter_files = os.listdir(year_path_7)

        for quarter_file in quarter_files:
            file_path_7 = os.path.join(year_path_7, quarter_file)
            with open(file_path_7, 'r') as f:
                data = json.load(f)

            try:
                Top_transaction_data = data['data'].get('pincodes', [])
                for pin in Top_transaction_data:
                    name = pin.get('entityName')
                    metric = pin.get('metric', {})
                    count = metric.get('count', 0)
                    amount = metric.get('amount', 0.0)

                    clm7['State'].append(state)
                    clm7['Year'].append(int(year))
                    clm7['Quarter'].append(int(quarter_file.rstrip('.json')))
                    clm7['Pincode'].append(name)
                    clm7['Transaction_Count'].append(count)
                    clm7['Transaction_Amount'].append(amount)

            except Exception as e:
                print(f"Skipping file due to error: {file_path_7} - {e}")

# Convert to DataFrame
Top_transaction_datas = pd.DataFrame(clm7)

# Display the DataFrame
print("\n Top Transaction Data:")
print(Top_transaction_datas)

insert_query = """
    INSERT INTO Top_transaction (State, Year, Quarter, Pincode, Transaction_count, transaction_amount)
    VALUES (%s, %s, %s, %s, %s, %s)
"""
for Top_transaction, row in Top_transaction_datas.iterrows():
    cursor.execute(insert_query, tuple(row))
conn.commit()
print("Data inserted into Top_transaction")


#*************************************************************************************************************

# Define path
base_path_8 = r"C:\Users\user\Desktop\DS SETS\Phone-pe\data\top\user\country\india\state"
print( base_path_8)

# Get list of state folders
Top_user_list = os.listdir(base_path_8)
print("States found:", Top_user_list)

# Data structure
clm8 = {
    'State': [],'Year': [],'Quarter': [],
    'Pincode': [],'RegisteredUsers': []
}

# Loop through each state
for state in Top_user_list:
    state_path_8 = os.path.join(base_path_8, state)
    year_list_8 = os.listdir(state_path_8)

    for year in year_list_8:
        year_path_8 = os.path.join(state_path_8, year)
        quarter_files = os.listdir(year_path_8)

        for quarter_file in quarter_files:
            file_path_8 = os.path.join(year_path_8, quarter_file)
            with open(file_path_8, 'r') as f:
                data = json.load(f)

            try:
                user_datas = data['data'].get('pincodes', [])
                for pin in user_datas:
                    name = pin.get('name')
                    registered = pin.get('registeredUsers', 0)

                    clm8['State'].append(state)
                    clm8['Year'].append(int(year))
                    clm8['Quarter'].append(int(quarter_file.rstrip('.json')))
                    clm8['Pincode'].append(name)
                    clm8['RegisteredUsers'].append(registered)
            except Exception as e:
                print(f"Error processing file: {file_path_8} — {e}")

# Create DataFrame
Top_user_data = pd.DataFrame(clm8)

# Display the DataFrame
print("\n Top User Data:")
print(Top_user_data)

insert_query = """
    INSERT INTO Top_user (State, Year, Quarter, Pincode, RegisteredByUsers)
    VALUES (%s, %s, %s, %s, %s)
"""
for Top_user, row in Top_user_data.iterrows():
    cursor.execute(insert_query, tuple(row))
conn.commit()
print("Data inserted into Top_user")


#************************************************************************************************************

# Define base path
base_path_9 = r"C:\Users\user\Desktop\DS SETS\Phone-pe\data\top\insurance\country\india\state"
print(base_path_9)

# Get the list of state folders
Top_insurance_list = os.listdir(base_path_9)
print(Top_insurance_list)

clm9 = {
    'State': [],'Year': [],'Quarter': [],
    'Pincode': [],'Transaction_Count': [],'Transaction_Amount': []
}

# Loop through each state
for state in Top_insurance_list:
    state_path_9 = os.path.join(base_path_9, state)
    year_list_9 = os.listdir(state_path_9)

    for year in year_list_9:
        year_path_9 = os.path.join(state_path_9, year)
        quarter_files = os.listdir(year_path_9)

        for quarter_file in quarter_files:
            file_path_9 = os.path.join(year_path_9, quarter_file)
            with open(file_path_9, 'r') as f:
                data = json.load(f)

            try:
             
                insurance_datas = data.get('data',{}).get('pincodes', [])
                for entry in insurance_datas:
                    name = entry.get('entityName')
                    count = entry.get('metric', {}).get('count', 0)
                    amount = entry.get('metric', {}).get('amount', 0.0)

                    clm9['State'].append(state)
                    clm9['Year'].append(int(year))
                    clm9['Quarter'].append(int(quarter_file.rstrip('.json')))
                    clm9['Pincode'].append(name)
                    clm9['Transaction_Count'].append(count)
                    clm9['Transaction_Amount'].append(amount)

            except Exception as e:
                print(f" Error processing file: {file_path_9} — {e}")

# Create DataFrame
Top_insurance_datas = pd.DataFrame(clm9)

# Display the DataFrame
print("\n Top Insurance Data:")
print(Top_insurance_datas)

insert_query = """
    INSERT INTO Top_insurance (State, Year, Quarter, Pincode, Transaction_count, transaction_amount)
    VALUES (%s, %s, %s, %s, %s, %s)
"""
for top_insurance, row in Top_insurance_datas.iterrows():
    cursor.execute(insert_query, tuple(row))
conn.commit()
print("Data inserted into Top_insurance")

#**********************************************************************************************************

