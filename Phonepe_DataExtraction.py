
import git
import os
os.environ['GIT_PYTHON_GIT_EXECUTABLE'] = '/cmd/git'
import pandas as pd
import json
import psycopg2

def repository_cloning():
    repo_url = 'https://github.com/PhonePe/pulse.git'
    local_directory = "F:/Phone_pe_data"
    repo = git.Repo.clone_from(repo_url, local_directory)

def state_conversion(df):
    df['State'].replace(
        {'andaman-&-nicobar-islands': 'Andaman & Nicobar Island', 'andhra-pradesh': 'Andhra Pradesh',
         'arunachal-pradesh': 'Arunanchal Pradesh', 'assam': 'Assam', 'bihar': 'Bihar', 'chandigarh': 'Chandigarh',
         'chhattisgarh': 'Chhattisgarh', 'dadra-&-nagar-haveli-&-daman-&-diu': 'Dadra and Nagar Haveli and Daman and Diu',
         'delhi': 'Delhi', 'goa': 'Goa', 'gujarat': 'Gujarat', 'haryana': 'Haryana', 'himachal-pradesh': 'Himachal Pradesh',
         'jammu-&-kashmir': 'Jammu & Kashmir', 'jharkhand': 'Jharkhand', 'karnataka': 'Karnataka', 'kerala': 'Kerala',
         'ladakh': 'Ladakh', 'lakshadweep': 'Lakshadweep', 'madhya-pradesh': 'Madhya Pradesh', 'maharashtra': 'Maharashtra',
         'manipur': 'Manipur', 'meghalaya': 'Meghalaya', 'mizoram': 'Mizoram', 'nagaland': 'Nagaland', 'odisha': 'Odisha',
         'puducherry': 'Puducherry', 'punjab': 'Punjab', 'rajasthan': 'Rajasthan', 'sikkim': 'Sikkim', 'tamil-nadu': 'Tamil Nadu',
         'telangana': 'Telangana', 'tripura': 'Tripura', 'uttar-pradesh': 'Uttar Pradesh', 'uttarakhand': 'Uttarakhand',
         'west-bengal': 'West Bengal'}, inplace = True)
    return (df)

def Aggregated_transaction():
    Aggregated_transaction =  {"State":[],"Year":[],"Quarter":[],"Txn_type":[],"No_of_txn":[],"amount":[]}
    path = r"F:\Phone_pe_data\data\aggregated\transaction\country\india\state"
    at = os.listdir(path)
    for state in at:
      states = os.listdir(path +'/'+state)
      for year in states:
        years =os.listdir(path +'/'+ state +'/'+ year)
        for file in years:
          f = path +'/'+ state +'/'+ year +'/'+ file
          rawdata = (open(f,"rt")).read()
          data = json.loads(rawdata)
          for i in data["data"]["transactionData"]:
            Aggregated_transaction["State"].append(str(state))
            Aggregated_transaction["Year"].append(int(year))
            Aggregated_transaction["Quarter"].append(int(file[0:1]))
            Aggregated_transaction["Txn_type"].append(str(i["name"]))
            Aggregated_transaction["No_of_txn"].append(int(i["paymentInstruments"][0]["count"]))
            Aggregated_transaction["amount"].append(float(i["paymentInstruments"][0]["amount"]))
    df = pd.DataFrame(Aggregated_transaction)
    state_conversion(df).to_csv("Aggregated_transaction.csv")

def Aggregated_users():
    Aggregated_users =  {"State":[],"Year":[],"Quarter":[],"Registered_Users":[],"Brand":[],"Count":[],"Percentage":[]}
    path = r"F:\Phone_pe_data/data/aggregated/user/country/india/state"
    at = os.listdir(path)
    for state in at:
      states = os.listdir(path +'/'+state)
      for year in states:
        years =os.listdir(path +'/'+ state +'/'+ year)
        for file in years:
          f = path +'/'+ state +'/'+ year +'/'+ file
          rawdata = (open(f,"rt")).read()
          data = json.loads(rawdata)
          try:
            for i in data["data"]["usersByDevice"]:
                Aggregated_users["State"].append(str(state))
                Aggregated_users["Year"].append(int(year))
                Aggregated_users["Quarter"].append(int(file[0:1]))
                Aggregated_users["Brand"].append(str(i["brand"]))
                Aggregated_users["Count"].append(int(i["count"]))
                Aggregated_users["Percentage"].append(float(i["percentage"]))
                Aggregated_users["Registered_Users"].append(int(data["data"]["aggregated"]["registeredUsers"]))
          except TypeError:
              Aggregated_users["State"].append(str(state))
              Aggregated_users["Year"].append(int(year))
              Aggregated_users["Quarter"].append(int(file[0:1]))
              Aggregated_users["Brand"].append("Undefined")
              Aggregated_users["Count"].append(0)
              Aggregated_users["Percentage"].append(0)
              Aggregated_users["Registered_Users"].append(int(data["data"]["aggregated"]["registeredUsers"]))

    df = pd.DataFrame(Aggregated_users)
    state_conversion(df).to_csv("Aggregated_users.csv")

def map_transaction():
    map_transaction =  {"State":[],"Year":[],"Quarter":[],"District":[],"Count":[],"Amount":[]}
    path = r"F:\Phone_pe_data/data/map/transaction/hover/country/india/state"
    at = os.listdir(path)
    for state in at:
      states = os.listdir(path +'/'+state)
      for year in states:
        years =os.listdir(path +'/'+ state +'/'+ year)
        for file in years:
          f = path +'/'+ state +'/'+ year +'/'+ file
          rawdata = (open(f,"rt")).read()
          data = json.loads(rawdata)
          for i in data["data"]["hoverDataList"]:
                map_transaction["State"].append(str(state))
                map_transaction["Year"].append(int(year))
                map_transaction["Quarter"].append(int(file[0:1]))
                map_transaction["District"].append(i["name"])
                map_transaction["Count"].append(int(i["metric"][0]["count"]))
                map_transaction["Amount"].append(float(i["metric"][0]["amount"]))
    df = pd.DataFrame(map_transaction)
    state_conversion(df).to_csv("map_transaction.csv")

def map_users():
    map_users = {"State": [], "Year": [], "Quarter": [], "District": [],"RegisteredUser": [], "AppOpens": []}
    path = r"F:\Phone_pe_data/data/map/user/hover/country/india/state"
    at = os.listdir(path)
    for state in at:
      states = os.listdir(path +'/'+state)
      for year in states:
        years =os.listdir(path +'/'+ state +'/'+ year)
        for file in years:
          f = path +'/'+ state +'/'+ year +'/'+ file
          rawdata = (open(f,"rt")).read()
          data = json.loads(rawdata)
          for i in data["data"]["hoverData"].items():
            map_users["State"].append(str(state))
            map_users["Year"].append(int(year))
            map_users["Quarter"].append(int(file[0:1]))
            map_users["District"].append(str(i[0]))
            map_users["RegisteredUser"].append(int(i[1]["registeredUsers"]))
            map_users["AppOpens"].append(int(i[1]["appOpens"]))
    df = pd.DataFrame(map_users)
    state_conversion(df).to_csv("map_users.csv")

def top_transaction():
    top_transaction = {'State': [], 'Year': [], 'Quarter': [], 'Pincode': [], 'Transaction_count': [],'Transaction_amount': []}
    path = r"F:\Phone_pe_data/data/top/transaction/country/india/state"
    at = os.listdir(path)
    for state in at:
      states = os.listdir(path +'/'+state)
      for year in states:
        years =os.listdir(path +'/'+ state +'/'+ year)
        for file in years:
          f = path +'/'+ state +'/'+ year +'/'+ file
          rawdata = (open(f,"rt")).read()
          data = json.loads(rawdata)
          for i in data["data"]["pincodes"]:
            top_transaction["State"].append(str(state))
            top_transaction["Year"].append(int(year))
            top_transaction["Quarter"].append(int(file[0:1]))
            top_transaction["Pincode"].append(str(i["entityName"]))
            top_transaction["Transaction_count"].append(int(i["metric"]["count"]))
            top_transaction["Transaction_amount"].append(float(i["metric"]["amount"]))
    df = pd.DataFrame(top_transaction)
    state_conversion(df).to_csv("top_transaction.csv")

def top_users():
    top_users = {'State': [], 'Year': [], 'Quarter': [], 'Pincode': [], 'Registered_users': []}
    path = r"F:\Phone_pe_data/data/top/user/country/india/state"
    at = os.listdir(path)
    for state in at:
      states = os.listdir(path +'/'+state)
      for year in states:
        years =os.listdir(path +'/'+ state +'/'+ year)
        for file in years:
          f = path +'/'+ state +'/'+ year +'/'+ file
          rawdata = (open(f,"rt")).read()
          data = json.loads(rawdata)
          for i in data["data"]["pincodes"]:
            top_users["State"].append(str(state))
            top_users["Year"].append(int(year))
            top_users["Quarter"].append(int(file[0:1]))
            top_users["Pincode"].append(str(i["name"]))
            top_users["Registered_users"].append(int(i["registeredUsers"]))

    df = pd.DataFrame(top_users)
    state_conversion(df).to_csv("top_users.csv")


def to_sql():
    db = psycopg2.connect(host='localhost', user='postgres', password='your_password', port=5432, database='Phonepe')
    exe = db.cursor()
    exe.execute("DROP table IF EXISTS Aggregated_transaction")
    db.commit()
    exe.execute("DROP table IF EXISTS Aggregated_users")
    db.commit()
    exe.execute("DROP table IF EXISTS map_transaction")
    db.commit()
    exe.execute("DROP table IF EXISTS map_users")
    db.commit()
    exe.execute("DROP table IF EXISTS top_transaction")
    db.commit()
    exe.execute("DROP table IF EXISTS top_users")
    db.commit()
    exe.execute("""create table Aggregated_transaction (
    State varchar,
    year int,
    Quarter int,
    Transaction_type varchar,
    No_of_transaction int,
    Amount DOUBLE PRECISION)""")
    db.commit()
    exe.execute("""create table Aggregated_users (
    State varchar,
    year int,
    Quarter int,
    Registered_Users int,
    Brand varchar,
    Count int,
    Percentage DOUBLE PRECISION )""")
    db.commit()
    exe.execute("""create table map_transaction (
    State varchar,
    year int,
    Quarter int,
    District varchar,
    Count int,
    Amount DOUBLE PRECISION )""")
    db.commit()
    exe.execute("""create table map_users (
    State varchar,
    year int,
    Quarter int,
    District varchar,
    RegisteredUser int,
    AppOpens int) """)
    db.commit()
    exe.execute("""create table top_transaction (
    State varchar,
    year int,
    Quarter int,
    Pincode int,
    Transaction_count bigint,
    Transaction_amount DOUBLE PRECISION )""")
    db.commit()

    exe.execute("""create table top_users (
    State varchar,
    year int,
    Quarter int,
    Pincode int,
    Registered_users int )""")
    db.commit()

    f = pd.read_csv("Aggregated_transaction.csv")
    for i in f.itertuples():
        res = (i.State, i.Year, i.Quarter, i.Txn_type, i.No_of_txn, i.amount)
        exe.execute("insert into Aggregated_transaction values(%s,%s,%s,%s,%s,%s)",res)
        db.commit()
    f = pd.read_csv("Aggregated_users.csv")
    for i in f.itertuples():
        res = (i.State, i.Year, i.Quarter, i.Registered_Users, i.Brand, i.Count, i.Percentage)
        exe.execute("insert into Aggregated_users values(%s,%s,%s,%s,%s,%s,%s)",res)
        db.commit()
    f = pd.read_csv("map_transaction.csv")
    for i in f.itertuples():
        res = (i.State, i.Year, i.Quarter, i.District, i.Count, i.Amount)
        exe.execute("insert into map_transaction values(%s,%s,%s,%s,%s,%s)",res)
        db.commit()
    f = pd.read_csv("map_users.csv")
    for i in f.itertuples():
        res = (i.State, i.Year, i.Quarter, i.District, i.RegisteredUser, i.AppOpens)
        exe.execute("insert into map_users values(%s,%s,%s,%s,%s,%s)",res)
        db.commit()
    f = pd.read_csv("top_transaction.csv")
    f.dropna(inplace = True)
    for i in f.itertuples():
        res = (i.State, i.Year, i.Quarter, i.Pincode, i.Transaction_count, i.Transaction_amount)
        exe.execute("insert into top_transaction values(%s,%s,%s,%s,%s,%s)",res)
        db.commit()
    f = pd.read_csv("top_users.csv")
    f.dropna(inplace = True)
    for i in f.itertuples():
        res = (i.State, i.Year, i.Quarter,i.Pincode,i.Registered_users)
        exe.execute("insert into top_users values(%s,%s,%s,%s,%s)",res)
        db.commit()

def main():
    Aggregated_transaction()
    Aggregated_users()
    map_transaction()
    map_users()
    top_transaction()
    top_users()
    to_sql()


repository_cloning()
main()
