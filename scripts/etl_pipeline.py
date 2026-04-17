import pandas as pd
print("ETL process started")

#EXTRACT
df = pd.read_csv("C:/Users/Lenovo/etl-project/datas/train.csv")
print("data is Loaded")

#Transform
#remove null values
df = df.dropna()

#convert date
df['Order Date'] = pd.to_datetime(df['Order Date'], dayfirst = True)
df['Ship Date'] = pd.to_datetime(df['Ship Date'], dayfirst=True)

#add new Column
df['order processing days'] = (df['Ship Date'] - df['Order Date']).dt.days

#clean column names
df.columns = df.columns.str.replace(" ","_")

print("Transformed successfully!")

#output
print(df.head())

from sqlalchemy import create_engine

# Create connection
engine = create_engine("mysql+pymysql://root:root@localhost/test1")

# Load data into MySQL
df.to_sql("sales_data", con=engine, if_exists="replace", index=False)

print("Data loaded into MySQL successfully")