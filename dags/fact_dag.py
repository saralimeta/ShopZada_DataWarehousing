from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime
import requests
import mysql.connector
from sqlalchemy import create_engine, MetaData, Table, Column, VARCHAR, DATETIME, DATE, FLOAT, INT

import pandas as pd


def order_with_merchant_cleaning():
    # loading data frame
    df_order_with_merchant1 = pd.read_parquet("/opt/airflow/dags/order_with_merchant_data1.parquet")
    df_order_with_merchant2 = pd.read_parquet("/opt/airflow/dags/order_with_merchant_data2.parquet")
    df_order_with_merchant3 = pd.read_csv("/opt/airflow/dags/order_with_merchant_data3.csv")

    dfs = [df_order_with_merchant1, df_order_with_merchant2, df_order_with_merchant3]
    merged_df_order_with_merchant = pd.concat(dfs, ignore_index=True)

    merged_df_order_with_merchant = merged_df_order_with_merchant.drop_duplicates()

    mask = merged_df_order_with_merchant['merchant_id'].str.contains('^MERCHANT\d{4}$')
    merged_df_order_with_merchant.loc[mask, 'merchant_id'] = 'MERCHANT0' + merged_df_order_with_merchant.loc[mask, 'merchant_id'].str[8:]

    
    merged_df_order_with_merchant.to_parquet("/opt/airflow/dags/order_with_merchant_data_cleaned.parquet")
    print("Successfully cleaned the order_with_merchant")

def fact_table():
    # loading data frame
    df_cleaned_lineData = pd.read_parquet(("/opt/airflow/dags/line_product_and_prices_cleaned.parquet"))
    df_order_merchant = pd.read_parquet("/opt/airflow/dags/order_with_merchant_data_cleaned.parquet")
    df_order_data = pd.read_parquet("/opt/airflow/dags/cleaned_order_data.parquet")
    df_transac_campaign = pd.read_parquet("/opt/airflow/dags/transactional_campaign_data_standardized.parquet")
    
    merged_lineData_orderMerchant = pd.merge(df_cleaned_lineData, df_order_merchant, on='order_id', how='inner')
    merged_lineData_orderMerchant = merged_lineData_orderMerchant.drop('Unnamed: 0', axis=1, errors='ignore')

    merged_LD_OM_OD = pd.merge(merged_lineData_orderMerchant, df_order_data[['order_id', 'user_id']], on='order_id', how='inner')

    merged_lineData_orderMerchant_orderData_campaign = pd.merge(merged_LD_OM_OD, df_transac_campaign[['order_id', 'campaign_id']], on='order_id', how='left')
    # Fill NaN values in 'campaign_id' with "No Campaign"
    merged_lineData_orderMerchant_orderData_campaign['campaign_id'] = merged_lineData_orderMerchant_orderData_campaign['campaign_id'].fillna("CAMPAIGN00000")

    merged_lineData_orderMerchant_orderData_campaign = merged_lineData_orderMerchant_orderData_campaign.rename(columns= {'order_id': 'ORDER_ID', 'quantity': 'QUANTITY', 'product_id': 'PRODUCT_ID', 'price': 'PRODUCT_PRICE', 'merchant_id': 'MERCHANT_ID', 'staff_id': 'STAFF_ID', 'user_id': 'USER_ID',  'campaign_id': 'CAMPAIGN_ID'})
    
    merged_lineData_orderMerchant_orderData_campaign.to_parquet("/opt/airflow/dags/fact_table.parquet")


def fact_table_to_db():

# Step 1: Read Parquet File
    parquet_file_path = "/opt/airflow/dags/fact_table.parquet"
    df = pd.read_parquet(parquet_file_path)

# Step 3: Establish Connection to MySQL Server
    host = 'host.docker.internal'
    user = 'root'
    password = 'root'

    connection = mysql.connector.connect(
        host=host,
        user=user,
        password=password
)

# Step 4: Create a Database
    database_name = 'DWFinalProj'

    create_database_query = f"CREATE DATABASE IF NOT EXISTS {database_name}"

    with connection.cursor() as cursor:
        cursor.execute(create_database_query)

# Step 5: Switch to the New Database
    connection.database = database_name

# Step 2: Establish Connection to MySQL Database
    engine = create_engine(f"mysql+mysqlconnector://{user}:{password}@{host}/{database_name}")

# Step 3: Create Table
    metadata = MetaData()
    my_table = Table(
        'transaction_fact_table',
        metadata,
        Column('ORDER_ID', VARCHAR(255)),
        Column('PRODUCT_ID', VARCHAR(255)),
        Column('USER_ID', VARCHAR(255)),
        Column('MERCHANT_ID', VARCHAR(255)),
        Column('STAFF_ID', VARCHAR(255)),
        Column('CAMPAIGN_ID', VARCHAR(255)),
        Column('PRODUCT_PRICE', FLOAT),
        Column('QUANTITY', VARCHAR(255)),
    )

    metadata.create_all(engine, checkfirst=True)

# Optional: Close the Connection (Not necessary if running as a script, but good practice)
    engine.dispose()

# Step 4: Insert Data into MySQL
    df.to_sql(name='transaction_fact_table', con=engine, if_exists='replace', index=False)
    print("Inserted into DB")

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(0),
    #'retries': 1,
    #'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'transaction_pipeline',
    default_args=default_args,
    description='A DAG to execute multiple Python scripts',
    start_date= days_ago(0),
    schedule_interval = '@hourly',
    max_active_runs = 1,
)

with dag:
    owm_cleaning = PythonOperator(
        task_id = 'order_with_merchant_cleaning',
        python_callable = order_with_merchant_cleaning
    )

    ft = PythonOperator(
        task_id = 'fact_table',
        python_callable = fact_table,
    )

    ft_to_db = PythonOperator(
        task_id = 'fact_table_to_db',
        python_callable = fact_table_to_db,
    )

owm_cleaning >> ft >> ft_to_db