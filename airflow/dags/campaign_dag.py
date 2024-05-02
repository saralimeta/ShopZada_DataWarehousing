from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime
import requests
import mysql.connector
from sqlalchemy import create_engine, MetaData, Table, Column, VARCHAR, DATETIME, DATE, FLOAT, INT

import pandas as pd

def campaign_data_load_database():
    df_campaign_data = pd.read_csv("/opt/airflow/departments/Marketing Department/campaign_data.csv", sep = '\t')

    # Drop Unnamed Col
    df_campaign_data = df_campaign_data.drop("Unnamed: 0", axis=1)

    df_campaign_data.to_parquet("/opt/airflow/dimensions/Campaign Dimension/campaign_data.parquet")

    print("Successfully loaded the campaign data database...")

def campaign_data_standardize_price():
    df_campaign_data = pd.read_parquet("/opt/airflow/dimensions/Campaign Dimension/campaign_data.parquet")

    # REPLACE FORMAT OF PERCENT TO %
    def campaign_data_replace_discount(value):
        return value.replace(r'(\d+).*', r'\1 %')

    format_mapping = {
        'pct': '%',
        '%%': '%',
        'percent': '%'
    }

    df_campaign_data['discount'] = df_campaign_data['discount'].replace(format_mapping, regex=True)
    df_campaign_data['discount'] = df_campaign_data['discount'].apply(campaign_data_replace_discount)

    df_campaign_data.to_parquet("/opt/airflow/dimensions/Campaign Dimension/campaign_data_standardized_discount.parquet")
    print("Successfully standardized discount column in campaign data database...")

def campaign_data_add_noCampaign():
    df_campaign_data = pd.read_parquet("/opt/airflow/dimensions/Campaign Dimension/campaign_data_standardized_discount.parquet") # Insert full path.
    new_row_data = {
        'campaign_id': 'CAMPAIGN00000',
        'campaign_name': 'No Campaign',
        'campaign_description': 'No Campaign',
        'discount': '0%'
    }

    # Create a new DataFrame with the new row
    new_row = pd.DataFrame(new_row_data, index=[0])

    # Concatenate the existing DataFrame with the new row
    df_campaign_data = pd.concat([df_campaign_data, new_row]).reset_index(drop=True)
    df_campaign_data.to_parquet("/opt/airflow/dimensions/Campaign Dimension/campaign_data_added_noCampaign.parquet")
    print("Successfully standardized discount column in campaign data database...")

def campaign_data_edit_col_names():
    df_campaign_data = pd.read_parquet("/opt/airflow/dimensions/Campaign Dimension/campaign_data_added_noCampaign.parquet") # Insert full path.

    # Change Column names
    df_campaign_data = df_campaign_data.rename(columns={'campaign_id': 'CAMPAIGN_ID', 'campaign_name': 'CAMPAIGN_NAME', 'campaign_description': 'CAMPAIGN_DESCRIPTION', 'discount': 'CAMPAIGN_DISCOUNT'})

    df_campaign_data.to_parquet("/opt/airflow/dimensions/Dimensional Model/campaign_dimension.parquet")
    print("Successfully saved the campaign dimension...")


def campaign_dimension_to_db():
    # Step 1: Read Parquet File
    df = pd.read_parquet("/opt/airflow/dimensions/Dimensional Model/campaign_dimension.parquet")

    # Step 2: Establish Connection to MySQL Server
    host = 'host.docker.internal'
    user = 'root'
    password = 'root'
    connection = mysql.connector.connect(
        host = host,
        user = user,
        password = password
    )

    # Step 3: Create a Database
    database_name = 'DWFinalProj'
    create_database_query = f"CREATE DATABASE IF NOT EXISTS {database_name}"
    with connection.cursor() as cursor: 
        cursor.execute(create_database_query)
    
    # Step 4: Switch to the New Database
    connection.database = database_name

    # Step 5: Establish Connection to MySQL Database
    engine = create_engine(f"mysql+mysqlconnector://{user}:{password}@{host}/{database_name}")

    # Step 6: Create Table
    metadata = MetaData()
    my_table = Table(
        'campaign_dimension',
        metadata,
        Column('CAMPAIGN_ID', VARCHAR(255)),
        Column('CAMPAIGN_NAME', VARCHAR(255)),
        Column('CAMPAIGN_DESCRIPTION', VARCHAR(255)),
        Column('CAMPAIGN_DISCOUNT', VARCHAR(255)),
    )
    metadata.create_all(engine, checkfirst=True)

    # Optional: Close the Connection (Not necessary if running as a script, but good practice)
    engine.dispose()

    # Step 7: Insert Data into MySQL
    df.to_sql(name='campaign_dimension', con=engine, if_exists='replace', index=False)
    print("Inserted into DB")

args = {
    'owner': 'Group 1',
    'start_date': days_ago(0),
}

dag = DAG(
    dag_id = 'campaign_pipeline',
    default_args = args,
    schedule_interval = '@hourly',
    max_active_runs = 1,
)

with dag:
    # campaign data
    cd_load_database = PythonOperator(
        task_id = 'campaign_data_load_database',
        python_callable = campaign_data_load_database
    )

    cd_standardize_price = PythonOperator(
        task_id = 'campaign_data_standardize_price',
        python_callable = campaign_data_standardize_price
    )

    cd_add_noCampaign = PythonOperator(
        task_id = 'campaign_data_add_noCampaign',
        python_callable = campaign_data_add_noCampaign
    )

    cd_edit_col_names = PythonOperator(
        task_id = 'campaign_data_edit_col_names',
        python_callable = campaign_data_edit_col_names
    )

    cd_to_db = PythonOperator(
        task_id = 'campaign_dimension_to_db',
        python_callable = campaign_dimension_to_db
    )

cd_load_database >> cd_standardize_price >> cd_add_noCampaign >> cd_edit_col_names >> cd_to_db