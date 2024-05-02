from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime
import requests
import mysql.connector
from sqlalchemy import create_engine, MetaData, Table, Column, VARCHAR, DATETIME, DATE, FLOAT

import pandas as pd

def product_list_load():
    # loading data frame
    df_product_list = pd.read_excel("/opt/airflow/departments/Business Department/product_list.xlsx")
    df_product_list.to_parquet("/opt/airflow/dimensions/Product Dimension/product_list_load.parquet")
    print("Successfully loaded the product list...")

def product_list_drop_col_one():
    df_product_list = pd.read_parquet("/opt/airflow/dimensions/Product Dimension/product_list_load.parquet")
    # drop 1st column
    df_product_list = df_product_list.drop(df_product_list.columns[0], axis=1)
    df_product_list.to_parquet("/opt/airflow/dimensions/Product Dimension/product_list_drop_col_one.parquet")
    print("Successfully dropped 1st column...")

def product_list_name_to_title():
    df_product_list = pd.read_parquet("/opt/airflow/dimensions/Product Dimension/product_list_drop_col_one.parquet")
    
    # capitalize product_name
    df_product_list['product_name'] = df_product_list['product_name'].str.title()
    
    df_product_list.to_parquet("/opt/airflow/dimensions/Product Dimension/product_list_name_to_title.parquet")
    print("Successfully modified the product_name...")

def product_list_standardized_type():
    df_product_list = pd.read_parquet("/opt/airflow/dimensions/Product Dimension/product_list_name_to_title.parquet")
    
    # standardize product_type
    type_mapping = {
        'cosmetic': 'cosmetics',
        'technology': 'electronics and technology',
        'toolss': 'tools',
        'school supplies' : 'stationary and school supplies',
        'stationary' : 'stationary and school supplies',
    }
    df_product_list['product_type'] = df_product_list['product_type'].replace(type_mapping)
    
    df_product_list.to_parquet("/opt/airflow/dimensions/Product Dimension/product_list_standardized_type.parquet")
    print("Product type is successfully modified...")

def product_list_remove_underscore():
    df_product_list = pd.read_parquet("/opt/airflow/dimensions/Product Dimension/product_list_standardized_type.parquet")
    
    #remove - in product_type
    df_product_list['product_type'] = df_product_list['product_type'].str.replace('_', ' ')
    
    df_product_list.to_parquet("/opt/airflow/dimensions/Product Dimension/product_list_remove_underscore.parquet")
    print("Successfully removed underscore...")

def product_list_fill_NaN():
    df_product_list = pd.read_parquet("/opt/airflow/dimensions/Product Dimension/product_list_remove_underscore.parquet")
    
    #replace NaN with 'others' in product_type
    df_product_list['product_type'].fillna('others', inplace=True)
    
    df_product_list.to_parquet("/opt/airflow/dimensions/Product Dimension/product_list_fill_NaN.parquet")
    print("Successfully filled NaN records...")

def product_list_drop_dups():
    df_product_list = pd.read_parquet("/opt/airflow/dimensions/Product Dimension/product_list_fill_NaN.parquet")
    
    # drop duplicates after cleaning if any
    df_product_list.drop_duplicates()
    
    df_product_list.to_parquet("/opt/airflow/dimensions/Product Dimension/product_list_drop_dups.parquet")
    print("Successfully dropped duplicates...")

def product_list_keep_first_names():
    df_product_list = pd.read_parquet("/opt/airflow/dimensions/Product Dimension/product_list_drop_dups.parquet")
    
    # drop duplicated product_names. keep first occurence
    df_product_list = df_product_list.drop_duplicates(subset=['product_name'], keep='first')
    
    df_product_list.to_parquet("/opt/airflow/dimensions/Product Dimension/product_list_keep_first_names.parquet")
    print("Successfully dropped names that are repeating...")

def product_list_modify_product_id():
    df_product_list = pd.read_parquet("/opt/airflow/dimensions/Product Dimension/product_list_keep_first_names.parquet")
    
    # add one in product_id number for duplicated product_ids detected
    duplicates = df_product_list.duplicated(subset='product_id', keep='first') | df_product_list.duplicated(subset='product_id', keep='last')
    occurrences = {}
    for index, row in df_product_list[duplicates].iterrows():
        original_id = row['product_id']
        
        if original_id not in occurrences:
            occurrences[original_id] = 1
        else:
            occurrences[original_id] += 1
            df_product_list.at[index, 'product_id'] = original_id[:-5] + str(int(original_id[-5:]) + 1).zfill(5)
    
    df_product_list.to_parquet("/opt/airflow/dimensions/Product Dimension/product_list_modify_product_id.parquet")
    print("Successfully modified product ids...")

def product_list_modify_two_decimal_price():
    df_product_list = pd.read_parquet("/opt/airflow/dimensions/Product Dimension/product_list_modify_product_id.parquet")
    
    # Format the 'price' column as strings with two decimal places and trailing zeros
    df_product_list['price'] = df_product_list['price'].round(2)
    df_product_list['price'] = df_product_list['price'].apply(lambda x: '{:.2f}'.format(x))
    
    df_product_list.to_parquet("/opt/airflow/dimensions/Product Dimension/product_list_two_decimal_price.parquet")
    print("Successfully modified prices to have 2 decimal places...")

def product_list_edit_col_names():
    df_product_list = pd.read_parquet("/opt/airflow/dimensions/Product Dimension/product_list_two_decimal_price.parquet")
    
    df_product_list = df_product_list.rename(columns={'product_id': 'PRODUCT_ID', 'product_name': 'PRODUCT_NAME', 'product_type': 'PRODUCT_TYPE', 'price': 'PRODUCT_PRICE'})
        
    df_product_list.to_parquet("/opt/airflow/dimensions/Dimensional Model/product_dimension.parquet")
    print("Successfully created Product Dimension...")

def product_dimension_to_db():
    # Step 1: Read Parquet File
    df = pd.read_parquet("/opt/airflow/dimensions/Dimensional Model/product_dimension.parquet")

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
        'product_dimension',
        metadata,
        Column('PRODUCT_ID', VARCHAR(255)),
        Column('PRODUCT_NAME', VARCHAR(255)),
        Column('PRODUCT_TYPE', VARCHAR(255)),
        Column('PRODUCT_PRICE', FLOAT),
    )
    metadata.create_all(engine, checkfirst=True)

    # Optional: Close the Connection (Not necessary if running as a script, but good practice)
    engine.dispose()

    # Step 7: Insert Data into MySQL
    df.to_sql(name='product_dimension', con=engine, if_exists='replace', index=False)
    print("Inserted into DB")

args = {
    'owner': 'Group 1',
    'start_date': days_ago(0),
}

dag = DAG(
    dag_id = 'product_pipeline',
    default_args = args,
    schedule_interval = '@hourly',
    max_active_runs = 1,
)

with dag:
    # product list
    pl_load = PythonOperator(
        task_id = 'product_list_load',
        python_callable = product_list_load
    )

    pl_drop_col_one = PythonOperator(
        task_id = 'product_list_drop_col_one',
        python_callable = product_list_drop_col_one
    )

    pl_name_to_title = PythonOperator(
        task_id = 'product_list_name_to_title',
        python_callable = product_list_name_to_title
    )

    pl_standardized_type = PythonOperator(
        task_id = 'product_list_standardized_type',
        python_callable = product_list_standardized_type
    )

    pl_remove_underscore = PythonOperator(
        task_id = 'product_list_remove_underscore',
        python_callable = product_list_remove_underscore
    )

    pl_fill_Nan = PythonOperator(
        task_id = 'product_list_fill_NaN',
        python_callable = product_list_fill_NaN
    )

    pl_drop_dups = PythonOperator(
        task_id = 'product_list_drop_dups',
        python_callable = product_list_drop_dups
    )

    pl_keep_first_names = PythonOperator(
        task_id = 'product_list_keep_first_names',
        python_callable = product_list_keep_first_names
    )

    pl_modify_product_id = PythonOperator(
        task_id = 'product_list_modify_product_id',
        python_callable = product_list_modify_product_id
    )

    pl_modify_two_decimal_price = PythonOperator(
        task_id = 'product_list_modify_two_decimal_price',
        python_callable = product_list_modify_two_decimal_price
    )

    pl_edit_col_names = PythonOperator(
        task_id = 'product_list_edit_col_names',
        python_callable = product_list_edit_col_names
    )

    pd_to_db = PythonOperator(
        task_id = 'product_dimension_to_db',
        python_callable = product_dimension_to_db
    )

pl_load >> pl_drop_col_one >> pl_name_to_title >> pl_standardized_type >> pl_remove_underscore >> pl_fill_Nan >> pl_drop_dups >> pl_keep_first_names >> pl_modify_product_id >> pl_modify_two_decimal_price >> pl_edit_col_names >> pd_to_db