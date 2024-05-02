from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime
import requests
import mysql.connector
from sqlalchemy import create_engine, MetaData, Table, Column, VARCHAR, DATETIME, DATE, FLOAT, INT


import pandas as pd
import numpy as np
from unidecode import unidecode


#user_data
def user_data_load_database():
    df_user_data = pd.read_json("/opt/airflow/departments/Customer Management Department/user_data.json")
    df_user_data.to_parquet("/opt/airflow/dimensions/User Dimension/user_data_load_database.parquet") 
    print("Successfully loaded the user data database...")

def user_data_replace_duplicated_values():
    data = pd.read_parquet("/opt/airflow/dimensions/User Dimension/user_data_load_database.parquet")
    def replace_duplicated_ids(df):
        # Extract numbers from 'user_id' and convert to set
        used_numbers = set(df['user_id'].str.extract('(\d+)')[0])
    
        all_numbers = set([str(i).zfill(5) for i in range(10000, 100000)])
        unused_numbers = all_numbers - used_numbers
        unused_numbers_list = list(unused_numbers)
    
        # Find indices of duplicated 'user_id'
        duplicated_user_id = df['user_id'].duplicated(keep='first')
        dup_indices = df[duplicated_user_id].index
    
        if len(unused_numbers_list) < len(dup_indices):
            print("There are not enough unused numbers to replace all duplicates.")
        else:
            # Replace the duplicated 'user_id' with the unused numbers
            for i in range(len(dup_indices)):
                old_id = df.loc[dup_indices[i], 'user_id']
                new_num = unused_numbers_list[i]
                new_id = old_id[:-len(new_num)] + new_num
                df.loc[dup_indices[i], 'user_id'] = new_id

        print("All duplicated 'user_id's have been replaced with unused numbers.")

    replace_duplicated_ids(data)
    print(data['user_id'].is_unique)

    data.to_parquet("/opt/airflow/dimensions/User Dimension/cleaned_user_data_standardized_format.parquet")
    print("Succesfully saved the user data")

def user_data_changeTo_datetime():
    df_user_data = pd.read_parquet("/opt/airflow/dimensions/User Dimension/cleaned_user_data_standardized_format.parquet")
    
    # convert creation_date & birthdate to datetime data types
    df_user_data['creation_date'] = pd.to_datetime(df_user_data['creation_date'])
    df_user_data['birthdate'] = pd.to_datetime(df_user_data['birthdate'])
    df_user_data.to_parquet("/opt/airflow/dimensions/User Dimension/user_data_changeTo_datetime.parquet")
    print("Successfully changed to datetime data type of user data...")

def user_data_standardize_street():
    df_user_data = pd.read_parquet("/opt/airflow/dimensions/User Dimension/user_data_changeTo_datetime.parquet") #change file path
    
    #standardize 'street' to 00000 First Second Third 
    def standardize_street(street):
        parts = street.split()
        formatted_numbers = [f"{int(num):05d}" if num.isdigit() else num for num in parts]
        formatted_street = ' '.join(word.capitalize() for word in formatted_numbers)
        return formatted_street
    
    # Apply the function to the 'street' column
    df_user_data['street'] = df_user_data['street'].apply(standardize_street)
    
    # replace Louiseville/Jefferson to Louiseville
    df_user_data['city'] = df_user_data['city'].replace('Louisville/Jefferson', 'Louisville')
    df_user_data.to_parquet("/opt/airflow/dimensions/User Dimension/user_data_standardize_street.parquet")
    print("Successfully standardized street of user data...")

def user_data_replace_special_char():
    df_user_data = pd.read_parquet("/opt/airflow/dimensions/User Dimension/user_data_standardize_street.parquet") #change file path

    #replace special characters with regular
    df_user_data['country'] = df_user_data['country'].apply(unidecode)
    
    # remove parentheses
    df_user_data['country'] = df_user_data['country'].str.replace(r"\s*\(.*\)", "", regex=True)
    
    # correct Moldova format
    replace_dict = {
        'Moldova, Republic of': 'Republic of Moldova',
        'Tanzania, United Republic of': 'United Republic of Tanzania',
        'Taiwan, Province of China': 'Taiwan',
        'Palestine, State of': 'State of Palestine',
        'Korea, Republic of': 'Korea',
        'Congo, Democratic Republic of the': 'Congo'
    }
    df_user_data['country'] = df_user_data['country'].replace(replace_dict)
    
    # Replace specified country names
    df_user_data['country'] = df_user_data['country'].replace(replace_dict)
    df_user_data.to_parquet("/opt/airflow/dimensions/User Dimension/user_data_replace_special_char.parquet")
    print("Successfully replaced special characters of user data...")

def user_data_drop_duplicates():

    df_user_data = pd.read_parquet("/opt/airflow/dimensions/User Dimension/user_data_replace_special_char.parquet") #change file path

    # drop duplicates after cleaning if any
    df_user_data.drop_duplicates()
    
    df_user_data.to_parquet("/opt/airflow/dimensions/User Dimension/user_data_standardized_format.parquet")
    print("Successfully standardized the user data...")

#user_credit_card
def user_credit_card_load_database():
    df_user_credit_card = pd.read_pickle("/opt/airflow/departments/Customer Management Department/user_credit_card.pickle")
    df_user_credit_card.to_parquet("/opt/airflow/dimensions/User Dimension/user_credit_card.parquet")
    print("Successfully loaded the user credit card database...")

def user_credit_card_replace_duplicated_values():
    credit_card = pd.read_parquet("/opt/airflow/dimensions/User Dimension/user_credit_card.parquet")
    def replace_duplicated_ids(df):
        # Extract numbers from 'user_id' and convert to set
        used_numbers = set(df['user_id'].str.extract('(\d+)')[0])
    
        all_numbers = set([str(i).zfill(5) for i in range(10000, 100000)])
        unused_numbers = all_numbers - used_numbers
        unused_numbers_list = list(unused_numbers)
    
        # Find indices of duplicated 'user_id'
        duplicated_user_id = df['user_id'].duplicated(keep='first')
        dup_indices = df[duplicated_user_id].index
    
        if len(unused_numbers_list) < len(dup_indices):
            print("There are not enough unused numbers to replace all duplicates.")
        else:
            # Replace the duplicated 'user_id' with the unused numbers
            for i in range(len(dup_indices)):
                old_id = df.loc[dup_indices[i], 'user_id']
                new_num = unused_numbers_list[i]
                new_id = old_id[:-len(new_num)] + new_num
                df.loc[dup_indices[i], 'user_id'] = new_id

        print("All duplicated 'user_id's have been replaced with unused numbers.")

    replace_duplicated_ids(credit_card)
    print(credit_card['user_id'].is_unique)

    credit_card.to_parquet("/opt/airflow/dimensions/User Dimension/cleaned_user_credit_card_standardized_format.parquet")
    print("Succesfully saved the user credit card data")

def user_credit_card_standardize_creditCard_number():
    df_user_credit_card = pd.read_parquet("/opt/airflow/dimensions/User Dimension/cleaned_user_credit_card_standardized_format.parquet")
    
    # standardize credit_card_number by appending 0s to the entries until there are 10 digits
    df_user_credit_card['credit_card_number'] = df_user_credit_card['credit_card_number'].apply(lambda x: str(x).zfill(10))
    df_user_credit_card.to_parquet("/opt/airflow/dimensions/User Dimension/user_credit_card_standardized_creditCard_number.parquet")
    print("Successfully standardized the credit card number...")

def user_credit_card_drop_duplicates():
    df_user_credit_card = pd.read_parquet("/opt/airflow/dimensions/User Dimension/user_credit_card_standardized_creditCard_number.parquet")
    
   # drop duplicates after cleaning if any
    df_user_credit_card.drop_duplicates()
    df_user_credit_card.to_parquet("/opt/airflow/dimensions/User Dimension/user_credit_card_standardized_format.parquet")
    print("Successfully dropped duplicates again...")

#user_job
def user_job_load_database():
    df_user_job = pd.read_csv("/opt/airflow/departments/Customer Management Department/user_job.csv")
    df_user_job.to_parquet("/opt/airflow/dimensions/User Dimension/user_job_load_database.parquet")
    print("Successfully loaded the user data database...")

def user_job_replace_duplicated_values():
    job = pd.read_parquet("/opt/airflow/dimensions/User Dimension/user_job_load_database.parquet")
    def replace_duplicated_ids(df):
        # Extract numbers from 'user_id' and convert to set
        used_numbers = set(df['user_id'].str.extract('(\d+)')[0])
    
        all_numbers = set([str(i).zfill(5) for i in range(10000, 100000)])
        unused_numbers = all_numbers - used_numbers
        unused_numbers_list = list(unused_numbers)
    
        # Find indices of duplicated 'user_id'
        duplicated_user_id = df['user_id'].duplicated(keep='first')
        dup_indices = df[duplicated_user_id].index
    
        if len(unused_numbers_list) < len(dup_indices):
            print("There are not enough unused numbers to replace all duplicates.")
        else:
            # Replace the duplicated 'user_id' with the unused numbers
            for i in range(len(dup_indices)):
                old_id = df.loc[dup_indices[i], 'user_id']
                new_num = unused_numbers_list[i]
                new_id = old_id[:-len(new_num)] + new_num
                df.loc[dup_indices[i], 'user_id'] = new_id

        print("All duplicated 'user_id's have been replaced with unused numbers.")

    replace_duplicated_ids(job)
    print(job['user_id'].is_unique)

    job.to_parquet("/opt/airflow/dimensions/User Dimension/cleaned_user_job_standardized_format.parquet")
    print("Succesfully saved the user job data")

def user_job_drop_column1():
    df_user_job = pd.read_parquet("/opt/airflow/dimensions/User Dimension/cleaned_user_job_standardized_format.parquet")

    # drop 1st column
    df_user_job = df_user_job.drop(df_user_job.columns[0], axis=1)
    df_user_job.to_parquet("/opt/airflow/dimensions/User Dimension/user_job_drop_column1.parquet")
    print("Successfully dropped unnamed column of the user data...")

def user_job_no_null_job_title():
    df_user_job = pd.read_parquet("/opt/airflow/dimensions/User Dimension/user_job_drop_column1.parquet")

    #replace NaN in job_level with'Student' if job_title =='Student' 
    df_user_job.loc[(df_user_job['job_title'] == 'Student') & (df_user_job['job_level'].isna()), 'job_level'] = 'Student'
    df_user_job.to_parquet("/opt/airflow/dimensions/User Dimension/user_job_no_null_job_title.parquet")
    print("Successfully remove null in job title of the user data...")

def user_job_drop_duplicates():
    df_user_job = pd.read_parquet("/opt/airflow/dimensions/User Dimension/user_job_no_null_job_title.parquet")

   # drop duplicates after cleaning if any
    df_user_job.drop_duplicates()
    df_user_job.to_parquet("/opt/airflow/dimensions/User Dimension/user_job_standardized_format.parquet")
    print("Successfully standardized the user data...")

def user_dimension_merge():
    data = pd.read_parquet("/opt/airflow/dimensions/User Dimension/user_data_standardized_format.parquet")
    credit_card = pd.read_parquet("/opt/airflow/dimensions/User Dimension/user_credit_card_standardized_format.parquet")
    job = pd.read_parquet("/opt/airflow/dimensions/User Dimension/user_job_standardized_format.parquet")

    user_dimension = pd.merge(data, credit_card, on=['user_id', 'name'])
    user_dimension = pd.merge(user_dimension, job, on=['user_id', 'name'])

    user_dimension = user_dimension.rename(columns= {'user_id': 'USER_ID', 'name': 'USER_NAME','credit_card_number': 'USER_CREDIT_CARD_NUMBER', 'issuing_bank': 'USER_ISSUING_BANK', 'creation_date': 'USER_CREATION_DATE', 'street': 'USER_STREET', 'state': 'USER_STATE','city':'USER_CITY', 'country': 'USER_COUNTRY','birthdate': 'USER_BIRTHDATE','gender': 'USER_GENDER','device_address': 'USER_DEVICE_ADDRESS','user_type': 'USER_TYPE','job_title': 'USER_JOB_TITLE','job_level': 'USER_JOB_LEVEL'})

    user_dimension.to_parquet('/opt/airflow/dimensions/Dimensional Model/user_dimension.parquet')
    print("Succesfully saved the user dimension")

def user_dimension_to_db():
    # Step 1: Read Parquet File
    df = pd.read_parquet("/opt/airflow/dimensions/Dimensional Model/user_dimension.parquet")

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
        'user_dimension',
        metadata,
        Column('USER_ID', VARCHAR(255)),
        Column('USER_CREATION_DATE', DATETIME),
        Column('USER_NAME', VARCHAR(255)),
        Column('USER_STREET', VARCHAR(255)),
        Column('USER_STATE', VARCHAR(255)),
        Column('USER_CITY', VARCHAR(255)),
        Column('USER_COUNTRY', VARCHAR(255)),
        Column('USER_BIRTHDATE', DATETIME),
        Column('USER_GENDER', VARCHAR(255)),
        Column('USER_DEVICE_ADDRESS', VARCHAR(255)),
        Column('USER_TYPE', VARCHAR(255)),
        Column('USER_CREDIT_CARD_NUMBER', INT),
        Column('USER_ISSUING_BANK', VARCHAR(255)),
        Column('USER_JOB_TITLE', VARCHAR(255)),
        Column('USER_JOB_LEVEL', VARCHAR(255)),
    )
    metadata.create_all(engine, checkfirst=True)

    # Optional: Close the Connection (Not necessary if running as a script, but good practice)
    engine.dispose()

    # Step 7: Insert Data into MySQL
    df.to_sql(name='user_dimension', con=engine, if_exists='replace', index=False)
    print("Inserted into DB")

args = {
    'owner': 'Group 1',
    'start_date': days_ago(0),
}

dag = DAG(
    dag_id = 'user_pipeline',
    default_args = args,
    schedule_interval = '@hourly',
    max_active_runs = 1,
)

with dag:
    # user_data
    ud_load_database = PythonOperator(
        task_id = 'user_data_load_database',
        python_callable = user_data_load_database
    )

    ud_replace_duplicated_values = PythonOperator(
        task_id = 'user_data_replace_duplicated_values',
        python_callable = user_data_replace_duplicated_values
    )

    ud_changeTo_datetime = PythonOperator(
        task_id = 'user_data_changeTo_datetime',
        python_callable = user_data_changeTo_datetime
    )

    ud_standardize_street = PythonOperator(
        task_id = 'user_data_standardize_street',
        python_callable = user_data_standardize_street
    )

    ud_replace_special_char = PythonOperator(
        task_id = 'user_data_replace_special_char',
        python_callable = user_data_replace_special_char
    )

    ud_drop_duplicates = PythonOperator(
        task_id = 'user_data_drop_duplicates',
        python_callable = user_data_drop_duplicates
    )

    # user_credit_card
    ucc_load_database = PythonOperator(
        task_id = 'user_credit_card_load_database',
        python_callable = user_credit_card_load_database
    )

    ucc_replace_duplicated_values = PythonOperator(
        task_id = 'user_credit_card_replace_duplicated_values',
        python_callable = user_credit_card_replace_duplicated_values
    )

    ucc_standardize_creditCard_number = PythonOperator(
        task_id = 'user_credit_card_standardize_creditCard_number',
        python_callable = user_credit_card_standardize_creditCard_number
    )

    ucc_drop_duplicates = PythonOperator(
        task_id = 'user_credit_card_drop_duplicates',
        python_callable = user_credit_card_drop_duplicates
    )

    # user_job
    uj_load_database = PythonOperator(
        task_id = 'user_job_load_database',
        python_callable = user_job_load_database
    )

    uj_replace_duplicated_values = PythonOperator(
        task_id = 'user_job_replace_duplicated_values',
        python_callable = user_job_replace_duplicated_values
    )

    uj_drop_column1 = PythonOperator(
        task_id = 'user_job_drop_column1',
        python_callable = user_job_drop_column1
    )

    uj_no_null_job_title = PythonOperator(
        task_id = 'user_job_no_null_job_title',
        python_callable = user_job_no_null_job_title
    )

    uj_drop_duplicates = PythonOperator(
        task_id = 'user_job_drop_duplicates',
        python_callable = user_job_drop_duplicates
    )

    #user_dimension
    ud_merge = PythonOperator(
        task_id = 'user_dimension_merge',
        python_callable = user_dimension_merge
    )

    ud_to_db = PythonOperator(
        task_id = 'user_dimension_to_db',
        python_callable = user_dimension_to_db
    )

ud_load_database >> ud_replace_duplicated_values  >> ud_changeTo_datetime >> ud_standardize_street >> ud_replace_special_char >> ud_drop_duplicates >> ucc_load_database >> ucc_replace_duplicated_values >> ucc_standardize_creditCard_number >> ucc_drop_duplicates >> uj_load_database >> uj_replace_duplicated_values >> uj_drop_column1 >> uj_no_null_job_title >> uj_drop_duplicates >> ud_merge >> ud_to_db