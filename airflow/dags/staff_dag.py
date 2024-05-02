from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago
import pandas as pd
import numpy as np
import psycopg2
import mysql.connector
from sqlalchemy import create_engine, MetaData, Table, Column, VARCHAR, DATETIME, DATE, FLOAT, INT
#from pendulum import timezone

#local_tz = timezone("Asia/Manila")  # Replace with your actual time zone

default_args = {
    'owner': 'Group 1',
    'start_date': days_ago(0),
    #'retries': 1,
    #'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'staff_pipeline',
    default_args=default_args,
    description='A DAG to execute multiple Python scripts',
    start_date= days_ago(0),
    schedule_interval = '@hourly',
    max_active_runs = 1,
)

def staff_load_database():
        
        df_staff_data = pd.read_html("/opt/airflow/departments/Enterprise Department/staff_data.html")[0] # Insert full path.

        df_staff_data.to_parquet("/opt/airflow/dimensions/Staff Dimension/staff_data.parquet")

        print("Successfully loaded the staff data database...")

staff_load_database = PythonOperator(
    task_id='staff_load_database',
    python_callable=staff_load_database,
    dag=dag,
)


def staff_drop_unnamed_column():


        df_staff_data = pd.read_parquet("/opt/airflow/dimensions/Staff Dimension/staff_data.parquet") #change file path

    # drop 1st column
        df_staff_data = df_staff_data.drop(df_staff_data.columns[0], axis=1)
    
        df_staff_data.to_parquet("/opt/airflow/dimensions/Staff Dimension/staff_data_drop_column1.parquet")
    
        print("Successfully dropped unnamed column of the staff data...")

staff_drop_unnamed_column = PythonOperator(
    task_id='staff_drop_unnamed_column',
    python_callable=staff_drop_unnamed_column,
    dag=dag,
)

def staff_drop_duplicates():

        df_staff_data = pd.read_parquet("/opt/airflow/dimensions/Staff Dimension/staff_data_drop_column1.parquet") #change file path

        df_staff_data = df_staff_data.drop_duplicates(subset='staff_id')
    
        df_staff_data.to_parquet("/opt/airflow/dimensions/Staff Dimension/staff_data_drop_duplicate_staffID.parquet")
    
        print("Successfully dropped duplicated staff id in staff data...")

staff_drop_duplicates = PythonOperator(
    task_id='staff_drop_duplicates',
    python_callable=staff_drop_duplicates,
    dag=dag,
)

def staff_date_to_datetime():

        df_staff_data = pd.read_parquet("/opt/airflow/dimensions/Staff Dimension/staff_data_drop_duplicate_staffID.parquet") #change file path

        df_staff_data['creation_date'] = pd.to_datetime(df_staff_data['creation_date'])
    
        df_staff_data.to_parquet("/opt/airflow/dimensions/Staff Dimension/staff_data_changeTo_datetime.parquet")
    
        print("Successfully changed creation date to datetime in staff data...")

staff_date_to_datetime = PythonOperator(
    task_id='staff_date_to_datetime',
    python_callable=staff_date_to_datetime,
    dag=dag,
)

def staff_standarize_street():
    df_staff_data = pd.read_parquet("/opt/airflow/dimensions/Staff Dimension/staff_data_changeTo_datetime.parquet")
    def staff_data_standardize_street(street):
    
        parts = street.split()
        formatted_numbers = [f"{int(num):05d}" if num.isdigit() else num for num in parts]
        formatted_street = ' '.join(word.capitalize() for word in formatted_numbers)
        return formatted_street

# Load the DataFrame outside the function


# Apply the function to standardize the street
    df_staff_data['street'] = df_staff_data['street'].apply(staff_data_standardize_street)
    df_staff_data['city'] = df_staff_data['city'].replace('Louisville/Jefferson', 'Louisville')
    df_staff_data['country'] = df_staff_data['country'].apply(lambda x: x.encode('latin1').decode('utf8'))
    df_staff_data['country'] = df_staff_data['country'].str.replace(r"\s*\(.*\)", "", regex=True)
    df_staff_data['country'] = df_staff_data['country'].replace('Moldova, Republic of', 'Republic of Moldova')

    df_staff_data.to_parquet("/opt/airflow/dimensions/Staff Dimension/staff_data_standardized_address.parquet")

    print("Successfully standardized street in staff data...")

staff_standarize_street = PythonOperator(
    task_id='staff_standarize_street',
    python_callable=staff_standarize_street,
    dag=dag,
)

def staff_fix_contact_number_format():
        
    df_staff_data = pd.read_parquet("/opt/airflow/dimensions/Staff Dimension/staff_data_standardized_address.parquet") #change file path

# Remove non-digit characters
    df_staff_data['contact_number'] = df_staff_data['contact_number'].str.replace(r'\D', '', regex=True)

# Remove leading '1' if it exists
    df_staff_data['contact_number'] = df_staff_data['contact_number'].str.replace(r'^1', '', regex=True)

# Add '+' to the start
    df_staff_data['contact_number'] = '+' + df_staff_data['contact_number']
    df_staff_data['contact_number'].unique()

    df_staff_data.to_parquet("/opt/airflow/dimensions/Staff Dimension/staff_data_standardized_contact_number.parquet")
    
    print("Successfully standardized contact number in staff data...")

staff_fix_contact_number_format = PythonOperator(
    task_id='staff_fix_contact_number_format',
    python_callable=staff_fix_contact_number_format,
    dag=dag,
)

def staff_data_standardize_country():
    df_staff_data = pd.read_parquet("/opt/airflow/dimensions/Staff Dimension/staff_data_standardized_contact_number.parquet") #change file path

    replace_dict = {
        'Tanzania, United Republic of': 'United Republic of Tanzania',
        'Taiwan, Province of China': 'Taiwan',
        'Palestine, State of': 'State of Palestine',
        'Korea, Republic of': 'Korea',
        'Congo, Democratic Republic of the': 'Congo'
    }

    # Replace specified country names
    df_staff_data['country'] = df_staff_data['country'].replace(replace_dict)
    df_staff_data.to_parquet("/opt/airflow/dimensions/Staff Dimension/staff_data_standardize_country.parquet")

print("Successfully standardized country in staff data...")

staff_data_standardize_country = PythonOperator(
    task_id='staff_data_standardize_country',
    python_callable=staff_data_standardize_country,
    dag=dag,
)

def staff_edit_col_names():
    df_staff_data = pd.read_parquet("/opt/airflow/dimensions/Staff Dimension/staff_data_standardize_country.parquet") # Insert full path.

# Change Column names
    df_staff_data = df_staff_data.rename(columns={'staff_id': 'STAFF_ID', 'name': 'STAFF_NAME', 'job_level': 'STAFF_JOB_LEVEL', 'street': 'STAFF_STREET', 'state': 'STAFF_STATE','city': 'STAFF_CITY', 'country': 'STAFF_COUNTRY', 'contact_number': 'STAFF_CONTACT_NUMBER', 'creation_date': 'STAFF_CREATION_DATE'})
    df_staff_data.to_parquet("/opt/airflow/dimensions/Dimensional Model/staff_dimension.parquet")
    print("Successfully saved the staff dimension...")

staff_edit_col_names = PythonOperator(
    task_id='staff_edit_col_names',
    python_callable=staff_edit_col_names,
    dag=dag,
)


def staff_dim_to_db():

# Step 1: Read Parquet File
    parquet_file_path = "/opt/airflow/dimensions/Dimensional Model/staff_dimension.parquet"
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
        'staff_dimension',
        metadata,
        Column('STAFF_ID', VARCHAR(255)),
        Column('STAFF_NAME', VARCHAR(255)),
        Column('STAFF_JOB_LEVEL', VARCHAR(255)),
        Column('STAFF_STREET', VARCHAR(255)),
        Column('STAFF_STATE', VARCHAR(255)),
        Column('STAFF_CITY', VARCHAR(255)),
        Column('STAFF_COUNTRY', VARCHAR(255)),
        Column('STAFF_CONTACT_NUMBER', VARCHAR(255)),
        Column('STAFF_CREATION_DATE', DATETIME)
    )

    metadata.create_all(engine, checkfirst=True)

# Optional: Close the Connection (Not necessary if running as a script, but good practice)
    engine.dispose()

# Step 4: Insert Data into MySQL
    df.to_sql(name='staff_dimension', con=engine, if_exists='replace', index=False)
    print("Inserted into DB")

staff_dim_to_db = PythonOperator(
        task_id='staff_dim_to_db',
        python_callable=staff_dim_to_db,
        dag=dag,
)

"""def execute_fourteen_script():

    host = 'host.docker.internal'
    user = 'root'
    password = 'root'

connection = mysql.connector.connect(
    host=host,
    user=user,
    password=password
)

database_name = 'DWFinalProj'
engine = create_engine(f"mysql+mysqlconnector://{user}:{password}@{host}/{database_name}")

create_view_query = 
    CREATE OR REPLACE VIEW my_view AS
    SELECT
        YEAR(avail_date) AS year,
        WEEK(avail_date, 1) AS week,
        service,
        SUM(price) AS total_price
    FROM
        tabletrial2
    GROUP BY
        YEAR(avail_date),
        WEEK(avail_date, 1),
        service
    ORDER BY
        year ASC, week ASC;


with engine.connect() as connection:
        result = connection.execute(text(create_view_query))

print("View created")

execute_fourteen_script_task = PythonOperator(
    task_id='execute_fourteen_script',
    python_callable=execute_fourteen_script,
    dag=dag,
)"""

# Set up task dependencies
staff_load_database >> staff_drop_unnamed_column >> staff_drop_duplicates >> staff_date_to_datetime >> staff_standarize_street >> staff_fix_contact_number_format >> staff_data_standardize_country >> staff_edit_col_names >> staff_dim_to_db