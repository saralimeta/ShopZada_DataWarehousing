from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import pandas as pd
import numpy as np
import psycopg2
from sqlalchemy import create_engine, text  # Include 'text' in your import
#from pendulum import timezone

#local_tz = timezone("Asia/Manila")  # Replace with your actual time zone

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 12, 10),
    #'retries': 1,
    #'retry_delay': timedelta(minutes=5),
}



dag = DAG(
    'StaffDag',
    default_args=default_args,
    description='A DAG to execute multiple Python scripts',
    start_date= datetime(2023, 12, 10),
    #schedule_interval='@hourly',  # You can adjust the schedule interval as needed
    #catchup = False
)

def staff_task1():
        
        df_staff_data = pd.read_html("/opt/airflow/dags/staff_data.html")[0] # Insert full path.

        df_staff_data.to_parquet("/opt/airflow/dags/staff_data.parquet")

        print("Successfully loaded the staff data database...")

staff_task1 = PythonOperator(
    task_id='staff_task1',
    python_callable=staff_task1,
    dag=dag,
)


def staff_task2():


        df_staff_data = pd.read_parquet("/opt/airflow/dags/staff_data.parquet") #change file path

    # drop 1st column
        df_staff_data = df_staff_data.drop(df_staff_data.columns[0], axis=1)
    
        df_staff_data.to_parquet("/opt/airflow/dags/staff_data_drop_column1.parquet")
    
        print("Successfully dropped unnamed column of the staff data...")

staff_task2 = PythonOperator(
    task_id='staff_task2',
    python_callable=staff_task2,
    dag=dag,
)

def staff_task3():

        df_staff_data = pd.read_parquet("/opt/airflow/dags/staff_data_drop_column1.parquet") #change file path

        df_staff_data = df_staff_data.drop_duplicates(subset='staff_id')
    
        df_staff_data.to_parquet("/opt/airflow/dags/staff_data_drop_duplicate_staffID.parquet")
    
        print("Successfully dropped duplicated staff id in staff data...")

staff_task3 = PythonOperator(
    task_id='staff_task3',
    python_callable=staff_task3,
    dag=dag,
)

def staff_task4():

        df_staff_data = pd.read_parquet("/opt/airflow/dags/staff_data_drop_duplicate_staffID.parquet") #change file path

        df_staff_data['creation_date'] = pd.to_datetime(df_staff_data['creation_date'])
    
        df_staff_data.to_parquet("/opt/airflow/dags/staff_data_changeTo_datetime.parquet")
    
        print("Successfully changed creation date to datetime in staff data...")

staff_task4 = PythonOperator(
    task_id='staff_task4',
    python_callable=staff_task4,
    dag=dag,
)

def staff_task5():
    df_staff_data = pd.read_parquet("/opt/airflow/dags/staff_data_changeTo_datetime.parquet")
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

    df_staff_data.to_parquet("/opt/airflow/dags/staff_data_standardized_address.parquet")

    print("Successfully standardized street in staff data...")

staff_task5 = PythonOperator(
    task_id='staff_task5',
    python_callable=staff_task5,
    dag=dag,
)

def staff_task6():
        
    df_staff_data = pd.read_parquet("/opt/airflow/dags/staff_data_standardized_address.parquet") #change file path

# Remove non-digit characters
    df_staff_data['contact_number'] = df_staff_data['contact_number'].str.replace(r'\D', '', regex=True)

# Remove leading '1' if it exists
    df_staff_data['contact_number'] = df_staff_data['contact_number'].str.replace(r'^1', '', regex=True)

# Add '+' to the start
    df_staff_data['contact_number'] = '+' + df_staff_data['contact_number']
    df_staff_data['contact_number'].unique()

    df_staff_data.to_parquet("/opt/airflow/dags/staff_data_standardized_contact_number.parquet")
    
    print("Successfully standardized contact number in staff data...")

staff_task6 = PythonOperator(
    task_id='staff_task6',
    python_callable=staff_task6,
    dag=dag,
)

def staff_data_standardize_country():
    df_staff_data = pd.read_parquet("/opt/airflow/dags/staff_data_standardized_contact_number.parquet") #change file path

    replace_dict = {
        'Tanzania, United Republic of': 'United Republic of Tanzania',
        'Taiwan, Province of China': 'Taiwan',
        'Palestine, State of': 'State of Palestine',
        'Korea, Republic of': 'Korea',
        'Congo, Democratic Republic of the': 'Congo'
    }

    # Replace specified country names
    df_staff_data['country'] = df_staff_data['country'].replace(replace_dict)
    df_staff_data.to_parquet("/opt/airflow/dags/staff_data_standardize_country.parquet")

print("Successfully standardized country in staff data...")

staff_data_standardize_country = PythonOperator(
    task_id='staff_data_standardize_country',
    python_callable=staff_data_standardize_country,
    dag=dag,
)

def staff_task8():
    df_staff_data = pd.read_parquet("/opt/airflow/dags/staff_data_standardize_country.parquet") # Insert full path.

# Change Column names
    df_staff_data = df_staff_data.rename(columns={'staff_id': 'STAFF_ID', 'name': 'STAFF_NAME', 'job_level': 'STAFF_JOB_LEVEL', 'street': 'STAFF_STREET', 'state': 'STAFF_STATE','city': 'STAFF_CITY', 'country': 'STAFF_COUNTRY', 'contact_number': 'STAFF_CONTACT_NUMBER', 'creation_date': 'STAFF_CREATION_DATE'})
    df_staff_data.to_parquet("/opt/airflow/dags/staff_dimension.parquet")
print("Successfully saved the staff dimension...")

staff_task8 = PythonOperator(
    task_id='staff_task8',
    python_callable=staff_task8,
    dag=dag,
)


def staff_dim_to_db():
    import pandas as pd
    import mysql.connector
    from sqlalchemy import create_engine, MetaData, Table, Column, VARCHAR, DATETIME, DATE, FLOAT

# Step 1: Read Parquet File
    parquet_file_path = "/opt/airflow/dags/staff_dimension.parquet"
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
        Column('txn_id', VARCHAR(255)),
        Column('avail_date', DATE),
        Column('last_name', VARCHAR(255)),
        Column('first_name', VARCHAR(255)),
        Column('birthday', DATE),
        Column('branch_name', VARCHAR(255)),
        Column('service', VARCHAR(255)),
        Column('price', FLOAT)
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
staff_task1 >> staff_task2 >> staff_task3 >> staff_task4 >> staff_task5 >> staff_task6 >> staff_data_standardize_country >> staff_task8 >> staff_dim_to_db