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
    'MerchantDag',
    default_args=default_args,
    description='A DAG to execute multiple Python scripts',
    start_date= datetime(2023, 12, 10),
    #schedule_interval='@hourly',  # You can adjust the schedule interval as needed
    #catchup = False
)

def merchant_taskOne():
    #convert last_name and first_name to uppercase
    df_merchant_data = pd.read_html("/opt/airflow/dags/merchant_data.html")[0] # Insert full path.

    mask = df_merchant_data['merchant_id'].str.contains('^MERCHANT\d{4}$')
    df_merchant_data.loc[mask, 'merchant_id'] = 'MERCHANT0' + df_merchant_data.loc[mask, 'merchant_id'].str[8:]
    print("Succesfully reformatted the 4 digits")

    df_merchant_data.to_parquet("/opt/airflow/dags/merchant_data_replace_4_digits.parquet")
    print("Succesfully saved the user data for task 1")

merchant_taskOne = PythonOperator(
    task_id='merchant_taskOne',
    python_callable=merchant_taskOne,
    dag=dag,
)

def merchant_task2():
    #convert last_name and first_name to uppercase
    df_merchant_data = pd.read_parquet("/opt/airflow/dags/merchant_data_replace_4_digits.parquet") # Insert full path.

    df_merchant_data = df_merchant_data.drop_duplicates(subset='merchant_id')
    print("Succesfully removed merchant ID duplicates")

    df_merchant_data.to_parquet("/opt/airflow/dags/merchant_data_remove_duplicates.parquet")
    print("Succesfully saved the user data for task 2")

merchant_task2 = PythonOperator(
    task_id='merchant_task2',
    python_callable=merchant_task2,
    dag=dag,
)

def merchant_task3():
    df_merchant_data = pd.read_parquet("/opt/airflow/dags/merchant_data_remove_duplicates.parquet") # Insert full path.

    df_merchant_data['creation_date'] = pd.to_datetime(df_merchant_data['creation_date'])
    print("Succesfully set merchant creation date to date object")

    df_merchant_data.to_parquet("/opt/airflow/dags/merchant_data_date_to_date_type.parquet")
    print("Succesfully saved the user data for task 3")

merchant_task3 = PythonOperator(
    task_id='merchant_task3',
    python_callable=merchant_task3,
    dag=dag,
)

def merchant_task4():
    df_merchant_data = pd.read_parquet("/opt/airflow/dags/merchant_data_date_to_date_type.parquet") # Insert full path

    def standardize_street(street):
        parts = street.split()
        formatted_numbers = [f"{int(num):05d}" if num.isdigit() else num for num in parts]
        formatted_street = ' '.join(word.capitalize() for word in formatted_numbers)
        return formatted_street

    df_merchant_data['street'] = df_merchant_data['street'].apply(standardize_street)
    print("Succesfully set merchant street to correct format")

    df_merchant_data.to_parquet("/opt/airflow/dags/merchant_data_street_format.parquet")
    print("Succesfully saved the merchant data for task 4")

merchant_task4 = PythonOperator(
    task_id='merchant_task4',
    python_callable=merchant_task4,
    dag=dag,
)


def merchant_task5():
    df_merchant_data = pd.read_parquet("/opt/airflow/dags/merchant_data_street_format.parquet") # Insert full path

    df_merchant_data['city'] = df_merchant_data['city'].replace('Louisville/Jefferson', 'Louisville')
    print("Succesfully set Louisville/Jefferson to Louisville")

    df_merchant_data.to_parquet("/opt/airflow/dags/merchant_data_ctiy_name.parquet")
    print("Succesfully saved the merchant data for task 5")

merchant_task5 = PythonOperator(
    task_id='merchant_task5',
    python_callable=merchant_task5,
    dag=dag,
)

def merchant_task6():
    df_merchant_data = pd.read_parquet("/opt/airflow/dags/merchant_data_ctiy_name.parquet") # Insert full path

    df_merchant_data['country'] = df_merchant_data['country'].apply(lambda x: x.encode('latin1').decode('utf8'))
    print("Succesfully reformatted city names with special characters")

    df_merchant_data['country'] = df_merchant_data['country'].str.replace(r"\s*\(.*\)", "", regex=True)
    print("Succesfully removed parenthesis & those inside the parenthesis of country names")

    df_merchant_data.to_parquet("/opt/airflow/dags/merchant_data_country_format.parquet")
    print("Succesfully saved the merchant data for task 4")

merchant_task6 = PythonOperator(
    task_id='merchant_task6',
    python_callable=merchant_task6,
    dag=dag,
)

def merchant_task7():
    df_merchant_data = pd.read_parquet("/opt/airflow/dags/merchant_data_country_format.parquet") # Insert full path

    df_merchant_data['contact_number'] = df_merchant_data['contact_number'].str.replace(r'\D', '', regex=True)
    df_merchant_data['contact_number'] = df_merchant_data['contact_number'].str.replace(r'^1', '', regex=True)
    df_merchant_data['contact_number'] = '+' + df_merchant_data['contact_number']
    print("Succesfully set merchant contact number to correct format")

    df_merchant_data.to_parquet("/opt/airflow/dags/merchant_data_fixed_contact.parquet")
    print("Succesfully saved the merchant data for task 7")

merchant_task7 = PythonOperator(
    task_id='merchant_task7',
    python_callable=merchant_task7,
    dag=dag,
)

def merchant_task8():
    df_merchant_data = pd.read_parquet("/opt/airflow/dags/merchant_data_fixed_contact.parquet") # Insert full path

    df_merchant_data['name'] = df_merchant_data['name'].str.replace('.com', '')  # Remove '.com'
    df_merchant_data['name'] = df_merchant_data['name'].str.replace(', Inc', ' Inc')  # Replace ', Inc' with ' Inc'
    print("Succesfully cleaned name")

    df_merchant_data.to_parquet("/opt/airflow/dags/merchant_data_fixed_name.parquet")
    print("Succesfully saved the merchant data for task 8")

merchant_task8 = PythonOperator(
    task_id='merchant_task8',
    python_callable=merchant_task8,
    dag=dag,
)

def merchant_task9():
    df_merchant_data = pd.read_parquet("/opt/airflow/dags/merchant_data_fixed_name.parquet") # Insert full path

    replace_dict = {
        'Tanzania, United Republic of': 'United Republic of Tanzania',
        'Taiwan, Province of China': 'Taiwan',
        'Palestine, State of': 'State of Palestine',
        'Korea, Republic of': 'Korea',
        'Congo, Democratic Republic of the': 'Congo'
}

# Replace specified country names
    df_merchant_data['country'] = df_merchant_data['country'].replace(replace_dict)
    print("Succesfully cleaned country again")

    df_merchant_data.to_parquet("/opt/airflow/dags/merchant_data_modified.parquet")
    print("Succesfully saved the merchant data for task 9")

merchant_task9 = PythonOperator(
    task_id='merchant_task9',
    python_callable=merchant_task9,
    dag=dag,
)

def order_with_merchant_modified():
    #convert last_name and first_name to uppercase
    df_order_with_merchant_data = pd.read_parquet("/opt/airflow/dags/order_with_merchant_data1.parquet")
    df_order_with_merchant_data2 = pd.read_parquet("/opt/airflow/dags/order_with_merchant_data2.parquet")
    df_order_with_merchant_data3 = pd.read_csv("/opt/airflow/dags/order_with_merchant_data3.csv")

    df_order_with_merchant_data = df_order_with_merchant_data.drop_duplicates()
    df_order_with_merchant_data2 = df_order_with_merchant_data2.drop_duplicates()
    df_order_with_merchant_data3 = df_order_with_merchant_data3.drop_duplicates()

    mask = df_order_with_merchant_data['merchant_id'].str.contains('^MERCHANT\d{4}$')
    df_order_with_merchant_data.loc[mask, 'merchant_id'] = 'MERCHANT0' + df_order_with_merchant_data.loc[mask, 'merchant_id'].str[8:]

    mask = df_order_with_merchant_data2['merchant_id'].str.contains('^MERCHANT\d{4}$')
    df_order_with_merchant_data2.loc[mask, 'merchant_id'] = 'MERCHANT0' + df_order_with_merchant_data2.loc[mask, 'merchant_id'].str[8:]

    mask = df_order_with_merchant_data3['merchant_id'].str.contains('^MERCHANT\d{4}$')
    df_order_with_merchant_data3.loc[mask, 'merchant_id'] = 'MERCHANT0' + df_order_with_merchant_data3.loc[mask, 'merchant_id'].str[8:]

    df_order_with_merchant_data.to_parquet("/opt/airflow/dags/order_with_merchant_data1_modified.parquet")
    df_order_with_merchant_data2.to_parquet("/opt/airflow/dags/order_with_merchant_data2_modified.parquet")
    df_order_with_merchant_data3.to_parquet("/opt/airflow/dags/order_with_merchant_data3_modified.parquet")

order_with_merchant_modified = PythonOperator(
    task_id='order_with_merchant_modified',
    python_callable=order_with_merchant_modified,
    dag=dag,
)

def merging_order_with_merchant():
    #convert last_name and first_name to uppercase
    df_order_data1 = pd.read_parquet("/opt/airflow/dags/order_with_merchant_data1_modified.parquet")

    df_order_data2 = pd.read_parquet("/opt/airflow/dags/order_with_merchant_data2_modified.parquet")

    df_order_data3 = pd.read_parquet("/opt/airflow/dags/order_with_merchant_data3_modified.parquet")

    order_data = pd.merge(df_order_data1, df_order_data2, how="outer", on=["order_id", "merchant_id" , "staff_id"])
    order_data = pd.merge(order_data, df_order_data3, how="outer", on=["order_id", "merchant_id" , "staff_id"])

    order_data.to_parquet("/opt/airflow/dags/order_with_merchant.parquet")

merging_order_with_merchant = PythonOperator(
    task_id='merging_order_with_merchant',
    python_callable=merging_order_with_merchant,
    dag=dag,
)

def merged_merchant():
    order_with_merchant = pd.read_parquet("/opt/airflow/dags/order_with_merchant.parquet")

    order_with_merchant = order_with_merchant[['merchant_id']]

    df_merchant_data = pd.read_parquet("/opt/airflow/dags/merchant_data_modified.parquet")

    df_merged = pd.merge(order_with_merchant, df_merchant_data)

    df_merged.to_parquet("/opt/airflow/dags/merged_merchant_final.parquet")




merged_merchant = PythonOperator(
    task_id='merged_merchant',
    python_callable=merged_merchant,
    dag=dag,
)


def merchant_dimension():

    df_merchant = pd.read_parquet("/opt/airflow/dags/merged_merchant_final.parquet")

    print("Dropping Duplicates")
    print(df_merchant.shape)
    df_merchant = df_merchant.drop_duplicates()
    print(df_merchant.shape)

    df_merchant = df_merchant.rename(columns={'merchant_id': 'MERCHANT_ID', 'creation_date': 'MERCHANT_CREATION_DATE', 'name': 'MERCHANT_NAME', 'street': 'MERCHANT_STREET', 'state': 'MERCHANT_STATE', 'city': 'MERCHANT_CITY', 'country': 'MERCHANT_COUNTRY', 'contact_number': 'MERCHANT_CONTACT_NUMBER'})

    df_merchant.to_parquet("/opt/airflow/dags/merchant_dimension.parquet")
    print("Successfully saved the data")
    

merchant_dimension = PythonOperator(
    task_id='merchant_dimension',
    python_callable=merchant_dimension,
    dag=dag,
)

def merchant_dim_to_db():
    import pandas as pd
    import mysql.connector
    from sqlalchemy import create_engine, MetaData, Table, Column, VARCHAR, DATETIME, DATE, FLOAT

# Step 1: Read Parquet File
    parquet_file_path = "/opt/airflow/dags/merchant_dimension.parquet"
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
        'merchant_dimension',
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
    df.to_sql(name='merchant_dimension', con=engine, if_exists='replace', index=False)
    print("Inserted into DB")


merchant_dim_to_db = PythonOperator(
        task_id='merchant_dim_to_db',
        python_callable=merchant_dim_to_db,
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
merchant_taskOne >> merchant_task2 >> merchant_task3 >> merchant_task4 >> merchant_task5 >> merchant_task6 >> merchant_task7 >> merchant_task8 >> merchant_task9 >> order_with_merchant_modified >> merging_order_with_merchant >> merged_merchant >> merchant_dimension >> merchant_dim_to_db