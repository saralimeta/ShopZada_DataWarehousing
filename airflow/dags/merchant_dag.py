from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime
import requests
import mysql.connector
from sqlalchemy import create_engine, MetaData, Table, Column, VARCHAR, DATETIME, DATE, FLOAT, INT

import pandas as pd
#from pendulum import timezone

#local_tz = timezone("Asia/Manila")  # Replace with your actual time zone

args = {
    'owner': 'Group 1',
    'start_date': days_ago(0),
    #'retries': 1,
    #'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    dag_id = 'merchant_pipeline',
    default_args = args,
    schedule_interval = '@hourly',
    max_active_runs = 1,
)


def merchant_load_database():
    #convert last_name and first_name to uppercase
    df_merchant_data = pd.read_html("/opt/airflow/departments/Enterprise Department/merchant_data.html")[0] # Insert full path.

    mask = df_merchant_data['merchant_id'].str.contains('^MERCHANT\d{4}$')
    df_merchant_data.loc[mask, 'merchant_id'] = 'MERCHANT0' + df_merchant_data.loc[mask, 'merchant_id'].str[8:]
    print("Succesfully reformatted the 4 digits")

    df_merchant_data.to_parquet("/opt/airflow/dimensions/Merchant Dimension/merchant_data_replace_4_digits.parquet")
    print("Succesfully saved the user data for task 1")

merchant_load_database = PythonOperator(
    task_id='merchant_load_database',
    python_callable=merchant_load_database,
    dag=dag,
)

def merchant_drop_duplicates():
    #convert last_name and first_name to uppercase
    df_merchant_data = pd.read_parquet("/opt/airflow/dimensions/Merchant Dimension/merchant_data_replace_4_digits.parquet") # Insert full path.

    df_merchant_data = df_merchant_data.drop_duplicates(subset='merchant_id')
    print("Succesfully removed merchant ID duplicates")

    df_merchant_data.to_parquet("/opt/airflow/dimensions/Merchant Dimension/merchant_data_remove_duplicates.parquet")
    print("Succesfully saved the user data for task 2")

merchant_drop_duplicates = PythonOperator(
    task_id='merchant_drop_duplicates',
    python_callable=merchant_drop_duplicates,
    dag=dag,
)

def merchant_to_datetime():
    df_merchant_data = pd.read_parquet("/opt/airflow/dimensions/Merchant Dimension/merchant_data_remove_duplicates.parquet") # Insert full path.

    df_merchant_data['creation_date'] = pd.to_datetime(df_merchant_data['creation_date'])
    print("Succesfully set merchant creation date to date object")

    df_merchant_data.to_parquet("/opt/airflow/dimensions/Merchant Dimension/merchant_data_date_to_date_type.parquet")
    print("Succesfully saved the user data for task 3")

merchant_to_datetime = PythonOperator(
    task_id='merchant_to_datetime',
    python_callable=merchant_to_datetime,
    dag=dag,
)

def merchant_standarize_street():
    df_merchant_data = pd.read_parquet("/opt/airflow/dimensions/Merchant Dimension/merchant_data_date_to_date_type.parquet") # Insert full path

    def standardize_street(street):
        parts = street.split()
        formatted_numbers = [f"{int(num):05d}" if num.isdigit() else num for num in parts]
        formatted_street = ' '.join(word.capitalize() for word in formatted_numbers)
        return formatted_street

    df_merchant_data['street'] = df_merchant_data['street'].apply(standardize_street)
    print("Succesfully set merchant street to correct format")

    df_merchant_data.to_parquet("/opt/airflow/dimensions/Merchant Dimension/merchant_data_street_format.parquet")
    print("Succesfully saved the merchant data for task 4")

merchant_standarize_street = PythonOperator(
    task_id='merchant_standarize_street',
    python_callable=merchant_standarize_street,
    dag=dag,
)


def merchant_standarize_city():
    df_merchant_data = pd.read_parquet("/opt/airflow/dimensions/Merchant Dimension/merchant_data_street_format.parquet") # Insert full path

    df_merchant_data['city'] = df_merchant_data['city'].replace('Louisville/Jefferson', 'Louisville')
    print("Succesfully set Louisville/Jefferson to Louisville")

    df_merchant_data.to_parquet("/opt/airflow/dimensions/Merchant Dimension/merchant_data_ctiy_name.parquet")
    print("Succesfully saved the merchant data for task 5")

merchant_standarize_city = PythonOperator(
    task_id='merchant_standarize_city',
    python_callable=merchant_standarize_city,
    dag=dag,
)

def merchant_fix_country_format():
    df_merchant_data = pd.read_parquet("/opt/airflow/dimensions/Merchant Dimension/merchant_data_ctiy_name.parquet") # Insert full path

    df_merchant_data['country'] = df_merchant_data['country'].apply(lambda x: x.encode('latin1').decode('utf8'))
    print("Succesfully reformatted city names with special characters")

    df_merchant_data['country'] = df_merchant_data['country'].str.replace(r"\s*\(.*\)", "", regex=True)
    print("Succesfully removed parenthesis & those inside the parenthesis of country names")

    df_merchant_data.to_parquet("/opt/airflow/dimensions/Merchant Dimension/merchant_data_country_format.parquet")
    print("Succesfully saved the merchant data for task 4")

merchant_fix_country_format = PythonOperator(
    task_id='merchant_fix_country_format',
    python_callable=merchant_fix_country_format,
    dag=dag,
)

def merchant_fix_contact_number_format():
    df_merchant_data = pd.read_parquet("/opt/airflow/dimensions/Merchant Dimension/merchant_data_country_format.parquet") # Insert full path

    df_merchant_data['contact_number'] = df_merchant_data['contact_number'].str.replace(r'\D', '', regex=True)
    df_merchant_data['contact_number'] = df_merchant_data['contact_number'].str.replace(r'^1', '', regex=True)
    df_merchant_data['contact_number'] = '+' + df_merchant_data['contact_number']
    print("Succesfully set merchant contact number to correct format")

    df_merchant_data.to_parquet("/opt/airflow/dimensions/Merchant Dimension/merchant_data_fixed_contact.parquet")
    print("Succesfully saved the merchant data for task 7")

merchant_fix_contact_number_format = PythonOperator(
    task_id='merchant_fix_contact_number_format',
    python_callable=merchant_fix_contact_number_format,
    dag=dag,
)

def merchant_standarize_name():
    df_merchant_data = pd.read_parquet("/opt/airflow/dimensions/Merchant Dimension/merchant_data_fixed_contact.parquet") # Insert full path

    df_merchant_data['name'] = df_merchant_data['name'].str.replace('.com', '')  # Remove '.com'
    df_merchant_data['name'] = df_merchant_data['name'].str.replace(', Inc', ' Inc')  # Replace ', Inc' with ' Inc'
    print("Succesfully cleaned name")

    df_merchant_data.to_parquet("/opt/airflow/dimensions/Merchant Dimension/merchant_data_fixed_name.parquet")
    print("Succesfully saved the merchant data for task 8")

merchant_standarize_name = PythonOperator(
    task_id='merchant_standarize_name',
    python_callable=merchant_standarize_name,
    dag=dag,
)

def merchant_standarize_country():
    df_merchant_data = pd.read_parquet("/opt/airflow/dimensions/Merchant Dimension/merchant_data_fixed_name.parquet") # Insert full path

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

    df_merchant_data.to_parquet("/opt/airflow/dimensions/Merchant Dimension/merchant_data_modified.parquet")
    print("Succesfully saved the merchant data for task 9")

merchant_standarize_country = PythonOperator(
    task_id='merchant_standarize_country',
    python_callable=merchant_standarize_country,
    dag=dag,
)

def order_with_merchant_modified():
    #convert last_name and first_name to uppercase
    df_order_with_merchant_data = pd.read_parquet("/opt/airflow/departments/Enterprise Department/order_with_merchant_data1.parquet")
    df_order_with_merchant_data2 = pd.read_parquet("/opt/airflow/departments/Enterprise Department/order_with_merchant_data2.parquet")
    df_order_with_merchant_data3 = pd.read_csv("/opt/airflow/departments/Enterprise Department/order_with_merchant_data3.csv")

    df_order_with_merchant_data = df_order_with_merchant_data.drop_duplicates()
    df_order_with_merchant_data2 = df_order_with_merchant_data2.drop_duplicates()
    df_order_with_merchant_data3 = df_order_with_merchant_data3.drop_duplicates()

    mask = df_order_with_merchant_data['merchant_id'].str.contains('^MERCHANT\d{4}$')
    df_order_with_merchant_data.loc[mask, 'merchant_id'] = 'MERCHANT0' + df_order_with_merchant_data.loc[mask, 'merchant_id'].str[8:]

    mask = df_order_with_merchant_data2['merchant_id'].str.contains('^MERCHANT\d{4}$')
    df_order_with_merchant_data2.loc[mask, 'merchant_id'] = 'MERCHANT0' + df_order_with_merchant_data2.loc[mask, 'merchant_id'].str[8:]

    mask = df_order_with_merchant_data3['merchant_id'].str.contains('^MERCHANT\d{4}$')
    df_order_with_merchant_data3.loc[mask, 'merchant_id'] = 'MERCHANT0' + df_order_with_merchant_data3.loc[mask, 'merchant_id'].str[8:]

    df_order_with_merchant_data.to_parquet("/opt/airflow/dimensions/Merchant Dimension/order_with_merchant_data1_modified.parquet")
    df_order_with_merchant_data2.to_parquet("/opt/airflow/dimensions/Merchant Dimension/order_with_merchant_data2_modified.parquet")
    df_order_with_merchant_data3.to_parquet("/opt/airflow/dimensions/Merchant Dimension/order_with_merchant_data3_modified.parquet")

order_with_merchant_modified = PythonOperator(
    task_id='order_with_merchant_modified',
    python_callable=order_with_merchant_modified,
    dag=dag,
)

def merging_order_with_merchant():
    #convert last_name and first_name to uppercase
    df_order_data1 = pd.read_parquet("/opt/airflow/dimensions/Merchant Dimension/order_with_merchant_data1_modified.parquet")

    df_order_data2 = pd.read_parquet("/opt/airflow/dimensions/Merchant Dimension/order_with_merchant_data2_modified.parquet")

    df_order_data3 = pd.read_parquet("/opt/airflow/dimensions/Merchant Dimension/order_with_merchant_data3_modified.parquet")

    order_data = pd.merge(df_order_data1, df_order_data2, how="outer", on=["order_id", "merchant_id" , "staff_id"])
    order_data = pd.merge(order_data, df_order_data3, how="outer", on=["order_id", "merchant_id" , "staff_id"])

    order_data.to_parquet("/opt/airflow/dimensions/Merchant Dimension/order_with_merchant.parquet")

merging_order_with_merchant = PythonOperator(
    task_id='merging_order_with_merchant',
    python_callable=merging_order_with_merchant,
    dag=dag,
)

def merged_merchant():
    order_with_merchant = pd.read_parquet("/opt/airflow/dimensions/Merchant Dimension/order_with_merchant.parquet")

    order_with_merchant = order_with_merchant[['merchant_id']]

    df_merchant_data = pd.read_parquet("/opt/airflow/dimensions/Merchant Dimension/merchant_data_modified.parquet")

    df_merged = pd.merge(order_with_merchant, df_merchant_data)

    df_merged.to_parquet("/opt/airflow/dimensions/Merchant Dimension/merged_merchant_final.parquet")

merged_merchant = PythonOperator(
    task_id='merged_merchant',
    python_callable=merged_merchant,
    dag=dag,
)

def merchant_dimension():

    df_merchant = pd.read_parquet("/opt/airflow/dimensions/Merchant Dimension/merged_merchant_final.parquet")

    print("Dropping Duplicates")
    print(df_merchant.shape)
    df_merchant = df_merchant.drop_duplicates()
    print(df_merchant.shape)

    df_merchant = df_merchant.rename(columns={'merchant_id': 'MERCHANT_ID', 'creation_date': 'MERCHANT_CREATION_DATE', 'name': 'MERCHANT_NAME', 'street': 'MERCHANT_STREET', 'state': 'MERCHANT_STATE', 'city': 'MERCHANT_CITY', 'country': 'MERCHANT_COUNTRY', 'contact_number': 'MERCHANT_CONTACT_NUMBER'})

    df_merchant = df_merchant.drop("Unnamed: 0", axis=1)

    df_merchant.to_parquet("/opt/airflow/dimensions/Dimensional Model/merchant_dimension.parquet")
    print("Successfully saved the data")
    

merchant_dimension = PythonOperator(
    task_id='merchant_dimension',
    python_callable=merchant_dimension,
    dag=dag,
)

def merchant_dim_to_db():
# Step 1: Read Parquet File
    parquet_file_path = "/opt/airflow/dimensions/Dimensional Model/merchant_dimension.parquet"
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
        Column('MERCHANT_ID', VARCHAR(255)),
        Column('CREATION_DATE', DATE),
        Column('MERCHANT_NAME', VARCHAR(255)),
        Column('MERCHANT_STREET', VARCHAR(255)),
        Column('MERCHANT_STATE', DATE),
        Column('MERCHANT_CITY', VARCHAR(255)),
        Column('MERCHANT_COUNTRY', VARCHAR(255)),
        Column('CONTACT_NUMBER', VARCHAR(255))
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
merchant_load_database >> merchant_drop_duplicates >> merchant_to_datetime >> merchant_standarize_street >> merchant_standarize_city >> merchant_fix_country_format >> merchant_fix_contact_number_format >> merchant_standarize_name >> merchant_standarize_country >> order_with_merchant_modified >> merging_order_with_merchant >> merged_merchant >> merchant_dimension >> merchant_dim_to_db