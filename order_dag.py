from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
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
    'OrderDag',
    default_args=default_args,
    description='A DAG to execute multiple Python scripts',
    start_date= datetime(2023, 12, 10),
    #schedule_interval='@hourly',  # You can adjust the schedule interval as needed
    #catchup = False
)

def line_item_data_products_merging():
    # loading data frame
    df_line_item_data_products1 = pd.read_csv("/opt/airflow/dags/line_item_data_products1.csv")
    df_line_item_data_products2 = pd.read_csv("/opt/airflow/dags/line_item_data_products2.csv")
    df_line_item_data_products3 = pd.read_parquet("/opt/airflow/dags/line_item_data_products3.parquet")
    print("Successfully loaded data...")
    
    dfs_products = [df_line_item_data_products1, df_line_item_data_products2, df_line_item_data_products3]
    merged_df_products = pd.concat(dfs_products, ignore_index=True)
    print("Successfully merged data...")
    
    merged_df_products  = merged_df_products.drop('Unnamed: 0', axis=1, errors='ignore')
    print("line_item_data_products shape:")
    merged_df_products.shape
    print("Successfully dropped col 1...")
    merged_df_products.to_parquet("/opt/airflow/dags/cleaned_line_item_data_products.parquet")

line_item_data_products_merging = PythonOperator(
    task_id='line_item_data_products_merging',
    python_callable=line_item_data_products_merging,
    dag=dag,
)

def line_item_data_prices_merging():
    #convert last_name and first_name to uppercase
    df_line_item_data_prices1 = pd.read_csv("/opt/airflow/dags/line_item_data_prices1.csv")
    df_line_item_data_prices2 = pd.read_csv("/opt/airflow/dags/line_item_data_prices2.csv")
    df_line_item_data_prices3 = pd.read_parquet("/opt/airflow/dags/line_item_data_prices3.parquet")
    
    #df_line_item_data_prices1.to_parquet("/opt/airflow/dags/line_item_data_prices1.parquet")
    #df_line_item_data_prices2.to_parquet("/opt/airflow/dags/line_item_data_prices2.parquet")
    print("Successfully converted the files to parquet...")
    
    dfs_prices = [df_line_item_data_prices1, df_line_item_data_prices2, df_line_item_data_prices3]
    merged_df_prices = pd.concat(dfs_prices, ignore_index=True)
    print("Successfully merged parquets...")

    merged_df_prices  = merged_df_prices.drop('Unnamed: 0', axis=1, errors='ignore')
    print("merged_df_prices shape:")
    merged_df_prices.shape
    print("Successfully dropped col 1...")
    merged_df_prices.to_parquet("/opt/airflow/dags/cleaned_line_item_data_prices.parquet")

line_item_data_prices_merging = PythonOperator(
    task_id='line_item_data_prices_merging',
    python_callable=line_item_data_prices_merging,
    dag=dag,
)

def merging_line_prices_and_product():
    # loading data frame
    df_line_item_data_products = pd.read_parquet("/opt/airflow/dags/cleaned_line_item_data_products.parquet")
    df_line_item_data_prices = pd.read_parquet("/opt/airflow/dags/cleaned_line_item_data_prices.parquet")

    df_merged_pricesANDproducts = pd.merge(df_line_item_data_products, df_line_item_data_prices, left_index=True, right_index=True)
    print("Successfully merged line_item_data_products and prices...")
    
    # Rename the columns if needed
    df_merged_pricesANDproducts = df_merged_pricesANDproducts.rename(columns={'order_id_x': 'order_id','product_name_x': 'product_name', 'product_id_x': 'product_id', 'price_x': 'price', 'quantity_x': 'quantity'})

    #  Drop unnecessary columns
    df_merged_pricesANDproducts = df_merged_pricesANDproducts[['order_id', 'product_name', 'product_id', 'price', 'quantity']]
    
    # Reset the index if needed
    df_merged_pricesANDproducts = df_merged_pricesANDproducts.reset_index(drop=True)
    df_merged_pricesANDproducts.to_parquet("/opt/airflow/dags/merged_line_item_data_prices_and_products.parquet")

merging_line_prices_and_product = PythonOperator(
    task_id='merging_line_prices_and_product',
    python_callable=merging_line_prices_and_product,
    dag=dag,
)

def merging_line_prices_and_product2():
     # loading data frame
    merged_line_product_price = pd.read_parquet("/opt/airflow/dags/merged_line_item_data_prices_and_products.parquet")
    df_product_list = pd.read_parquet(("/opt/airflow/dags/Product_Dimension.parquet")) #update files path
    
    merged_line_product_price = merged_line_product_price.rename(columns={'product_id': 'PRODUCT_ID', 'product_name': 'PRODUCT_NAME', 'product_type': 'PRODUCT_TYPE', 'price': 'PRODUCT_PRICE'})
    merged_line_product_price['PRODUCT_NAME'] =  merged_line_product_price['PRODUCT_NAME'].str.title()

    # Sets the price to 2 decimal
    merged_line_product_price['PRODUCT_PRICE'] = merged_line_product_price['PRODUCT_PRICE'].round(2)
    merged_line_product_price['PRODUCT_PRICE'] = merged_line_product_price['PRODUCT_PRICE'].apply(lambda x: '{:.2f}'.format(x))

    # Reset the index if needed
    merged_line_product_price = merged_line_product_price.drop(['PRODUCT_ID', 'PRODUCT_PRICE'], axis=1)

    merged_pricesANDproducts_updated = pd.merge(merged_line_product_price, df_product_list, on='PRODUCT_NAME', how='inner')

    #drop product_type
    merged_pricesANDproducts_updated = merged_pricesANDproducts_updated.drop(['PRODUCT_TYPE'], axis=1)
    
    merged_pricesANDproducts_updated = merged_pricesANDproducts_updated.drop_duplicates()
    
    merged_pricesANDproducts_updated.to_parquet("/opt/airflow/dags/line_product_and_prices_cleaned.parquet")
    print("Successfully Dropped Duplicates...")

merging_line_prices_and_product2 = PythonOperator(
    task_id='merging_line_prices_and_product2',
    python_callable=merging_line_prices_and_product2,
    dag=dag,
)


def converting_order_datas_to_parquet():
    # Load DataFrames from different file formats
    df_orderdata_1 = pd.read_csv("/opt/airflow/dags/order_data_20211001-20220101.csv")
    df_orderdata_2 = pd.read_pickle("/opt/airflow/dags/order_data_20200701-20211001.pickle")
    df_orderdata_3 = pd.read_json("/opt/airflow/dags/order_data_20221201-20230601.json")
    df_orderdata_4 = pd.read_html("/opt/airflow/dags/order_data_20230601-20240101.html")[0]  
    df_orderdata_5 = pd.read_excel("/opt/airflow/dags/order_data_20220101-20221201.xlsx")
    df_orderdata_6 = pd.read_parquet("/opt/airflow/dags/order_data_20200101-20200701.parquet")

    order_data_frames = [df_orderdata_1, df_orderdata_2, df_orderdata_3, df_orderdata_4, df_orderdata_5, df_orderdata_6]
    
    # Concatenate DataFrames along rows
    merged_order_data = pd.concat(order_data_frames, axis=0, ignore_index=True)

    # Drop Unnamed Col
    merged_order_data  = merged_order_data.drop('Unnamed: 0', axis=1, errors='ignore')

    # Convert 'transaction_date' to datetime
    merged_order_data['transaction_date'] = pd.to_datetime(merged_order_data['transaction_date'])

    merged_order_data.to_parquet("/opt/airflow/dags/cleaned_order_data.parquet")

converting_order_datas_to_parquet = PythonOperator(
    task_id='converting_order_datas_to_parquet',
    python_callable=converting_order_datas_to_parquet,
    dag=dag,
)

def cleaning_order_delays():
    # Load DataFrames from different file formats
    order_delays =  pd.read_html("/opt/airflow/dags/order_delays.html")[0]

    #dropping unnamed col
    order_delays  = order_delays.drop('Unnamed: 0', axis=1, errors='ignore')
    
    order_delays.to_parquet("/opt/airflow/dags/cleaned_order_delays.parquet")
    print("Successfully dropped unnamed col...")

cleaning_order_delays = PythonOperator(
    task_id='cleaning_order_delays',
    python_callable=cleaning_order_delays,
    dag=dag,
)

def transactional_campaign_data_cleaning():
    df_transac_campaign_data = pd.read_csv("/opt/airflow/dags/transactional_campaign_data.csv") #change file path
    
    df_transac_campaign_data = df_transac_campaign_data.drop("Unnamed: 0", axis=1)

    df_transac_campaign_data['transaction_date'] = pd.to_datetime(df_transac_campaign_data['transaction_date'])

    df_transac_campaign_data['availed'] = df_transac_campaign_data['availed'].map({1: 'Yes', 0: 'No'})

    df_transac_campaign_data.drop_duplicates()

    df_transac_campaign_data.to_parquet("/opt/airflow/dags/transactional_campaign_data_standardized.parquet")

transactional_campaign_data_cleaning = PythonOperator(
    task_id='transactional_campaign_data_cleaning',
    python_callable=transactional_campaign_data_cleaning,
    dag=dag,
)

def merge_order_data_to_order_delays():
    # Load DataFrames from different file formats
    order_delays =  pd.read_parquet("/opt/airflow/dags/cleaned_order_delays.parquet")
    merged_order_data = pd.read_parquet("/opt/airflow/dags/cleaned_order_data.parquet")
    transactional_campaign = pd.read_parquet("/opt/airflow/dags/transactional_campaign_data_standardized.parquet")
    merged_pricesANDproducts = pd.read_parquet("/opt/airflow/dags/line_product_and_prices_cleaned.parquet")

    merged_data = pd.merge(merged_order_data, order_delays, on='order_id', how='outer')

    # Fill missing values with 0
    merged_data.fillna(0, inplace=True)
    merged_data = merged_data.astype({'delay in days': int})

    merged_df = pd.merge(merged_pricesANDproducts, transactional_campaign[['order_id', 'campaign_id', 'availed']], on='order_id', how='left')
    
    # Fill NaN values in 'campaign_id' with "None"
    merged_df['campaign_id'] = merged_df['campaign_id'].fillna("No Campaign")
    merged_df['availed'] = merged_df['availed'].fillna(0)
    merged_df['availed'] = merged_df['availed'].astype(int)

        
    result_df = pd.merge(merged_df[['order_id', 'availed']], merged_data[['order_id', 'estimated arrival',	'transaction_date',	'delay in days']], on='order_id', how='inner')

    result_df =  result_df.drop_duplicates()

    result_df = result_df.rename(columns={'order_id': 'ORDER_ID', 'availed': 'ORDER_AVAILED', 'estimated arrival': 'ORDER_ESTIMATED_ARRIVAL', 'transaction_date': 'ORDER_TRANSACTION_DATE', 'delay in days': 'ORDER_DELAYS_IN_DAYS'})

    result_df.to_parquet("/opt/airflow/dags/Order_Dim.parquet")

merge_order_data_to_order_delays = PythonOperator(
    task_id='merge_order_data_to_order_delays',
    python_callable=merge_order_data_to_order_delays,
    dag=dag,
)


def order_dim_to_db():
    import pandas as pd
    import mysql.connector
    from sqlalchemy import create_engine, MetaData, Table, Column, VARCHAR, DATETIME, DATE, FLOAT

# Step 1: Read Parquet File
    parquet_file_path = "/opt/airflow/dags/Order_Dim.parquet"
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
        'order_dimension',
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
    df.to_sql(name='order_dimension', con=engine, if_exists='replace', index=False)
    print("Inserted into DB")


order_dim_to_db = PythonOperator(
        task_id='order_dim_to_db',
        python_callable=order_dim_to_db,
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
line_item_data_products_merging >> line_item_data_prices_merging >> merging_line_prices_and_product >> merging_line_prices_and_product2 >> converting_order_datas_to_parquet >> cleaning_order_delays >> transactional_campaign_data_cleaning >> merge_order_data_to_order_delays >> order_dim_to_db