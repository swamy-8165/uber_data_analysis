import os
import sys
import json
import warnings
from uuid import uuid4

import numpy as np
import pandas as pd
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
from dotenv import load_dotenv
from kaggle.api.kaggle_api_extended import KaggleApi
from colorama import init, Fore, Back, Style
init()
warnings.filterwarnings("ignore")
load_dotenv()


def authenticate_kaggle():
    """Authenticates with Kaggle API."""
    api = KaggleApi()
    api.authenticate()
    return api

def download_dataset(api, dataset_path):
    """Downloads the Kaggle dataset if not already present."""
    if not os.path.exists(dataset_path):
      api.dataset_download_files(os.getenv("kaggle_dataset"), path=dataset_path, unzip=True)

def load_and_concat_csv(data_path):
    """Loads and concatenates all CSV files from the given path."""
    df = pd.concat(
        (pd.read_csv(os.path.join(data_path, file))
         for file in os.listdir(data_path) if file.endswith(".csv")),
        ignore_index=True, # Avoid index conflicts during concatenation
        )
    return df

def create_cassandra_session():
    """Creates a Cassandra session."""
    config = json.loads(f'{{"secure_connect_bundle":"{os.getenv("cloud_config")}"}}')
    auth_provider = PlainTextAuthProvider(
        username=os.getenv("Cassndra_clientId"), password=os.getenv("Cassandra_secret")
    )
    cluster = Cluster(cloud=config, auth_provider=auth_provider)
    session = cluster.connect()

    if session:
        print("Connected to Cassandra")
        return session
    else: 
        print("Failed to connect to Cassandra")
        sys.exit(1)
        
def list_cassandra_tables(session, target_tables):
  """Lists tables in Cassandra based on target tables."""
  result = session.execute("SELECT table_name FROM system_schema.tables;")
  tables = [row.table_name for row in result if row.table_name in target_tables]
  return tables

def create_reconciliation_table(session, keyspace="bigdata_1", table_name="uber_reconciliation_bronze"):
    """Creates the reconciliation table in Cassandra."""
    print(f"\n{Fore.CYAN}{'='*80}")
    print(f"{Fore.WHITE}🔄 Creating Reconciliation Bronze Table")
    print(f"{Fore.CYAN}{'='*80}{Style.RESET_ALL}\n")

    session.execute(f"DROP TABLE IF EXISTS {keyspace}.{table_name};")
    print(f"{Fore.YELLOW}🗑 Dropped existing {table_name} table{Style.RESET_ALL}")

    create_table_query = f"""
        CREATE TABLE {keyspace}.{table_name} (
            trip_id UUID PRIMARY KEY,
            vendor_id TEXT,
            pickup_datetime TEXT,
            dropoff_datetime TEXT,
            passenger_count TEXT,
            trip_distance TEXT,
            pickup_longitude TEXT,
            pickup_latitude TEXT,
            rate_code_id TEXT,
            store_and_fwd_flag TEXT,
            dropoff_longitude TEXT,
            dropoff_latitude TEXT,
            payment_type TEXT,
            fare_amount TEXT,
            extra TEXT,
            mta_tax TEXT,
            tip_amount TEXT,
            tolls_amount TEXT,
            improvement_surcharge TEXT,
            total_amount TEXT
        );
        """
    session.execute(create_table_query)
    print(f"{Fore.GREEN}✓ {table_name} table created successfully{Style.RESET_ALL}")

def create_bronze_table(session, keyspace="bigdata_1", table_name="uber_bronze"):
    """Creates the bronze table in Cassandra."""
    print(f"\n{Fore.CYAN}{'='*80}")
    print(f"{Fore.WHITE}🏭 Creating Bronze Table")
    print(f"{Fore.CYAN}{'='*80}{Style.RESET_ALL}\n")

    create_table_query = f"""
    CREATE TABLE {keyspace}.{table_name} (
        trip_id UUID PRIMARY KEY,
        vendor_id TEXT,
        pickup_datetime TEXT,
        dropoff_datetime TEXT,
        passenger_count TEXT,
        trip_distance TEXT,
        pickup_longitude TEXT,
        pickup_latitude TEXT,
        rate_code_id TEXT,
        store_and_fwd_flag TEXT,
        dropoff_longitude TEXT,
        dropoff_latitude TEXT,
        payment_type TEXT,
        fare_amount TEXT,
        extra TEXT,
        mta_tax TEXT,
        tip_amount TEXT,
        tolls_amount TEXT,
        improvement_surcharge TEXT,
        total_amount TEXT
    );
    """
    session.execute(create_table_query)
    print(f"{Fore.GREEN}✓ {table_name} table created successfully{Style.RESET_ALL}")

def fetch_bronze_data(session, keyspace="bigdata_1", table_name="uber_bronze"):
    """Fetches data from the bronze table."""
    query = f"""
        SELECT vendor_id, pickup_datetime, dropoff_datetime, 
               passenger_count, trip_distance, pickup_longitude, pickup_latitude,
               rate_code_id, store_and_fwd_flag, dropoff_longitude, dropoff_latitude,
               payment_type, fare_amount, extra, mta_tax, tip_amount, 
               tolls_amount, improvement_surcharge, total_amount 
        FROM {keyspace}.{table_name};
        """
    rows = session.execute(query)
    df = pd.DataFrame(rows)
    print(f"Dataframe shape: {df.shape}")
    return df

def find_missing_records(df, df_bronze):
    """Finds records in df that are not in df_bronze."""

    df_bronze_renamed = df_bronze.rename(columns={
        'vendor_id': 'VendorID',
        'pickup_datetime': 'tpep_pickup_datetime',
        'dropoff_datetime': 'tpep_dropoff_datetime',
        'rate_code_id': 'RatecodeID'
    })

    df_str = df.astype(str)
    df_bronze_str = df_bronze_renamed.astype(str)

    missing_records = df_str.merge(
        df_bronze_str,
        how='left',
        indicator=True
    ).query('_merge == "left_only"').drop('_merge', axis=1)

    print(f"Number of missing records: {len(missing_records)}")
    return missing_records

def insert_records_to_cassandra(session, df, bronze_table, reconciliation_table, keyspace="bigdata_1"):
    """Inserts records into Cassandra tables."""
    print(f"\n{Fore.CYAN}{'='*80}")
    print(f"{Fore.WHITE}📥 Inserting Records to Bronze Layer")
    print(f"{Fore.CYAN}{'='*80}{Style.RESET_ALL}\n")

    reconciliation_count = 0
    bronze_count = 0

    insert_bronze_query = f"""
        INSERT INTO {keyspace}.{bronze_table} (
            trip_id, vendor_id, pickup_datetime, dropoff_datetime, 
            passenger_count, trip_distance, pickup_longitude, pickup_latitude,
            rate_code_id, store_and_fwd_flag, dropoff_longitude, dropoff_latitude,
            payment_type, fare_amount, extra, mta_tax, tip_amount, 
            tolls_amount, improvement_surcharge, total_amount
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    insert_reconciliation_query = f"""
        INSERT INTO {keyspace}.{reconciliation_table} (
            trip_id, vendor_id, pickup_datetime, dropoff_datetime, 
            passenger_count, trip_distance, pickup_longitude, pickup_latitude,
            rate_code_id, store_and_fwd_flag, dropoff_longitude, dropoff_latitude,
            payment_type, fare_amount, extra, mta_tax, tip_amount, 
            tolls_amount, improvement_surcharge, total_amount
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
        
    for index, row in df.iterrows():
        try:
            trip_id = uuid4()
            values = [
                trip_id,  # Generate new UUID for each record
                str(row['VendorID']) if pd.notna(row['VendorID']) else None,
                str(row['tpep_pickup_datetime']) if pd.notna(row['tpep_pickup_datetime']) else None,
                str(row['tpep_dropoff_datetime']) if pd.notna(row['tpep_dropoff_datetime']) else None,
                str(row['passenger_count']) if pd.notna(row['passenger_count']) else None,
                str(row['trip_distance']) if pd.notna(row['trip_distance']) else None,
                str(row['pickup_longitude']) if pd.notna(row['pickup_longitude']) else None,
                str(row['pickup_latitude']) if pd.notna(row['pickup_latitude']) else None,
                str(row['RatecodeID']) if pd.notna(row['RatecodeID']) else None,
                str(row['store_and_fwd_flag']) if pd.notna(row['store_and_fwd_flag']) else None,
                str(row['dropoff_longitude']) if pd.notna(row['dropoff_longitude']) else None,
                str(row['dropoff_latitude']) if pd.notna(row['dropoff_latitude']) else None,
                str(row['payment_type']) if pd.notna(row['payment_type']) else None,
                str(row['fare_amount']) if pd.notna(row['fare_amount']) else None,
                str(row['extra']) if pd.notna(row['extra']) else None,
                str(row['mta_tax']) if pd.notna(row['mta_tax']) else None,
                str(row['tip_amount']) if pd.notna(row['tip_amount']) else None,
                str(row['tolls_amount']) if pd.notna(row['tolls_amount']) else None,
                str(row['improvement_surcharge']) if pd.notna(row['improvement_surcharge']) else None,
                str(row['total_amount']) if pd.notna(row['total_amount']) else None
            ]

            session.execute(insert_bronze_query, values)
            bronze_count += 1

            session.execute(insert_reconciliation_query, values)
            reconciliation_count += 1
            
            if index % 100 == 0:
                print(f"{Fore.CYAN}↳ Processed {index} records...{Style.RESET_ALL}")
                

        except Exception as e:
            print(f"{Fore.RED}❌ Error on record {index}: {e}")
            print(f"Record data: {row.to_dict()}{Style.RESET_ALL}")
            sys.exit(1)
            
    print(f"\n{Fore.GREEN}📊 Insertion Summary:")
    print(f"✓ Records inserted into {reconciliation_table}: {reconciliation_count}")
    print(f"✓ Records inserted into {bronze_table}: {bronze_count}{Style.RESET_ALL}")

def process_reconciliation_bronze_to_silver(session, keyspace="bigdata_1", bronze_reconciliation_table="uber_reconciliation_bronze", silver_table="uber_silver"):
    """Processes data from reconciliation bronze to silver table."""
    print(f"\n{Fore.CYAN}{'='*80}")
    print(f"{Fore.WHITE}🔄 Processing Bronze to Silver Transformation")
    print(f"{Fore.CYAN}{'='*80}{Style.RESET_ALL}\n")
    
    query = f"SELECT * FROM {keyspace}.{bronze_reconciliation_table};"
    rows = session.execute(query)
    df_bronze_reconciliation = pd.DataFrame(rows)
    
    if len(df_bronze_reconciliation) == 0:
        print(f"{Fore.RED}❌ No data to process{Style.RESET_ALL}")
        return
    
    print(f"{Fore.GREEN}✓ Loaded {len(df_bronze_reconciliation)} records from bronze layer{Style.RESET_ALL}")
    
    # Data type conversions
    print(f"\n{Fore.YELLOW}🔄 Converting data types...{Style.RESET_ALL}")
    df_bronze_reconciliation = df_bronze_reconciliation[[
    'vendor_id','pickup_datetime','dropoff_datetime','passenger_count','trip_distance','pickup_longitude','pickup_latitude','rate_code_id','store_and_fwd_flag','dropoff_longitude','dropoff_latitude','payment_type','fare_amount','extra','mta_tax','tip_amount','tolls_amount','improvement_surcharge','total_amount'
    ]]

    df_bronze_reconciliation['pickup_datetime'] = pd.to_datetime(df_bronze_reconciliation['pickup_datetime'])
    df_bronze_reconciliation['dropoff_datetime'] = pd.to_datetime(df_bronze_reconciliation['dropoff_datetime'])

    df_bronze_reconciliation['pickup_datetime'] = df_bronze_reconciliation['pickup_datetime'].apply(lambda x: x.isoformat() if not pd.isnull(x) else None)
    df_bronze_reconciliation['dropoff_datetime'] = df_bronze_reconciliation['dropoff_datetime'].apply(lambda x: x.isoformat() if not pd.isnull(x) else None)

    df_bronze_reconciliation = df_bronze_reconciliation.drop_duplicates().reset_index(drop = True)

    # Convert data types
    df_bronze_reconciliation = df_bronze_reconciliation.astype({
        'vendor_id': 'int',
        'passenger_count': 'int',
        'trip_distance': 'float',
        'pickup_longitude': 'float',
        'pickup_latitude': 'float',
        'rate_code_id': 'int',
        'store_and_fwd_flag': 'str',
        'dropoff_longitude': 'float',
        'dropoff_latitude': 'float',
        'payment_type': 'int',
        'fare_amount': 'float',
        'extra': 'float',
        'mta_tax': 'float',
        'tip_amount': 'float',
        'tolls_amount': 'float',
        'improvement_surcharge': 'float',
        'total_amount': 'float'
    })
    print(f"{Fore.GREEN}✓ Data type conversions completed{Style.RESET_ALL}")

    # Table creation
    tables = list_cassandra_tables(session, target_tables=[silver_table])
    if silver_table not in tables:
        print(f"\n{Fore.YELLOW}📝 Creating {silver_table} table...{Style.RESET_ALL}")
        create_silver_table = f"""
            CREATE TABLE {keyspace}.{silver_table} (
                trip_id UUID PRIMARY KEY,
                vendor_id INT,
                pickup_datetime TIMESTAMP,
                dropoff_datetime TIMESTAMP,
                passenger_count INT,
                trip_distance DOUBLE,
                pickup_longitude DOUBLE,
                pickup_latitude DOUBLE,
                rate_code_id INT,
                store_and_fwd_flag TEXT,
                dropoff_longitude DOUBLE,
                dropoff_latitude DOUBLE,
                payment_type INT,
                fare_amount DOUBLE,
                extra DOUBLE,
                mta_tax DOUBLE,
                tip_amount DOUBLE,
                tolls_amount DOUBLE,
                improvement_surcharge DOUBLE,
                total_amount DOUBLE
            );
        """
        session.execute(create_silver_table)
        print(f"{Fore.GREEN}✓ {silver_table} table created successfully{Style.RESET_ALL}")

    # Data insertion
    print(f"\n{Fore.YELLOW}📥 Inserting data into silver layer...{Style.RESET_ALL}")
    insert_query = f"""
        INSERT INTO {keyspace}.{silver_table} (
            trip_id, vendor_id, pickup_datetime, dropoff_datetime, 
            passenger_count, trip_distance, pickup_longitude, pickup_latitude,
            rate_code_id, store_and_fwd_flag, dropoff_longitude, dropoff_latitude,
            payment_type, fare_amount, extra, mta_tax, tip_amount, 
            tolls_amount, improvement_surcharge, total_amount
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
    """

    silver_count = 0

    for index, row in df_bronze_reconciliation.iterrows():
        try:
            values = [
                uuid4(),
                int(row['vendor_id']),
                row['pickup_datetime'],
                row['dropoff_datetime'],
                int(row['passenger_count']),
                float(row['trip_distance']),
                float(row['pickup_longitude']),
                float(row['pickup_latitude']),
                int(row['rate_code_id']),
                str(row['store_and_fwd_flag']),
                float(row['dropoff_longitude']),
                float(row['dropoff_latitude']),
                int(row['payment_type']),
                float(row['fare_amount']),
                float(row['extra']),
                float(row['mta_tax']),
                float(row['tip_amount']),
                float(row['tolls_amount']),
                float(row['improvement_surcharge']),
                float(row['total_amount'])
            ]

            session.execute(insert_query, values)
            silver_count += 1
            if index % 100 == 0:
                print(f"{Fore.CYAN}↳ Processed {index} records...{Style.RESET_ALL}")
                

        except Exception as e:
            print(f"{Fore.RED}❌ Error on record {index}: {e}")
            print(f"Record data: {row.to_dict()}{Style.RESET_ALL}")
            with open("error_log.txt", "a") as log_file:
                log_file.write(f"Error on record {index}: {e}\nRecord data: {row.to_dict()}\n")
            continue

    print(f"\n{Fore.GREEN}📊 Insertion Summary:")
    print(f"✓ Records inserted into {silver_table}: {silver_count}")
    print(f"✓ Data insertion completed into Silver table{Style.RESET_ALL}")
        
def transform_silver_to_gold(session, keyspace="bigdata_1", silver_table="uber_silver"):
    """Transforms data from silver to gold layer."""
        
    query = f"SELECT * FROM {keyspace}.{silver_table};"
    rows = session.execute(query)
    df_silver = pd.DataFrame(rows)
        
    if df_silver.empty:
        print(f"{Fore.RED}❌ No data found in silver layer to transform.{Style.RESET_ALL}")
        return
    
    print(f"{Fore.GREEN}✓ Successfully loaded {len(df_silver)} records from silver layer{Style.RESET_ALL}")
    

    # Create datetime dimension with UUID
    print(f"{Fore.WHITE}⏰ Creating DateTime Dimension from silver layer load into pandas dataframe...")
    datetime_dim = df_silver[['pickup_datetime','dropoff_datetime']]
    datetime_dim['pickup_datetime'] = pd.to_datetime(datetime_dim['pickup_datetime'])
    datetime_dim['dropoff_datetime'] = pd.to_datetime(datetime_dim['dropoff_datetime'])
    datetime_dim['datetime_id'] = datetime_dim.apply(lambda x: uuid4(), axis=1)  # Add UUID

    # Pickup Time
    datetime_dim['pick_hour'] = pd.to_datetime(datetime_dim['pickup_datetime']).dt.hour
    datetime_dim['pick_day'] = pd.to_datetime(datetime_dim['pickup_datetime']).dt.day
    datetime_dim['pick_month'] = pd.to_datetime(datetime_dim['pickup_datetime']).dt.month
    datetime_dim['pick_year'] = pd.to_datetime(datetime_dim['pickup_datetime']).dt.year
    datetime_dim['pick_weekday'] = pd.to_datetime(datetime_dim['pickup_datetime']).dt.weekday

    # Dropoff Time
    datetime_dim['drop_hour'] = pd.to_datetime(datetime_dim['dropoff_datetime']).dt.hour
    datetime_dim['drop_day'] = pd.to_datetime(datetime_dim['dropoff_datetime']).dt.day
    datetime_dim['drop_month'] = pd.to_datetime(datetime_dim['dropoff_datetime']).dt.month
    datetime_dim['drop_year'] = pd.to_datetime(datetime_dim['dropoff_datetime']).dt.year
    datetime_dim['drop_weekday'] = pd.to_datetime(datetime_dim['dropoff_datetime']).dt.weekday
    

    # Create passenger count dimension
    print(f"{Fore.WHITE}👥 Creating Passenger Count Dimension from silver layer load into pandas dataframe...")
    passenger_count_dim = df_silver[['passenger_count']].drop_duplicates().reset_index(drop=True)
    passenger_count_dim['passenger_count_id'] = passenger_count_dim.apply(lambda x: uuid4(), axis=1)
    passenger_count_dim = passenger_count_dim[['passenger_count_id','passenger_count']]


    # Create trip distance dimension
    print(f"{Fore.WHITE}🚕 Creating Trip Distance Dimension from silver layer load into pandas dataframe...")
    trip_distance_dim = df_silver[['trip_distance']].drop_duplicates().reset_index(drop=True)
    trip_distance_dim['trip_distance_id'] = trip_distance_dim.apply(lambda x: uuid4(), axis=1)
    trip_distance_dim = trip_distance_dim[['trip_distance_id','trip_distance']]


    # Create payment type dimension
    print(f"{Fore.WHITE}💳 Creating Payment Type Dimension from silver layer load into pandas dataframe...")
    payment_mode_type = {
            1:"Credit card",
            2:"Cash",
            3:"No charge",
            4:"Dispute",
            5:"Unknown",
            6:"Voided trip"
        }
    payment_type_dim = df_silver[['payment_type']].drop_duplicates().reset_index(drop=True)
    payment_type_dim['payment_type_id'] = payment_type_dim.apply(lambda x: uuid4(), axis=1)
    payment_type_dim['payment_type_desc'] = payment_type_dim['payment_type'].map(payment_mode_type)
    payment_type_dim = payment_type_dim[['payment_type_id','payment_type','payment_type_desc']]

    # Create rate code dimension
    print(f"{Fore.WHITE}📊 Creating Rate Code Dimension from silver layer load into pandas dataframe...")    
    rate_code_type = {
            1:"Standard rate",
            2:"JFK",
            3:"Newark",
            4:"Nassau or Westchester",
            5:"Negotiated fare",
            6:"Group ride"
        }
    rate_code_dim = df_silver[['rate_code_id']].drop_duplicates().reset_index(drop=True)
    rate_code_dim['rate_code_dim_id'] = rate_code_dim.apply(lambda x: uuid4(), axis=1)
    rate_code_dim['rate_code_desc'] = rate_code_dim['rate_code_id'].map(rate_code_type)
    rate_code_dim = rate_code_dim[['rate_code_dim_id','rate_code_id','rate_code_desc']]

    # Create location dimensions
    print(f"{Fore.WHITE}📍 Creating Location Dimensions from silver layer load into pandas dataframe...")
    dropoff_location_dim = df_silver[['dropoff_latitude','dropoff_longitude']].drop_duplicates().reset_index(drop=True)
    dropoff_location_dim['dropoff_loc_id'] = dropoff_location_dim.apply(lambda x: uuid4(), axis=1)
    dropoff_location_dim = dropoff_location_dim[['dropoff_loc_id','dropoff_latitude','dropoff_longitude']]

    pickup_location_dim = df_silver[['pickup_latitude','pickup_longitude']].drop_duplicates().reset_index(drop=True)
    pickup_location_dim['pickup_loc_id'] = pickup_location_dim.apply(lambda x: uuid4(), axis=1)
    pickup_location_dim = pickup_location_dim[['pickup_loc_id','pickup_latitude','pickup_longitude']]
 

    # Create fact table
    print(f"\n{Fore.YELLOW}📊 Creating Fact Table from silver layer load into pandas dataframe...")
    fact_table = df_silver.merge(datetime_dim[['datetime_id', 'pickup_datetime', 'dropoff_datetime']], 
                                on=['pickup_datetime', 'dropoff_datetime']) \
                    .merge(passenger_count_dim, on='passenger_count') \
                    .merge(trip_distance_dim, on='trip_distance') \
                    .merge(rate_code_dim, on='rate_code_id') \
                    .merge(pickup_location_dim, on=['pickup_longitude','pickup_latitude']) \
                    .merge(dropoff_location_dim, on=['dropoff_longitude', 'dropoff_latitude']) \
                    .merge(payment_type_dim, on=['payment_type'])

    fact_table = fact_table[['vendor_id', 'datetime_id', 'passenger_count_id', 
                            'trip_distance_id', 'rate_code_dim_id',
                            'store_and_fwd_flag', 'pickup_loc_id', 'dropoff_loc_id', 
                            'payment_type_id', 'fare_amount', 'extra', 'mta_tax', 
                            'tip_amount', 'tolls_amount', 'improvement_surcharge',
                            'total_amount']]


    print(f"\n{Fore.YELLOW}📝 Dropping existing Dimension and Fact tables...{Style.RESET_ALL}")
    # Drop tables if they exist
    tables_to_drop = [
        "datetime_dim",
        "passenger_count_dim",
        "trip_distance_dim",
        "payment_type_dim",
        "rate_code_dim",
        "dropoff_location_dim",
        "pickup_location_dim",
        "fact_table"
    ]

    for table in tables_to_drop:
        session.execute(f"DROP TABLE IF EXISTS {keyspace}.{table};")
        
    print(f"{Fore.GREEN}✓ Existing Dimension and Fact tables dropped{Style.RESET_ALL}")

    # Create tables
    print(f"\n{Fore.YELLOW}📝 Creating datetime` Dimension tables...{Style.RESET_ALL}")
    session.execute(f"""
    CREATE TABLE {keyspace}.datetime_dim (
        datetime_id UUID PRIMARY KEY,
        pickup_datetime TIMESTAMP,
        dropoff_datetime TIMESTAMP,
        pick_hour INT,
        pick_day INT,
        pick_month INT,
        pick_year INT,
        pick_weekday INT,
        drop_hour INT,
        drop_day INT,
        drop_month INT,
        drop_year INT,
        drop_weekday INT
    );
    """)
    print(f"{Fore.GREEN}✓ DateTime Dimension table created{Style.RESET_ALL}")

    print(f"\n{Fore.YELLOW}📝 Creating passenger_count Dimension table...{Style.RESET_ALL}")
    session.execute(f"""
    CREATE TABLE {keyspace}.passenger_count_dim (
        passenger_count_id UUID PRIMARY KEY,
        passenger_count INT
    );
    """)
    print(f"{Fore.GREEN}✓ Passenger Count Dimension table created{Style.RESET_ALL}")

    print(f"\n{Fore.YELLOW}📝 Creating trip_distance Dimension table...{Style.RESET_ALL}")
    session.execute(f"""
    CREATE TABLE {keyspace}.trip_distance_dim (
        trip_distance_id UUID PRIMARY KEY,
        trip_distance DOUBLE
    );
    """)
    print(f"{Fore.GREEN}✓ Trip Distance Dimension table created{Style.RESET_ALL}")

    print(f"\n{Fore.YELLOW}📝 Creating payment_type Dimension table...{Style.RESET_ALL}")
    session.execute(f"""
    CREATE TABLE {keyspace}.payment_type_dim (
        payment_type_id UUID PRIMARY KEY,
        payment_type INT,
        payment_type_desc TEXT
    );
    """)
    print(f"{Fore.GREEN}✓ Payment Type Dimension table created{Style.RESET_ALL}")

    print(f"\n{Fore.YELLOW}📝 Creating rate_code Dimension table...{Style.RESET_ALL}")

    session.execute(f"""
    CREATE TABLE {keyspace}.rate_code_dim (
        rate_code_dim_id UUID PRIMARY KEY,
        rate_code_id INT,
        rate_code_desc TEXT
    );
    """)
    print(f"{Fore.GREEN}✓ Rate Code Dimension table created{Style.RESET_ALL}")

    print(f"\n{Fore.YELLOW}📝 Creating dropoff_location Dimension table...{Style.RESET_ALL}")

    session.execute(f"""
    CREATE TABLE {keyspace}.dropoff_location_dim (
        dropoff_loc_id UUID PRIMARY KEY,
        dropoff_latitude DOUBLE,
        dropoff_longitude DOUBLE
    );
    """)
    print(f"{Fore.GREEN}✓ Dropoff Location Dimension table created{Style.RESET_ALL}")

    print(f"\n{Fore.YELLOW}📝 Creating pickup_location Dimension table...{Style.RESET_ALL}")

    session.execute(f"""
    CREATE TABLE {keyspace}.pickup_location_dim (
        pickup_loc_id UUID PRIMARY KEY,
        pickup_latitude DOUBLE,
        pickup_longitude DOUBLE
    );
    """)
    print(f"{Fore.GREEN}✓ Pickup Location Dimension table created{Style.RESET_ALL}")

    print(f"\n{Fore.YELLOW}📝 Creating fact_table...{Style.RESET_ALL}")

    session.execute(f"""
    CREATE TABLE {keyspace}.fact_table (
        vendor_id INT,
        datetime_id UUID,
        passenger_count_id UUID,
        trip_distance_id UUID,
        rate_code_dim_id UUID,
        store_and_fwd_flag TEXT,
        pickup_loc_id UUID,
        dropoff_loc_id UUID,
        payment_type_id UUID,
        fare_amount DOUBLE,
        extra DOUBLE,
        mta_tax DOUBLE,
        tip_amount DOUBLE,
        tolls_amount DOUBLE,
        improvement_surcharge DOUBLE,
        total_amount DOUBLE,
        PRIMARY KEY (vendor_id, datetime_id)
    );
    """)
    print(f"{Fore.GREEN}✓ Fact Table created{Style.RESET_ALL}")


    print(f"{Fore.GREEN}✓ All tables created successfully{Style.RESET_ALL}")
    
    def insert_into_table(table_name, columns, rows):
        placeholders = ", ".join(["%s"] * len(columns))
        insert_query = f"INSERT INTO {keyspace}.{table_name} ({', '.join(columns)}) VALUES ({placeholders});"
        
        records_inserted = 0
        print(f"\n{Fore.WHITE}📥 Inserting data into {table_name}...")
        
        for row in rows:
            formatted_row = []
            for value in row:
                if isinstance(value, pd.Timestamp):
                    formatted_row.append(value.strftime('%Y-%m-%d %H:%M:%S'))
                else:
                    formatted_row.append(value)
            
            try:
                session.execute(insert_query, formatted_row)
                records_inserted += 1
                if records_inserted % 1000 == 0:
                    print(f"{Fore.CYAN}↳ Inserted {records_inserted} records...{Style.RESET_ALL}")
            except Exception as e:
                print(f"{Fore.RED}Error inserting row: {e}")
                print(f"Row data: {formatted_row}{Style.RESET_ALL}")
                continue
        
        print(f"{Fore.GREEN}✓ Successfully inserted {records_inserted} records into {table_name}{Style.RESET_ALL}")

    # Insert data into tables
    print(f"\n{Fore.YELLOW}📊 Inserting Data into Datetime Dimensions...{Style.RESET_ALL}")

    # Insert all dimensions
    datetime_dim = datetime_dim[['datetime_id', 'pickup_datetime', 'dropoff_datetime', 'pick_hour', 'pick_day', 
                                'pick_month', 'pick_year', 'pick_weekday', 'drop_hour', 'drop_day', 
                                'drop_month', 'drop_year', 'drop_weekday']]
    insert_into_table(
        "datetime_dim",
        ["datetime_id", "pickup_datetime", "dropoff_datetime", "pick_hour", "pick_day", 
         "pick_month", "pick_year", "pick_weekday", "drop_hour", "drop_day", 
         "drop_month", "drop_year", "drop_weekday"],
        datetime_dim.values.tolist()
    )
    print(f"{Fore.GREEN}✓ DateTime Dimension created with {len(datetime_dim)} records{Style.RESET_ALL}")

    print(f"\n{Fore.YELLOW}📝 Inserting Data into Passenger Count Dimension...{Style.RESET_ALL}")

    passenger_count_dim = passenger_count_dim[['passenger_count_id','passenger_count']]
    insert_into_table(
        "passenger_count_dim",
        ["passenger_count_id", "passenger_count"],
        passenger_count_dim.values.tolist()
    )
    print(f"{Fore.GREEN}✓ Passenger Count Dimension created with {len(passenger_count_dim)} records{Style.RESET_ALL}")

    print(f"\n{Fore.YELLOW}📝 Inserting Data into Trip Distance Dimension...{Style.RESET_ALL}")


    trip_distance_dim = trip_distance_dim[['trip_distance_id','trip_distance']]
    insert_into_table(
        "trip_distance_dim",
        ["trip_distance_id", "trip_distance"],
        trip_distance_dim.values.tolist()
    )
    print(f"{Fore.GREEN}✓ Trip Distance Dimension created with {len(trip_distance_dim)} records{Style.RESET_ALL}")
    payment_type_dim = payment_type_dim[['payment_type_id','payment_type','payment_type_desc']]     
    insert_into_table(
        "payment_type_dim",
        ["payment_type_id", "payment_type", "payment_type_desc"],
        payment_type_dim.values.tolist()
    )
    print(f"{Fore.GREEN}✓ Payment Type Dimension created with {len(payment_type_dim)} records{Style.RESET_ALL}")

    print(f"\n{Fore.YELLOW}📝 Inserting Data into Rate Code Dimension...{Style.RESET_ALL}")

    rate_code_dim = rate_code_dim[['rate_code_dim_id','rate_code_id','rate_code_desc']]
    insert_into_table(
        "rate_code_dim",
        ["rate_code_dim_id", "rate_code_id", "rate_code_desc"],
        rate_code_dim.values.tolist()
    )
    print(f"{Fore.GREEN}✓ Rate Code Dimension created with {len(rate_code_dim)} records{Style.RESET_ALL}")

    print(f"\n{Fore.YELLOW}📝 Inserting Data into Dropoff Location Dimension...{Style.RESET_ALL}")

    # Insert data into dropoff_location_dim
    dropoff_location_dim = dropoff_location_dim[['dropoff_loc_id','dropoff_latitude','dropoff_longitude']]
    insert_into_table(
        "dropoff_location_dim",
        ["dropoff_loc_id", "dropoff_latitude", "dropoff_longitude"],
        dropoff_location_dim.values.tolist()
    )
    print(f"{Fore.GREEN}✓ Dropoff Location Dimension created with {len(dropoff_location_dim)} records{Style.RESET_ALL}")
    print(f"\n{Fore.YELLOW}📝 Inserting Data into Pickup Location Dimension...{Style.RESET_ALL}")

    pickup_location_dim = pickup_location_dim[['pickup_loc_id','pickup_latitude','pickup_longitude']]
    insert_into_table(
        "pickup_location_dim",
        ["pickup_loc_id", "pickup_latitude", "pickup_longitude"],
        pickup_location_dim.values.tolist()
    )
    print(f"{Fore.GREEN}✓ Pickup Location Dimension created with {len(pickup_location_dim)} records{Style.RESET_ALL}")

    print(f"\n{Fore.YELLOW}📝 Inserting Data into Fact Table...{Style.RESET_ALL}")

    fact_table = fact_table[['vendor_id', 'datetime_id', 'passenger_count_id', 'trip_distance_id', 
                            'rate_code_dim_id', 'store_and_fwd_flag', 'pickup_loc_id', 'dropoff_loc_id', 
                            'payment_type_id', 'fare_amount', 'extra', 'mta_tax', 'tip_amount', 
                            'tolls_amount', 'improvement_surcharge', 'total_amount']]
    insert_into_table(
        "fact_table",
        ["vendor_id", "datetime_id", "passenger_count_id", "trip_distance_id", 
         "rate_code_dim_id", "store_and_fwd_flag", "pickup_loc_id", "dropoff_loc_id", 
         "payment_type_id", "fare_amount", "extra", "mta_tax", "tip_amount", 
         "tolls_amount", "improvement_surcharge", "total_amount"],
        fact_table.values.tolist()
    )
    print(f"{Fore.GREEN}✓ Fact Table created with {len(fact_table)} records{Style.RESET_ALL}")

    print(f"\n{Fore.CYAN}{'='*80}")
    print(f"{Fore.WHITE}🎉 Silver to Gold Transformation Complete!")
    print(f"{Fore.CYAN}{'='*80}{Style.RESET_ALL}\n")

if __name__ == "__main__":
    dataset_path = "./data/"
    
    api = authenticate_kaggle()
    #download_dataset(api, dataset_path)
    df = load_and_concat_csv(dataset_path)

    session = create_cassandra_session()

    target_tables=["uber_bronze","uber_silver","uber_gold","uber_reconciliation_bronze"]
    tables = list_cassandra_tables(session, target_tables)

    create_reconciliation_table(session)
    if "uber_bronze" in tables:
        print("uber_bronze table exists")
        df_bronze = fetch_bronze_data(session)
        missing_records = find_missing_records(df, df_bronze)
        insert_records_to_cassandra(session, missing_records, "uber_bronze", "uber_reconciliation_bronze")
    else:
        print("uber_bronze table does not exist")
        create_bronze_table(session)
        insert_records_to_cassandra(session, df, "uber_bronze", "uber_reconciliation_bronze")

    process_reconciliation_bronze_to_silver(session)
    transform_silver_to_gold(session)
    
    session.shutdown()