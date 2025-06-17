import pandas as pd 
import numpy as np
import os
from sqlalchemy import Table, Column, Integer, Float, VARCHAR, DateTime, MetaData, ForeignKey, create_engine, update, insert, TIMESTAMP, Boolean
import sqlalchemy 
from sqlalchemy.sql import text, and_ 
from datetime import datetime
import schedule
import time
from dotenv import load_dotenv
import warnings

from data_quality_checker import DataQualityChecker

warnings.filterwarnings('ignore')

def process():
    load_dotenv()

    sqlalchemy_db = os.getenv("sqlalchemy_db")
    engine = create_engine(sqlalchemy_db)

    # Your folder path
    folder_path = os.getenv("folder_path")

    dataframes = {}

    # Loop through all files in the folder
    for file_name in os.listdir(folder_path):
        if file_name.endswith('.csv'):
            file_path = os.path.join(folder_path, file_name)
            df_name = file_name.replace('.csv', '').replace('olist_', '').replace('_dataset', '')

            # First read normally
            temp_df = pd.read_csv(
                file_path,
                sep=',',
                low_memory=False,
                na_values=['nan', '?']
            )

            # Detect datetime columns
            datetime_cols = [col for col in temp_df.columns if 'date' in col.lower() or 'time' in col.lower() or 'approved' in col.lower()]

            # Re-read parsing datetime if needed
            if datetime_cols:
                df = pd.read_csv(
                    file_path,
                    sep=',',
                    parse_dates=datetime_cols,
                    low_memory=False,
                    na_values=['nan', '?']
                )
            else:
                df = temp_df

            # Save dataframe
            dataframes[df_name] = df

    # dataframes['order_payments'] = order_payments
    # dataframes['order_reviews'] = order_reviews

    tables_config = {
        "product_category_name_translation": {
            "schema": [
                Column('product_category_name', VARCHAR(100), primary_key=True),
                Column('product_category_name_english', VARCHAR(100)),
            ]
        },
        "customers": {
            "schema": [
                Column('customer_id', VARCHAR(100), primary_key=True),
                Column('customer_unique_id', VARCHAR(100)),
                Column('customer_zip_code_prefix', Integer),
                Column('customer_city', VARCHAR(50)),
                Column('customer_state', VARCHAR(50)),
            ]
        },
        "geolocation": {
            "schema": [
                Column('geolocation_zip_code_prefix', Integer),
                Column('geolocation_lat', Float),
                Column('geolocation_lng', Float),
                Column('geolocation_city', VARCHAR(50)),
                Column('geolocation_state', VARCHAR(50)),
            ]
        },
        "orders": {
            "schema": [
                Column('order_id', VARCHAR(100), primary_key=True),
                Column('customer_id', VARCHAR(100), ForeignKey('customers.customer_id')),
                Column('order_status', VARCHAR(50)),
                Column('order_purchase_timestamp', DateTime),
                Column('order_approved_at', DateTime),
                Column('order_delivered_carrier_date', DateTime),
                Column('order_delivered_customer_date', DateTime),
                Column('order_estimated_delivery_date', DateTime),
            ]
        },
        "products": {
            "schema": [
                Column('product_id', VARCHAR(100), primary_key=True),
                Column('product_category_name', VARCHAR(100), ForeignKey('product_category_name_translation.product_category_name')),
                Column('product_name_lenght', Integer),
                Column('product_description_lenght', Integer),
                Column('product_photos_qty', Integer),
                Column('product_weight_g', Integer),
                Column('product_length_cm', Integer),
                Column('product_height_cm', Integer),
                Column('product_width_cm', Integer),  
                Column('updated_at', TIMESTAMP, server_default=text('CURRENT_TIMESTAMP')),
                Column('is_deleted', Boolean, default=False)
            ]
        },
        "products_history": {
            "schema": [
                Column('product_id', VARCHAR(100), primary_key=True),
                Column('product_category_name', VARCHAR(100)),
                Column('product_name_lenght', Integer),
                Column('product_description_lenght', Integer),
                Column('product_photos_qty', Integer),
                Column('product_weight_g', Integer),
                Column('product_length_cm', Integer),
                Column('product_height_cm', Integer),
                Column('product_width_cm', Integer),
                Column('is_deleted', Boolean, default=False),
                Column('valid_from', DateTime, primary_key=True),
                Column('valid_to', DateTime, primary_key=True),
            ]
        },
        "sellers": {
            "schema": [
                Column('seller_id', VARCHAR(100), primary_key=True),
                Column('seller_zip_code_prefix', Integer),
                Column('seller_city', VARCHAR(50)),
                Column('seller_state', VARCHAR(50)),
                Column('updated_at', TIMESTAMP, server_default=text('CURRENT_TIMESTAMP')),
                Column('is_deleted', Boolean, default=False)
            ]
        },
        "sellers_history": {
            "schema": [
                Column('seller_id', VARCHAR(100), primary_key=True),
                Column('seller_zip_code_prefix', Integer),
                Column('seller_city', VARCHAR(50)),
                Column('seller_state', VARCHAR(50)),
                Column('is_deleted', Boolean, default=False),
                Column('valid_from', DateTime, primary_key=True),
                Column('valid_to', DateTime, primary_key=True),
            ]
        },
        "order_items": {
            "schema": [
                Column('order_id', VARCHAR(100), ForeignKey('orders.order_id'), primary_key=True),
                Column('order_item_id', Integer, primary_key=True),
                Column('product_id', VARCHAR(100), primary_key=True),
                Column('seller_id', VARCHAR(100)),
                Column('shipping_limit_date', DateTime),
                Column('price', Float),
                Column('freight_value', Float)
            ]
        },
        "order_payments": {
            "schema": [
                Column('order_id', VARCHAR(100), ForeignKey('orders.order_id'), primary_key=True),
                Column('payment_sequential', Integer, primary_key=True),
                Column('payment_type', VARCHAR(50)),
                Column('payment_installments', Integer),
                Column('payment_value', Float),
            ]
        },
        "order_reviews": {
            "schema": [
                Column('review_id', VARCHAR(100), primary_key=True),
                Column('order_id', VARCHAR(100), ForeignKey('orders.order_id'), primary_key=True),
                Column('review_score', Integer),
                Column('review_comment_title', VARCHAR(50)),
                Column('review_comment_message', VARCHAR(500)),
                Column('review_creation_date', DateTime),
                Column('review_answer_timestamp', DateTime),
            ]
        },
        "geo_city_state": {
            "schema": [
                Column('city_id', Integer, primary_key=True, autoincrement=True),
                Column('city', VARCHAR(50)),
                Column('state', VARCHAR(50)),
            ]
        },
        "geo_zip": {
            "schema": [
                Column('zip_code', Integer, primary_key=True),
                Column('city_id', Integer, ForeignKey('geo_city_state.city_id'))
            ]
        },
        "geo_coordinates": {
            "schema": [
                Column('zip_code', Integer,ForeignKey('geo_zip.zip_code')),   
                Column('longitude', Float, primary_key=True),
                Column('latitude', Float, primary_key=True)
            ]
        },
    }
    def split_and_save_geolocation(geolocation_df, dataframes):
        geo_city_state = (
            geolocation_df[['geolocation_city', 'geolocation_state']]
            .drop_duplicates()
            .reset_index(drop=True)
            .rename(columns={'geolocation_city': 'city', 'geolocation_state': 'state'})
        )
        geo_city_state['city_id'] = geo_city_state.index + 1

        city_map = geo_city_state.set_index(['city', 'state'])['city_id'].to_dict()
        geolocation_df['city_id'] = geolocation_df.apply(
            lambda row: city_map.get((row['geolocation_city'], row['geolocation_state'])),
            axis=1
        )

        geo_zip = (
            geolocation_df[['geolocation_zip_code_prefix', 'city_id']]
            .drop_duplicates(subset=['geolocation_zip_code_prefix'])
            .rename(columns={'geolocation_zip_code_prefix': 'zip_code'})
        )

        geo_coordinates = (
            geolocation_df[['geolocation_zip_code_prefix', 'geolocation_lng', 'geolocation_lat']]
            .drop_duplicates()
            .rename(columns={
                'geolocation_zip_code_prefix': 'zip_code',
                'geolocation_lng': 'longitude',
                'geolocation_lat': 'latitude'
            })
        )

        dataframes['geo_city_state'] = geo_city_state
        dataframes['geo_zip'] = geo_zip
        dataframes['geo_coordinates'] = geo_coordinates


        return dataframes
    geolocation_df = dataframes['geolocation']
    dataframes = split_and_save_geolocation(geolocation_df, dataframes)
    dataframes.pop('geolocation', None)

    geo_zip = dataframes['geo_zip']

    
    
    checker = DataQualityChecker(dataframes, tables_config)
    print(dataframes['products'].info())
    # Run all checks
    checker.run_all_checks()

    # Run business rules checks
    checker.run_business_rules()

    # Get cleaned data as a dictionary
    cleaned_data = checker.get_cleaned_data_dict()

    cleaned_data['geo_zip'] = geo_zip

    metadata = MetaData()

    for table_name, config in tables_config.items():
        Table(table_name, metadata, *config["schema"])

    metadata.create_all(engine)

    
    def get_changed_rows(new_df, existing_df, pk):
        """
        Find rows from new_df where any non-PK column differs.
        Returns changed rows from new_df.
        """
        for col in pk:
            new_df[col] = new_df[col].astype(str)
            existing_df[col] = existing_df[col].astype(str)

        # Use .copy() for safe manipulation
        new_df_indexed = new_df.set_index(pk).copy()
        existing_df_indexed = existing_df.set_index(pk).copy()

        common_idx = new_df_indexed.index.intersection(existing_df_indexed.index)
        common_cols = new_df_indexed.columns.intersection(existing_df_indexed.columns)

        new_common = new_df_indexed.loc[common_idx, common_cols].sort_index().copy()
        existing_common = existing_df_indexed.loc[common_idx, common_cols].sort_index().copy()

        existing_common = existing_common[new_common.columns]

        diff = new_common.compare(existing_common, keep_equal=False)

        changed = new_df_indexed.loc[diff.index].reset_index().copy()
        changed = changed.where(pd.notna(changed), None)

        return changed

    def get_primary_keys(table_name):
        return [col.name for col in tables_config[table_name]['schema'] if col.primary_key]

    def load_cleaned_dataframe(table_name, df, chunksize=5000):
        table = Table(table_name, metadata, autoload_with=engine)
        pk = get_primary_keys(table_name)
        if isinstance(pk, str):
            pk = [pk]

        df = df.copy()  # safe copy
        for col in df.columns:
            df[col] = df[col].astype(object).where(pd.notna(df[col]), None)

        existing_df = pd.read_sql_table(table_name, engine)

        if existing_df.empty:
            for i in range(0, len(df), chunksize):
                chunk = df.iloc[i:i+chunksize]
                with engine.begin() as conn:
                    conn.execute(table.insert(), chunk.to_dict(orient='records'))
            print(f"{table_name}: Initial load successful.")
        else:
            existing_df = existing_df.copy()  # safe copy

            for col in pk:
                df[col] = df[col].astype(str)
                existing_df[col] = existing_df[col].astype(str)

            df = df.reset_index(drop=True).copy()
            existing_df = existing_df.reset_index(drop=True).copy()

            existing_keys = existing_df[pk].drop_duplicates().copy()
            new_rows = df.merge(existing_keys, on=pk, how='left', indicator=True)
            new_rows = new_rows[new_rows['_merge'] == 'left_only'].drop(columns=['_merge'])

            if not new_rows.empty:
                print(f"{table_name}: Inserting {len(new_rows)} new rows.")
                for i in range(0, len(new_rows), chunksize):
                    chunk = new_rows.iloc[i:i+chunksize]
                    with engine.begin() as conn:
                        conn.execute(table.insert(), chunk.to_dict(orient='records'))
            else:
                print(f"{table_name}: No new rows to insert.")

        # Get changed rows (non-PK values) by comparing existing_df and df
            changed_rows = get_changed_rows(df, existing_df, pk)
            # print(f"Changed rows for {table_name}:\n{changed_rows}")

            if not changed_rows.empty:
                for _, row in changed_rows.iterrows():
                    # Handle updates and move old row to history for products and sellers
                    if table_name in ['products', 'sellers']:
                        # Get the existing row that is being updated
                        existing_row = existing_df[existing_df[pk[0]] == row[pk[0]]].iloc[0]

                        # Move old row to history table
                        with engine.begin() as conn:
                            history_table = Table(f"{table_name}_history", metadata)
                            history_data = existing_row.to_dict()
                            history_data = {k: (None if isinstance(v, float) and np.isnan(v) else v) for k, v in history_data.items()}
                            history_data['valid_from'] = existing_row['updated_at']
                            history_data['valid_to'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                            history_data['is_deleted'] = existing_row['is_deleted']

                            # conn.execute(history_table.insert(), history_data)

                            try:
                                # Insert into history table and catch potential duplicate primary key errors
                                conn.execute(history_table.insert(), history_data)
                            except sqlalchemy.exc.IntegrityError:
                                print(f"Duplicate entry in {table_name}_history, skipping insert")


                        # Update the main table (set new values and update `updated_at`)
                        with engine.begin() as conn:
                            update_stmt = update(table).where(
                                and_(*[table.c[k] == row[k] for k in pk])
                            ).values(
                                **{col: row[col] for col in df.columns if col not in ['_merge', 'is_deleted']},
                                updated_at=datetime.now(),
                                is_deleted=row.get('is_deleted', False)
                            )
                            conn.execute(update_stmt)

            # Find records in DB that are not in incoming data
            deleted_rows = existing_df[~existing_df[pk[0]].isin(df[pk[0]])]

            if not deleted_rows.empty and table_name in ['products', 'sellers']:
                for _, row in deleted_rows.iterrows():
                    # Move old row to history table
                    with engine.begin() as conn:
                        history_table = Table(f"{table_name}_history", metadata)
                        history_data = row.to_dict()
                        history_data = {k: (None if isinstance(v, float) and np.isnan(v) else v) for k, v in history_data.items()}
                        history_data['valid_from'] = row['updated_at']
                        history_data['valid_to'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                        history_data['is_deleted'] = True
                        # conn.execute(history_table.insert(), history_data)

                        try:
                            # Insert into history table and catch potential duplicate primary key errors
                            conn.execute(history_table.insert(), history_data)
                        except sqlalchemy.exc.IntegrityError:
                            print(f"Duplicate entry in {table_name}_history, skipping insert for seller_id {history_data['seller_id']}")


                    # Mark the main table record as deleted
                    with engine.begin() as conn:
                        update_stmt = update(table).where(
                            table.c[pk[0]] == row[pk[0]]
                        ).values(
                            is_deleted=True,
                            updated_at=datetime.now()
                        )
                        conn.execute(update_stmt)


            print(f"{table_name} cleaned data loaded successfully.")


    table_order = [
        "product_category_name_translation",  
        "customers",                         
        "geo_city_state",
        "geo_zip",
        "geo_coordinates",                 
        "products",                                            
        "sellers",                                        
        "orders",                            
        "order_items",                     
        "order_payments",               
        "order_reviews"             
    ]


    for table_name in table_order:
        df = cleaned_data.get(table_name)
        if df is not None:

            load_cleaned_dataframe(table_name, df.copy())


    table_order = [
        "product_category_name_translation",  
        "customers",                         
        "geo_city_state",
        "geo_zip",
        "geo_coordinates",                 
        "products",                                            
        "sellers",                                        
        "orders",                            
        "order_items",                     
        "order_payments",               
        "order_reviews"             
    ]


    for table_name in table_order:
        df = cleaned_data.get(table_name)
        if df is not None:
            load_cleaned_dataframe(table_name, df.copy())
    print("Task executed successfully!")

# Schedule the task
schedule.every(8).minutes.do(process)


while True:
    schedule.run_pending()
    time.sleep(30)

# process()