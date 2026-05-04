import pandas as pd
import os
from sqlalchemy import create_engine, inspect, text
import numpy as np
import logging
import hashlib
import sys
import uuid
import random

# --- LOGGING CONFIGURATION ---
# Terminal: Clean and brief
# File: Detailed and technical
LOG_FILE = "../etl_errors.log"

# Clear log file on startup
with open(LOG_FILE, 'w') as f:
    f.write("--- ETL Error Log Started ---\n")

# Configure root logger
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

# File Handler (Detailed)
file_handler = logging.FileHandler(LOG_FILE)
file_handler.setLevel(logging.DEBUG)
file_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(name)s - %(message)s')
file_handler.setFormatter(file_formatter)

# Stream Handler (Brief)
stream_handler = logging.StreamHandler(sys.stdout)
stream_handler.setLevel(logging.INFO)
stream_formatter = logging.Formatter('%(levelname)s: %(message)s')
stream_handler.setFormatter(stream_formatter)

logger.addHandler(file_handler)
logger.addHandler(stream_handler)

# Suppress SQLAlchemy internal logging
logging.getLogger('sqlalchemy.engine').setLevel(logging.WARNING)

# --- DATABASE & PATHS ---
DB_URL = "postgresql://postgres:password@localhost:5432/ecommerce_db"
engine = create_engine(DB_URL)

RAW_DATA_PATH = "../raw_data"

def load_csv(filename, delimiter=','):
    """Loads CSV with fallback encoding and row limit."""
    path = os.path.join(RAW_DATA_PATH, filename)
    if not os.path.exists(path):
        logging.warning(f"File not found: {path}")
        return pd.DataFrame()
        
    read_params = {
        'sep': delimiter,
        'low_memory': False,
        'on_bad_lines': 'skip',
        'nrows': 5000
    }
    try:
        df = pd.read_csv(path, encoding='utf-8', **read_params)
    except UnicodeDecodeError:
        df = pd.read_csv(path, encoding='ISO-8859-1', **read_params)
    
    df.columns = df.columns.str.strip()
    return df

def get_target_schema(table_name):
    """Dynamically fetches column names, types, and nullability from the DB."""
    inspector = inspect(engine)
    columns = inspector.get_columns(table_name)
    return {col['name']: {'nullable': col['nullable'], 'type': str(col['type']).lower()} for col in columns}

def generate_numeric_id(val):
    """Generates a consistent positive integer from any input using hashing."""
    if pd.isna(val) or val == '':
        return 0
    if isinstance(val, (int, float, np.integer, np.floating)):
        try:
            return int(val)
        except:
            pass
    hash_object = hashlib.md5(str(val).encode())
    return int(hash_object.hexdigest(), 16) % (10**9)

def autonomous_transform(df, table_name, manual_mapping=None):
    """Adapts the DataFrame to the database table schema autonomously."""
    if df.empty:
        return df

    target_schema = get_target_schema(table_name)
    target_cols = list(target_schema.keys())
    
    if manual_mapping:
        df = df.rename(columns=manual_mapping)

    df = df[[col for col in df.columns if col in target_cols]].copy()
    
    if 'id' in df.columns:
        df = df.drop(columns=['id'])

    for col, info in target_schema.items():
        if col == 'id': continue
        
        if col not in df.columns:
            if not info['nullable']:
                if 'int' in info['type'] or 'decimal' in info['type'] or 'float' in info['type']:
                    df[col] = 0
                elif 'bool' in info['type']:
                    df[col] = False
                else:
                    df[col] = "Unknown"
            else:
                df[col] = None
        else:
            if 'int' in info['type'] or 'decimal' in info['type'] or 'float' in info['type']:
                if col.endswith('_id'):
                    df[col] = df[col].apply(generate_numeric_id)
                else:
                    # For prices and quantities, try to convert to numeric, fallback to 0
                    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
                
    final_cols = [c for c in target_cols if c != 'id']
    return df[final_cols]

def run_etl():
    logging.info("--- Starting Enhanced Autonomous ETL Pipeline ---")

    # --- CLEANUP PHASE (Reverse Dependency Order) ---
    # To prevent UniqueViolation on second run, we clear tables before loading.
    # We use 'DELETE FROM' to keep the schema and sequences intact.
    cleanup_tables = [
        'reviews', 'shipments', 'order_items', 'orders', 
        'products', 'customer_profiles', 'stores', 'categories', 'users'
    ]
    logging.info("Cleaning up existing data...")
    for table_name in cleanup_tables:
        try:
            # Check if table exists before trying to delete
            inspector = inspect(engine)
            if table_name in inspector.get_table_names():
                with engine.connect() as conn:
                    conn.execute(text(f"DELETE FROM {table_name}"))
                    conn.commit()
        except Exception as e:
            logging.warning(f"Could not clean table {table_name}: {str(e)[:100]}")

    # 1. Extraction
    df_amazon = load_csv("Amazon Sale Report.csv")
    df_retail = load_csv("online_retail_final.csv")
    df_behavior = load_csv("E-commerce Customer Behavior - Sheet1.csv")
    df_train = load_csv("Train.csv")
    df_reviews_raw = load_csv("amazon_reviews_multilingual_US_v1_00.tsv", delimiter='\t')

    # 2. Sequential Loading
    tables_to_load = [
        {'name': 'users', 'df': df_behavior, 'mapping': {'Customer ID': 'id', 'Gender': 'gender'}},
        {'name': 'categories', 'df': pd.DataFrame(['Apparel', 'Home Decor', 'Kitchen', 'Accessories', 'Electronics', 'Books', 'Gifts', 'Other'], columns=['name']), 'mapping': {}},
        {'name': 'stores', 'df': pd.DataFrame([{'owner_id': 1, 'name': 'Global Store', 'status': 'ACTIVE'}]), 'mapping': {}},
        {'name': 'customer_profiles', 'df': df_behavior, 'mapping': {'Customer ID': 'user_id', 'Age': 'age', 'City': 'city', 'Membership Type': 'membership_type'}},
        {'name': 'products', 'df': df_retail, 'mapping': {'StockCode': 'sku', 'Description': 'name', 'UnitPrice': 'unit_price'}},
        {'name': 'orders', 'df': df_amazon, 'mapping': {'Order ID': 'user_id', 'Amount': 'grand_total', 'Status': 'status'}},
        {'name': 'order_items', 'df': df_retail, 'mapping': {'Quantity': 'quantity', 'UnitPrice': 'price', 'StockCode': 'product_id', 'InvoiceNo': 'order_id'}},
        {'name': 'shipments', 'df': df_train, 'mapping': {'Warehouse_block': 'warehouse', 'Mode_of_Shipment': 'mode'}},
        {'name': 'reviews', 'df': df_reviews_raw, 'mapping': {'star_rating': 'star_rating', 'product_id': 'product_id'}}
    ]

    for table in tables_to_load:
        try:
            logging.info(f"Processing table: {table['name']}...")
            
            # Special handling for products to map category names to IDs
            if table['name'] == 'products':
                df_categories = pd.read_sql("SELECT id, name FROM categories", engine)
                cat_map = dict(zip(df_categories['name'], df_categories['id']))
                
                # Keywords to category mapping
                keyword_map = {
                    't-light': 'Home Decor',
                    'candle': 'Home Decor',
                    'mug': 'Kitchen',
                    'box': 'Home Decor',
                    'bag': 'Accessories',
                    'doll': 'Gifts',
                    'toy': 'Gifts',
                    'shirt': 'Apparel',
                    'socks': 'Apparel',
                    'case': 'Home Decor',
                    'holder': 'Home Decor'
                }

                def get_cat_id(name):
                    name = str(name).lower()
                    for keyword, cat_name in keyword_map.items():
                        if keyword in name:
                            return cat_map.get(cat_name, cat_map.get('Other'))
                    return cat_map.get('Other')
                
                # Apply mapping based on product name
                table['df']['category_id'] = table['df']['Description'].apply(get_cat_id)
                
                # Manual transformation for products to be absolutely sure
                df_prod = table['df'].rename(columns=table['mapping'])
                df_prod = df_prod[['sku', 'name', 'unit_price', 'category_id']]
                df_prod['store_id'] = 1 # Force a valid store_id
                df_prod['image_url'] = None
                
                # Deduplicate by SKU to prevent DB errors
                df_prod = df_prod.drop_duplicates(subset=['sku'])
                
                df_prod.to_sql('products', engine, if_exists='append', index=False, method='multi', chunksize=500)
                logging.info(f"DONE: Loaded {len(df_prod)} products with dynamic categories.")
                continue # Skip the general processing below for products

            transformed_df = autonomous_transform(table['df'], table['name'], table['mapping'])
            
            if table['name'] == 'users':
                df_users = transformed_df
                if 'email' in df_users.columns:
                    mask = (df_users['email'].isna()) | (df_users['email'] == 'Unknown')
                    df_users.loc[mask, 'email'] = [f"user_{uuid.uuid4().hex[:8]}@example.com" for _ in range(mask.sum())]
                transformed_df = df_users
            
            # --- SYNTHETIC FOREIGN KEY ASSOCIATION ---
            if not transformed_df.empty:
                fk_checks = [
                    ('user_id', 'users'),
                    ('product_id', 'products'),
                    ('order_id', 'orders'),
                    ('store_id', 'stores')
                ]
                
                for col, parent_table in fk_checks:
                    if col in transformed_df.columns:
                        valid_ids = pd.read_sql(f"SELECT id FROM {parent_table}", engine)['id'].tolist()
                        if valid_ids:
                            transformed_df[col] = transformed_df[col].apply(
                                lambda x: x if x in valid_ids else random.choice(valid_ids)
                            )
                        else:
                            logging.warning(f"No valid IDs found in {parent_table}. Row integrity might be compromised for {col}.")

            # --- ONE-TO-ONE / UNIQUE CONSTRAINT ENFORCEMENT ---
            if table['name'] == 'customer_profiles' and 'user_id' in transformed_df.columns:
                transformed_df = transformed_df.drop_duplicates(subset=['user_id'])
            
            if table['name'] == 'shipments' and 'order_id' in transformed_df.columns:
                transformed_df = transformed_df.drop_duplicates(subset=['order_id'])

            if table['name'] == 'reviews':
                # Common uniqueness for reviews: one review per user-product or one per order
                if 'order_id' in transformed_df.columns:
                    transformed_df = transformed_df.drop_duplicates(subset=['order_id'])
                elif 'user_id' in transformed_df.columns and 'product_id' in transformed_df.columns:
                    transformed_df = transformed_df.drop_duplicates(subset=['user_id', 'product_id'])

            if not transformed_df.empty:
                transformed_df.to_sql(table['name'], engine, if_exists='append', index=False, method='multi', chunksize=500)
                logging.info(f"DONE: Loaded {len(transformed_df)} rows into {table['name']}.")
            else:
                logging.warning(f"SKIP: No data to load for {table['name']}.")
        except Exception as e:
            # Brief message for terminal
            brief_error = str(e).split('\n')[0][:100]
            logging.error(f"FAIL: Table '{table['name']}'. Reason: {brief_error}...")
            # Full technical details are automatically handled by file_handler at DEBUG level
            logging.debug(f"Full error for {table['name']}:", exc_info=True)

    logging.info("--- ETL Process Completed. Check 'etl_errors.log' for details. ---")

if __name__ == "__main__":
    run_etl()
