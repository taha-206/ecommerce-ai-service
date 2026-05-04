import pandas as pd
from sqlalchemy import create_engine, text
import os

# Veritabanı bağlantı bilgileri
DB_USER = "postgres"
DB_PASS = "Mete209."
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "ecommerce_db"

def get_engine():
    uri = f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    return create_engine(uri)

def setup_database(engine):
    with engine.connect() as conn:
        print("Cleaning up and creating tables...")
        
        # 1. Order Reviews
        conn.execute(text("DROP TABLE IF EXISTS order_reviews CASCADE;"))
        conn.execute(text("""
            CREATE TABLE order_reviews (
                review_id VARCHAR(255) PRIMARY KEY,
                order_id VARCHAR(255),
                review_score INT,
                review_comment_title TEXT,
                review_comment_message TEXT,
                review_creation_date TIMESTAMP,
                review_answer_timestamp TIMESTAMP
            );
        """))
        
        # 2. Order Items
        conn.execute(text("DROP TABLE IF EXISTS order_items CASCADE;"))
        conn.execute(text("""
            CREATE TABLE order_items (
                order_id VARCHAR(255),
                order_item_id INT,
                product_id VARCHAR(255),
                seller_id VARCHAR(255),
                shipping_limit_date TIMESTAMP,
                price DECIMAL(10, 2),
                freight_value DECIMAL(10, 2),
                PRIMARY KEY (order_id, order_item_id)
            );
        """))
        
        # 3. Products
        conn.execute(text("DROP TABLE IF EXISTS products CASCADE;"))
        conn.execute(text("""
            CREATE TABLE products (
                product_id VARCHAR(255) PRIMARY KEY,
                product_category_name VARCHAR(255),
                product_name_lenght INT,
                product_description_lenght INT,
                product_photos_qty INT,
                product_weight_g INT,
                product_length_cm INT,
                product_height_cm INT,
                product_width_cm INT
            );
        """))
        
        # 4. Customers
        conn.execute(text("DROP TABLE IF EXISTS customers CASCADE;"))
        conn.execute(text("""
            CREATE TABLE customers (
                customer_id VARCHAR(255) PRIMARY KEY,
                customer_unique_id VARCHAR(255),
                customer_zip_code_prefix INT,
                customer_city VARCHAR(255),
                customer_state VARCHAR(255)
            );
        """))
        
        conn.commit()
        print("Database schema ready.")

def import_data(engine):
    datasets = {
        'customers': ('olist_customers_dataset.csv', 'customer_id'),
        'products': ('olist_products_dataset.csv', 'product_id'),
        'order_items': ('olist_order_items_dataset.csv', ['order_id', 'order_item_id']),
        'order_reviews': ('olist_order_reviews_dataset.csv', 'review_id')
    }

    for table, (file_name, pk) in datasets.items():
        csv_path = os.path.join('datasets', file_name)
        if not os.path.exists(csv_path):
            print(f"File not found: {csv_path}")
            continue

        print(f"Processing {file_name}...")
        df = pd.read_csv(csv_path)
        
        # Duplicate temizliği
        original_len = len(df)
        df.drop_duplicates(subset=pk, inplace=True)
        new_len = len(df)
        if original_len > new_len:
            print(f"Dropped {original_len - new_len} duplicate rows.")

        # Tarih alanlarını dönüştür
        date_cols = [col for col in df.columns if 'date' in col or 'timestamp' in col]
        for col in date_cols:
            df[col] = pd.to_datetime(df[col], errors='coerce')

        # İçe aktar
        print(f"Importing {len(df)} rows into {table}...")
        df.to_sql(table, engine, if_exists='append', index=False)
        print(f"Successfully imported {table}.")

if __name__ == "__main__":
    engine = get_engine()
    setup_database(engine)
    import_data(engine)
    print("Olist E-commerce Full Integration Completed.")
