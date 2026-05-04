import pandas as pd
from sqlalchemy import create_engine
import os

def import_products():
    # Dosya yolu
    csv_path = os.path.join('datasets', 'olist_products_dataset.csv')
    
    # CSV dosyasını oku
    print(f"Reading {csv_path}...")
    df = pd.read_csv(csv_path)
    
    # Sütun eşleştirme: product_id -> name
    df.rename(columns={'product_id': 'name'}, inplace=True)
    
    # Veritabanı bağlantı bilgileri
    # database.py'deki bilgilere göre uyarlandı, şifre kullanıcı isteği üzerine 'password' yapıldı
    user = "postgres"
    password = "password"
    host = "localhost"
    port = "5432"
    database = "ecommerce_db"
    
    engine_uri = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}"
    
    try:
        engine = create_engine(engine_uri)
        
        # Veriyi aktar
        # if_exists='append' (ekle) veya 'replace' (tabloyu silip baştan oluştur) seçilebilir
        # Kullanıcı "aktarmalı" dediği için mevcut tabloya ekleme yapacağını varsayıyorum
        print("Transferring data to PostgreSQL...")
        df.to_sql('products', engine, if_exists='append', index=False)
        print("Successfully imported data to 'products' table.")
        
    except Exception as e:
        print(f"Error during import: {e}")

if __name__ == "__main__":
    import_products()
