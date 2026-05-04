print("🚀 Script başlatıldı, veritabanına bağlanmaya çalışılıyor...")
import pandas as pd
from sqlalchemy import create_engine

# Veritabanı bağlantı bilgilerini kontrol et (Şifren farklıysa güncelle)
engine = create_engine("postgresql+psycopg2://postgres:password@localhost:5432/ecommerce_db")

# Kontrol edilecek tabloların listesi
tables = ['users', 'customer_profiles', 'products', 'orders', 'order_items', 'shipments', 'reviews', 'categories', 'stores']

print("\n--- 📊 VERİTABANI DOLULUK RAPORU ---")
for table in tables:
    try:
        # SQL sorgusu ile satır sayısını alıyoruz
        count_df = pd.read_sql(f"SELECT COUNT(*) as total FROM {table}", engine)
        row_count = count_df['total'][0]
        
        if row_count > 0:
            print(f"✅ {table.upper():15} : {row_count} kayıt yüklendi.")
        else:
            print(f"⚠️ {table.upper():15} : Tablo var ama İÇİ BOŞ.")
            
    except Exception as e:
        print(f"❌ {table.upper():15} : Tablo bulunamadı veya bir hata oluştu!")

print("------------------------------------\n")