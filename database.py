from langchain_community.utilities import SQLDatabase
import sqlalchemy

def get_db():
    # BURAYI DÜZENLE: 'password' yerine kendi şifreni, 'ecommerce_db' yerine veritabanı adını yaz
    user = "postgres"
    password = "password" 
    host = "localhost"
    port = "5432"
    database = "ecommerce_db"
    
    postgres_uri = f"postgresql://{user}:{password}@{host}:{port}/{database}"
    try:
        # pool_pre_ping: her sorgudan önce bağlantıyı kontrol eder
        engine = sqlalchemy.create_engine(
            postgres_uri, 
            connect_args={'connect_timeout': 5},
            pool_pre_ping=True
        )
        # view_support=True: view'ları da şemaya dahil eder
        return SQLDatabase(engine, view_support=True)
    except Exception as e:
        print(f"Veritabanı bağlantı hatası: {e}")
        return None
