import pandas as pd
from sqlalchemy import create_engine

def load(processed_paths):
    print("Memulai proses pemuatan data ke Data PostgreSQL")
    
    engine = create_engine("postgresql://airflow:airflow@postgres:5432/airflow")
    
    load_order = ['users', 'products', 'carts', 'cart_items']
    
    for table_name in load_order:
        if table_name in processed_paths:
            file_path = processed_paths[table_name]
            print(f"Membaca file {file_path} untuk tabel {table_name}...")
            
            df = pd.read_csv(file_path)
            
            df.to_sql(table_name, engine, if_exists="replace", index=False)
            
            print(f"-> Tabel {table_name} berhasil dimuat ({len(df)} baris).")
        else:
            print(f"Peringatan: File path untuk tabel {table_name} tidak ditemukan dalam XCom.")
            
    print("Seluruh proses telah selesai")
    return True