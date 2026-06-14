import requests
import pandas as pd
from datetime import datetime
import os

def extract():
    endpoint = {
        'users' : 'https://dummyjson.com/users?limit=0',
        'products' : 'https://dummyjson.com/products?limit=0',
        'carts' : 'https://dummyjson.com/carts?limit=0'
    }
    file_paths = {}
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    raw_dir = "/opt/airflow/data/raw/dwh"
    os.makedirs(raw_dir, exist_ok=True)
    for entity, url in endpoint.items():
        try:
            response = requests.get(url)
            response.raise_for_status()

            data = response.json()

            records = data.get(entity, [])

            df = pd.json_normalize(records)

            file_path = f"{raw_dir}/{entity}_{timestamp}.csv"

            df.to_csv(file_path, index=False)

            file_paths[entity] = file_path

            print(f"Data {entity} berhasil disimpan ke {file_path}")
            
        except requests.exceptions.RequestException as e:
            print(f"Error saat mengekstrak data {entity}: {e}")
            raise
        except KeyError as e:
            print(f"Format JSON tidak sesuai ekspektasi untuk {entity}: {e}")
            raise

    return file_paths