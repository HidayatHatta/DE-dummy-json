import pandas as pd
import ast
import os

def transform(raw_file_paths):

    processed_file_paths = {}
    processed_dir = "/opt/airflow/data/processed/dwh"
    os.makedirs(processed_dir, exist_ok=True)


    df_users = pd.read_csv(raw_file_paths['users'])
    df_products = pd.read_csv(raw_file_paths['products'])
    df_carts = pd.read_csv(raw_file_paths['carts'])

    # 1. TRANSFORM: USERS (Dimensi)
    df_users.columns = [col.lower() for col in df_users.columns]
    
    cols_to_drop = ['password', 'macaddress', 'ip', 'bank.cardexpire', 'bank.cardnumber']
    df_users = df_users.drop(columns=[col for col in cols_to_drop if col in df_users.columns], errors='ignore')
    
    users_path = os.path.join(processed_dir, "dim_users.csv")
    df_users.to_csv(users_path, index=False)
    processed_file_paths['users'] = users_path

    # 2. TRANSFORM: PRODUCTS (Dimensi)
    df_products.columns = [col.lower() for col in df_products.columns]
    
    if '__v' in df_products.columns:
        df_products = df_products.drop(columns=['__v'])
    
    if 'rating' in df_products.columns:
        df_products['rating'] = df_products['rating'].fillna(df_products['rating'].mean())

    products_path = os.path.join(processed_dir, "dim_products.csv")
    df_products.to_csv(products_path, index=False)
    processed_file_paths['products'] = products_path

    # 3. TRANSFORM: CARTS (Fact & Bridge Table)
    df_carts.columns = [col.lower() for col in df_carts.columns]

    df_carts['products'] = df_carts['products'].apply(lambda x: ast.literal_eval(x) if isinstance(x, str) else x)

    df_carts_header = df_carts.drop(columns=['products']).drop_duplicates(subset=['id'])
    
    df_carts_exploded = df_carts[['id', 'products']].explode('products').reset_index(drop=True)
    
    df_items_normalized = pd.json_normalize(df_carts_exploded['products'])
    
    df_cart_items = pd.concat([df_carts_exploded['id'].rename('cart_id'), df_items_normalized], axis=1)
    df_cart_items.columns = [col.lower() for col in df_cart_items.columns]
    
    df_cart_items = df_cart_items.rename(columns={'id': 'product_id'})

    if 'quantity' in df_cart_items.columns and 'price' in df_cart_items.columns:
        df_cart_items['calculated_total'] = df_cart_items['quantity'] * df_cart_items['price']

    carts_header_path = os.path.join(processed_dir, "fact_carts.csv")
    cart_items_path = os.path.join(processed_dir, "bridge_cart_items.csv")
    
    df_carts_header.to_csv(carts_header_path, index=False)
    df_cart_items.to_csv(cart_items_path, index=False)
    
    processed_file_paths['carts'] = carts_header_path
    processed_file_paths['cart_items'] = cart_items_path

    print("Data berhasil ditransformasi dan dipetakan menjadi model relasional.")
    
    return processed_file_paths