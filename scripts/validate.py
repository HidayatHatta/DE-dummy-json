import pandas as pd

def validate_users(df_users):
    assert len(df_users) > 0, "Tabel Users kosong."
    assert df_users["id"].is_unique, "Ditemukan ID User duplikat."
    assert df_users["email"].notnull().all(), "Ditemukan nilai Null pada kolom email."
    assert df_users["email"].astype(str).str.contains("@").all(), "Ditemukan format email tidak valid."
    return True

def validate_products(df_products):
    assert df_products["id"].is_unique, "Ditemukan ID Product duplikat."
    assert (df_products["price"] > 0).all(), "Ditemukan harga produk <= 0."
    return True

def validate_carts_header(df_carts_header, df_users):
    assert df_carts_header["id"].is_unique, "Ditemukan ID Cart duplikat."
    
    invalid_users = df_carts_header[~df_carts_header["userid"].isin(df_users["id"])]
    assert len(invalid_users) == 0, f"Ditemukan userid di tabel Carts yang tidak terdaftar di tabel Users: {invalid_users['userid'].unique()}"
    return True

def validate_cart_items(df_cart_items, df_carts_header, df_products):
    assert len(df_cart_items) > 0, "Tabel Cart Items kosong."
    assert (df_cart_items["quantity"] > 0).all(), "Ditemukan kuantitas barang <= 0."

    invalid_carts = df_cart_items[~df_cart_items["cart_id"].isin(df_carts_header["id"])]
    assert len(invalid_carts) == 0, "Ditemukan item dengan cart_id yang tidak terdaftar di Carts Header."

    invalid_products = df_cart_items[~df_cart_items["product_id"].isin(df_products["id"])]
    assert len(invalid_products) == 0, "Ditemukan item dengan product_id yang tidak terdaftar di tabel Products."
    return True

def validate(processed_file_paths):
    
    df_users = pd.read_csv(processed_file_paths['users'])
    df_products = pd.read_csv(processed_file_paths['products'])
    df_carts_header = pd.read_csv(processed_file_paths['carts'])
    df_cart_items = pd.read_csv(processed_file_paths['cart_items'])

    validate_users(df_users)
    validate_products(df_products)
    validate_carts_header(df_carts_header, df_users)
    validate_cart_items(df_cart_items, df_carts_header, df_products)

    print("Seluruh validasi kualitas data berhasil dilewati (Passed).")
    
    return processed_file_paths