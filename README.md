# End-to-End Automated Data Pipeline with Integrated Data Quality Checks

## Deskripsi Proyek
Proyek ini adalah sebuah *Automated Data Pipeline* yang dirancang untuk mensimulasikan arsitektur *Data Warehouse* (DWH) modern berskala produksi. Pipeline ini mengekstrak data transaksi *e-commerce* dari publik REST API (**DummyJSON**), melakukan transformasi dan pembersihan data (*Data Cleaning & Standardization*), menerapkan pemeriksaan kualitas data (*Data Quality Checks*) sebagai *circuit breaker*, serta memuat data ke dalam Data Warehouse **PostgreSQL**.

Seluruh alur kerja diorkestrasi secara otomatis menggunakan **Apache Airflow** dan berjalan di dalam lingkungan terisolasi menggunakan **Docker Compose** di atas **WSL (Windows Subsystem for Linux)**.

---

## Arsitektur ETL (Extract, Transform, Load)

Pipeline ini mengikuti pola arsitektur ETL tradisional yang terbagi menjadi 4 tahap utama di dalam DAG Airflow:

[Extract Task] ──> [Transform Task] ──> [Validate Task] (DQE) ──> [Load Task]

### 1. Extract (`extract.py`)
* **Proses:** Mengambil data secara *asynchronous* dari tiga *endpoint* utama DummyJSON: `/users`, `/products`, dan `/carts`.
* **DQE Mitigasi:** Menggunakan parameter `?limit=0` pada URL permintaan untuk mengambil seluruh rekaman secara utuh. Hal ini dilakukan untuk menghindari masalah *pagination default* (hanya 30 data) yang dapat menyebabkan kegagalan *Referential Integrity* pada tahap validasi.
* **Output:** Menyimpan data mentah ke dalam folder lokal `/opt/airflow/data/raw/dwh/` dalam format CSV (opsi *overwrite* untuk efisiensi penyimpanan).

### 2. Transform (`transform.py`)
* **Standardisasi:** Mengubah semua nama kolom menjadi huruf kecil (*lowercase*) untuk memastikan kompatibilitas yang aman di PostgreSQL.
* **Pembersihan Data:** Menghapus kolom teknis dan informasi sensitif dari tabel pelanggan seperti `password`, `macaddress`, `ip`, dan detail kartu bank.
* **Strukturisasi & Pemetaan:** Membongkar data *nested JSON array* pada kolom `products` di dalam tabel *carts* menggunakan fungsi `ast.literal_eval` dan `explode` di Pandas.
* **Pengayaan Data:** Menambahkan kolom kalkulasi manual (`calculated_total = quantity * price`) sebagai indikator audit finansial.
* **Output:** Menghasilkan 4 file CSV bersih di folder `/opt/airflow/data/processed/dwh/`.

### 3. Validate (`validate.py`) - *Data Quality Engineering*
Tahap ini bertindak sebagai **Circuit Breaker**. Jika ada satu saja aturan kualitas data yang melanggar, *task* ini akan melempar `AssertionError`, menghentikan pipeline, dan mencegah data kotor masuk ke DWH.
* **Completeness Check:** Memastikan tabel tidak kosong dan kolom penting seperti `email` tidak bernilai *Null*.
* **Uniqueness Check:** Memastikan *Primary Key* (`id`) pada tabel master bersifat unik dan tidak duplikat.
* **Validity Check:** Memastikan format teks (seperti karakter `@` pada email) dan batasan nilai logika bisnis (`price > 0` dan `quantity > 0`).
* **Referential Integrity Check:** Memastikan setiap `userid` dan `product_id` yang ada pada data transaksi keranjang belanja benar-benar terdaftar di tabel master *Users* dan *Products*.

### 4. Load (`load.py`)
* **Proses:** Memuat data yang telah dinyatakan "bersih" ke dalam PostgreSQL Data Warehouse menggunakan `sqlalchemy.create_engine` dan *method* `to_sql` pada Pandas.
* **Urutan Pemuatan (*Load Order*):** Menetapkan urutan kaku: `users` -> `products` -> `carts` -> `cart_items`. Tabel Dimensi (master) wajib dimuat terlebih dahulu sebelum Tabel Fakta untuk menjaga hubungan relasional.
* **Strategi:** Menggunakan `if_exists="replace"` untuk menyegarkan skema secara penuh pada setiap jadwal eksekusi harian (*Daily Batch*).

---

## Data Modeling: Pembagian Fact & Dimension Table

Untuk mendukung analisis data analitikal yang optimal (OLAP) dan memungkinkan penulisan *query* SQL tingkat lanjut seperti `JOIN` bertingkat, `Window Functions`, dan agregasi kompleks, struktur data dari API dipecah menjadi **Model Data Relasional (Star Schema/Snowflake Schema)**.



### Mengapa Memisahkan Tabel Menjadi Fact dan Dimension?
1. **Eliminasi Redundansi Data:** Data profil pelanggan (*Users*) dan spesifikasi barang (*Products*) tidak perlu ditulis berulang-ulang pada setiap item transaksi. Cukup direferensikan melalui ID.
2. **Optimasi Performa Query:** Tabel Fakta yang berukuran besar hanya menyimpan angka (metrik) dan ID kunci (*Foreign Keys*), membuat proses agregasi dan filtering ratusan ribu transaksi menjadi sangat cepat.
3. **Fleksibilitas Analisis:** Memisahkan struktur *nested array* produk dari keranjang belanja menjadi tabel penghubung (*bridge*) memungkinkan analis data melihat performa penjualan per item produk, per kategori, atau per wilayah pengguna secara modular.

### Detail Skema Tabel di Data Warehouse

#### 1. `dim_users` (Tabel Dimensi Pengguna)
* **Karakteristik:** Menyimpan data master profil pengguna.
* **Kolom Utama:** `id` (PK), `firstname`, `lastname`, `email`, `gender`, `age`, `address.city`.

#### 2. `dim_products` (Tabel Dimensi Produk)
* **Karakteristik:** Menyimpan data master barang yang dijual.
* **Kolom Utama:** `id` (PK), `title`, `category`, `price`, `stock`, `brand`, `rating`.

#### 3. `fact_carts` (Tabel Fakta Keranjang/Transaksi)
* **Karakteristik:** Menyimpan ringkasan *header* dari transaksi keranjang belanja.
* **Kolom Utama:** `id` (PK), `userid` (FK ke `dim_users`), `total`, `discountedtotal`, `totalproducts`, `totalquantity`.

#### 4. `bridge_cart_items` (Tabel Detail Item Transaksi)
* **Karakteristik:** Tabel penghubung (*junction/bridge table*) hasil *flattening* dari array produk di keranjang. Menyimpan detail performa per item di setiap transaksi.
* **Kolom Utama:** `cart_id` (FK ke `fact_carts`), `product_id` (FK ke `dim_products`), `title`, `price`, `quantity`, `total`, `discountpercentage`, `discountedprice`, `calculated_total` (Enriched).

---

## Orkestrasi Apache Airflow (DAG)

Pipeline diatur dengan jadwal harian (`@daily`) dengan parameter `catchup=False`.

### Tangkapan Layar Alur Graph DAG
*(Silakan sematkan screenshot UI Airflow Anda di sini setelah pipeline berhasil berjalan)*

![Airflow DAG Graph Success Placeholder](https://raw.githubusercontent.com/HidayatHatta/assets-ml-terapan/main/ETL.png)

* **extract**: Menarik data mentah dari API DummyJSON dan menyimpannya ke CSV Raw.
* **transform**: Mengambil CSV Raw, melakukan pembersihan data, menormalisasi struktur tabel, dan menghasilkan pola skema bintang (*Star Schema*).
* **validate**: Mengevaluasi 4 pilar DQE. Jika gagal, alur berhenti di sini.
* **load**: Memasukkan data bersih ke tabel PostgreSQL sesuai dengan *load order*.

---

## Cara Menjalankan Proyek

1. **Prasyarat:** Pastikan Docker Desktop dan WSL2 (Ubuntu) sudah terinstal di komputer Anda.
2. **Kloning Repositori:**
   ```bash
   git clone https://github.com/HidayatHatta/DE-dummy-json
   cd dummyjson-airflow-pipeline
   ```
3. **Penyusunan Folder Lokal:** Pastikan folder volume sudah siap dengan menjalankan command WSL:
   ```bash
   mkdir -p dags scripts data logs
   ```
4. **Menjalankan Docker Compose:**
   ```bash
   docker-compose up -d
   ```
5. **Akses Airflow Webserver:**
    - Buka browser dan arahkan ke http://localhost:8081
    - Login menggunakan kredensial default: Username: admin | Password: admin
6. **Trigger DAG:**\
   Aktifkan DAG advanced_data_pipeline dan jalankan secara manual untuk melihat aliran data berjalan dengan sukses dari hijau ke hijau.
