import psycopg2
from psycopg2 import sql

HOST = "localhost"
PORT = "5432"
USER = "jeremie"
PASSWORD = "20112001"
DB_NAME = "retail_repository"

# Hàm kết nối đến PostgreSQL
def connect_postgres(dbname="postgres"):
    try:
        conn = psycopg2.connect(
            host=HOST,
            port=PORT,
            user=USER,
            password=PASSWORD,
            dbname=dbname
        )
        conn.autocommit = True  # Đảm bảo tự động thực thi lệnh
        return conn
    except Exception as e:
        print("Kết nối thất bại:", e)
        return None

# Hàm tạo database
def create_database(db_name):
    conn = connect_postgres()  # Kết nối đến database mặc định
    if conn:
        try:
            cursor = conn.cursor()
            cursor.execute(
                sql.SQL("SELECT 1 FROM pg_database WHERE datname = %s"),
                [db_name]
            )
            if not cursor.fetchone():
                cursor.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(db_name)))
                print(f"Đã tạo cơ sở dữ liệu: {db_name}")
            else:
                print(f"Cơ sở dữ liệu {db_name} đã tồn tại.")
        except Exception as e:
            print("Lỗi khi tạo cơ sở dữ liệu:", e)
        finally:
            conn.close()

# Hàm tạo bảng 
def create_table():
    conn = connect_postgres(dbname=DB_NAME)  
    if conn:
        try:
            cursor = conn.cursor()
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS import_invoices (
                    import_id VARCHAR(10) PRIMARY KEY,
                    delivery_hour TIMESTAMP NOT NULL,     
                    total_invoice_amount DECIMAL(15, 2) NOT NULL,
                    timestamp_inserted TIMESTAMP NOT NULL DEFAULT NOW()
                );
                
                CREATE TABLE import_items (
                    id SERIAL PRIMARY KEY,
                    import_id VARCHAR(10) NOT NULL,
                    product_id VARCHAR(10) NOT NULL,        
                    product_name VARCHAR(100) NOT NULL,
                    quantity INT NOT NULL,
                    price DECIMAL(15, 2) NOT NULL,
                    total_amount DECIMAL(15, 2) NOT NULL,

                    
                    CONSTRAINT fk_import_id FOREIGN KEY (import_id) REFERENCES import_invoices(import_id)
                        ON DELETE CASCADE
                );
            """)
            print("Đã tạo bảng cho import.")
            
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS inventory (
                    product_id VARCHAR(10),
                    product_name VARCHAR(100) NOT NULL,
                    quantity INT NOT NULL,
                    sale_prices DECIMAL(15, 2) NOT NULL,     
                    reorder_level INT NOT NULL,
                    last_updated TIMESTAMP NOT NULL DEFAULT NOW(),
                    
                    PRIMARY KEY (product_id, last_updated)
                );
            """)
            print("Đã tạo bảng inventory.")
            
            
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS product_dim (
                    product_id VARCHAR(10) NOT NULL,
                    product_name VARCHAR(100) NOT NULL,
                    
                    PRIMARY KEY (product_id)
                );
            """)
            
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS customer_dim (
                    customer_id VARCHAR(10) NOT NULL,
                    customer_name VARCHAR(100) NOT NULL,
                    
                    PRIMARY KEY (customer_id)
                );
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS time_dim (
                    id SERIAL PRIMARY KEY,
                    date DATE NOT NULL,
                    year INT NOT NULL,
                    month INT NOT NULL,
                    day INT NOT NULL,
                    hour INT NOT NULL,
                    minute INT NOT NULL,
                    second INT NOT NULL,
                    weekday INT NOT NULL
                );
            """)
            
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS sales_fact (
                    product_id VARCHAR(10) NOT NULL,
                    customer_id VARCHAR(10) NOT NULL,
                    time_id INT NOT NULL,
                    sale_id SERIAL,
                    quantity INT NOT NULL,
                    sale_prices DECIMAL(15, 2) NOT NULL,
                    total_amount DECIMAL(15, 2) NOT NULL,
                    
                    PRIMARY KEY (product_id, customer_id, time_id, sale_id),
                    CONSTRAINT fk_product FOREIGN KEY (product_id) REFERENCES product_dim(product_id)
                        ON DELETE CASCADE,
                    CONSTRAINT fk_customer FOREIGN KEY (customer_id) REFERENCES customer_dim(customer_id)
                        ON DELETE CASCADE,
                    CONSTRAINT fk_time FOREIGN KEY (time_id) REFERENCES time_dim(id)
                        ON DELETE CASCADE
                );
            """)
            print("Đã tạo bảng cho datawarehouse chính.")
        except Exception as e:
            print("Lỗi khi tạo bảng:", e)
        finally:
            conn.close()

if __name__ == "__main__":
    create_database(DB_NAME)
    create_table()