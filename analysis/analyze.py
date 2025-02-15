import pandas as pd
import matplotlib.pyplot as plt
import psycopg2

# Kết nối đến PostgreSQL
conn = psycopg2.connect(
    dbname="retail_repository",
    user="jeremie",
    password="20112001",
    host="localhost",
    port="5432"
)

# Truy vấn doanh thu theo phut
query_1 = """
    SELECT 
        t.year, t.month, t.day, t.hour, t.minute,
        SUM(s.total_amount) AS total_revenue, 
        SUM(s.quantity) AS total_quantity_sold
    FROM sales_fact s
    JOIN time_dim t ON s.time_id = t.id
    GROUP BY t.year, t.month, t.day, t.hour, t.minute
    ORDER BY t.year DESC, t.month DESC, t.day DESC, t.hour DESC, t.minute;
"""


query_2 = """
    SELECT 
        p.product_name, 
        SUM(s.quantity) AS total_sold
    FROM sales_fact s
    JOIN product_dim p ON s.product_id = p.product_id
    GROUP BY p.product_name
    ORDER BY total_sold DESC
"""

query_3 = """
    SELECT 
        c.customer_name, 
        SUM(s.total_amount) AS total_spent
    FROM sales_fact s
    JOIN customer_dim c ON s.customer_id = c.customer_id
    GROUP BY c.customer_name
    ORDER BY total_spent DESC
"""

query_4 = """
    SELECT 
        p.product_name, 
        SUM(s.total_amount) AS total_revenue
    FROM sales_fact s
    JOIN product_dim p ON s.product_id = p.product_id
    GROUP BY p.product_name
    ORDER BY total_revenue DESC;
"""

query_5 = """
    SELECT 
        c.customer_name, 
        SUM(s.total_amount) AS total_spent
    FROM sales_fact s
    JOIN customer_dim c ON s.customer_id = c.customer_id
    GROUP BY c.customer_name
    ORDER BY total_spent DESC;
"""

df_1 = pd.read_sql(query_1, conn)
df_2 = pd.read_sql(query_2, conn)
df_3 = pd.read_sql(query_3, conn)
df_4 = pd.read_sql(query_4, conn)
df_5 = pd.read_sql(query_5, conn)

conn.close()

# Vẽ biểu đồ
plt.figure(figsize=(10, 5))
plt.bar(df_1["minute"], df_1["total_revenue"], color="skyblue")

plt.xlabel("Minute")
plt.ylabel("Total Revenue")
plt.title("Minute Revenue")
plt.grid(axis="y")
plt.show()

# graph 2
# Vẽ biểu đồ cột
plt.figure(figsize=(12, 6))
bars = plt.bar(df_2["product_name"], df_2["total_sold"], color="skyblue")

# Thêm nhãn trên từng cột
for bar in bars:
    height = bar.get_height()
    plt.text(bar.get_x() + bar.get_width() / 2, height, str(height), 
             ha="center", va="bottom", fontsize=10, fontweight="bold")

# Thêm nhãn trục và tiêu đề
plt.xlabel("Product Name")
plt.ylabel("Total Sold")
plt.title("Total Quantity Sold per Product")
plt.xticks(rotation=45, ha="right")  # Xoay nhãn trục x để dễ đọc
plt.grid(axis="y", linestyle="--", alpha=0.7)  # Lưới ngang

# Hiển thị biểu đồ
plt.show()

# graph 3

# Vẽ biểu đồ cột doanh thu theo khách hàng
plt.figure(figsize=(12, 6))
bars = plt.bar(df_3["customer_name"], df_3["total_spent"], color="skyblue")

# Thêm nhãn giá trị doanh thu trên từng cột
for bar in bars:
    height = bar.get_height()
    plt.text(bar.get_x() + bar.get_width() / 2, height, f"{height:,.0f}", 
             ha="center", va="bottom", fontsize=10, fontweight="bold")

# Thêm nhãn trục và tiêu đề
plt.xlabel("Customer Name")
plt.ylabel("Total Revenue (VND)")
plt.title("Total Revenue per Customer")
plt.xticks(rotation=45, ha="right")  # Xoay nhãn trục X để dễ đọc
plt.grid(axis="y", linestyle="--", alpha=0.7)  # Lưới ngang

# Hiển thị biểu đồ
plt.show()


plt.figure(figsize=(8, 8))
plt.pie(df_4["total_revenue"], labels=df_4["product_name"], autopct="%1.1f%%", startangle=140, colors=plt.cm.Paired.colors)
plt.title("Revenue Share by Product")
plt.show()


plt.figure(figsize=(8, 8))
plt.pie(df_5["total_spent"], labels=df_5["customer_name"], autopct="%1.1f%%", startangle=140, colors=plt.cm.Pastel1.colors)
plt.title("Customer Contribution to Total Revenue")
plt.show()