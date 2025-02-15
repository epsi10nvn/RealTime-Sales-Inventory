query_select_current_inventory = """
    (
        SELECT i.product_id, i.product_name, i.quantity, i.sale_prices, i.reorder_level, i.last_updated
        FROM inventory i
        JOIN (
            SELECT product_id, MAX(last_updated) AS max_last_updated
            FROM inventory
            GROUP BY product_id
        ) latest
        ON i.product_id = latest.product_id AND i.last_updated = latest.max_last_updated
    ) AS latest_inventory
"""

"""
DROP TABLE inventory;
DROP TABLE import_items;
DROP TABLE import_invoices;
DROP TABLE product_dim CASCADE;
DROP TABLE customer_dim CASCADE;
DROP TABLE time_dim CASCADE;
DROP TABLE sales_fact;


TRUNCATE TABLE time_dim CASCADE;
TRUNCATE TABLE product_dim CASCADE;
TRUNCATE TABLE customer_dim CASCADE;
"""

"""
tong doanh thu va sl theo phut

SELECT 
    t.year, t.month, t.day, t.hour, t.minute,
    SUM(s.total_amount) AS total_revenue, 
    SUM(s.quantity) AS total_quantity_sold
FROM sales_fact s
JOIN time_dim t ON s.time_id = t.id
GROUP BY t.year, t.month, t.day, t.hour, t.minute
ORDER BY t.year DESC, t.month DESC, t.day DESC, t.hour DESC, t.minute;
"""

"""
5 sp ban chay nhat

SELECT 
    p.product_name, 
    SUM(s.quantity) AS total_sold
FROM sales_fact s
JOIN product_dim p ON s.product_id = p.product_id
GROUP BY p.product_name
ORDER BY total_sold DESC
LIMIT 5;


"""

"""
doanh thu theo tung khach hang

SELECT 
    c.customer_name, 
    SUM(s.total_amount) AS total_spent
FROM sales_fact s
JOIN customer_dim c ON s.customer_id = c.customer_id
GROUP BY c.customer_name
ORDER BY total_spent DESC;

"""

# Bieu do tron
"""
Doanh thu theo san pham

SELECT 
    p.product_name, 
    SUM(s.total_amount) AS total_revenue
FROM sales_fact s
JOIN product_dim p ON s.product_id = p.product_id
GROUP BY p.product_name
ORDER BY total_revenue DESC;

# Vẽ biểu đồ tròn doanh thu theo sản phẩm
plt.figure(figsize=(8, 8))
plt.pie(df["total_revenue"], labels=df["product_name"], autopct="%1.1f%%", startangle=140, colors=plt.cm.Paired.colors)
plt.title("Revenue Share by Product")
plt.show()
"""

"""
Ty le khach hang theo tong chi tieu

SELECT 
    c.customer_name, 
    SUM(s.total_amount) AS total_spent
FROM sales_fact s
JOIN customer_dim c ON s.customer_id = c.customer_id
GROUP BY c.customer_name
ORDER BY total_spent DESC;

plt.figure(figsize=(8, 8))
plt.pie(df["total_spent"], labels=df["customer_name"], autopct="%1.1f%%", startangle=140, colors=plt.cm.Pastel1.colors)
plt.title("Customer Contribution to Total Revenue")
plt.show()
"""

