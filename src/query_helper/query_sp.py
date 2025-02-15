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