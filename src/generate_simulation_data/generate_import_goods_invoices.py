import json
import random
import time
from datetime import datetime
import os
import shutil

# Sample list of items
ITEMS = [
    {"product_id": "A1", "product_name": "Ballpoint Pen", "price": 5000},
    {"product_id": "A2", "product_name": "Notebook", "price": 15000},
    {"product_id": "A3", "product_name": "Ruler", "price": 10000},
    {"product_id": "A4", "product_name": "Eraser", "price": 7000},
    {"product_id": "A5", "product_name": "Backpack", "price": 300000},
    {"product_id": "A6", "product_name": "Pencil", "price": 4000},
    {"product_id": "A7", "product_name": "Calculator", "price": 150000},
    {"product_id": "A8", "product_name": "A4 Paper", "price": 50000},
    {"product_id": "A9", "product_name": "Scissors", "price": 12000},
    {"product_id": "A10", "product_name": "Correction Pen", "price": 8000}
]
INPUT_PATH = "./input/filestreaming_import_goods"
BACKUP_PATH = "./input/backup/import"

# Function to generate a single item entry
def generate_item(item_pool):
    item = random.choice(item_pool)
    
    quantity = random.randint(50, 100)
    # quantity = 5
    
    total_amount = quantity * item["price"]
    
    return {
        "product_id": item["product_id"],
        "product_name": item["product_name"],
        "quantity": quantity,
        "price": item["price"],
        "total_amount": total_amount
    }

# Function to generate an import invoice with multiple items
def generate_invoice(import_id, num_items):
    item_pool = ITEMS.copy()
    
    # Create a list of unique items for the invoice
    items = []
    for _ in range(num_items):
        item = generate_item(item_pool)
        items.append(item)
        item_pool.remove(next(i for i in item_pool if i["product_id"] == item["product_id"]))  # Remove selected item from pool
    
    # Calculate the total amount for the entire invoice
    total_invoice_amount = sum(item["total_amount"] for item in items)
    
    # Create the invoice
    invoice = {
        "import_id": f"I{import_id:04d}",  # Format import ID (e.g., I0001, I0002, ...)
        "delivery_hour": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "total_invoice_amount": total_invoice_amount,
        "items": items
    }
    return invoice

# Function to write the invoice to a JSON file
def write_invoice_to_file(invoice, file_name):
    with open(file_name, "w") as file:
        file.write(json.dumps(invoice, ensure_ascii=False) + "\n")

def get_current_id():
    json_files = [int(f.split('.')[0]) for f in os.listdir(INPUT_PATH) if f.endswith('.json')]
    return max(json_files)

def copy_file(source, des):
    shutil.copy(source, des)
    print(f"File copied from {source} to {des}")

# Main program
def main():
    try:
        import_id = get_current_id() + 1
    except Exception as e:
        import_id = 0
    print('Current id import file: ', import_id)
    
    while True:
        # Number of items per invoice (random between 3 and 7)
        if import_id == 0:
            num_items = 10
        else:
            num_items = random.randint(3, 6)
        # num_items = 10
        
        invoice = generate_invoice(import_id, num_items)
        
        # Print the invoice to the console (optional)
        print(f"Generated Invoice: {invoice}")
        
        # Write the invoice to the JSON file
        write_invoice_to_file(invoice, f"././input/filestreaming_import_goods/{import_id}.json")
        
        source = INPUT_PATH + f'/{str(import_id)}.json'
        des = BACKUP_PATH + f'/{str(import_id)}.json'
        copy_file(source, des)
        
        import_id += 1
        time.sleep(120)  # 3600 seconds = 1 hour

if __name__ == "__main__":
    main()
