from fastapi import FastAPI
import csv
import os


app = FastAPI()

PAYMENT_FILE = r"C:\Users\rodai\Dropbox\PC\Downloads\Team Project Atos\Ecommerce Dataset\olist_order_payments_dataset.csv"
REVIEWS_FILE = r"C:\Users\rodai\Dropbox\PC\Downloads\Team Project Atos\Ecommerce Dataset\olist_order_reviews_dataset.csv"

def read_csv(file_path: str):
    try:
        if not os.path.exists(file_path):
            return {"error": f"File not found: {file_path}"}
        data = []
        with open(file_path, 'r', encoding='utf-8', newline='') as f:
            reader = csv.DictReader(f)
            for row in reader:
                data.append(row)
        return data
    except Exception as e:
        return {"error": str(e)}

@app.get("/payments")
def get_payments():
    return read_csv(PAYMENT_FILE)

@app.get("/reviews")
def get_reviews():
    return read_csv(REVIEWS_FILE)
