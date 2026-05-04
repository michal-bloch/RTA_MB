from fastapi import FastAPI
from pydantic import BaseModel
import pickle
import numpy as np

app = FastAPI()

# Ładowanie modelu
with open('fraud_model.pkl', 'rb') as f:
    model = pickle.load(f)

class Transaction(BaseModel):
    amount: float
    is_electronics: int
    tx_per_minute: int

@app.get("/health")
def health():
    """Endpoint do sprawdzania czy API żyje"""
    return {
        "status": "healthy",
        "model_loaded": True,
        "service": "fraud-detection-ml"
    }

@app.post("/score")
def score(tx: Transaction):
    features = np.array([[tx.amount, tx.is_electronics, tx.tx_per_minute]])
    prediction = model.predict(features)[0]
    probability = model.predict_proba(features)[0][1]
    return {
        "is_fraud": bool(prediction), 
        "fraud_probability": float(probability)
    }
