from fastapi import FastAPI
from pydantic import BaseModel

app = FastAPI()


class Transaction(BaseModel):
    payment_id: str
    user_id: str
    amount: float
    currency: str


@app.post("/score")
def score_transaction(txn: Transaction):
    """
    Simple rule-based fraud detection.
    Later: replace with ML model.
    """
    fraud = False
    reason = "ok"

    # Rule 1: High-value transactions
    if txn.amount > 10000:
        fraud = True
        reason = "amount_exceeds_threshold"

    # Rule 2: Unsupported currency
    allowed_currencies = ["INR", "USD", "EUR", "GBP"]
    if txn.currency not in allowed_currencies:
        fraud = True
        reason = "unsupported_currency"

    return {
        "payment_id": txn.payment_id,
        "fraud": fraud,
        "reason": reason,
        "score": 0.9 if fraud else 0.1,
    }


@app.get("/health")
def health():
    return {"status": "ok"}
