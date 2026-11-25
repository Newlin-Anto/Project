"""
Generate a payment gateway transactions CSV (dirty on purpose).
A ready file (payment_gateway_raw.csv) is already included, so run this ONLY if you want a BIGGER file.
Usage:  python 01_generate_data.py 10000000
"""
import csv, random, sys
from datetime import datetime, timedelta

ROWS = int(sys.argv[1]) if len(sys.argv) > 1 else 1_000_000
OUT = "payment_gateway_raw.csv"
random.seed(7)

gateways = {  # name: success probability
    "Razorpay": 0.95, "Payu": 0.92, "Paytm": 0.93, "Ccavenue": 0.89, "Stripe": 0.96}
methods = {"UPI": 0.45, "Credit Card": 0.2, "Debit Card": 0.17, "Net Banking": 0.12, "Wallet": 0.06}
banks = ["SBI", "HDFC", "ICICI", "Axis", "Kotak", "Canara Bank", "Bank of Baroda", "IndusInd"]
cities = ["Chennai", "Mumbai", "Delhi", "Bangalore", "Hyderabad", "Kolkata", "Pune", "Coimbatore"]
categories = ["Food", "Travel", "Shopping", "Utilities", "Entertainment", "Education", "Healthcare"]
devices = ["Android", "iOS", "Web", "POS"]
fail_reasons = ["Insufficient Funds", "Bank Server Down", "Invalid OTP", "Card Expired",
                "Network Timeout", "Fraud Suspected", "Daily Limit Exceeded"]
hour_weights = [1,1,1,1,1,2,3,5,7,8,8,8,9,8,7,7,8,9,10,10,9,7,4,2]
start = datetime(2024, 1, 1)
days = 730
gw_names, gw_p = list(gateways), list(gateways.values())
m_names, m_w = list(methods), list(methods.values())

def dirty_text(v):
    r = random.random()
    if r < 0.03: return v.upper()
    if r < 0.05: return v.lower()
    if r < 0.07: return " " + v + " "
    if r < 0.08: return v.replace(" ", "_")
    return v

with open(OUT, "w", newline="") as f:
    w = csv.writer(f)
    w.writerow(["transaction_id", "merchant_id", "merchant_category", "customer_id", "amount",
                "currency", "payment_method", "gateway", "bank", "status", "failure_reason",
                "txn_timestamp", "city", "device_type", "processing_time_ms"])
    for i in range(1, ROWS + 1):
        gw = random.choice(gw_names)
        ok = random.random() < gateways[gw]
        status = "SUCCESS" if ok else random.choice(["FAILED", "FAILED", "FAILED", "PENDING"])
        if ok and random.random() < 0.02: status = "REFUNDED"
        reason = "" if status != "FAILED" else random.choice(fail_reasons)
        if status == "FAILED" and random.random() < 0.03: reason = ""      # missing reason
        method = random.choices(m_names, m_w)[0]
        amount = round(random.lognormvariate(6.5, 1.0), 2)                  # mostly small, few big
        currency = "USD" if gw == "Stripe" and random.random() < 0.3 else "INR"
        if currency == "USD": amount = round(amount / 83, 2)               # USD amounts are smaller numbers
        ts = start + timedelta(days=random.randint(0, days - 1),
                               hours=random.choices(range(24), hour_weights)[0],
                               minutes=random.randint(0, 59), seconds=random.randint(0, 59))
        ts_s = ts.strftime("%Y-%m-%d %H:%M:%S")
        pt = int(random.gauss(900, 300)) if ok else int(random.gauss(2500, 900))
        pt = max(pt, 50)

        r = random.random()                                   # inject problems
        if r < 0.01: amount = ""
        elif r < 0.02: amount = -amount
        elif r < 0.025: amount = "N/A"
        r = random.random()
        if r < 0.02: ts_s = ts.strftime("%d/%m/%Y %H:%M")
        elif r < 0.03: ts_s = "invalid_time"
        if random.random() < 0.01: pt = -pt
        if random.random() < 0.005: currency = "XXX"
        if random.random() < 0.01: bank_v = ""
        else: bank_v = dirty_text(random.choice(banks))

        row = [f"TXN{i:010d}", f"M{random.randint(1, 5000):05d}", random.choice(categories),
               f"CUST{random.randint(1, 800000):06d}", amount, currency, dirty_text(method),
               dirty_text(gw), bank_v, status if random.random() > 0.03 else status.lower(),
               reason, ts_s, dirty_text(random.choice(cities)) if random.random() > 0.02 else "",
               random.choice(devices), pt]
        w.writerow(row)
        if random.random() < 0.01: w.writerow(row)            # duplicate
        if i % 500000 == 0: print(f"{i:,} rows...")
print("Done ->", OUT)
