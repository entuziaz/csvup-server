import io
import pytest


# Successful Upload
def test_successful_upload(client):
    csv_content = """transaction_id,timestamp,user_id,account_age_days,customer_tier,kyc_level,has_multiple_accounts,linked_card_count,transaction_amount,transaction_currency,transaction_type,merchant_category,merchant_id,merchant_risk_score,transaction_hour,transaction_day_of_week,is_weekend_transaction,is_nighttime_transaction,device_id,device_os,device_type,is_vpn_used,is_proxy_used,ip_address,has_multiple_devices,is_blacklisted_card,is_blacklisted_device,is_high_risk_country,distance_from_last_transaction,has_chargeback_history,previous_fraudulent_activity,account_fraud_reported,is_high_risk_behavior,label,ip_risk_score,location_country,location_city,is_new_device,is_new_location,num_failed_attempts_24h,prev_avg_txn_amount,txn_amount_deviation,daily_avg_spend,total_spend_last_7d,transaction_recency,txn_velocity_1h,txn_velocity_24h,transaction_success_rate_24h
1,2025-08-28 12:00:00,101,30,Gold,Level1,0,1,100.0,USD,Purchase,Retail,501,2,12,Monday,0,0,dev123,iOS,Mobile,0,0,192.168.0.1,0,0,0,0,5.0,0,0,0,0,0,0,USA,NYC,0,0,0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0
"""

    file = {"file": ("test.csv", io.BytesIO(csv_content.encode("utf-8")), "text/csv")}
    response = client.post("/api/v1/uploads/csv/", files=file)
    assert response.status_code == 201
    assert response.json()["data"]["rows"] == 1


# Invalid file type
def test_invalid_file_type(client):
    file = {"file": ("test.txt", io.BytesIO(b"some text"), "text/plain")}
    response = client.post("/api/v1/uploads/csv/", files=file)
    assert response.status_code == 400
    assert "Invalid file type" in response.json()["message"]

# Missing columns
def test_missing_columns(client):
    csv_content = "transaction_id,user_id\n1,101"
    file = {"file": ("test.csv", io.BytesIO(csv_content.encode("utf-8")), "text/csv")}
    response = client.post("/api/v1/uploads/csv/", files=file)
    assert response.status_code == 400
    assert "Missing columns" in response.json()["message"]

# Empty CSV
def test_empty_csv(client):
    file = {"file": ("empty.csv", io.BytesIO(b""), "text/csv")}
    response = client.post("/api/v1/uploads/csv/", files=file)
    assert response.status_code == 400

# Malformed CSV
def test_malformed_csv(client):
    csv_content = "transaction_id,timestamp,user_id\n1,2025-08-28"
    file = {"file": ("malformed.csv", io.BytesIO(csv_content.encode("utf-8")), "text/csv")}
    response = client.post("/api/v1/uploads/csv/", files=file)
    assert response.status_code in [400, 500] 
