import io
import pytest
import pandas as pd
from sqlalchemy.orm import Session
from app.uploads import models
import tempfile
import os


# Large CSV (Performance Test)

def test_large_csv_performance(client, db_session: Session):
    """Test uploading a large CSV file to check performance and memory handling"""
    header = """transaction_id,timestamp,user_id,account_age_days,customer_tier,kyc_level,has_multiple_accounts,linked_card_count,transaction_amount,transaction_currency,transaction_type,merchant_category,merchant_id,merchant_risk_score,transaction_hour,transaction_day_of_week,is_weekend_transaction,is_nighttime_transaction,device_id,device_os,device_type,is_vpn_used,is_proxy_used,ip_address,has_multiple_devices,is_blacklisted_card,is_blacklisted_device,is_high_risk_country,distance_from_last_transaction,has_chargeback_history,previous_fraudulent_activity,account_fraud_reported,is_high_risk_behavior,label,ip_risk_score,location_country,location_city,is_new_device,is_new_location,num_failed_attempts_24h,prev_avg_txn_amount,txn_amount_deviation,daily_avg_spend,total_spend_last_7d,transaction_recency,txn_velocity_1h,txn_velocity_24h,transaction_success_rate_24h
"""
    
    # Generate 1000 rows of data
    rows = []
    for i in range(1000):
        row = f"{i},2025-08-28 12:00:00,user{i},30,Gold,Level1,0,1,100.0,USD,Purchase,Retail,501,2,12,Monday,0,0,dev{i},iOS,Mobile,0,0,192.168.0.{i},0,0,0,0,5.0,0,0,0,0,0,0,USA,NYC,0,0,0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0"
        rows.append(row)
    
    csv_content = header + "\n".join(rows)
    
    # Test with in-memory file
    file = {"file": ("large_test.csv", io.BytesIO(csv_content.encode("utf-8")), "text/csv")}
    
    # Measure performance 
    import time
    start_time = time.time()
    
    response = client.post("/api/v1/uploads/csv/", files=file)
    
    end_time = time.time()
    processing_time = end_time - start_time
    
    assert response.status_code == 201
    assert response.json()["data"]["rows"] == 1000
    
    # Verify all records were inserted
    transaction_count = db_session.query(models.Transaction).count()
    assert transaction_count == 1000, f"Expected 1000 transactions, got {transaction_count}"
    
    # Log performance metrics
    print(f"Processed 1000 rows in {processing_time:.2f} seconds")
    print(f"Processing rate: {1000/processing_time:.2f} rows/second")
    
    # Performance assertion
    assert processing_time < 10.0, f"Processing took too long: {processing_time:.2f} seconds"



# Extreme Values Test

def test_extreme_values(client, db_session: Session):
    """Test CSV with extreme values to ensure no crashes"""
    csv_content = """transaction_id,timestamp,user_id,account_age_days,customer_tier,kyc_level,has_multiple_accounts,linked_card_count,transaction_amount,transaction_currency,transaction_type,merchant_category,merchant_id,merchant_risk_score,transaction_hour,transaction_day_of_week,is_weekend_transaction,is_nighttime_transaction,device_id,device_os,device_type,is_vpn_used,is_proxy_used,ip_address,has_multiple_devices,is_blacklisted_card,is_blacklisted_device,is_high_risk_country,distance_from_last_transaction,has_chargeback_history,previous_fraudulent_activity,account_fraud_reported,is_high_risk_behavior,label,ip_risk_score,location_country,location_city,is_new_device,is_new_location,num_failed_attempts_24h,prev_avg_txn_amount,txn_amount_deviation,daily_avg_spend,total_spend_last_7d,transaction_recency,txn_velocity_1h,txn_velocity_24h,transaction_success_rate_24h
1,2025-08-28 12:00:00,user_with_very_long_id_that_exceeds_normal_length_1234567890,999999,VeryLongCustomerTierNameThatMightBreakThings,ExtremelyLongKYCLevelName,1,999999,999999999.99,USD,VeryLongTransactionTypeName,ExtremelyLongMerchantCategoryNameThatCouldCauseIssues,merchant_id_with_many_characters_123456,999.99,23,Monday,1,1,device_id_with_many_many_many_characters_abcdefghijklmnopqrstuvwxyz,iOS,VeryLongDeviceTypeName,1,1,255.255.255.255,1,1,1,1,999999.99,1,1,1,1,999.99,999.99,CountryWithVeryLongName,CityWithExtremelyLongNameThatMightBreakDatabaseFields,1,1,999999,999999999.99,999999999.99,999999999.99,999999999.99,999999.99,999999,999999,999.99
2,2025-08-28 12:00:00,user_min_values,0,Min,Level0,0,0,0.0,USD,Min,Min,min,0.0,0,Monday,0,0,min,iOS,Min,0,0,0.0.0.0,0,0,0,0,0.0,0,0,0,0,0.0,0.0,Min,Min,0,0,0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0
3,2025-08-28 12:00:00,101,30,Gold,Level1,0,1,0.000000001,USD,Purchase,Retail,501,0.0000001,0,Monday,0,0,dev123,iOS,Mobile,0,0,192.168.0.1,0,0,0,0,0.000000001,0,0,0,0,0.0000001,0.0000001,USA,NYC,0,0,0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0
4,2025-08-28 12:00:00,user_special_chars,30,Gold$Tier,Level#1,0,1,123.45,USD,Purchase@Type,Retail&More,Merchant-ID,7.5,12,Monday,0,0,Device_ID-123,iOS-15,Mobile-Phone,0,0,192.168.0.1,0,0,0,0,123.45,0,0,0,0,7.5,7.5,USA-CA,San Francisco,0,0,0,100.0,23.45,500.0,3500.0,2.5,3,60,95.5
"""
    
    file = {"file": ("extreme_values.csv", io.BytesIO(csv_content.encode("utf-8")), "text/csv")}
    response = client.post("/api/v1/uploads/csv/", files=file)
    
    # Check if it was successful or if it failed with a specific error message
    if response.status_code != 201:
        print(f"Response status: {response.status_code}")
        print(f"Response content: {response.text}")
        # If it's a 400, check if it's due to missing columns or validation
        if response.status_code == 400:
            response_data = response.json()
            if "message" in response_data:
                print(f"Error message: {response_data['message']}")
    
    assert response.status_code == 201, f"Expected 201, got {response.status_code}. Response: {response.text}"
    assert response.json()["data"]["rows"] == 4
    
    # Verify records were inserted without crashing
    transaction_count = db_session.query(models.Transaction).count()
    assert transaction_count == 4
    
    # Verify specific extreme values were handled
    transaction1 = db_session.query(models.Transaction).filter_by(transaction_id="1").first()
    assert transaction1 is not None
    assert transaction1.transaction_amount == 999999999.99
    assert len(transaction1.user_id) > 50  # Very long user ID
    
    transaction3 = db_session.query(models.Transaction).filter_by(transaction_id="3").first()
    assert transaction3 is not None
    assert transaction3.transaction_amount == 0.000000001  # Very small value
