
import io
import pytest

import io
import pytest
from sqlalchemy.orm import Session
from app.uploads import models


# Duplicate Filenames
def test_duplicate_filenames(client, db_session: Session):
    """Test uploading the same file twice - should handle gracefully with merge"""
    csv_content = """transaction_id,timestamp,user_id,account_age_days,customer_tier,kyc_level,has_multiple_accounts,linked_card_count,transaction_amount,transaction_currency,transaction_type,merchant_category,merchant_id,merchant_risk_score,transaction_hour,transaction_day_of_week,is_weekend_transaction,is_nighttime_transaction,device_id,device_os,device_type,is_vpn_used,is_proxy_used,ip_address,has_multiple_devices,is_blacklisted_card,is_blacklisted_device,is_high_risk_country,distance_from_last_transaction,has_chargeback_history,previous_fraudulent_activity,account_fraud_reported,is_high_risk_behavior,label,ip_risk_score,location_country,location_city,is_new_device,is_new_location,num_failed_attempts_24h,prev_avg_txn_amount,txn_amount_deviation,daily_avg_spend,total_spend_last_7d,transaction_recency,txn_velocity_1h,txn_velocity_24h,transaction_success_rate_24h
1,2025-08-28 12:00:00,101,30,Gold,Level1,0,1,100.0,USD,Purchase,Retail,501,2,12,Monday,0,0,dev123,iOS,Mobile,0,0,192.168.0.1,0,0,0,0,5.0,0,0,0,0,0,0,USA,NYC,0,0,0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0
"""

    # First upload
    file = {"file": ("duplicate_test.csv", io.BytesIO(csv_content.encode("utf-8")), "text/csv")}
    response1 = client.post("/api/v1/uploads/csv/", files=file)
    assert response1.status_code == 201
    
    # Second upload w/ same filename
    response2 = client.post("/api/v1/uploads/csv/", files=file)
    assert response2.status_code == 201
    
    # Check that we still have only 1 transaction record 
    transaction_count = db_session.query(models.Transaction).count()
    assert transaction_count == 1, f"Expected 1 transaction, got {transaction_count}"



# Duplicate Transactions (same transaction_id)
def test_duplicate_transactions(client, db_session: Session):
    """Test uploading transactions with same ID - should update existing record"""
    # First transaction
    csv_content1 = """transaction_id,timestamp,user_id,account_age_days,customer_tier,kyc_level,has_multiple_accounts,linked_card_count,transaction_amount,transaction_currency,transaction_type,merchant_category,merchant_id,merchant_risk_score,transaction_hour,transaction_day_of_week,is_weekend_transaction,is_nighttime_transaction,device_id,device_os,device_type,is_vpn_used,is_proxy_used,ip_address,has_multiple_devices,is_blacklisted_card,is_blacklisted_device,is_high_risk_country,distance_from_last_transaction,has_chargeback_history,previous_fraudulent_activity,account_fraud_reported,is_high_risk_behavior,label,ip_risk_score,location_country,location_city,is_new_device,is_new_location,num_failed_attempts_24h,prev_avg_txn_amount,txn_amount_deviation,daily_avg_spend,total_spend_last_7d,transaction_recency,txn_velocity_1h,txn_velocity_24h,transaction_success_rate_24h
999,2025-08-28 12:00:00,101,30,Gold,Level1,0,1,100.0,USD,Purchase,Retail,501,2,12,Monday,0,0,dev123,iOS,Mobile,0,0,192.168.0.1,0,0,0,0,5.0,0,0,0,0,0,0,USA,NYC,0,0,0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0
"""

    # Second transaction w/ same ID but different data
    csv_content2 = """transaction_id,timestamp,user_id,account_age_days,customer_tier,kyc_level,has_multiple_accounts,linked_card_count,transaction_amount,transaction_currency,transaction_type,merchant_category,merchant_id,merchant_risk_score,transaction_hour,transaction_day_of_week,is_weekend_transaction,is_nighttime_transaction,device_id,device_os,device_type,is_vpn_used,is_proxy_used,ip_address,has_multiple_devices,is_blacklisted_card,is_blacklisted_device,is_high_risk_country,distance_from_last_transaction,has_chargeback_history,previous_fraudulent_activity,account_fraud_reported,is_high_risk_behavior,label,ip_risk_score,location_country,location_city,is_new_device,is_new_location,num_failed_attempts_24h,prev_avg_txn_amount,txn_amount_deviation,daily_avg_spend,total_spend_last_7d,transaction_recency,txn_velocity_1h,txn_velocity_24h,transaction_success_rate_24h
999,2025-08-28 13:00:00,101,30,Gold,Level1,0,1,250.0,USD,Purchase,Electronics,502,3,13,Monday,0,0,dev123,iOS,Mobile,0,0,192.168.0.1,0,0,0,0,8.0,0,0,0,0,0,0,USA,NYC,0,0,0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0
"""

    # Upload first transaction
    file1 = {"file": ("test1.csv", io.BytesIO(csv_content1.encode("utf-8")), "text/csv")}
    response1 = client.post("/api/v1/uploads/csv/", files=file1)
    assert response1.status_code == 201
    
    # Upload second transaction w/ same ID
    file2 = {"file": ("test2.csv", io.BytesIO(csv_content2.encode("utf-8")), "text/csv")}
    response2 = client.post("/api/v1/uploads/csv/", files=file2)
    assert response2.status_code == 201
    
    # Check that we have only 1 transaction record (merge should update, not duplicate)
    transaction_count = db_session.query(models.Transaction).count()
    assert transaction_count == 1, f"Expected 1 transaction, got {transaction_count}"
    
    # Verify the record was updated with the new data
    transaction = db_session.query(models.Transaction).filter_by(transaction_id="999").first()
    assert transaction is not None
    assert transaction.transaction_amount == 250.0  # Updated value from second upload
    assert transaction.merchant_category == "Electronics"  # Updated value from second upload
    assert transaction.merchant_risk_score == 3  # Updated value from second upload



# Mixed Transactions (some duplicates, some new)
def test_mixed_duplicate_and_new_transactions(client, db_session: Session):
    """Test uploading a mix of duplicate and new transactions"""
    # First batch with transaction 100 and 101
    csv_content1 = """transaction_id,timestamp,user_id,account_age_days,customer_tier,kyc_level,has_multiple_accounts,linked_card_count,transaction_amount,transaction_currency,transaction_type,merchant_category,merchant_id,merchant_risk_score,transaction_hour,transaction_day_of_week,is_weekend_transaction,is_nighttime_transaction,device_id,device_os,device_type,is_vpn_used,is_proxy_used,ip_address,has_multiple_devices,is_blacklisted_card,is_blacklisted_device,is_high_risk_country,distance_from_last_transaction,has_chargeback_history,previous_fraudulent_activity,account_fraud_reported,is_high_risk_behavior,label,ip_risk_score,location_country,location_city,is_new_device,is_new_location,num_failed_attempts_24h,prev_avg_txn_amount,txn_amount_deviation,daily_avg_spend,total_spend_last_7d,transaction_recency,txn_velocity_1h,txn_velocity_24h,transaction_success_rate_24h
100,2025-08-28 12:00:00,101,30,Gold,Level1,0,1,100.0,USD,Purchase,Retail,501,2,12,Monday,0,0,dev123,iOS,Mobile,0,0,192.168.0.1,0,0,0,0,5.0,0,0,0,0,0,0,USA,NYC,0,0,0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0
101,2025-08-28 12:30:00,102,45,Silver,Level2,1,2,75.0,USD,Purchase,Food,502,1,12,Monday,0,0,dev124,Android,Mobile,0,0,192.168.0.2,0,0,0,0,3.0,0,0,0,0,0,0,USA,LA,0,0,0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0
"""

    # Second batch with transaction 100 (duplicate) and 102 (new)
    csv_content2 = """transaction_id,timestamp,user_id,account_age_days,customer_tier,kyc_level,has_multiple_accounts,linked_card_count,transaction_amount,transaction_currency,transaction_type,merchant_category,merchant_id,merchant_risk_score,transaction_hour,transaction_day_of_week,is_weekend_transaction,is_nighttime_transaction,device_id,device_os,device_type,is_vpn_used,is_proxy_used,ip_address,has_multiple_devices,is_blacklisted_card,is_blacklisted_device,is_high_risk_country,distance_from_last_transaction,has_chargeback_history,previous_fraudulent_activity,account_fraud_reported,is_high_risk_behavior,label,ip_risk_score,location_country,location_city,is_new_device,is_new_location,num_failed_attempts_24h,prev_avg_txn_amount,txn_amount_deviation,daily_avg_spend,total_spend_last_7d,transaction_recency,txn_velocity_1h,txn_velocity_24h,transaction_success_rate_24h
100,2025-08-28 13:00:00,101,30,Gold,Level1,0,1,150.0,USD,Purchase,Electronics,501,3,13,Monday,0,0,dev123,iOS,Mobile,0,0,192.168.0.1,0,0,0,0,6.0,0,0,0,0,0,0,USA,NYC,0,0,0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0
102,2025-08-28 13:30:00,103,60,Bronze,Level1,0,1,50.0,USD,Purchase,Retail,503,2,13,Monday,0,0,dev125,iOS,Mobile,0,0,192.168.0.3,0,0,0,0,2.0,0,0,0,0,0,0,USA,Chicago,0,0,0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0
"""

    # Upload first batch
    file1 = {"file": ("batch1.csv", io.BytesIO(csv_content1.encode("utf-8")), "text/csv")}
    response1 = client.post("/api/v1/uploads/csv/", files=file1)
    assert response1.status_code == 201
    
    # Upload second batch
    file2 = {"file": ("batch2.csv", io.BytesIO(csv_content2.encode("utf-8")), "text/csv")}
    response2 = client.post("/api/v1/uploads/csv/", files=file2)
    assert response2.status_code == 201
    
    # Check that we have 3 total transactions (100, 101, 102) - 100 should be updated
    transaction_count = db_session.query(models.Transaction).count()
    assert transaction_count == 3, f"Expected 3 transactions, got {transaction_count}"
    
    # Verify transaction 100 was updated
    transaction_100 = db_session.query(models.Transaction).filter_by(transaction_id="100").first()
    assert transaction_100.transaction_amount == 150.0  # Updated value
    assert transaction_100.merchant_category == "Electronics"  # Updated value
    
    # Verify transaction 101 remains unchanged
    transaction_101 = db_session.query(models.Transaction).filter_by(transaction_id="101").first()
    assert transaction_101.transaction_amount == 75.0  # Original value
    
    # Verify transaction 102 was created
    transaction_102 = db_session.query(models.Transaction).filter_by(transaction_id="102").first()
    assert transaction_102 is not None