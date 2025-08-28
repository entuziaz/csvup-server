import logging
import pandas as pd
from fastapi.responses import JSONResponse


logger = logging.getLogger(__name__)


EXPECTED_COLUMNS = [
    "transaction_id","timestamp","user_id","account_age_days","customer_tier",
    "kyc_level","has_multiple_accounts","linked_card_count","transaction_amount",
    "transaction_currency","transaction_type","merchant_category","merchant_id",
    "merchant_risk_score","transaction_hour","transaction_day_of_week",
    "is_weekend_transaction","is_nighttime_transaction","device_id","device_os",
    "device_type","is_vpn_used","is_proxy_used","ip_address","ip_risk_score",
    "location_country","location_city","is_new_device","is_new_location",
    "num_failed_attempts_24h","prev_avg_txn_amount","txn_amount_deviation",
    "daily_avg_spend","total_spend_last_7d","transaction_recency","txn_velocity_1h",
    "txn_velocity_24h","transaction_success_rate_24h","has_multiple_devices",
    "is_blacklisted_card","is_blacklisted_device","is_high_risk_country",
    "distance_from_last_transaction","has_chargeback_history",
    "previous_fraudulent_activity","account_fraud_reported","is_high_risk_behavior","label"
]

def validate_csv_columns(df: pd.DataFrame):
    """
    Validates that all expected columns exist in the DataFrame.
    Returns a tuple: (is_valid: bool, missing_columns: list)
    """
    missing_cols = [col for col in EXPECTED_COLUMNS if col not in df.columns]
    if missing_cols:
        logger.warning(f"---Missing columns in CSV: {missing_cols}")
        return False, missing_cols
    return True, []
