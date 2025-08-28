import pandas as pd
from sqlalchemy.orm import Session
from . import models
from datetime import datetime
import logging


logger = logging.getLogger(__name__)


def process_csv_upload(df: pd.DataFrame, file_name: str, db: Session):
    """
    Reads a CSV file and inserts/updates records in the database.
    Returns summary info for response.
    """
    try:
        # df = pd.read_csv(file.file)
        
        # df["timestamp"] = pd.to_datetime(df["timestamp"], format="%Y-%m-%d %H:%M:%S")
        # df["transaction_day_of_week"] = df["transaction_day_of_week"].map({
        #     "Monday": 0,
        #     "Tuesday": 1,
        #     "Wednesday": 2,
        #     "Thursday": 3,
        #     "Friday": 4,
        #     "Saturday": 5,
        #     "Sunday": 6
        # })
        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce", format="%Y-%m-%d %H:%M:%S")
        df["transaction_day_of_week"] = df["transaction_day_of_week"].map({
                "Monday": 0,
                "Tuesday": 1,
                "Wednesday": 2,
                "Thursday": 3,
                "Friday": 4,
                "Saturday": 5,
                "Sunday": 6
            })

        for _, row in df.iterrows():
            # timestamp = datetime.strptime(row["timestamp"], "%Y-%m-%d %H:%M:%S")
            # transaction_day_of_week = day_to_int(row["transaction_day_of_week"])

            # Convert day names to integers
            

            txn = models.Transaction(
                transaction_id=row["transaction_id"],
                 timestamp=row["timestamp"].to_pydatetime() if not pd.isnull(row["timestamp"]) else None,  
                user_id=row["user_id"],
                account_age_days=row["account_age_days"],
                customer_tier=row["customer_tier"],
                kyc_level=row["kyc_level"],
                has_multiple_accounts=bool(row["has_multiple_accounts"]),
                linked_card_count=row["linked_card_count"],
                transaction_amount=row["transaction_amount"],
                transaction_currency=row["transaction_currency"],
                transaction_type=row["transaction_type"],
                merchant_category=row["merchant_category"],
                merchant_id=row["merchant_id"],
                merchant_risk_score=row["merchant_risk_score"],
                transaction_hour=row["transaction_hour"],
                transaction_day_of_week=row["transaction_day_of_week"],
                is_weekend_transaction=bool(row["is_weekend_transaction"]),
                is_nighttime_transaction=bool(row["is_nighttime_transaction"]),
                device_id=row["device_id"],
                device_os=row["device_os"],
                device_type=row["device_type"],
                is_vpn_used=bool(row["is_vpn_used"]),
                is_proxy_used=bool(row["is_proxy_used"]),
                ip_address=row["ip_address"],
                has_multiple_devices=bool(row["has_multiple_devices"]),
                is_blacklisted_card=bool(row["is_blacklisted_card"]),
                is_blacklisted_device=bool(row["is_blacklisted_device"]),
                is_high_risk_country=bool(row["is_high_risk_country"]),
                distance_from_last_transaction=row["distance_from_last_transaction"],
                has_chargeback_history=bool(row["has_chargeback_history"]),
                previous_fraudulent_activity=bool(row["previous_fraudulent_activity"]),
                account_fraud_reported=bool(row["account_fraud_reported"]),
                is_high_risk_behavior=bool(row["is_high_risk_behavior"]),
                label=row["label"]
            )
            db.merge(txn)

        db.commit()

        return {"filename": file_name, "rows": len(df)}

    except Exception as e:
        logger.error(f"---Error while parsing CSV: {str(e)}")
        raise


def day_to_int(day_name: str) -> int:
    day_map = {
        "Monday": 0,
        "Tuesday": 1,
        "Wednesday": 2,
        "Thursday": 3,
        "Friday": 4,
        "Saturday": 5,
        "Sunday": 6
    }
    return day_map.get(day_name, -1)  # -1 if invalid