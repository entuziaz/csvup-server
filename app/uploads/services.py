from operator import index
import pandas as pd
from sqlalchemy.orm import Session
from . import models
from datetime import datetime
from fastapi import status, HTTPException
import logging


logger = logging.getLogger(__name__)


def process_csv_upload(df: pd.DataFrame, file_name: str, db: Session):
    """
    Reads a CSV file and bulk inserts/updates records in the database.
    Returns summary info for response.
    """   

    try:
        upload_record = models.UploadHistory(
            filename=file_name,
            uploaded_at=datetime.utcnow(),
            rows_processed=0,
            status="processing",
        )
        db.add(upload_record)
        db.flush()  
       
        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce", format="%Y-%m-%d %H:%M:%S")
        days_map = {"Monday": 0, "Tuesday": 1, "Wednesday": 2, "Thursday": 3, 
                  "Friday": 4, "Saturday": 5, "Sunday": 6}
        df["transaction_day_of_week"] = df["transaction_day_of_week"].map(days_map)

        # checking for duplicates in BATCHES of 1000
        existing_txn_id = set()
        transaction_ids = df["transaction_id"].tolist()

        batch_size = 1000
        for i in range(0, len(transaction_ids), batch_size):
            batch_ids = transaction_ids[i:i + batch_size]
            batch_existing = {id[0] for id in db.query(models.Transaction.transaction_id)
                            .filter(models.Transaction.transaction_id.in_(batch_ids))
                            .all()}
            existing_txn_id.update(batch_existing)
                
        new_rows = df[~df["transaction_id"].isin(existing_txn_id)]
        successful_rows = 0
        # failed_rows = 0
        duplicate_rows = len(df) - len(new_rows)
        transactions = []
        errors = []

        for index, row in new_rows.itertuples():
            try:
                transactions.append(
                    models.Transaction(
                        transaction_id=row.transaction_id,
                        timestamp=row.timestamp.to_pydatetime() if not pd.isnull(row.timestamp) else None,
                        user_id=row.user_id,

                        account_age_days=row.account_age_days,
                        customer_tier=row.customer_tier,
                        kyc_level=row.kyc_level,
                        has_multiple_accounts=bool(row.has_multiple_accounts),
                        linked_card_count=row.linked_card_count,
                        transaction_amount=row.transaction_amount,
                        transaction_currency=row.transaction_currency,
                        transaction_type=row.transaction_type,
                        merchant_category=row.merchant_category,
                        merchant_id=row.merchant_id,
                        merchant_risk_score=row.merchant_risk_score,
                        transaction_hour=row.transaction_hour,
                        transaction_day_of_week=row.transaction_day_of_week,
                        is_weekend_transaction=bool(row.is_weekend_transaction),
                        is_nighttime_transaction=bool(row.is_nighttime_transaction),
                        device_id=row.device_id,
                        device_os=row.device_os,
                        device_type=row.device_type,
                        is_vpn_used=bool(row.is_vpn_used),
                        is_proxy_used=bool(row.is_proxy_used),
                        ip_address=row.ip_address,
                        has_multiple_devices=bool(row.has_multiple_devices),
                        is_blacklisted_card=bool(row.is_blacklisted_card),
                        is_blacklisted_device=bool(row.is_blacklisted_device),
                        is_high_risk_country=bool(row.is_high_risk_country),
                        distance_from_last_transaction=row.distance_from_last_transaction,
                        has_chargeback_history=bool(row.has_chargeback_history),
                        previous_fraudulent_activity=bool(row.previous_fraudulent_activity),
                        account_fraud_reported=bool(row.account_fraud_reported),
                        is_high_risk_behavior=bool(row.is_high_risk_behavior),
                        label=row.label
                    )
                )
                successful_rows += 1
            
            except Exception as e:
                # failed_rows += 1
                errors.append({
                    "row": index + 1,  # 1-based index for readability
                    "error": str(e),
                    "transaction_id": getattr(row, 'transaction_id', 'unknown')
                })
                logger.error(f"Error processing row {index + 1}: {e}")

        # BATCH insert in 1000 txn chunks
        if transactions:
            batch_size = 1000
            for i in range(0, len(transactions), batch_size):
                batch = transactions[i:i + batch_size]
                try:
                    db.bulk_save_objects(batch)
                    db.flush()  # Flush after each batch-catch DB errors early & manage memory
                except Exception as batch_error:
                    logger.error(f"Error inserting batch {i//batch_size + 1}: {batch_error}")
                    for j, transaction in enumerate(batch):
                        errors.append({
                            "row": "batch_error",
                            "error": str(batch_error),
                            "transaction_id": transaction.transaction_id
                        })
                    successful_rows -= len(batch)  # Adjust successful count

        # Update upload history record
        upload_record.rows_processed = successful_rows
        upload_record.status = "success" if not errors else "partial"
        upload_record.details = {
            "errors": errors[:10], # Store first 10 errors
            "duplicates": duplicate_rows 
        }  
        
        db.commit()

        return {
            "filename": file_name,
            "total_rows": len(df),
            "successful_rows": successful_rows,
            "failed_rows": len(errors),
            "duplicate_rows": duplicate_rows,
            "upload_id": upload_record.upload_id
        }

    except Exception as e:
        db.rollback()
        logger.error(f"---Error while parsing CSV: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="CSV processing failed."
        )
