from urllib import request
from fastapi import APIRouter, UploadFile, File, Depends, HTTPException, status
import pandas as pd
import io
from fastapi.responses import JSONResponse
from sqlalchemy.orm import Session
from app.core.database import get_db
from app.uploads.services import process_csv_upload
from app.uploads.validators import validate_csv_columns
from app.uploads.schemas import (
    UploadResponse, UploadHistoryResponse
)
from app.uploads import models
import logging


logger = logging.getLogger(__name__)

router = APIRouter(
    tags=['Uploads'],
    prefix='/api/v1/uploads'
)


@router.post("/csv/", status_code=status.HTTP_201_CREATED, response_model=UploadResponse)
async def upload_csv(file: UploadFile = File(...), db: Session = Depends(get_db)):
    """
    Upload a CSV file, parse it, and save transactions in the database.
    """
    try:
        # client_ip = request.client.host if request.client else None
        
        file_name = file.filename
        if not file_name.endswith(".csv"):
            return JSONResponse(
                status_code=status.HTTP_400_BAD_REQUEST,
                content={"success": False, "message": "Invalid file type. Please upload a valid CSV file."}
            )

        # Read CSV into a DataFrame
        content = await file.read()
        if not content:
            return JSONResponse(
                status_code=status.HTTP_400_BAD_REQUEST,
                content={"success": False, "message": "Empty CSV file."}
            )

        df = pd.read_csv(io.StringIO(content.decode("utf-8")))

        # Validate CSV columns
        is_valid, missing_cols = validate_csv_columns(df)
        if not is_valid:
            return JSONResponse(
                status_code=status.HTTP_400_BAD_REQUEST,
                content={"success": False, "message": f"Missing columns: {missing_cols}"}
            )

        # Process CSV
        result = process_csv_upload(df, file_name, db) 
        logger.info(f"Uploaded file {file_name} with {result['successful_rows']} successful rows, {result['failed_rows']} failed rows")

        return {
            "success": True,
            "message": "File processed successfully",
            "data": result
        }

    except pd.errors.ParserError as e:
        return JSONResponse(
            status_code=status.HTTP_400_BAD_REQUEST,
            content={"success": False, "message": f"Malformed CSV: {e}"}
        )
    except Exception as e:
        logger.error(f"---Unexpected error while processing file: {str(e)}")
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={"success": False, "message": "An unexpected error occurred while processing the file."}
        )


@router.get("/history/", response_model=list[UploadHistoryResponse])
async def get_upload_history(
    skip: int = 0,
    limit: int = 100,
    db: Session = Depends(get_db)
):
    """
    Get history of file uploads.
    """
    history = db.query(models.UploadHistory).order_by(
        models.UploadHistory.uploaded_at.desc()
    ).offset(skip).limit(limit).all()
    
    return history