from fastapi import APIRouter, UploadFile, File, Depends, HTTPException, status
from fastapi.responses import JSONResponse
from sqlalchemy.orm import Session
from app.core.database import get_db
from app.uploads.services import process_csv_upload
from app.uploads.validators import validate_csv_columns
from app.uploads.schemas import UploadResponse
import logging


logger = logging.getLogger(__name__)

router = APIRouter(
    tags=['Uploads'],
    prefix='/api/v1/uploads'
)


@router.post("/csv/", status_code=status.HTTP_201_CREATED, 
             response_model=UploadResponse)
async def upload_csv(file: UploadFile = File(...), db: Session = Depends(get_db)):
    """
    Upload a CSV file, parse it, and save transactions in the database.
    """
    try:
        if not file.filename.endswith(".csv"):
            return JSONResponse(
                status_code=status.HTTP_400_BAD_REQUEST,
                content={"success": False, "message": "Invalid file type. Please upload a valid CSV file."}
            )

        # Validate CSV columns
        is_valid, missing_cols = validate_csv_columns(df)
        if not is_valid:
            return JSONResponse(
                status_code=status.HTTP_400_BAD_REQUEST,
                content={"success": False, "message": f"Missing columns: {missing_cols}"}
            )

        # Reading and processing the CSV file
        file.file.seek(0)
        result = process_csv_upload(file, db)
        logger.info(f"---Uploaded file {file.filename} with {result['rows']} rows")
        return {
            "message": "File processed successfully",
            "data": result
        }

    except Exception as e:
        logger.error(f"---Unexpected error while processing file: {str(e)}")
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={"success": False, "message": f"An unexpected error occurred while processing the file. Please try again later."}
        )