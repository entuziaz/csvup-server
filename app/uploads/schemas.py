from pydantic import BaseModel
from typing import Optional
from uuid import UUID
from datetime import datetime

class UploadResponse(BaseModel):
    success: bool
    message: str
    data: dict


class UploadHistoryResponse(BaseModel):
    id: int
    upload_id: str
    filename: str
    uploaded_at: datetime
    rows_processed: int
    status: str
    details: Optional[dict] = None
    # user_ip: Optional[str] = None
    
    class Config:
        from_attributes = True