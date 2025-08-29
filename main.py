import os
from fastapi import FastAPI
from app.core import database
from app.uploads import models
from app.uploads.routers import router as uploads_router
from fastapi.middleware.cors import CORSMiddleware


# MODEL CREATION (for sqlite only)
if os.getenv("DATABASE_URL", "").startswith("sqlite"):
    models.Base.metadata.create_all(bind=database.engine)

app = FastAPI(
    title="CSV Upload API",
    version="1.0.0",
    description="API for uploading CSV files and saving transaction data.",
    docs_url="/",
    redoc_url="/redoc",
)

# CORS
origins = os.getenv("CORS_ORIGINS", "http://localhost:5173,https://csvup-client.vercel.app").split(",")

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,            
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ROUTERS
app.include_router(uploads_router)


@app.get("/health")
async def health_check():
    return {"status": "healthy", "database": "connected"} 