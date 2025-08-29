FROM python:3.11-slim

WORKDIR /app

# system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# application code
COPY app/ ./app/

EXPOSE 8000

# alembic files
COPY alembic.ini .
COPY alembic/ ./alembic/

# startup script
COPY start.sh .
RUN chmod +x start.sh

# Use the startup script instead of direct uvicorn
CMD ["./start.sh"]