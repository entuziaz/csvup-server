# Stage 1: Builder stage (for compiling dependencies)
# FROM python:3.11-slim-bookworm as builder
# FROM python:3.11-alpine as builder
FROM python:3.11-slim-bullseye as builder

WORKDIR /app

# Install build dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Create virtual environment
RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Stage 2: Runtime stage (minimal image)
# FROM python:3.11-slim-bookworm
# FROM python:3.11-alpine
FROM python:3.11-slim-bullseye

WORKDIR /app

# Install only runtime system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq5 \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get clean

# Set Python path
ENV PYTHONPATH=/app

# Copy virtual environment from builder
COPY --from=builder /opt/venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# application code
COPY app/ ./app/
COPY main.py .  

# alembic files
COPY alembic.ini .
COPY alembic/ ./alembic/

# migrations and startup script
COPY run_migrations.py .
COPY start.sh .
RUN chmod +x start.sh

EXPOSE 8000

# Using startup script instead of direct uvicorn
CMD ["./start.sh"]