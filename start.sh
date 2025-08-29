#!/bin/bash
# Wait for database to be ready (if needed)
sleep 5

# Run migrations
alembic upgrade head

# Start the application
exec uvicorn main:app --host 0.0.0.0 --port 8000