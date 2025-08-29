#!/bin/bash
echo "Installing dependencies..."
pip install -r requirements.txt

echo "Running database migrations..."
python run_migrations.py

if [ $? -eq 0 ]; then
    echo "---Build completed successfully"
else
    echo "---Build failed"
    exit 1
fi