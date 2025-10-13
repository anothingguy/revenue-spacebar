#!/bin/bash

# Quick start script for CSV import to PostgreSQL
# This script helps you set up and run the import with minimal configuration

set -e  # Exit on error

echo "========================================"
echo "CSV to PostgreSQL Bulk Import"
echo "========================================"
echo ""

# Check if Python is available
if ! command -v python3 &> /dev/null; then
    echo "❌ Error: python3 is not installed"
    exit 1
fi

# Check if psycopg2 is installed
if ! python3 -c "import psycopg2" 2>/dev/null; then
    echo "⚠️  Warning: psycopg2-binary is not installed"
    echo "Installing dependencies..."
    pip install -r requirements.txt
    echo ""
fi

# Set default environment variables if not already set
export DB_HOST="${DB_HOST:-localhost}"
export DB_PORT="${DB_PORT:-5432}"
export DB_NAME="${DB_NAME:-venture_db}"
export DB_USER="${DB_USER:-postgres}"
export DB_PASSWORD="${DB_PASSWORD:-postgres}"
export TABLE_NAME="${TABLE_NAME:-releases_org_export}"
export CSV_FOLDER_PATH="${CSV_FOLDER_PATH:-20250922/org/csv}"

echo "Configuration:"
echo "  Database: ${DB_HOST}:${DB_PORT}/${DB_NAME}"
echo "  User: ${DB_USER}"
echo "  Table: ${TABLE_NAME}"
echo "  Folder: ${CSV_FOLDER_PATH}"
echo ""

# Confirm before proceeding
read -p "Continue with import? (y/n) " -n 1 -r
echo ""
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo "Import cancelled"
    exit 0
fi

echo ""
echo "Starting import..."
echo ""

# Run the import script
python3 import_csv_to_postgres.py

echo ""
echo "✓ Script completed!"

