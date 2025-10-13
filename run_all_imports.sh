#!/bin/bash

# Shell wrapper for CSV import to PostgreSQL
# This script provides an easy interface to run the imports

set -e  # Exit on error

echo "========================================================================"
echo "CSV to PostgreSQL Import - All Data Types"
echo "========================================================================"
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

# Prompt for password if not set
if [ -z "$DB_PASSWORD" ]; then
    echo "Database password not set."
    read -s -p "Enter PostgreSQL password for user '$DB_USER': " DB_PASSWORD
    export DB_PASSWORD
    echo ""
    echo ""
fi

echo "Configuration:"
echo "  Database: ${DB_HOST}:${DB_PORT}/${DB_NAME}"
echo "  User: ${DB_USER}"
echo ""

# Show menu
echo "Select import option:"
echo "  1) Import ALL data types (ORG + PER + RAW_FEED_PER)"
echo "  2) Import ORGANIZATION (ORG) data only"
echo "  3) Import PERSON (PER) data only"
echo "  4) Import RAW FEED PERSON data only"
echo "  5) Exit"
echo ""

read -p "Enter choice [1-5]: " choice

case $choice in
    1)
        echo ""
        echo "Running ALL imports..."
        python3 import_all_to_postgres.py
        ;;
    2)
        echo ""
        echo "Running ORGANIZATION import..."
        python3 import_org_to_postgres.py
        ;;
    3)
        echo ""
        echo "Running PERSON import..."
        python3 import_per_to_postgres.py
        ;;
    4)
        echo ""
        echo "Running RAW FEED PERSON import..."
        python3 import_raw_feed_per_to_postgres.py
        ;;
    5)
        echo "Exiting..."
        exit 0
        ;;
    *)
        echo "Invalid choice. Exiting."
        exit 1
        ;;
esac

echo ""
echo "✓ Script completed!"

