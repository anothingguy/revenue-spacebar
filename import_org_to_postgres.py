#!/usr/bin/env python3
"""
Import Organization (ORG) CSV files to PostgreSQL
Processes all CSV files from 20250922/org/csv folder
"""

import csv
import os
import sys
import logging
import gzip
from pathlib import Path
from typing import Optional, List
import psycopg2
from psycopg2.extras import execute_batch
import dotenv

dotenv.load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Database configuration
DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': os.getenv('DB_PORT', '5432'),
    'database': os.getenv('DB_NAME', 'venture_db'),
    'user': os.getenv('DB_USER', 'postgres'),
    'password': os.getenv('DB_PASSWORD', 'postgres')
}

TABLE_NAME = 'releases_org_export'
# CSV_FOLDER_PATH = os.getenv('CSV_FOLDER_PATH', '20250922/org/csv')
CSV_FOLDER_PATH = os.path.join(os.getenv('CSV_FOLDER_PATH'), '20250922/org/csv')

# Column definitions for ORG table
COLUMN_DEFINITIONS = [
    ('ABOUT_US', 'TEXT'),
    ('CATEGORY_CRUNCHBASE', 'TEXT'),
    ('CATEGORY_G2', 'TEXT'),
    ('COMPANY_ENTITY_TYPE', 'TEXT'),
    ('COMPANY_LEGAL_TYPE', 'TEXT'),
    ('COMPANY_NAME', 'TEXT'),
    ('COMPANY_NAME_LANGUAGE', 'TEXT'),
    ('EMPLOYEE_COUNT_MAX', 'INTEGER'),
    ('EMPLOYEE_COUNT_MIN', 'INTEGER'),
    ('EMPLOYEE_COUNT_RANGE', 'TEXT'),
    ('EMPLOYEE_PROFILES_ON_LINKEDIN', 'INTEGER'),
    ('FOUNDED', 'INTEGER'),
    ('HEADQUARTERS_CITY', 'TEXT'),
    ('HEADQUARTERS_COUNTRY_CODE', 'TEXT'),
    ('HEADQUARTERS_COUNTRY_NAME', 'TEXT'),
    ('HEADQUARTERS_COUNTRY_REGION', 'TEXT'),
    ('HEADQUARTERS_CONTINENT', 'TEXT'),
    ('HEADQUARTERS_POSTCODE', 'TEXT'),
    ('HEADQUARTERS_STATE_CODE', 'TEXT'),
    ('HEADQUARTERS_STATE_NAME', 'TEXT'),
    ('HEADQUARTERS_STREET', 'TEXT'),
    ('INDUSTRY_LINKEDIN', 'TEXT'),
    ('INDUSTRY_SIC_CODE', 'TEXT'),
    ('INDUSTRY_SIC_DESCRIPTION', 'TEXT'),
    ('INDUSTRY_NAICS_CODE', 'TEXT'),
    ('INDUSTRY_NAICS_DESCRIPTION', 'TEXT'),
    ('INDUSTRY_NAICS_2022_CODE', 'TEXT'),
    ('INDUSTRY_NAICS_2022_DESCRIPTION', 'TEXT'),
    ('PREDICTED_INDUSTRY_NAICS_2022_CODE', 'TEXT'),
    ('PREDICTED_INDUSTRY_NAICS_2022_DESCRIPTION', 'TEXT'),
    ('INDUSTRY_UK_STANDARD_2007_CODE', 'TEXT'),
    ('INDUSTRY_UK_STANDARD_2007_DESCRIPTION', 'TEXT'),
    ('IS_LINKEDIN_URL_CLAIMED', 'BOOLEAN'),
    ('LINKEDIN_FOLLOWERS', 'INTEGER'),
    ('LINKEDIN_URL', 'TEXT'),
    ('LINKEDIN_URL_ID', 'NUMERIC'),
    ('LOCATION_CITY', 'TEXT'),
    ('LOCATION_COUNT', 'INTEGER'),
    ('LOCATION_COUNTRY_CODE', 'TEXT'),
    ('LOCATION_COUNTRY_NAME', 'TEXT'),
    ('LOCATION_COUNTRY_REGION', 'TEXT'),
    ('LOCATION_CONTINENT', 'TEXT'),
    ('LOCATION_IS_PRIMARY', 'BOOLEAN'),
    ('LOCATION_POSTCODE', 'TEXT'),
    ('LOCATION_STATE_CODE', 'TEXT'),
    ('LOCATION_STATE_NAME', 'TEXT'),
    ('LOCATION_STREET', 'TEXT'),
    ('PHONE', 'TEXT'),
    ('RBID', 'TEXT'),
    ('REVENUE_MAX', 'NUMERIC'),
    ('REVENUE_MIN', 'NUMERIC'),
    ('REVENUE_RANGE', 'TEXT'),
    ('SPECIALTIES', 'TEXT'),
    ('UPDATED_AT', 'DATE'),
    ('DOMAIN', 'TEXT'),
    ('DOMAIN_TLD', 'TEXT'),
    ('WEBSITE', 'TEXT'),
    ('IS_WEBSITE_WORKING', 'BOOLEAN'),
    ('IS_WEBSITE_FOR_SALE', 'BOOLEAN')
]


def clean_value(value: str) -> Optional[str]:
    if value in (r'\N', '\\N', ''):
        return None
    return value


def parse_boolean(value: Optional[str]) -> Optional[bool]:
    if value is None or value == '':
        return None
    if value.lower() in ('true', 't', '1', 'yes'):
        return True
    if value.lower() in ('false', 'f', '0', 'no'):
        return False
    return None


def parse_integer(value: Optional[str]) -> Optional[int]:
    if value is None or value == '':
        return None
    try:
        return int(float(value))
    except (ValueError, TypeError):
        return None


def parse_numeric(value: Optional[str]) -> Optional[float]:
    if value is None or value == '':
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None


def process_row(row: dict) -> tuple:
    processed = []
    for col_name, col_type in COLUMN_DEFINITIONS:
        value = clean_value(row.get(col_name, ''))
        
        if col_type == 'BOOLEAN':
            processed.append(parse_boolean(value))
        elif col_type == 'INTEGER':
            processed.append(parse_integer(value))
        elif col_type == 'NUMERIC':
            processed.append(parse_numeric(value))
        elif col_type == 'DATE':
            processed.append(value)
        else:
            processed.append(value)
    
    return tuple(processed)


def open_csv_file(file_path: str):
    if file_path.endswith('.gz'):
        return gzip.open(file_path, 'rt', encoding='utf-8')
    else:
        return open(file_path, 'r', encoding='utf-8')


def get_csv_files(folder_path: str) -> List[str]:
    folder = Path(folder_path)
    if not folder.exists():
        raise FileNotFoundError(f"Folder not found: {folder_path}")
    
    csv_files = []
    csv_files.extend(folder.glob('*.csv'))
    csv_files.extend(folder.glob('*.csv.gz'))
    csv_files = sorted([str(f) for f in csv_files])
    
    logger.info(f"Found {len(csv_files)} ORG CSV files")
    return csv_files


def create_table(cursor) -> None:
    logger.info(f"Creating table {TABLE_NAME}...")
    
    cursor.execute(f"DROP TABLE IF EXISTS {TABLE_NAME} CASCADE")
    
    columns_sql = []
    for col_name, col_type in COLUMN_DEFINITIONS:
        columns_sql.append(f"{col_name} {col_type}")
    
    create_query = f"""
        CREATE TABLE {TABLE_NAME} (
            id SERIAL PRIMARY KEY,
            {', '.join(columns_sql)},
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """
    
    cursor.execute(create_query)
    logger.info(f"✓ Table {TABLE_NAME} created")


def import_csv_data(cursor, csv_file_path: str, batch_size: int = 1000) -> int:
    file_name = os.path.basename(csv_file_path)
    
    columns = [col_name for col_name, _ in COLUMN_DEFINITIONS]
    placeholders = ','.join(['%s'] * len(columns))
    insert_query = f"""
        INSERT INTO {TABLE_NAME} ({','.join(columns)})
        VALUES ({placeholders})
    """
    
    total_rows = 0
    batch = []
    
    try:
        with open_csv_file(csv_file_path) as csvfile:
            reader = csv.DictReader(csvfile)
            
            for row in reader:
                try:
                    processed_row = process_row(row)
                    batch.append(processed_row)
                    
                    if len(batch) >= batch_size:
                        execute_batch(cursor, insert_query, batch)
                        total_rows += len(batch)
                        batch = []
                        
                except Exception as e:
                    logger.debug(f"Error processing row: {e}")
                    continue
            
            if batch:
                execute_batch(cursor, insert_query, batch)
                total_rows += len(batch)
    
    except Exception as e:
        logger.error(f"Error reading {file_name}: {e}")
        raise
    
    return total_rows


def import_multiple_files(cursor, csv_files: List[str], batch_size: int = 1000) -> int:
    total_rows = 0
    total_files = len(csv_files)
    
    logger.info(f"Starting import of {total_files} ORG files...")
    
    for idx, csv_file in enumerate(csv_files, 1):
        try:
            file_name = os.path.basename(csv_file)
            rows_imported = import_csv_data(cursor, csv_file, batch_size)
            total_rows += rows_imported
            
            cursor.connection.commit()
            
            progress_pct = (idx / total_files) * 100
            logger.info(f"[ORG] [{idx}/{total_files}] {file_name}: +{rows_imported:,} rows | Progress: {progress_pct:.1f}% | Total: {total_rows:,}")
            
        except Exception as e:
            logger.error(f"Failed to process {csv_file}: {e}")
            continue
    
    return total_rows


def create_indexes(cursor) -> None:
    logger.info("Creating indexes...")
    
    indexes = [
        ('idx_org_company_name', 'COMPANY_NAME'),
        ('idx_org_domain', 'DOMAIN'),
        ('idx_org_rbid', 'RBID'),
        ('idx_org_country_code', 'HEADQUARTERS_COUNTRY_CODE'),
    ]
    
    for idx_name, col_name in indexes:
        try:
            cursor.execute(f"CREATE INDEX IF NOT EXISTS {idx_name} ON {TABLE_NAME} ({col_name})")
        except Exception as e:
            logger.warning(f"Could not create index {idx_name}: {e}")


def main():
    logger.info("=" * 80)
    logger.info("ORGANIZATION (ORG) DATA IMPORT")
    logger.info("=" * 80)
    logger.info(f"Database: {DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}")
    logger.info(f"Table: {TABLE_NAME}")
    logger.info(f"Folder: {CSV_FOLDER_PATH}")
    
    connection = None
    cursor = None
    
    try:
        connection = psycopg2.connect(**DB_CONFIG)
        cursor = connection.cursor()
        logger.info("✓ Connected to PostgreSQL\n")
        
        create_table(cursor)
        connection.commit()
        
        csv_files = get_csv_files(CSV_FOLDER_PATH)
        
        if not csv_files:
            logger.error("No CSV files found")
            sys.exit(1)
        
        total_rows = import_multiple_files(cursor, csv_files)
        
        logger.info(f"\n{'=' * 80}")
        logger.info(f"✓ ORG Data import completed: {total_rows:,} rows")
        logger.info(f"{'=' * 80}\n")
        
        create_indexes(cursor)
        connection.commit()
        
        cursor.execute(f"SELECT COUNT(*) FROM {TABLE_NAME}")
        count = cursor.fetchone()[0]
        
        cursor.execute(f"SELECT pg_size_pretty(pg_total_relation_size('{TABLE_NAME}'))")
        size = cursor.fetchone()[0]
        
        logger.info(f"Final Statistics:")
        logger.info(f"  • Rows: {count:,}")
        logger.info(f"  • Size: {size}")
        logger.info(f"\n✓ Import completed successfully!")
        
    except Exception as e:
        logger.error(f"Error: {e}")
        if connection:
            connection.rollback()
        sys.exit(1)
        
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()


if __name__ == "__main__":
    main()

