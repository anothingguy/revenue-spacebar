#!/usr/bin/env python3
"""
Import Raw Feed Person (RAW_FEED_PER) CSV files to PostgreSQL
Processes all CSV files from 20250922/raw_feed_per folder
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

TABLE_NAME = 'releases_raw_feed_per_export'
CSV_FOLDER_PATH = os.getenv('CSV_FOLDER_PATH', '20250922/raw_feed_per')

# Column definitions for RAW_FEED_PER table
COLUMN_DEFINITIONS = [
    ('RBID', 'TEXT'),
    ('RBID_ORG', 'TEXT'),
    ('RBID_PAO', 'TEXT'),
    ('RBUUID', 'TEXT'),
    ('CREATED_AT', 'DATE'),
    ('UPDATED_AT', 'DATE'),
    ('FULL_NAME', 'TEXT'),
    ('TITLE_NAME', 'TEXT'),
    ('FIRST_NAME', 'TEXT'),
    ('MIDDLE_NAME', 'TEXT'),
    ('LAST_NAME', 'TEXT'),
    ('SUFFIX_NAME', 'TEXT'),
    ('NICKNAME_NAME', 'TEXT'),
    ('LINKEDIN_CONNECTIONS_COUNT', 'INTEGER'),
    ('ABOUT_ME', 'TEXT'),
    ('EDUCATION', 'TEXT'),
    ('LINKEDIN_HEADLINE', 'TEXT'),
    ('LINKEDIN_URL', 'TEXT'),
    ('LINKEDIN_URL_SLUG', 'TEXT'),
    ('LINKEDIN_INDUSTRY', 'TEXT'),
    ('CITY', 'TEXT'),
    ('STATE_NAME', 'TEXT'),
    ('STATE_CODE', 'TEXT'),
    ('COUNTRY_NAME', 'TEXT'),
    ('COUNTRY_CODE', 'TEXT'),
    ('COUNTRY_REGION', 'TEXT'),
    ('CONTINENT', 'TEXT'),
    ('RBUUID_ORG', 'TEXT'),
    ('JOB_IS_CURRENT', 'BOOLEAN'),
    ('JOB_COUNT', 'INTEGER'),
    ('JOB_TITLE', 'TEXT'),
    ('JOB_LEVEL', 'TEXT'),
    ('JOB_FUNCTION', 'TEXT'),
    ('JOB_DESCRIPTION', 'TEXT'),
    ('JOB_START_DATE', 'TEXT'),
    ('JOB_END_DATE', 'TEXT'),
    ('JOB_LOCATION_CITY', 'TEXT'),
    ('JOB_LOCATION_STATE', 'TEXT'),
    ('JOB_LOCATION_STATE_CODE', 'TEXT'),
    ('JOB_LOCATION_COUNTRY', 'TEXT'),
    ('JOB_LOCATION_COUNTRY_CODE', 'TEXT'),
    ('JOB_LOCATION_COUNTRY_REGION', 'TEXT'),
    ('JOB_LOCATION_CONTINENT', 'TEXT'),
    ('JOB_ORDER_IN_PROFILE', 'INTEGER'),
    ('JOB_ORG_LINKEDIN_URL', 'TEXT'),
    ('JOB_ORG_NAME', 'TEXT'),
    ('IS_MEMORIALIZED_PERSON', 'BOOLEAN'),
    ('LINKEDIN_NUM_ID', 'TEXT')
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


def process_row(row: dict) -> tuple:
    processed = []
    for col_name, col_type in COLUMN_DEFINITIONS:
        value = clean_value(row.get(col_name, ''))
        
        if col_type == 'BOOLEAN':
            processed.append(parse_boolean(value))
        elif col_type == 'INTEGER':
            processed.append(parse_integer(value))
        elif col_type in ('DATE', 'TIMESTAMP'):
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
    
    logger.info(f"Found {len(csv_files)} RAW_FEED_PER CSV files")
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
            import_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
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
    
    logger.info(f"Starting import of {total_files} RAW_FEED_PER files...")
    
    for idx, csv_file in enumerate(csv_files, 1):
        try:
            file_name = os.path.basename(csv_file)
            rows_imported = import_csv_data(cursor, csv_file, batch_size)
            total_rows += rows_imported
            
            cursor.connection.commit()
            
            progress_pct = (idx / total_files) * 100
            logger.info(f"[RAW_FEED_PER] [{idx}/{total_files}] {file_name}: +{rows_imported:,} rows | Progress: {progress_pct:.1f}% | Total: {total_rows:,}")
            
        except Exception as e:
            logger.error(f"Failed to process {csv_file}: {e}")
            continue
    
    return total_rows


def create_indexes(cursor) -> None:
    logger.info("Creating indexes...")
    
    indexes = [
        ('idx_raw_rbid', 'RBID'),
        ('idx_raw_rbid_org', 'RBID_ORG'),
        ('idx_raw_rbid_pao', 'RBID_PAO'),
        ('idx_raw_rbuuid', 'RBUUID'),
        ('idx_raw_linkedin_url', 'LINKEDIN_URL'),
        ('idx_raw_linkedin_num_id', 'LINKEDIN_NUM_ID'),
    ]
    
    for idx_name, col_name in indexes:
        try:
            cursor.execute(f"CREATE INDEX IF NOT EXISTS {idx_name} ON {TABLE_NAME} ({col_name})")
        except Exception as e:
            logger.warning(f"Could not create index {idx_name}: {e}")


def main():
    logger.info("=" * 80)
    logger.info("RAW FEED PERSON (RAW_FEED_PER) DATA IMPORT")
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
        logger.info(f"✓ RAW_FEED_PER Data import completed: {total_rows:,} rows")
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

