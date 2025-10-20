#!/usr/bin/env python3
"""
Import Person (PER) CSV files to PostgreSQL
Processes all CSV files from 20250922/per/csv folder
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

# Configure logging with file handler
LOG_FILE_PATH = os.getenv('PER_IMPORT_LOG_FILE', 'import_per_to_postgres.log')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE_PATH, encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

def log(message: str):
    logger.info(message)

# Database configuration
DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': os.getenv('DB_PORT', '5432'),
    'database': os.getenv('DB_NAME', 'venture_db'),
    'user': os.getenv('DB_USER', 'postgres'),
    'password': os.getenv('DB_PASSWORD', 'postgres')
}

TABLE_NAME = 'releases_per_export'
# CSV_FOLDER_PATH = os.getenv('CSV_FOLDER_PATH', '20250922/per/csv')
CSV_FOLDER_PATH = os.path.join(os.getenv('CSV_FOLDER_PATH'), '20250922/per/csv')

# Column definitions for PER table
COLUMN_DEFINITIONS = [
    ('LINKEDIN_URL', 'TEXT'),
    ('ABOUT_ME', 'TEXT'),
    ('CELLPHONE', 'TEXT'),
    ('CITY', 'TEXT'),
    ('COUNTRY_CODE', 'TEXT'),
    ('COUNTRY_NAME', 'TEXT'),
    ('COUNTRY_REGION', 'TEXT'),
    ('CONTINENT', 'TEXT'),
    ('DIRECT_PHONE', 'TEXT'),
    ('EDUCATION', 'TEXT'),
    ('FIRST_NAME', 'TEXT'),
    ('FULL_NAME', 'TEXT'),
    ('INTERESTS', 'TEXT'),
    ('JOB_COUNT', 'INTEGER'),
    ('JOB_DESCRIPTION', 'TEXT'),
    ('JOB_END_DATE', 'TEXT'),
    ('JOB_IS_CURRENT', 'BOOLEAN'),
    ('JOB_LEVEL', 'TEXT'),
    ('JOB_LOCATION_CITY', 'TEXT'),
    ('JOB_LOCATION_COUNTRY', 'TEXT'),
    ('JOB_LOCATION_COUNTRY_CODE', 'TEXT'),
    ('JOB_LOCATION_COUNTRY_REGION', 'TEXT'),
    ('JOB_LOCATION_CONTINENT', 'TEXT'),
    ('JOB_LOCATION_STATE', 'TEXT'),
    ('JOB_LOCATION_STATE_CODE', 'TEXT'),
    ('JOB_START_DATE', 'TEXT'),
    ('JOB_ORDER_IN_PROFILE', 'INTEGER'),
    ('JOB_ORG_LINKEDIN_URL', 'TEXT'),
    ('JOB_TITLE', 'TEXT'),
    ('JOB_FUNCTION', 'TEXT'),
    ('LANGUAGES', 'TEXT'),
    ('LAST_NAME', 'TEXT'),
    ('LINKEDIN_CONNECTIONS_COUNT', 'INTEGER'),
    ('LINKEDIN_HEADLINE', 'TEXT'),
    ('LINKEDIN_INDUSTRY', 'TEXT'),
    ('MIDDLE_NAME', 'TEXT'),
    ('NICKNAME_NAME', 'TEXT'),
    ('RBID', 'TEXT'),
    ('RBID_ORG', 'TEXT'),
    ('RBID_PAO', 'TEXT'),
    ('SKILLS', 'TEXT'),
    ('CERTIFICATIONS', 'TEXT'),
    ('PATENTS', 'TEXT'),
    ('PUBLICATIONS', 'TEXT'),
    ('WEBSITES', 'TEXT'),
    ('STATE_CODE', 'TEXT'),
    ('STATE_NAME', 'TEXT'),
    ('SUFFIX_NAME', 'TEXT'),
    ('TITLE_NAME', 'TEXT'),
    ('EMAIL_DOMAIN', 'TEXT'),
    ('UPDATED_AT', 'TIMESTAMP'),
    ('RN', 'INTEGER'),
    ('EMAIL_STATUS', 'TEXT'),
    ('EMAIL_ADDRESS', 'TEXT'),
    ('EMAIL_LAST_VERIFIED_AT', 'TIMESTAMP'),
    ('PERSONA', 'TEXT')
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
        elif col_type in ('TIMESTAMP', 'DATE'):
            processed.append(value)
        else:
            processed.append(value)
    return tuple(processed)


def open_csv_file(file_path: str):
    if file_path.endswith('.gz'):
        log(f"Opening compressed CSV file: {file_path}")
        return gzip.open(file_path, 'rt', encoding='utf-8')
    else:
        log(f"Opening CSV file: {file_path}")
        return open(file_path, 'r', encoding='utf-8')


def get_csv_files(folder_path: str) -> List[str]:
    folder = Path(folder_path)
    if not folder.exists():
        log(f"Folder not found: {folder_path}")
        raise FileNotFoundError(f"Folder not found: {folder_path}")
    csv_files = []
    csv_files.extend(folder.glob('*.csv'))
    csv_files.extend(folder.glob('*.csv.gz'))
    csv_files = sorted([str(f) for f in csv_files])
    log(f"Found {len(csv_files)} PER CSV files")
    return csv_files


def create_table(cursor) -> None:
    log(f"Creating table {TABLE_NAME}...")
    # cursor.execute(f"DROP TABLE IF EXISTS {TABLE_NAME} CASCADE")
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
    log(f"✓ Table {TABLE_NAME} created")


def is_csv_file_imported(cursor, csv_file_path: str) -> bool:
    """
    Test if the CSV data already imported by selecting the first row of data.
    Returns True if first row in file already exists in table, otherwise False.
    """
    columns = [col_name for col_name, _ in COLUMN_DEFINITIONS]
    try:
        with open_csv_file(csv_file_path) as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                # Process the very first row only
                processed_row = process_row(row)
                # Build the WHERE clause with all fields in the row
                where_clause_pieces = []
                values = []
                for i, value in enumerate(processed_row):
                    col = columns[i]
                    if value is None:
                        where_clause_pieces.append(f"{col} IS NULL")
                    else:
                        where_clause_pieces.append(f"{col} = %s")
                        values.append(value)
                where_sql = " AND ".join(where_clause_pieces)
                query = f"SELECT 1 FROM {TABLE_NAME} WHERE {where_sql} LIMIT 1"
                log(f"Checking if the first row of {os.path.basename(csv_file_path)} is already imported...")
                cursor.execute(query, values)
                if cursor.fetchone():
                    log(f"First row of {os.path.basename(csv_file_path)} is already in table.")
                    return True
                log(f"First row of {os.path.basename(csv_file_path)} is not in table.")
                return False
    except Exception as e:
        logger.warning(f"Could not check if CSV {csv_file_path} is already imported: {e}")
        return False
    return False  # If file is empty, treat as not imported


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

    log(f"Importing data from {file_name}...")
    try:
        with open_csv_file(csv_file_path) as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                try:
                    processed_row = process_row(row)
                    batch.append(processed_row)
                    if len(batch) >= batch_size:
                        execute_batch(cursor, insert_query, batch)
                        log(f"Inserted batch of {len(batch)} rows from {file_name}")
                        total_rows += len(batch)
                        batch = []
                except Exception as e:
                    logger.debug(f"Error processing row: {e}")
                    continue
            if batch:
                execute_batch(cursor, insert_query, batch)
                log(f"Inserted final batch of {len(batch)} rows from {file_name}")
                total_rows += len(batch)
    except Exception as e:
        logger.error(f"Error reading {file_name}: {e}")
        raise
    log(f"Imported {total_rows} rows from {file_name}")
    return total_rows


def import_multiple_files(cursor, csv_files: List[str], batch_size: int = 1000) -> int:
    total_rows = 0
    total_files = len(csv_files)
    logger.info(f"Starting import of {total_files} PER files...")
    for idx, csv_file in enumerate(csv_files, 1):
        try:
            file_name = os.path.basename(csv_file)
            if is_csv_file_imported(cursor, csv_file):
                logger.info(f"[PER] [{idx}/{total_files}] {file_name}: Already imported, skipping.")
                continue
            log(f"Starting import for file {file_name}")
            rows_imported = import_csv_data(cursor, csv_file, batch_size)
            total_rows += rows_imported
            cursor.connection.commit()
            progress_pct = (idx / total_files) * 100
            logger.info(f"[PER] [{idx}/{total_files}] {file_name}: +{rows_imported:,} rows | Progress: {progress_pct:.1f}% | Total: {total_rows:,}")
        except Exception as e:
            logger.error(f"Failed to process {csv_file}: {e}")
            continue
    log(f"Imported a total of {total_rows} rows from all files.")
    return total_rows


def create_indexes(cursor) -> None:
    logger.info("Creating indexes...")
    indexes = [
        ('idx_per_rbid', 'RBID'),
        ('idx_per_rbid_org', 'RBID_ORG'),
        ('idx_per_rbid_pao', 'RBID_PAO'),
        ('idx_per_full_name', 'FULL_NAME'),
        ('idx_per_email', 'EMAIL_ADDRESS'),
        ('idx_per_linkedin_url', 'LINKEDIN_URL'),
    ]
    for idx_name, col_name in indexes:
        try:
            cursor.execute(f"CREATE INDEX IF NOT EXISTS {idx_name} ON {TABLE_NAME} ({col_name})")
            log(f"Created index: {idx_name} on {col_name}")
        except Exception as e:
            logger.warning(f"Could not create index {idx_name}: {e}")


def main():
    logger.info("=" * 80)
    logger.info("PERSON (PER) DATA IMPORT")
    logger.info("=" * 80)
    logger.info(f"Database: {DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}")
    logger.info(f"Table: {TABLE_NAME}")
    logger.info(f"Folder: {CSV_FOLDER_PATH}")
    logger.info(f"Log file: {LOG_FILE_PATH}")

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
        logger.info(f"✓ PER Data import completed: {total_rows:,} rows")
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

