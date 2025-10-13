#!/usr/bin/env python3
"""
Master Import Script - Import All CSV Types to PostgreSQL
Runs all three import scripts sequentially:
1. ORG (Organization) data
2. PER (Person) data  
3. RAW_FEED_PER (Raw Feed Person) data
"""

import os
import sys
import time
import logging
import subprocess
import dotenv
from datetime import datetime, timedelta

dotenv.load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def format_duration(seconds: float) -> str:
    """Format duration in human-readable format"""
    duration = timedelta(seconds=int(seconds))
    hours, remainder = divmod(duration.seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    
    if duration.days > 0:
        return f"{duration.days}d {hours}h {minutes}m {seconds}s"
    elif hours > 0:
        return f"{hours}h {minutes}m {seconds}s"
    elif minutes > 0:
        return f"{minutes}m {seconds}s"
    else:
        return f"{seconds}s"


def run_import_script(script_name: str, data_type: str) -> tuple[bool, float]:
    """
    Run an import script and return success status and duration
    """
    logger.info(f"\n{'=' * 80}")
    logger.info(f"Starting {data_type} import...")
    logger.info(f"Script: {script_name}")
    logger.info(f"{'=' * 80}\n")
    
    start_time = time.time()
    
    try:
        # Run the script as a subprocess
        result = subprocess.run(
            [sys.executable, script_name],
            check=True,
            capture_output=False,
            text=True
        )
        
        duration = time.time() - start_time
        logger.info(f"\n✓ {data_type} import completed in {format_duration(duration)}")
        return True, duration
        
    except subprocess.CalledProcessError as e:
        duration = time.time() - start_time
        logger.error(f"\n✗ {data_type} import failed after {format_duration(duration)}")
        logger.error(f"Error: {e}")
        return False, duration
    except Exception as e:
        duration = time.time() - start_time
        logger.error(f"\n✗ {data_type} import failed: {e}")
        return False, duration


def main():
    """
    Main function to orchestrate all imports
    """
    logger.info("=" * 80)
    logger.info("MASTER IMPORT SCRIPT - ALL DATA TYPES")
    logger.info("=" * 80)
    logger.info(f"Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 80)
    
    # Check database configuration
    db_host = os.getenv('DB_HOST', 'localhost')
    db_port = os.getenv('DB_PORT', '5432')
    db_name = os.getenv('DB_NAME', 'venture_db')
    db_user = os.getenv('DB_USER', 'postgres')
    
    logger.info(f"\nDatabase Configuration:")
    logger.info(f"  Host: {db_host}")
    logger.info(f"  Port: {db_port}")
    logger.info(f"  Database: {db_name}")
    logger.info(f"  User: {db_user}")
    
    # Define import jobs
    import_jobs = [
        {
            'script': 'import_org_to_postgres.py',
            'name': 'ORGANIZATION (ORG)',
            'table': 'releases_org_export',
            'folder': '20250922/org/csv'
        },
        {
            'script': 'import_per_to_postgres.py',
            'name': 'PERSON (PER)',
            'table': 'releases_per_export',
            'folder': '20250922/per/csv'
        },
        {
            'script': 'import_raw_feed_per_to_postgres.py',
            'name': 'RAW FEED PERSON',
            'table': 'releases_raw_feed_per_export',
            'folder': '20250922/raw_feed_per'
        }
    ]
    
    logger.info(f"\nImport Plan:")
    for idx, job in enumerate(import_jobs, 1):
        logger.info(f"  {idx}. {job['name']}")
        logger.info(f"     → Table: {job['table']}")
        logger.info(f"     → Folder: {job['folder']}")
    
    # Confirm before starting
    logger.info(f"\n{'=' * 80}")
    response = input("Ready to start imports? (y/n): ")
    if response.lower() != 'y':
        logger.info("Import cancelled by user")
        sys.exit(0)
    
    # Track results
    results = []
    total_start_time = time.time()
    
    # Run each import
    for idx, job in enumerate(import_jobs, 1):
        logger.info(f"\n\n{'#' * 80}")
        logger.info(f"JOB {idx}/{len(import_jobs)}: {job['name']}")
        logger.info(f"{'#' * 80}")
        
        success, duration = run_import_script(job['script'], job['name'])
        
        results.append({
            'name': job['name'],
            'table': job['table'],
            'success': success,
            'duration': duration
        })
        
        # If import failed, ask if we should continue
        if not success:
            logger.warning(f"\n⚠️  {job['name']} import failed!")
            response = input("Continue with remaining imports? (y/n): ")
            if response.lower() != 'y':
                logger.info("Stopping imports as requested")
                break
    
    # Print final summary
    total_duration = time.time() - total_start_time
    
    logger.info(f"\n\n{'=' * 80}")
    logger.info("IMPORT SUMMARY")
    logger.info(f"{'=' * 80}")
    logger.info(f"Total Duration: {format_duration(total_duration)}")
    logger.info(f"End Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("")
    
    # Results table
    logger.info("Results:")
    logger.info("-" * 80)
    for result in results:
        status = "✓ SUCCESS" if result['success'] else "✗ FAILED"
        duration_str = format_duration(result['duration'])
        logger.info(f"{status:12} | {result['name']:25} | {duration_str:15} | {result['table']}")
    logger.info("-" * 80)
    
    # Success/failure counts
    success_count = sum(1 for r in results if r['success'])
    failed_count = len(results) - success_count
    
    logger.info(f"\nSummary: {success_count} succeeded, {failed_count} failed out of {len(results)} jobs")
    
    if failed_count == 0:
        logger.info(f"\n🎉 All imports completed successfully!")
        sys.exit(0)
    else:
        logger.warning(f"\n⚠️  Some imports failed. Please check the logs above.")
        sys.exit(1)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.warning("\n\nImport interrupted by user (Ctrl+C)")
        sys.exit(130)
    except Exception as e:
        logger.error(f"\n\nUnexpected error: {e}")
        sys.exit(1)

