import os
import ftplib
import datetime
import requests
import time
import logging
from dotenv import load_dotenv
from datetime import datetime, timedelta
from logging.handlers import RotatingFileHandler
import ssl
from typing import List, Optional
import concurrent.futures
from functools import wraps
import sys

# Set up logging with rotation
def setup_logging():
    log_dir = "logs"
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    
    log_file = os.path.join(log_dir, "ftp-purge.log")
    handler = RotatingFileHandler(log_file, maxBytes=10485760, backupCount=5)  # 10MB per file, keep 5 backups
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    
    logger = logging.getLogger('ftp-purge')
    logger.setLevel(logging.INFO)
    logger.addHandler(handler)
    
    # Also log to console
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    return logger

logger = setup_logging()

# Load environment variables
load_dotenv()

# Validate environment variables
def validate_env_vars():
    required_vars = [
        "FTP_HOST", "FTP_USER", "FTP_PASS", "FTP_DIR",
        "DAYS_TO_KEEP", "DISCORD_WEBHOOK_URL",
        "FTP_PURGE_INTERVAL", "HEALTH_CHECK_INTERVAL"
    ]
    
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    if missing_vars:
        raise ValueError(f"Missing required environment variables: {', '.join(missing_vars)}")

# FTP server configuration with validation
try:
    validate_env_vars()
    FTP_HOST = os.getenv("FTP_HOST")
    FTP_USER = os.getenv("FTP_USER")
    FTP_PASS = os.getenv("FTP_PASS")
    FTP_DIR = os.getenv("FTP_DIR")
    DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL")
    DAYS_TO_KEEP = int(os.getenv("DAYS_TO_KEEP"))
    FTP_PURGE_INTERVAL = int(os.getenv("FTP_PURGE_INTERVAL"))
    HEALTH_CHECK_INTERVAL = int(os.getenv("HEALTH_CHECK_INTERVAL"))
except Exception as e:
    logger.error(f"Configuration error: {e}")
    sys.exit(1)

# Retry decorator for FTP operations
def retry_on_failure(max_retries=3, delay=5):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    if attempt == max_retries - 1:
                        logger.error(f"Operation failed after {max_retries} attempts: {e}")
                        raise
                    logger.warning(f"Attempt {attempt + 1} failed: {e}. Retrying in {delay} seconds...")
                    time.sleep(delay)
        return wrapper
    return decorator

def send_discord_notification(deleted_files):
    """Send notification to Discord about deleted files"""
    if not deleted_files:
        deleted_files = ["No files deleted"]
        file_count = 0
    else:
        file_count = len(deleted_files)
    
    file_list = "\n \n".join(deleted_files)
    
    message = {
        "content": f"🗑️ **{file_count} files deleted from FTP server**",
        "embeds": [{
            "title": "Deleted Files",
            "description": file_list,
            "color": 15158332,  # Red color
            "footer": {
                "text": f"Files older than {DAYS_TO_KEEP} days were purged"
            },
            "timestamp": datetime.now().isoformat()
        }]
    }
    
    try:
        response = requests.post(DISCORD_WEBHOOK_URL, json=message)
        response.raise_for_status()
        logger.info("Discord notification sent successfully")
    except Exception as e:
        logger.error(f"Failed to send Discord notification: {e}")

def create_ftp_connection() -> ftplib.FTP:
    """Create a secure FTP connection with retry logic"""
    try:
        # Try to use secure connection first
        try:
            ftp = ftplib.FTP_TLS(FTP_HOST)
            ftp.login(FTP_USER, FTP_PASS)
            ftp.prot_p()  # Switch to secure data connection
            logger.info("Established secure FTP connection")
        except:
            # Fall back to regular FTP if secure connection fails
            ftp = ftplib.FTP(FTP_HOST)
            ftp.login(FTP_USER, FTP_PASS)
            logger.info("Established regular FTP connection")
        return ftp
    except Exception as e:
        logger.error(f"Failed to establish FTP connection: {e}")
        raise

@retry_on_failure()
def process_directory(ftp: ftplib.FTP, current_path: str, cutoff_date: datetime, deleted_files: List[str]) -> None:
    """Recursively process directories and delete old files with improved error handling"""
    try:
        # Get list of files and directories
        file_list = []
        ftp.dir(file_list.append)
        
        # Process files in parallel using thread pool
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            futures = []
            for file_info in file_list:
                parts = file_info.split(None, 8)
                if len(parts) < 9:
                    continue
                    
                item_type = parts[0][0]
                item_name = parts[8]
                
                if item_type == '-':  # Regular file
                    futures.append(
                        executor.submit(process_file, ftp, current_path, item_name, cutoff_date, deleted_files)
                    )
                elif item_type == 'd' and item_name not in ['.', '..']:  # Directory
                    futures.append(
                        executor.submit(process_subdirectory, ftp, current_path, item_name, cutoff_date, deleted_files)
                    )
            
            # Wait for all tasks to complete
            concurrent.futures.wait(futures)
            
    except Exception as e:
        logger.error(f"Error processing directory {current_path}: {e}")
        raise

def process_file(ftp: ftplib.FTP, current_path: str, file_name: str, cutoff_date: datetime, deleted_files: List[str]) -> None:
    """Process a single file"""
    try:
        full_path = f"{current_path}/{file_name}" if current_path else file_name
        mod_time_str = ftp.sendcmd(f"MDTM {file_name}")[4:]
        mod_time = datetime.strptime(mod_time_str, "%Y%m%d%H%M%S")
        
        if mod_time < cutoff_date:
            logger.info(f"Deleting file: {full_path} (modified: {mod_time})")
            ftp.delete(file_name)
            deleted_files.append(full_path)
    except Exception as e:
        logger.error(f"Error processing file {file_name}: {e}")

def process_subdirectory(ftp: ftplib.FTP, current_path: str, dir_name: str, cutoff_date: datetime, deleted_files: List[str]) -> None:
    """Process a subdirectory"""
    try:
        ftp.cwd(dir_name)
        subdir_path = f"{current_path}/{dir_name}" if current_path else dir_name
        logger.info(f"Processing directory: {subdir_path}")
        
        process_directory(ftp, subdir_path, cutoff_date, deleted_files)
        
        # Check if directory is empty
        dir_list = []
        ftp.dir(dir_list.append)
        actual_items = [item for item in dir_list if item.split(None, 8)[-1] not in ['.', '..']]
        
        if not actual_items:
            ftp.cwd('..')
            logger.info(f"Deleting empty directory: {subdir_path}")
            ftp.rmd(dir_name)
            deleted_files.append(f"{subdir_path}/ (empty directory)")
        else:
            ftp.cwd('..')
    except Exception as e:
        logger.error(f"Error processing directory {dir_name}: {e}")
        try:
            ftp.cwd('..')
        except:
            pass

def purge_ftp():
    # Calculate the cutoff date
    cutoff_date = datetime.now() - timedelta(days=DAYS_TO_KEEP)
    deleted_files = []
    
    try:
        # Connect to FTP server
        ftp = create_ftp_connection()
        
        # Change to the specified directory
        if FTP_DIR and FTP_DIR != '/':
            ftp.cwd(FTP_DIR)
        
        # Process the directory recursively
        current_path = FTP_DIR if FTP_DIR and FTP_DIR != '/' else ""
        process_directory(ftp, current_path, cutoff_date, deleted_files)
        
        # Close FTP connection
        ftp.quit()
        
        # Send Discord notification
        send_discord_notification(deleted_files)
        
        logger.info(f"Purge complete. {len(deleted_files)} items deleted.")
        
    except Exception as e:
        logger.error(f"Error: {e}")
        # Try to send notification about the error
        try:
            requests.post(DISCORD_WEBHOOK_URL, json={
                "content": f"❌ **Error during FTP purge**: {str(e)}"
            })
        except:
            pass

def check_ftp_health():
    """Check if FTP server is reachable and responding"""
    try:
        # Connect to FTP server
        ftp = create_ftp_connection()
        
        # Try to get current directory to verify connection
        ftp.pwd()
        
        # Close connection
        ftp.quit()
        
        # Send success notification
        try:
            requests.post(DISCORD_WEBHOOK_URL, json={
                "content": "✅ **FTP Server Health Check**: Server is reachable and responding",
                "embeds": [{
                    "title": "Health Check Status",
                    "description": "FTP server is operational",
                    "color": 3066993,  # Green color
                    "timestamp": datetime.now().isoformat()
                }]
            })
        except Exception as e:
            logger.error(f"Failed to send health check notification: {e}")
            
        logger.info("FTP health check successful")
        return True
        
    except Exception as e:
        # Send failure notification
        try:
            requests.post(DISCORD_WEBHOOK_URL, json={
                "content": f"❌ **FTP Server Health Check Failed**: {str(e)}",
                "embeds": [{
                    "title": "Health Check Status",
                    "description": f"FTP server is not responding: {str(e)}",
                    "color": 15158332,  # Red color
                    "timestamp": datetime.now().isoformat()
                }]
            })
        except Exception as notif_error:
            logger.error(f"Failed to send health check notification: {notif_error}")
            
        logger.error(f"FTP health check failed: {e}")
        return False

def main():
    logger.info("FTP Purge service and health check started")

    # Initialize last health check and purge times
    last_health_check = datetime.now()
    last_purge = datetime.now()

    # Calculate next health check and purge times
    next_health_check = last_health_check + timedelta(seconds=HEALTH_CHECK_INTERVAL)
    next_purge = last_purge + timedelta(seconds=FTP_PURGE_INTERVAL)

    logger.info(f"Next health check scheduled at: {next_health_check.strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"Next purge scheduled at: {next_purge.strftime('%Y-%m-%d %H:%M:%S')}")

    
    while True:
        current_time = datetime.now()
        
        # Check if it's time for a health check
        if (current_time - last_health_check).total_seconds() >= HEALTH_CHECK_INTERVAL:
            logger.info("Starting FTP health check cycle")
            check_ftp_health()
            last_health_check = current_time
            next_health_check = current_time + timedelta(seconds=HEALTH_CHECK_INTERVAL)
            logger.info(f"Next health check scheduled at: {next_health_check.strftime('%Y-%m-%d %H:%M:%S')}")
        
        # Check if it's time for a purge
        if (current_time - last_purge).total_seconds() >= FTP_PURGE_INTERVAL:
            logger.info("Starting FTP purge cycle")
            purge_ftp()
            last_purge = current_time
            next_purge = current_time + timedelta(seconds=FTP_PURGE_INTERVAL)
            logger.info(f"Next purge scheduled at: {next_purge.strftime('%Y-%m-%d %H:%M:%S')}")
        
        # Sleep for 1 second before checking again
        time.sleep(1)

if __name__ == "__main__":
    main()
