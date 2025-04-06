import os
import ftplib
import datetime
import requests
import time
import logging
from dotenv import load_dotenv
from datetime import datetime, timedelta

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('ftp-purge')

load_dotenv()

# FTP server configuration
FTP_HOST = os.getenv("FTP_HOST")
FTP_USER = os.getenv("FTP_USER")
FTP_PASS = os.getenv("FTP_PASS")
FTP_DIR = os.getenv("FTP_DIR")

# Discord webhook URL
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL")

# Number of days to keep files (delete older files)
DAYS_TO_KEEP = int(os.getenv("DAYS_TO_KEEP"))

# Sleep time in seconds (24 hours = 86400 seconds)
FTP_PURGE_INTERVAL = int(os.getenv("FTP_PURGE_INTERVAL"))
HEALTH_CHECK_INTERVAL = int(os.getenv("HEALTH_CHECK_INTERVAL"))

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

def process_directory(ftp, current_path, cutoff_date, deleted_files):
    """Recursively process directories and delete old files"""
    # Get list of files and directories
    file_list = []
    ftp.dir(file_list.append)
    
    # First pass: Process files
    for file_info in file_list:
        # Parse the file information
        parts = file_info.split(None, 8)
        if len(parts) < 9:
            continue
            
        item_type = parts[0][0]
        item_name = parts[8]
        
        # Skip directories in this pass
        if item_type == 'd' and item_name not in ['.', '..']:
            continue
            
        # Process regular files
        if item_type == '-':
            try:
                # Get file modification time
                full_path = f"{current_path}/{item_name}" if current_path else item_name
                mod_time_str = ftp.sendcmd(f"MDTM {item_name}")[4:]
                mod_time = datetime.strptime(mod_time_str, "%Y%m%d%H%M%S")
                
                # Check if file is older than cutoff date
                if mod_time < cutoff_date:
                    logger.info(f"Deleting file: {full_path} (modified: {mod_time})")
                    ftp.delete(item_name)
                    deleted_files.append(full_path)
            except Exception as e:
                logger.error(f"Error processing file {item_name}: {e}")
    
    # Second pass: Process subdirectories recursively
    subdirs_to_check = []
    for file_info in file_list:
        parts = file_info.split(None, 8)
        if len(parts) < 9:
            continue
            
        item_type = parts[0][0]
        item_name = parts[8]
        
        # Process directories (but skip . and ..)
        if item_type == 'd' and item_name not in ['.', '..']:
            subdirs_to_check.append(item_name)
    
    # Process each subdirectory
    for subdir in subdirs_to_check:
        try:
            # Navigate into subdirectory
            ftp.cwd(subdir)
            subdir_path = f"{current_path}/{subdir}" if current_path else subdir
            logger.info(f"Processing directory: {subdir_path}")
            
            # Process the subdirectory
            process_directory(ftp, subdir_path, cutoff_date, deleted_files)
            
            # Check if directory is empty
            dir_list = []
            ftp.dir(dir_list.append)
            
            # Only count actual items (not . and ..)
            actual_items = [item for item in dir_list if item.split(None, 8)[-1] not in ['.', '..']]
            
            # If empty, delete the directory
            if not actual_items:
                # Go back to parent directory before deleting
                ftp.cwd('..')
                logger.info(f"Deleting empty directory: {subdir_path}")
                ftp.rmd(subdir)
                deleted_files.append(f"{subdir_path}/ (empty directory)")
            else:
                # Just go back to parent directory
                ftp.cwd('..')
        except Exception as e:
            logger.error(f"Error processing directory {subdir}: {e}")
            # Try to go back to parent directory
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
        ftp = ftplib.FTP(FTP_HOST)
        ftp.login(FTP_USER, FTP_PASS)
        
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
        ftp = ftplib.FTP(FTP_HOST)
        ftp.login(FTP_USER, FTP_PASS)
        
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
