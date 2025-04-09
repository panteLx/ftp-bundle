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
from typing import List, Dict, Optional, Any, Tuple
import concurrent.futures
from functools import wraps
import sys
import docker
from dataclasses import dataclass
from pathlib import Path


@dataclass
class FileInfo:
    """Represents information about a file or directory."""
    name: str
    type: str  # 'd' for directory, '-' for file
    mtime: datetime
    path: str


class Logger:
    """Handles all logging related functionality."""
    
    def __init__(self):
        self.logger = self._setup_logging()
    
    def _setup_logging(self) -> logging.Logger:
        """Set up rotating file logger and console logger."""
        log_dir = Path("logs")
        try:
            log_dir.mkdir(exist_ok=True)
            log_file = log_dir / "ftp-purge.log"
            
            # Create a rotating file handler if we have permissions
            try:
                handler = RotatingFileHandler(log_file, maxBytes=10_485_760, backupCount=5)
                formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
                handler.setFormatter(formatter)
            except PermissionError:
                print(f"Warning: Permission denied for log file {log_file}. Logging to stderr only.")
                handler = None
            
            logger = logging.getLogger('ftp-purge')
            logger.setLevel(logging.INFO)
            
            if handler:
                logger.addHandler(handler)
            
            # Always add console handler
            console_handler = logging.StreamHandler(sys.stdout)
            console_handler.setFormatter(formatter)
            logger.addHandler(console_handler)
            
            return logger
        except Exception as e:
            print(f"Error setting up logger: {e}")
            return self._setup_basic_logger()
    
    def _setup_basic_logger(self) -> logging.Logger:
        """Set up a basic stderr logger as fallback."""
        basic_logger = logging.getLogger('ftp-purge-basic')
        basic_logger.setLevel(logging.INFO)
        handler = logging.StreamHandler(sys.stderr)
        handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        basic_logger.addHandler(handler)
        return basic_logger
    
    def info(self, message: str) -> None:
        """Log an info message."""
        self.logger.info(message)
    
    def error(self, message: str) -> None:
        """Log an error message."""
        self.logger.error(message)
    
    def warning(self, message: str) -> None:
        """Log a warning message."""
        self.logger.warning(message)


class ConfigManager:
    """Manages application configuration and environment variables."""
    
    def __init__(self, logger: Logger):
        self.logger = logger
        self.config = self._load_config()
    
    def _load_config(self) -> Dict[str, Any]:
        """Load and validate environment variables."""
        load_dotenv()
        
        required_vars = {
            "FTP_HOST": "FTP server hostname",
            "FTP_USER": "FTP server username",
            "FTP_PASS": "FTP server password",
            "FTP_DIR": "FTP directory to purge",
            "DAYS_TO_KEEP": "Number of days to keep files",
            "DISCORD_WEBHOOK_URL": "Discord webhook URL for notifications",
            "FTP_PURGE_INTERVAL": "Interval between purges in seconds",
            "HEALTH_CHECK_INTERVAL": "Interval between health checks in seconds"
        }
        
        # Validate required environment variables
        missing_vars = [var for var in required_vars if not os.getenv(var)]
        if missing_vars:
            err_msg = f"Missing required environment variables: {', '.join(missing_vars)}"
            self.logger.error(err_msg)
            raise ValueError(err_msg)
        
        # Create config dictionary with type conversion
        try:
            config = {
                "ftp_host": os.getenv("FTP_HOST"),
                "ftp_user": os.getenv("FTP_USER"),
                "ftp_pass": os.getenv("FTP_PASS"),
                "ftp_dir": os.getenv("FTP_DIR"),
                "discord_webhook_url": os.getenv("DISCORD_WEBHOOK_URL"),
                "days_to_keep": int(os.getenv("DAYS_TO_KEEP")),
                "ftp_purge_interval": int(os.getenv("FTP_PURGE_INTERVAL")),
                "health_check_interval": int(os.getenv("HEALTH_CHECK_INTERVAL"))
            }
            return config
        except ValueError as e:
            self.logger.error(f"Configuration error: Invalid number format in environment variables: {e}")
            raise
        except Exception as e:
            self.logger.error(f"Configuration error: {e}")
            raise
    
    def get(self, key: str) -> Any:
        """Get a configuration value."""
        return self.config.get(key)


class NotificationManager:
    """Handles notifications to Discord webhook."""
    
    def __init__(self, config: ConfigManager, logger: Logger):
        self.webhook_url = config.get("discord_webhook_url")
        self.days_to_keep = config.get("days_to_keep")
        self.logger = logger
    
    def send_purge_notification(self, deleted_files: List[str]) -> None:
        """Send notification to Discord about deleted files."""
        if not deleted_files:
            deleted_files = ["No files deleted"]
            file_count = 0
        else:
            file_count = len(deleted_files)
        
        file_list = "\n \n".join(deleted_files[:25])  # Limit to 25 files to avoid too large messages
        if len(deleted_files) > 25:
            file_list += f"\n \n... and {len(deleted_files) - 25} more files"
        
        message = {
            "content": f"🗑️ **{file_count} files deleted from FTP server**",
            "embeds": [{
                "title": "Deleted Files",
                "description": file_list,
                "color": 15158332,  # Red color
                "footer": {
                    "text": f"Files older than {self.days_to_keep} days were purged"
                },
                "timestamp": datetime.now().isoformat()
            }]
        }
        
        try:
            response = requests.post(self.webhook_url, json=message)
            response.raise_for_status()
            self.logger.info("Discord notification sent successfully")
        except Exception as e:
            self.logger.error(f"Failed to send Discord notification: {e}")
    
    def send_error_notification(self, error_message: str) -> None:
        """Send notification to Discord about an error."""
        try:
            message = {
                "content": f"❌ **Error during FTP purge**: {error_message}"
            }
            requests.post(self.webhook_url, json=message)
            self.logger.info("Error notification sent to Discord")
        except Exception as e:
            self.logger.error(f"Failed to send error notification: {e}")
    
    def send_restart_notification(self, error_message: str) -> None:
        """Send notification to Discord about FTP server restart."""
        try:
            message = {
                "content": f"🔄 **FTP Server Restarted**: The FTP server was restarted due to a failed health check. Error: {error_message}"
            }
            requests.post(self.webhook_url, json=message)
            self.logger.info("Restart notification sent to Discord")
        except Exception as e:
            self.logger.error(f"Failed to send restart notification: {e}")
    
    def send_restart_failure_notification(self, error_message: str) -> None:
        """Send notification to Discord about failed FTP server restart."""
        try:
            message = {
                "content": f"❌ **FTP Server Restart Failed**: Could not restart the FTP server container. Error: {error_message}"
            }
            requests.post(self.webhook_url, json=message)
            self.logger.info("Restart failure notification sent to Discord")
        except Exception as e:
            self.logger.error(f"Failed to send restart failure notification: {e}")


# Retry decorator for FTP operations
def retry_on_failure(max_retries=3, delay=5):
    """Decorator that retries a function on failure."""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            logger = args[0].logger if hasattr(args[0], 'logger') else logging.getLogger('ftp-purge')
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


class FtpManager:
    """Manages FTP connections and operations."""
    
    def __init__(self, config: ConfigManager, logger: Logger):
        self.config = config
        self.logger = logger
        self.host = config.get("ftp_host")
        self.user = config.get("ftp_user")
        self.password = config.get("ftp_pass")
        self.directory = config.get("ftp_dir")
    
    def create_connection(self) -> ftplib.FTP:
        """Create a secure FTP connection with retry logic."""
        try:
            # Try to use secure connection first
            try:
                ftp = ftplib.FTP_TLS(self.host)
                ftp.login(self.user, self.password)
                ftp.prot_p()  # Switch to secure data connection
                self.logger.info("Established secure FTP connection")
            except:
                # Fall back to regular FTP if secure connection fails
                ftp = ftplib.FTP(self.host)
                ftp.login(self.user, self.password)
                self.logger.info("Established regular FTP connection")
            return ftp
        except Exception as e:
            self.logger.error(f"Failed to establish FTP connection: {e}")
            raise


class FileSystemManager:
    """Manages direct file system operations for the FTP data container."""
    
    def __init__(self, logger: Logger):
        self.logger = logger
        self.ftp_data_path = Path("/ftp-data")
    
    def _get_full_path(self, path: str) -> Path:
        """Get the full path for a given relative path."""
        return self.ftp_data_path / path.lstrip('/')
    
    def list_directory(self, path: str) -> List[FileInfo]:
        """List contents of a directory."""
        try:
            full_path = self._get_full_path(path)
            items = []
            
            for item in full_path.iterdir():
                stat = item.stat()
                items.append(FileInfo(
                    name=item.name,
                    type='d' if item.is_dir() else '-',
                    mtime=datetime.fromtimestamp(stat.st_mtime),
                    path=str(item.relative_to(self.ftp_data_path))
                ))
            return items
        except Exception as e:
            self.logger.error(f"Error listing directory {path}: {e}")
            raise
    
    def delete_file(self, path: str) -> None:
        """Delete a file."""
        try:
            full_path = self._get_full_path(path)
            full_path.unlink()
        except Exception as e:
            self.logger.error(f"Error deleting file {path}: {e}")
            raise
    
    def delete_directory(self, path: str) -> None:
        """Delete an empty directory."""
        try:
            full_path = self._get_full_path(path)
            full_path.rmdir()
        except Exception as e:
            self.logger.error(f"Error deleting directory {path}: {e}")
            raise
    
    def is_directory_empty(self, path: str) -> bool:
        """Check if a directory is empty."""
        try:
            full_path = self._get_full_path(path)
            return not any(full_path.iterdir())
        except Exception as e:
            self.logger.error(f"Error checking if directory is empty {path}: {e}")
            raise


class FtpPurger:
    """Handles the purging of old files from FTP server."""
    
    def __init__(self, config: ConfigManager, logger: Logger, notifier: NotificationManager, ftp_manager: FtpManager):
        self.config = config
        self.logger = logger
        self.notifier = notifier
        self.ftp_manager = ftp_manager
        self.fs_manager = FileSystemManager(logger)
        self.days_to_keep = config.get("days_to_keep")
        self.directory = config.get("ftp_dir")
    
    @retry_on_failure()
    def process_directory(self, current_path: str, cutoff_date: datetime, deleted_files: List[str]) -> None:
        """Recursively process directories and delete old files."""
        try:
            items = self.fs_manager.list_directory(current_path)
            
            with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
                futures = []
                for item in items:
                    if item.type == '-':  # Regular file
                        futures.append(
                            executor.submit(
                                self.process_file,
                                item.path,
                                item.mtime,
                                cutoff_date,
                                deleted_files
                            )
                        )
                    elif item.type == 'd':  # Directory
                        futures.append(
                            executor.submit(
                                self.process_subdirectory,
                                item.path,
                                cutoff_date,
                                deleted_files
                            )
                        )
                
                concurrent.futures.wait(futures)
                
        except Exception as e:
            self.logger.error(f"Error processing directory {current_path}: {e}")
            raise
    
    def process_file(self, file_path: str, mod_time: datetime, cutoff_date: datetime, deleted_files: List[str]) -> None:
        """Process a single file."""
        try:
            if mod_time < cutoff_date:
                self.logger.info(f"Deleting file: {file_path} (modified: {mod_time})")
                self.fs_manager.delete_file(file_path)
                deleted_files.append(file_path)
        except Exception as e:
            self.logger.error(f"Error processing file {file_path}: {e}")
    
    def process_subdirectory(self, dir_path: str, cutoff_date: datetime, deleted_files: List[str]) -> None:
        """Process a subdirectory."""
        try:
            self.logger.info(f"Processing directory: {dir_path}")
            self.process_directory(dir_path, cutoff_date, deleted_files)
            
            if self.fs_manager.is_directory_empty(dir_path):
                self.logger.info(f"Deleting empty directory: {dir_path}")
                self.fs_manager.delete_directory(dir_path)
                deleted_files.append(f"{dir_path}/ (empty directory)")
                
        except Exception as e:
            self.logger.error(f"Error processing directory {dir_path}: {e}")
    
    def purge(self) -> None:
        """Main purge function to delete old files."""
        cutoff_date = datetime.now() - timedelta(days=self.days_to_keep)
        deleted_files = []
        
        try:
            current_path = self.directory if self.directory and self.directory != '/' else ""
            self.process_directory(current_path, cutoff_date, deleted_files)
            
            if deleted_files:
                self.notifier.send_purge_notification(deleted_files)
            
            self.logger.info(f"Purge complete. {len(deleted_files)} items deleted.")
            
        except Exception as e:
            self.logger.error(f"Error during purge: {e}")
            self.notifier.send_error_notification(str(e))


class DockerManager:
    """Manages Docker container operations."""
    
    def __init__(self, logger: Logger):
        self.logger = logger
    
    def restart_container(self, container_name: str) -> bool:
        """Restart a Docker container by name."""
        try:
            # Initialize Docker client
            client = docker.from_env()
            
            # Get the FTP server container
            container = client.containers.get(container_name)
            
            # Restart the container
            container.restart()
            self.logger.info(f"{container_name} container restarted successfully")
            return True
        except Exception as e:
            self.logger.error(f"Failed to restart {container_name} container: {e}")
            return False


class HealthChecker:
    """Performs health checks on the FTP server."""
    
    def __init__(self, logger: Logger, ftp_manager: FtpManager, notifier: NotificationManager, docker_manager: DockerManager):
        self.logger = logger
        self.ftp_manager = ftp_manager
        self.notifier = notifier
        self.docker_manager = docker_manager
    
    def check_ftp_health(self) -> bool:
        """Check if FTP server is reachable and responding."""
        try:
            # Try to connect to FTP server
            ftp = self.ftp_manager.create_connection()
            ftp.quit()
            self.logger.info("FTP health check passed")
            return True
        except Exception as e:
            self.logger.error(f"FTP health check failed: {e}")
            # Try to restart FTP server container
            if self.docker_manager.restart_container('ftp-server'):
                self.notifier.send_restart_notification(str(e))
            else:
                self.notifier.send_restart_failure_notification(str(e))
            return False


class FtpPurgeService:
    """Main service class that orchestrates health checks and purge operations."""
    
    def __init__(self):
        # Initialize components
        self.logger = Logger()
        
        try:
            self.config = ConfigManager(self.logger)
            self.ftp_manager = FtpManager(self.config, self.logger)
            self.notifier = NotificationManager(self.config, self.logger)
            self.docker_manager = DockerManager(self.logger)
            self.health_checker = HealthChecker(self.logger, self.ftp_manager, self.notifier, self.docker_manager)
            self.ftp_purger = FtpPurger(self.config, self.logger, self.notifier, self.ftp_manager)
            
            # Get interval settings
            self.health_check_interval = self.config.get("health_check_interval")
            self.purge_interval = self.config.get("ftp_purge_interval")
        except Exception as e:
            self.logger.error(f"Service initialization error: {e}")
            sys.exit(1)
    
    def run(self):
        """Run the service with scheduled health checks and purges."""
        self.logger.info("FTP Purge service and health check started")

        # Initialize last health check and purge times
        last_health_check = datetime.now()
        last_purge = datetime.now()

        # Calculate next health check and purge times
        next_health_check = last_health_check + timedelta(seconds=self.health_check_interval)
        next_purge = last_purge + timedelta(seconds=self.purge_interval)

        self.logger.info(f"Next health check scheduled at: {next_health_check.strftime('%Y-%m-%d %H:%M:%S')}")
        self.logger.info(f"Next purge scheduled at: {next_purge.strftime('%Y-%m-%d %H:%M:%S')}")

        
        while True:
            current_time = datetime.now()
            
            # Check if it's time for a health check
            if (current_time - last_health_check).total_seconds() >= self.health_check_interval:
                self.logger.info("Starting FTP health check cycle")
                self.health_checker.check_ftp_health()
                last_health_check = current_time
                next_health_check = current_time + timedelta(seconds=self.health_check_interval)
                self.logger.info(f"Next health check scheduled at: {next_health_check.strftime('%Y-%m-%d %H:%M:%S')}")
            
            # Check if it's time for a purge
            if (current_time - last_purge).total_seconds() >= self.purge_interval:
                self.logger.info("Starting FTP purge cycle")
                self.ftp_purger.purge()
                last_purge = current_time
                next_purge = current_time + timedelta(seconds=self.purge_interval)
                self.logger.info(f"Next purge scheduled at: {next_purge.strftime('%Y-%m-%d %H:%M:%S')}")
            
            # Sleep for 1 second before checking again
            time.sleep(1)


if __name__ == "__main__":
    service = FtpPurgeService()
    service.run()
