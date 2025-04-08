# FTP Bundle - FTP Purge + FTP Server

This is a simple script to purge files from an FTP server and send a notification to a Discord webhook. It also checks the heath status of the FTP server. It hosts the FTP server aswell.

## Requirements

- Docker
- Python 3.9+
- PIP

## Installation

```bash
git clone https://github.com/pantelx/ftp-bundle.git
cd ftp-bundle
cp example_docker-compose.yml docker-compose.yml
```

Edit the `docker-compose.yml` file with your own values.

```bash
docker compose up --build
```

## FTP Purge Environment Variables

- FTP_HOST: The host of the FTP server
- FTP_USER: The username for the FTP server
- FTP_PASS: The password for the FTP server
- FTP_DIR: The directory to purge
- DAYS_TO_KEEP: The number of days to keep files
- DISCORD_WEBHOOK_URL: The URL of the Discord webhook to send notifications to
- FTP_PURGE_INTERVAL: The time to sleep between purges in seconds (24 hours = 86400 seconds)
- HEALTH_CHECK_INTERVAL: The time to sleep between heath checks in seconds (24 hours = 86400 seconds)
