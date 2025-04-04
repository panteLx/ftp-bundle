# FTP Purge

This is a simple script to purge files from an FTP server and send a notification to a Discord webhook.

## Requirements

- Docker
- Python 3.9+
- PIP

## Installation

```bash
git clone https://github.com/yourusername/ftp-purge.git
cd ftp-purge
cp .env.example .env
```

Edit the `.env` file with your own values.

```bash
docker compose up --build
```

## Environment Variables

- FTP_HOST: The host of the FTP server
- FTP_USER: The username for the FTP server
- FTP_PASS: The password for the FTP server
- FTP_DIR: The directory to purge
- DAYS_TO_KEEP: The number of days to keep files
- DISCORD_WEBHOOK_URL: The URL of the Discord webhook to send notifications to
- SLEEP_TIME: The time to sleep between purges in seconds (24 hours = 86400 seconds)
