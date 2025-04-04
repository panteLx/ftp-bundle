# FTP Purge

This is a simple script to purge files from an FTP server.

## Usage

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
