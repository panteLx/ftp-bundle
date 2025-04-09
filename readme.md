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
cp .env.example .env
```

Edit the `.env` file with your own values.

```bash
docker compose up --build
```
