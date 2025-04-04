#!/bin/bash
cd ~/ftp-purge
python3 -m venv ftp-purge-venv
source ftp-purge-venv/bin/activate
pip install -r requirements.txt
python3 purge.py