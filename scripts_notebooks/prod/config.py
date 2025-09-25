"""
Configuration file for Weekly Supervision Pull script.

This file contains configuration constants and environment variable mappings.
"""

import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Database Configuration
DATABASE_CONFIG = {
    'server': os.getenv('CR_DWH_SERVER'),
    'username': os.getenv('CR_UN'),
    'password': os.getenv('CR_PW'),
    'database': 'insights'
}

# Google Drive Configuration
GOOGLE_DRIVE_CONFIG = {
    'raw_folder_id': os.getenv('RAW_FOLDER_ID'),
    'transformed_folder_id': os.getenv('TRANSFORMED_FOLDER_ID'),
    'transformed_archive_id': os.getenv('TRANSFORMED_ARCHIVE_ID'),
    'scopes': ['https://www.googleapis.com/auth/drive']
}

# File Configuration
FILE_CONFIG = {
    'raw_folder': '../../data/raw_pulls',
    'transformed_folder': '../../data/transformed_supervision_weekly',
    'archive_folder': '../../data/transformed_supervision_weekly/archived',
    'logs_folder': 'logs',
    'log_file': 'weekly_supervision_pull.log'
}

# SQL Query Configuration
SQL_CONFIG = {
    'service_codes': ['97155', '97153', 'Non-billable: PM Admin', 'PDS | BCBA'],
    'direct_service_code': '97153',
    'supervision_service_codes': ['97155', 'Non-billable: PM Admin', 'PDS | BCBA']
}

# Logging Configuration
LOGGING_CONFIG = {
    'level': 'INFO',
    'format': '%(asctime)s - %(levelname)s - %(message)s',
    'handlers': ['file', 'console']
}
