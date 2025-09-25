# Weekly Supervision Pull - Production Script

This production script extracts weekly supervision hours data from the database, transforms it, and uploads both raw and transformed data to Google Drive with proper archiving.

## Features

- **Database Integration**: Connects to SQL Server database using ODBC
- **Data Transformation**: Processes raw data into structured format with supervision percentages
- **Google Drive Integration**: Uploads files to Google Drive with automatic archiving
- **Local File Management**: Saves files locally with automatic archiving
- **Comprehensive Logging**: Detailed logging for monitoring and debugging
- **Error Handling**: Robust error handling throughout the process
- **Modular SQL Queries**: SQL templates stored in separate file for easy maintenance

## Prerequisites

### Environment Variables

Create a `.env` file in the project root with the following variables:

```env
# Database Configuration
CR_DWH_SERVER=your_database_server
CR_UN=your_username
CR_PW=your_password

# Google Drive Configuration
RAW_FOLDER_ID=your_raw_folder_id
TRANSFORMED_FOLDER_ID=your_transformed_folder_id
TRANSFORMED_ARCHIVE_ID=your_archive_folder_id
```

### Google Drive Setup

1. Create a Google Cloud Project and enable the Drive API
2. Create OAuth 2.0 credentials and download the `client_secret.json` file
3. Place `client_secret.json` in the same directory as the script
4. Run the script once to generate `token.json` for authentication

### Dependencies

Install required packages:

```bash
pip install -r requirements.txt
```

## Usage

### Basic Usage

```bash
python weekly_supervision_pull.py
```

### With Custom Start Date

```python
from weekly_supervision_pull import WeeklySupervisionPull

# Initialize the processor
processor = WeeklySupervisionPull()

# Run with custom start date
processor.run(start_date='2025-01-01')
```

## File Structure

```
scripts_notebooks/prod/
├── weekly_supervision_pull.py    # Main production script
├── config.py                     # Configuration file
├── sql_queries.py               # SQL query templates
├── README.md                     # This file
├── logs/                         # Log files directory
│   └── weekly_supervision_pull.log
├── client_secret.json           # Google OAuth credentials (add manually)
└── token.json                   # Generated after first run
```

## Output Files

### Local Files
- **Raw Data**: `../../data/raw_pulls/weekly_supervision_hours_YYYY-MM-DD.csv`
- **Transformed Data**: `../../data/transformed_supervision_weekly/weekly_supervision_hours_transformed_YYYY-MM-DD.csv`
- **Archived Files**: `../../data/transformed_supervision_weekly/archived/`

### Google Drive Files
- **Raw Data**: Uploaded to specified raw folder
- **Transformed Data**: Uploaded to specified transformed folder
- **Archived Files**: Moved to specified archive folder

## Data Transformation

The script transforms raw supervision data into a structured format with:

- **Clinic**: Extracted from service location names
- **Direct Provider**: Provider information and names
- **Direct Hours**: Total direct service hours
- **Supervision Hours**: Total supervision hours
- **Percentage Supervised**: Calculated percentage of direct hours supervised

## Logging

The script creates detailed logs in `logs/weekly_supervision_pull.log` including:

- Execution start/end times
- Database query results
- File operations
- Google Drive operations
- Error messages and stack traces

## Error Handling

The script includes comprehensive error handling for:

- Database connection issues
- SQL query failures
- File I/O operations
- Google Drive API errors
- Data transformation errors

## Scheduling

This script can be scheduled to run automatically using:

- **cron** (Linux/Mac)
- **Task Scheduler** (Windows)
- **Cloud schedulers** (AWS Lambda, Google Cloud Functions, etc.)

Example cron job to run weekly on Mondays at 9 AM:

```bash
0 9 * * 1 cd /path/to/project && python scripts_notebooks/prod/weekly_supervision_pull.py
```

## Troubleshooting

### Common Issues

1. **Database Connection**: Verify database credentials and network connectivity
2. **Google Drive Authentication**: Ensure `client_secret.json` is properly configured
3. **File Permissions**: Check write permissions for local file directories
4. **Missing Dependencies**: Run `pip install -r requirements.txt`

### Log Analysis

Check the log file for detailed error information:

```bash
tail -f logs/weekly_supervision_pull.log
```

## Support

For issues or questions, check the log files and ensure all environment variables are properly configured.
