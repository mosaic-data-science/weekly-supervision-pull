# Weekly Supervision Pull

Extracts, transforms, and manages weekly supervision hours data from the CR DWH. 
This system automates the process of pulling month-to-date supervision data, processing it into clinic x staff direct and supervision hours billed, and uploads raw and transformed data to Google Drive with proper archiving.

## Overview

This repository contains a complete data pipeline for weekly supervision hours analysis, including:

- **Database Integration**: Direct connection to CR database for data extraction
- **Data Processing**: Complex SQL queries to analyze supervision overlaps
- **Data Transformation**: Processing raw data into structured insights
- **Cloud Storage**: Automated upload to Google Drive with archiving
- **Production Ready**: Comprehensive error handling, logging, and monitoring

## Repository Structure

```
weekly-supervision-pull/
├── README.md                           # This file
├── requirements.txt                     # Python dependencies
├── .gitignore                          # Git ignore rules
├── credentials/                        # Authentication files (ignored by git)
│   ├── client_secret.json              # Google OAuth credentials
│   ├── service-account-key.json        # Service account key
│   ├── token.json                      # OAuth token
│   └── README.md                       # Credentials documentation
├── scripts_notebooks/
│   ├── dev/                            # Development notebooks
│   │   └── weekly_pull_dev.ipynb       # Development Jupyter notebook
│   └── prod/                           # Production scripts
│       ├── weekly_supervision_pull.py  # Main production script
│       ├── config.py                   # Configuration management
│       ├── sql_queries.py              # SQL query templates
│       ├── README.md                   # Production documentation
│       └── logs/                       # Log files directory
└── data/                               # Data storage (ignored by git)
    ├── raw_pulls/                      # Raw data files
    └── transformed_supervision_weekly/  # Processed data files
        └── archived/                   # Archived files
```

## Quick Start

### Prerequisites

1. **Python 3.8+** installed
2. **Database Access** to CR database
3. **Google Drive API** credentials
4. **Environment Variables** configured

### Installation

1. **Clone the repository**:
   ```bash
   git clone https://github.com/mosaic-data-science/weekly-supervision-pull.git
   cd weekly-supervision-pull
   ```

2. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

3. **Set up environment variables**:
   ```bash
   # Create .env file with your credentials
   cp .env.example .env
   # Edit .env with your actual values
   ```

4. **Configure Google Drive**:
   - Place `client_secret.json` in `credentials/` directory
   - Run the script once to generate `token.json` in the credentials directory

### Usage

**Production Script**:
```bash
cd scripts_notebooks/prod
python weekly_supervision_pull.py
```

**Development Notebook**:
```bash
cd scripts_notebooks/dev
jupyter notebook weekly_pull_dev.ipynb
```

## Configuration

### Environment Variables

Create a `.env` file in the project root:

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

1. **Create Google Cloud Project** and enable Drive API
2. **Create OAuth 2.0 credentials** and download `client_secret.json`
3. **Place credentials** in the appropriate directory
4. **Run script once** to generate authentication token

## Data Processing

### SQL Query Logic

The system uses complex SQL queries to:

1. **Extract base data** from billing entries with specific service codes
2. **Identify direct services** (97153) and supervision services (97155, etc.)
3. **Calculate overlaps** between direct and supervision hours
4. **Aggregate data** by provider, location, and client
5. **Calculate supervision percentages** for each provider

### Data Transformation

Raw data is transformed into:

- **Clinic information** extracted from service locations
- **Provider details** with names and IDs
- **Hour calculations** for direct and supervision time
- **Supervision percentages** for quality metrics
- **Structured output** ready for analysis

## Output Files

### Local Files
- **Raw Data**: `data/raw_pulls/weekly_supervision_hours_YYYY-MM-DD.csv`
- **Transformed Data**: `data/transformed_supervision_weekly/weekly_supervision_hours_transformed_YYYY-MM-DD.csv`
- **Archived Files**: `data/transformed_supervision_weekly/archived/`

### Google Drive Files
- **Raw Data**: Uploaded to specified raw folder
- **Transformed Data**: Uploaded to specified transformed folder
- **Archived Files**: Moved to archive folder automatically

## Workflow

1. **Data Extraction**: Query database for supervision hours data
2. **Data Processing**: Transform raw data into structured format
3. **Local Storage**: Save files locally with automatic archiving
4. **Cloud Upload**: Upload to Google Drive with archiving
5. **Logging**: Comprehensive logging for monitoring and debugging

## Development

### Branch Structure

- **`main`**: Production-ready code
- **`dev`**: Development and testing

### Development Workflow

1. **Create feature branch** from `dev`
2. **Make changes** in development environment
3. **Test thoroughly** before merging
4. **Create pull request** to `dev` branch
5. **Merge to main** after approval

### Code Structure

- **`weekly_supervision_pull.py`**: Main production script
- **`sql_queries.py`**: SQL templates and queries
- **`config.py`**: Configuration management
- **`logs/`**: Comprehensive logging system

## Monitoring & Logging

### Log Files

- **Location**: `scripts_notebooks/prod/logs/`
- **Format**: Detailed timestamped logs
- **Content**: Execution details, errors, and performance metrics

### Error Handling

- **Database connection** issues
- **SQL query** failures
- **File I/O** operations
- **Google Drive API** errors
- **Data transformation** errors

## Deployment

### Scheduling

The script can be scheduled using:

- **Cron** (Linux/Mac):
  ```bash
  0 9 * * 1 cd /path/to/project && python scripts_notebooks/prod/weekly_supervision_pull.py
  ```

- **Task Scheduler** (Windows)
- **Cloud Functions** (AWS Lambda, Google Cloud Functions)

### Production Considerations

- **Environment variables** properly configured
- **Database credentials** secured
- **Google Drive permissions** set up
- **Log rotation** configured
- **Error notifications** set up

## Security

### Credentials Management

- **Database credentials** stored in environment variables
- **Google OAuth tokens** generated automatically
- **Sensitive files** ignored by git
- **No hardcoded secrets** in code

### File Security

- **`.gitignore`** prevents credential leaks
- **Environment files** not committed
- **Log files** contain no sensitive data
- **Data files** stored locally and in secure cloud storage

## Support

### Troubleshooting

1. **Check log files** in `logs/` directory
2. **Verify environment variables** are set correctly
3. **Test database connection** independently
4. **Verify Google Drive** authentication

### Common Issues

- **Database connection** failures
- **Google Drive authentication** issues
- **File permission** problems
- **Missing dependencies**

## Contributing

1. **Fork the repository**
2. **Create feature branch** from `dev`
3. **Make your changes**
4. **Test thoroughly**
5. **Create pull request**

## License

This project is proprietary to Mosaic Data Science and is not open source.

## Contact

For questions or support, contact the Mosaic Data Science team.

---

**Repository**: https://github.com/mosaic-data-science/weekly-supervision-pull  
**Organization**: Mosaic Data Science  
**Last Updated**: September 2025
