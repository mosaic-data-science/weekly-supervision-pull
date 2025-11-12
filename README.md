# Daily Supervision Pull

Extracts, transforms, and manages daily supervision hours data from the CR DWH. 
This system automates the process of pulling month-to-date supervision data, processing it into clinic x staff direct and supervision hours billed, and uploads raw and transformed data to Google Drive with proper archiving.

## Overview

This repository contains a complete data pipeline for daily supervision hours analysis, including:

- **Database Integration**: Direct connection to CR database for data extraction
- **Data Processing**: Complex SQL queries to analyze supervision overlaps
- **Data Transformation**: Processing raw data into structured insights
- **Automated Scheduling**: Launchd job runs daily at 7:30 AM ET
- **Cloud Storage**: Automated sync to Google Drive via file system
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
│       ├── run_pipeline.py             # Main orchestrator script (runs all phases)
│       ├── run_pipeline_wrapper.sh     # Launchd wrapper script
│       ├── pull_data.py                # Phase 1: Pull data from database
│       ├── transform_data.py            # Phase 2: Transform raw data
│       ├── merge_data.py                # Phase 3: Merge BACB data
│       ├── sql_queries.py              # SQL query templates
│       └── logs/                       # Log files directory
├── com.mosaictherapy.daily-supervision-pull.plist  # Launchd configuration
├── setup_launchd.sh                    # Launchd installation script
└── data/                               # Data storage (ignored by git)
    ├── raw_pulls/                      # Raw data files
    │   ├── daily_supervision_hours_*.csv
    │   └── bacb_supervision_hours_*.csv
    └── transformed_supervision_daily/  # Processed data files
        ├── daily_supervision_hours_transformed_*.xlsx
        └── archived/                   # Archived files
```

## Quick Start

### Prerequisites

1. **Python 3.8+** installed
2. **Database Access** to CR database
3. **Environment Variables** configured
4. **Google Drive** (optional, for file sync via file system)

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
   # Create .env file in project root with your credentials
   # Required variables:
   # CR_DWH_SERVER=your_database_server
   # CR_UN=your_username
   # CR_PW=your_password
   ```

4. **Set up automated scheduling** (optional):
   ```bash
   # Install launchd job to run daily at 7:30 AM ET
   ./setup_launchd.sh
   ```

### Usage

**Run Full Pipeline** (Recommended):
```bash
cd scripts_notebooks/prod
python run_pipeline.py
```

**Run Individual Phases**:
```bash
cd scripts_notebooks/prod

# Phase 1: Pull data from database
python pull_data.py

# Phase 2: Transform raw data
python transform_data.py

# Phase 3: Merge BACB data
python merge_data.py
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
# Database Configuration (Required)
CR_DWH_SERVER=your_database_server
CR_UN=your_username
CR_PW=your_password
```

### Google Drive Setup (Optional)

The pipeline automatically syncs files to Google Drive via file system if Google Drive is installed and configured on your Mac. Files are saved to:
- Local: `data/transformed_supervision_daily/daily_supervision_hours_transformed_YYYY-MM-DD.xlsx`
- Google Drive: Synced automatically if Google Drive folder is configured

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
- **BACB supervision data** merged with main data
- **Structured Excel output** with conditional formatting, ready for analysis
- **Automatic filtering** of supervisors from direct provider lists

## Output Files

### Local Files
- **Raw Data**: `data/raw_pulls/daily_supervision_hours_YYYY-MM-DD.csv`
- **Raw BACB Data**: `data/raw_pulls/bacb_supervision_hours_YYYY-MM-DD.csv`
- **Transformed Data**: `data/transformed_supervision_daily/daily_supervision_hours_transformed_YYYY-MM-DD.xlsx`
- **Archived Files**: `data/transformed_supervision_daily/archived/`

### Google Drive Files
- Files are automatically synced to Google Drive via file system if Google Drive is installed
- Previous files are automatically archived before new files are saved

## Workflow

The pipeline consists of three distinct phases:

1. **Phase 1: Data Pull** (`pull_data.py`)
   - Executes main supervision hours query
   - Executes BACB supervision query
   - Saves raw data CSVs

2. **Phase 2: Data Transformation** (`transform_data.py`)
   - Reads raw supervision data
   - Removes rows where direct providers also appear as supervisors
   - Groups and aggregates by provider and location
   - Extracts clinic names from service locations
   - Saves transformed data

3. **Phase 3: Data Merge** (`merge_data.py`)
   - Merges BACB supervision data with transformed data
   - Calculates total supervision hours and percentages
   - Archives existing files before saving
   - Creates final Excel output with conditional formatting
   - Organizes data by clinic in separate sheets
   - Syncs to Google Drive via file system

Each phase logs its progress independently and can be run standalone or as part of the full pipeline.

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

**Pipeline Scripts:**
- **`run_pipeline.py`**: Main orchestrator that runs all three phases
- **`run_pipeline_wrapper.sh`**: Shell wrapper for launchd scheduling
- **`pull_data.py`**: Phase 1 - Pulls data from database
- **`transform_data.py`**: Phase 2 - Transforms raw data (filters supervisors)
- **`merge_data.py`**: Phase 3 - Merges BACB data and creates Excel output

**Supporting Files:**
- **`sql_queries.py`**: SQL query templates
- **`logs/`**: Comprehensive logging system (separate logs for each script)

**Scheduling:**
- **`com.mosaictherapy.daily-supervision-pull.plist`**: Launchd configuration
- **`setup_launchd.sh`**: Installation script for launchd job

## Monitoring & Logging

### Log Files

- **Location**: `scripts_notebooks/prod/logs/`
- **Format**: Detailed timestamped logs
- **Content**: Execution details, errors, and performance metrics
- **Files**:
  - `run_pipeline.log` - Main pipeline execution
  - `pull_data.log` - Database queries
  - `transform_data.log` - Data transformation
  - `merge_data.log` - Data merging
  - `launchd_stdout.log` - Launchd standard output
  - `launchd_stderr.log` - Launchd error output

### Error Handling

- **Database connection** issues
- **SQL query** failures
- **File I/O** operations
- **Data transformation** errors
- **Google Drive sync** failures (logged but non-fatal)

## Deployment

### Automated Scheduling (macOS)

The pipeline is configured to run automatically using launchd:

1. **Install the launchd job**:
   ```bash
   ./setup_launchd.sh
   ```

2. **Schedule**: Runs daily at 7:30 AM ET

3. **Check job status**:
   ```bash
   launchctl list | grep com.mosaictherapy.daily-supervision-pull
   ```

4. **Manual trigger** (for testing):
   ```bash
   launchctl kickstart gui/$(id -u)/com.mosaictherapy.daily-supervision-pull
   ```

5. **Unload job** (to stop scheduling):
   ```bash
   launchctl bootout gui/$(id -u)/com.mosaictherapy.daily-supervision-pull
   ```

### Alternative Scheduling Methods

- **Cron** (Linux/Mac):
  ```bash
  30 7 * * * cd /path/to/project/scripts_notebooks/prod && python run_pipeline.py
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

- **Database credentials** stored in environment variables (`.env` file)
- **Sensitive files** ignored by git
- **No hardcoded secrets** in code
- **Environment variables** loaded via `python-dotenv`

### File Security

- **`.gitignore`** prevents credential leaks
- **Environment files** not committed
- **Log files** contain no sensitive data
- **Data files** stored locally and in secure cloud storage

## Support

### Troubleshooting

1. **Check log files** in `scripts_notebooks/prod/logs/` directory
2. **Verify environment variables** are set correctly in `.env` file
3. **Test database connection** independently
4. **Verify launchd job** is loaded and scheduled correctly
5. **Check Google Drive sync** if using file system sync

### Common Issues

- **Database connection** failures - Check credentials in `.env` file
- **Launchd job not running** - Verify job is loaded: `launchctl list | grep mosaic`
- **File permission** problems - Ensure script has write access to data directories
- **Missing dependencies** - Run `pip install -r requirements.txt`
- **Python path issues** - Ensure virtual environment is activated or use wrapper script
- **Google Drive sync issues** - Verify Google Drive is installed and folder path is correct

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
**Last Updated**: November 2025