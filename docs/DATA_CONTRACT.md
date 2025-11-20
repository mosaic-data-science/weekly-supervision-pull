# Data Contract / Service Level Agreement (SLA)

**Pipeline**: Daily Supervision Pull  
**Version**: 1.0  
**Last Updated**: November 2025  
**Owner**: Mosaic Data Science

---

## Overview

This document defines the data contract and service level agreement for the Daily Supervision Pull pipeline. It specifies data availability, quality, schema, and operational expectations for downstream consumers of this data.

---

## 1. Data Availability & Freshness

### 1.1 Execution Schedule
- **Frequency**: Daily
- **Execution Time**: 7:30 AM Eastern Time (ET)
- **Time Window**: Pipeline completes within 30 minutes of scheduled time
- **Coverage Period**: Month-to-date (from the 1st of the current month through yesterday)

### 1.2 Data Freshness Guarantees
- **Target Freshness**: Data is refreshed daily with up to 1-day lag (yesterday's data available by 8:00 AM ET today)
- **Maximum Acceptable Lag**: 2 business days
- **Data Completeness**: 100% of expected records for the date range

### 1.3 Availability Windows
- **Expected Availability**: 24/7 (data files available after pipeline completion)
- **Maintenance Windows**: Scheduled maintenance will be communicated 48 hours in advance
- **Unplanned Downtime**: Target < 1% monthly uptime (approximately 7.2 hours/month)

---

## 2. Data Schema & Structure

### 2.1 Output Files

#### Primary Output: Transformed Excel File
- **Location**: `data/transformed_supervision_daily/daily_supervision_hours_transformed_YYYY-MM-DD.xlsx`
- **Format**: Microsoft Excel (.xlsx)
- **Structure**: Multiple sheets organized by clinic
- **Naming Convention**: `daily_supervision_hours_transformed_YYYY-MM-DD.xlsx`

#### Raw Data Files
- **Supervision Hours**: `data/raw_pulls/daily_supervision_hours_YYYY-MM-DD.csv`
- **BACB Supervision**: `data/raw_pulls/bacb_supervision_hours_YYYY-MM-DD.csv`
- **Format**: CSV (comma-separated values)

### 2.2 Data Schema

#### Transformed Data Columns (Expected)
The transformed Excel file contains the following key columns:
- Provider identification (ID, Name)
- Clinic/Location information
- Direct service hours
- Supervision hours
- Supervision percentages
- Date ranges
- BACB supervision data (merged)

*Note: Exact column names and structure may vary. Refer to the latest output file for current schema.*

### 2.3 Schema Stability
- **Breaking Changes**: Will be communicated 30 days in advance
- **Additive Changes**: New columns may be added without notice
- **Versioning**: Schema changes will be documented in release notes

---

## 3. Data Quality Standards

### 3.1 Completeness
- **Target**: 100% of expected records present
- **Null Handling**: Null values are handled according to business logic (supervisors filtered from direct provider lists)
- **Missing Data**: Missing data will be logged and reported in error notifications

### 3.2 Accuracy
- **Data Source**: CR Database (CR DWH)
- **Validation**: Data is validated against source system queries
- **Calculations**: Supervision percentages and hour calculations are verified against business rules

### 3.3 Consistency
- **Date Ranges**: Consistent month-to-date coverage (1st of month through yesterday)
- **Formatting**: Consistent file naming and date formats (YYYY-MM-DD)
- **Aggregation**: Consistent grouping by provider, location, and clinic

### 3.4 Data Quality Monitoring
- **Row Count Validation**: Expected row counts logged and monitored
- **Anomaly Detection**: Significant deviations from expected patterns trigger alerts
- **Error Logging**: All data quality issues logged to `scripts_notebooks/prod/logs/`

---

## 4. Performance SLAs

### 4.1 Execution Time
- **Target Execution Time**: < 15 minutes for full pipeline
- **Maximum Acceptable Time**: 30 minutes
- **Timeout Threshold**: Pipeline will timeout after 60 minutes

### 4.2 Data Processing
- **Query Performance**: Database queries complete within 5 minutes
- **Transformation Time**: Data transformation completes within 5 minutes
- **File Generation**: Excel file generation completes within 5 minutes

### 4.3 Resource Usage
- **Database Load**: Queries are optimized to minimize database impact
- **File Size**: Output files typically < 50 MB
- **Storage**: Archived files retained per retention policy (see Section 6)

---

## 5. Error Handling & Notifications

### 5.1 Error Categories

#### Critical Errors (Pipeline Failure)
- Database connection failures
- SQL query execution failures
- Critical data transformation errors
- **Notification**: Immediate email notification sent

#### Warning Errors (Non-Fatal)
- Google Drive sync failures (logged but non-fatal)
- Minor data quality issues
- **Notification**: Logged for review

### 5.2 Notification Channels
- **Email Notifications**: Sent on pipeline completion (success or failure)
- **Log Files**: Detailed logs in `scripts_notebooks/prod/logs/`
  - `run_pipeline.log` - Main pipeline execution
  - `pull_data.log` - Database queries
  - `transform_data.log` - Data transformation
  - `merge_data.log` - Data merging
  - `launchd_stdout.log` - Launchd standard output
  - `launchd_stderr.log` - Launchd error output

### 5.3 Error Recovery
- **Automatic Retry**: Not currently implemented (manual intervention required)
- **Manual Rerun**: Pipeline can be manually executed to recover from failures
- **Data Recovery**: Previous day's data remains available in archived files

---

## 6. Data Retention & Archival

### 6.1 File Retention
- **Active Files**: Latest transformed file kept in main directory
- **Archived Files**: Previous files moved to `data/transformed_supervision_daily/archived/`
- **Raw Data**: Raw CSV files retained in `data/raw_pulls/`

### 6.2 Retention Period
- **Transformed Files**: Retained indefinitely (archived after new file generation)
- **Raw Files**: Retained for 90 days minimum
- **Log Files**: Retained for 30 days minimum (rotation recommended)

### 6.3 Archival Process
- **Automatic**: Previous transformed file archived before new file is saved
- **Manual Cleanup**: Old files may be manually removed per organizational policy

---

## 7. Support & Maintenance

### 7.1 Support Availability
- **Business Hours**: Monday-Friday, 9:00 AM - 5:00 PM ET
- **Emergency Support**: Available for critical pipeline failures
- **Response Time**: 
  - Critical issues: < 4 hours
  - Non-critical issues: < 2 business days

### 7.2 Maintenance Windows
- **Scheduled Maintenance**: Second Saturday of each month, 2:00 AM - 4:00 AM ET
- **Emergency Maintenance**: As needed with 24-hour notice when possible
- **Communication**: Maintenance notifications sent via email

### 7.3 Change Management
- **Code Changes**: Deployed through standard development workflow (dev → main)
- **Schema Changes**: Documented in release notes and communicated 30 days in advance
- **Configuration Changes**: Documented in README and change logs

---

## 8. Dependencies & Prerequisites

### 8.1 External Dependencies
- **CR Database (CR DWH)**: Must be accessible and operational
- **Database Credentials**: Required environment variables configured
- **Google Drive** (Optional): For file synchronization

### 8.2 System Requirements
- **Python**: 3.8 or higher
- **Operating System**: macOS (for launchd scheduling)
- **Storage**: Sufficient disk space for data files and logs
- **Network**: Database connectivity required

### 8.3 Dependency Failures
- **Database Unavailable**: Pipeline will fail with error notification
- **Network Issues**: Pipeline will fail with error notification
- **Storage Issues**: Pipeline will fail with error notification

---

## 9. Data Lineage & Processing

### 9.1 Data Sources
1. **CR Database (CR DWH)**
   - Billing entries with service codes (97153, 97155, etc.)
   - Provider information
   - Service location data
   - Client information

### 9.2 Processing Steps
1. **Phase 1: Data Pull** (`pull_data.py`)
   - Executes main supervision hours query
   - Executes BACB supervision query
   - Saves raw data CSVs

2. **Phase 2: Data Transformation** (`transform_data.py`)
   - Filters supervisors from direct provider lists
   - Groups and aggregates by provider and location
   - Extracts clinic names from service locations

3. **Phase 3: Data Merge** (`merge_data.py`)
   - Merges BACB supervision data
   - Calculates total supervision hours and percentages
   - Creates Excel output with conditional formatting
   - Organizes data by clinic in separate sheets

### 9.3 Business Logic
- **Supervision Overlap Calculation**: Complex SQL queries analyze overlaps between direct and supervision hours
- **Supervision Percentage**: Calculated for each provider
- **Clinic Extraction**: Clinic names extracted from service locations

---

## 10. Compliance & Security

### 10.1 Data Security
- **Credentials**: Stored in environment variables, not in code
- **File Access**: Data files stored locally with appropriate permissions
- **Google Drive**: Files synced to secure Google Drive location
- **Logging**: Logs contain no sensitive data

### 10.2 Data Privacy
- **PII Handling**: Data may contain provider and client information
- **Access Control**: Files accessible only to authorized personnel
- **Retention Compliance**: Follows organizational data retention policies

---

## 11. Monitoring & Observability

### 11.1 Key Metrics
- **Pipeline Success Rate**: Target > 99%
- **Execution Time**: Monitored via logs
- **Data Volume**: Row counts logged for each execution
- **Error Rate**: Tracked via log analysis

### 11.2 Monitoring Tools
- **Log Files**: Primary monitoring mechanism
- **Email Notifications**: Success/failure notifications
- **File Timestamps**: Verify pipeline execution via file modification times

### 11.3 Alerting
- **Pipeline Failures**: Email notification sent immediately
- **Performance Degradation**: Logged for review (manual monitoring)
- **Data Quality Issues**: Logged for review (manual monitoring)

---

## 12. Version History

| Version | Date | Changes | Author |
|---------|------|---------|--------|
| 1.0 | November 2025 | Initial data contract | Mosaic Data Science |

---

## 13. Contact & Escalation

### 13.1 Primary Contact
- **Team**: Mosaic Data Science
- **Repository**: https://github.com/mosaic-data-science/daily-supervision-pull

### 13.2 Escalation Path
1. Check log files in `scripts_notebooks/prod/logs/`
2. Review README.md troubleshooting section
3. Contact Mosaic Data Science team
4. For critical issues, escalate to team lead

---

## 14. Definitions & Glossary

- **CR DWH**: CR Data Warehouse (source database)
- **BACB**: Behavior Analyst Certification Board
- **ET**: Eastern Time
- **PII**: Personally Identifiable Information
- **SLA**: Service Level Agreement
- **MTD**: Month-to-Date

---

## 15. Acceptance & Sign-off

This data contract is effective as of the date of implementation and applies to all consumers of the Daily Supervision Pull pipeline data.

**Consumers should acknowledge:**
- Understanding of data availability and freshness expectations
- Awareness of schema and data structure
- Acceptance of data quality standards
- Agreement to report issues through proper channels

---

*This document is a living document and will be updated as the pipeline evolves. Consumers will be notified of significant changes.*
