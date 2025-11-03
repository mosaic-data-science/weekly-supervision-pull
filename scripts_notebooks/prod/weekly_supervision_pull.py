#!/usr/bin/env python3
"""
Weekly Supervision Hours Pull Script

This script extracts supervision hours data from the CR database, transforms it,
and uploads both raw and transformed data to Google Drive with proper archiving.

Author: Generated from dev notebook
Date: 2025-01-27
"""

import pandas as pd
import pyodbc
import os
import shutil
import logging
import re
from datetime import datetime, timedelta
from dotenv import load_dotenv
import io
import json
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
import google.oauth2.credentials as oauth2_creds
from typing import Optional, List, Dict, Any, Tuple
from sql_queries import SUPERVISION_HOURS_SQL_TEMPLATE, BACB_SUPERVISION_TEMPLATE
from pull_data import pull_data_main
from transform_data import transform_data_main
from merge_data import merge_data_main


class WeeklySupervisionPull:
    """
    Main class for handling weekly supervision hours data extraction and processing.
    """
    
    def __init__(self, config_file: str = None):
        """
        Initialize the WeeklySupervisionPull class.
        
        Args:
            config_file (str, optional): Path to configuration file. Defaults to None.
        """
        # Load environment variables
        load_dotenv()
        
        # Set up logging
        self._setup_logging()
        
        # Database connection parameters
        self.server = os.getenv('CR_DWH_SERVER')
        self.username = os.getenv('CR_UN')
        self.password = os.getenv('CR_PW')
        
        # Google Drive configuration
        self.raw_folder_id = os.getenv('RAW_FOLDER_ID')
        self.transformed_folder_id = os.getenv('TRANSFORMED_FOLDER_ID')
        self.transformed_archive_id = os.getenv('TRANSFORMED_ARCHIVE_ID')
        
        # File naming
        self.raw_filename = f'weekly_supervision_hours_{datetime.now().strftime("%Y-%m-%d")}.csv'
        self.transformed_filename = f'weekly_supervision_hours_transformed_{datetime.now().strftime("%Y-%m-%d")}.csv'
        
        # SQL query template (imported from sql_queries.py)
        self.sql_template = SUPERVISION_HOURS_SQL_TEMPLATE
        
        # Local file paths
        self.raw_folder = '../../data/raw_pulls'
        self.transformed_folder = '../../data/transformed_supervision_weekly'
        self.archive_folder = f'{self.transformed_folder}/archived'
        
        # Credentials paths
        self.credentials_dir = '../../credentials'
        self.client_secret_path = os.path.join(self.credentials_dir, 'client_secret.json')
        self.token_path = os.path.join(self.credentials_dir, 'token.json')
        
        self.logger.info("WeeklySupervisionPull initialized successfully")
    
    def _get_latest_date_from_files(self) -> Optional[str]:
        """
        Get the latest date from existing files in the raw_pulls folder.
        
        Returns:
            str: Latest date found in YYYY-MM-DD format, or None if no files found
        """
        try:
            if not os.path.exists(self.raw_folder):
                self.logger.info("Raw pulls folder does not exist, using default date")
                return None
            
            # Get all CSV files in the raw_pulls folder
            csv_files = [f for f in os.listdir(self.raw_folder) if f.endswith('.csv')]
            
            if not csv_files:
                self.logger.info("No CSV files found in raw_pulls folder, using default date")
                return None
            
            # Extract dates from filenames using regex pattern
            # Pattern matches: weekly_supervision_hours_YYYY-MM-DD.csv
            date_pattern = r'(\d{4}-\d{2}-\d{2})'
            dates = []
            
            for filename in csv_files:
                match = re.search(date_pattern, filename)
                if match:
                    dates.append(match.group(1))
            
            if not dates:
                self.logger.info("No dates found in filenames, using default date")
                return None
            
            # Find the latest date
            latest_date = max(dates)
            self.logger.info(f"Found latest date from existing files: {latest_date}")
            return latest_date
            
        except Exception as e:
            self.logger.error(f"Error getting latest date from files: {e}")
            return None
    
    def _setup_logging(self) -> None:
        """Set up logging configuration."""
        # Ensure logs directory exists
        logs_dir = os.path.join(os.path.dirname(__file__), 'logs')
        os.makedirs(logs_dir, exist_ok=True)
        
        # Create log file path
        log_file = os.path.join(logs_dir, 'weekly_supervision_pull.log')
        
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
    
    
    def get_oauth_service(self):
        """
        Get authenticated Google Drive service.
        
        Returns:
            Google Drive service object
        """
        creds = None
        if os.path.exists(self.token_path):
            creds = oauth2_creds.Credentials.from_authorized_user_file(self.token_path)
        
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                if not os.path.exists(self.client_secret_path):
                    raise FileNotFoundError(f"Client secret file not found at {self.client_secret_path}")
                
                flow = InstalledAppFlow.from_client_secrets_file(
                    self.client_secret_path, 
                    ["https://www.googleapis.com/auth/drive"]
                )
                creds = flow.run_local_server(port=0)
            
            # Save token to credentials directory
            with open(self.token_path, "w") as f:
                f.write(creds.to_json())
        
        return build("drive", "v3", credentials=creds)
    
    def upload_dataframe_create_only(self, df: pd.DataFrame, folder_id: str, filename: str, service) -> Dict[str, Any]:
        """
        Upload a DataFrame as CSV to Google Drive.
        
        Args:
            df (pd.DataFrame): DataFrame to upload
            folder_id (str): Google Drive folder ID
            filename (str): Name for the uploaded file
            service: Google Drive service object
            
        Returns:
            Dict containing file information
        """
        csv_bytes = df.to_csv(index=False).encode("utf-8")
        media = MediaIoBaseUpload(io.BytesIO(csv_bytes), mimetype="text/csv", resumable=True)
        metadata = {"name": filename, "parents": [folder_id]}
        
        created = service.files().create(
            body=metadata,
            media_body=media,
            supportsAllDrives=True,
        ).execute()
        return created
    
    def archive_existing_files_in_folder(self, service, source_folder_id: str, archive_folder_id: str, exclude_filename: str = None) -> List[str]:
        """
        Move all files from source folder to archive folder, excluding the specified filename.
        
        Args:
            service: Google Drive service object
            source_folder_id (str): ID of source folder
            archive_folder_id (str): ID of archive folder
            exclude_filename (str, optional): Filename to exclude from archiving
            
        Returns:
            List of moved filenames
        """
        moved_files = []
        try:
            # List files in the source folder
            results = service.files().list(
                q=f"'{source_folder_id}' in parents and trashed=false",
                supportsAllDrives=True,
                fields="files(id, name)"
            ).execute()
            
            files_to_move = results.get("files", [])
            
            # Filter out the exclude filename if specified
            if exclude_filename:
                files_to_move = [f for f in files_to_move if f['name'] != exclude_filename]
            
            # Move each file to archive folder
            for file in files_to_move:
                try:
                    # Move file to archive folder
                    service.files().update(
                        fileId=file['id'],
                        addParents=archive_folder_id,
                        removeParents=source_folder_id,
                        supportsAllDrives=True
                    ).execute()
                    
                    moved_files.append(file['name'])
                    self.logger.info(f"Moved to archive: {file['name']}")
                    
                except Exception as e:
                    self.logger.error(f"Error moving {file['name']}: {e}")
                    
        except Exception as e:
            self.logger.error(f"Error listing files in source folder: {e}")
            
        return moved_files
    
    def execute_sql_query(self, start_date: str) -> pd.DataFrame:
        """
        Execute the SQL query and return results as DataFrame.
        
        Args:
            start_date (str): Start date for the query in YYYY-MM-DD format
            
        Returns:
            pd.DataFrame: Query results
        """
        # Calculate end date (tomorrow to include all of today)
        end_date = (datetime.now() + timedelta(days=1)).strftime('%Y-%m-%d')
        
        # Format the SQL query with the start and end dates
        sql_query = self.sql_template.format(start_date=start_date, end_date=end_date)
        
        self.logger.info(f"Executing SQL query with start_date: {start_date}, end_date: {end_date}")
        
        # Try ODBC Driver 17 first (for collaborator compatibility), then fall back to other options
        drivers_to_try = [
            ('ODBC Driver 17 for SQL Server', ''),
            ('ODBC Driver 18 for SQL Server', 'TrustServerCertificate=yes'),
            ('SQL Server', ''),
            ('ODBC Driver 18 for SQL Server', 'TrustServerCertificate=yes;Encrypt=no')
        ]
        
        for driver, extra_params in drivers_to_try:
            try:
                # Build connection string with optional extra parameters
                conn_str = f'DRIVER={{{driver}}};SERVER={self.server};DATABASE=insights;UID={self.username};PWD={self.password}'
                if extra_params:
                    conn_str += f';{extra_params}'
                
                self.logger.info(f"Attempting connection with {driver}")
                conn = pyodbc.connect(conn_str)
                df = pd.read_sql(sql_query, conn)
                conn.close()
                self.logger.info(f"Query executed successfully with {driver}. Retrieved {len(df)} rows.")
                return df
            except Exception as e:
                self.logger.warning(f"Failed to connect with {driver}: {e}")
                # Check if this is the last driver in the list
                current_index = drivers_to_try.index((driver, extra_params))
                if current_index == len(drivers_to_try) - 1:  # If this is the last driver to try
                    self.logger.error(f"All ODBC drivers failed. Last error: {e}")
                    raise
                else:
                    self.logger.info(f"Trying next driver...")
                    continue
    
    def execute_bacb_query(self, start_date: str) -> pd.DataFrame:
        """
        Execute the BACB supervision query and return results as DataFrame.
        
        Args:
            start_date (str): Start date for the query in YYYY-MM-DD format
            
        Returns:
            pd.DataFrame: Query results with ProviderContactId, BACBSupervisionCodes_binary, BACBSupervisionHours
        """
        # Calculate end date (tomorrow to include all of today)
        end_date = (datetime.now() + timedelta(days=1)).strftime('%Y-%m-%d')
        
        # Format the SQL query with the start and end dates
        sql_query = BACB_SUPERVISION_TEMPLATE.format(start_date=start_date, end_date=end_date)
        
        self.logger.info(f"Executing BACB supervision query with start_date: {start_date}, end_date: {end_date}")
        
        # Try ODBC Driver 17 first (for collaborator compatibility), then fall back to other options
        drivers_to_try = [
            ('ODBC Driver 17 for SQL Server', ''),
            ('ODBC Driver 18 for SQL Server', 'TrustServerCertificate=yes'),
            ('SQL Server', ''),
            ('ODBC Driver 18 for SQL Server', 'TrustServerCertificate=yes;Encrypt=no')
        ]
        
        for driver, extra_params in drivers_to_try:
            try:
                # Build connection string with optional extra parameters
                conn_str = f'DRIVER={{{driver}}};SERVER={self.server};DATABASE=insights;UID={self.username};PWD={self.password}'
                if extra_params:
                    conn_str += f';{extra_params}'
                
                self.logger.info(f"Attempting BACB query connection with {driver}")
                conn = pyodbc.connect(conn_str)
                df = pd.read_sql(sql_query, conn)
                conn.close()
                self.logger.info(f"BACB query executed successfully with {driver}. Retrieved {len(df)} rows.")
                return df
            except Exception as e:
                self.logger.warning(f"Failed to connect with {driver}: {e}")
                # Check if this is the last driver in the list
                current_index = drivers_to_try.index((driver, extra_params))
                if current_index == len(drivers_to_try) - 1:  # If this is the last driver to try
                    self.logger.error(f"All ODBC drivers failed for BACB query. Last error: {e}")
                    raise
                else:
                    self.logger.info(f"Trying next driver...")
                    continue
    
    def pull_data(self, start_date: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Phase 1: Pull all data from the database.
        
        Executes both the main supervision hours query and the BACB supervision query.
        
        Args:
            start_date (str): Start date for the queries in YYYY-MM-DD format
            
        Returns:
            tuple[pd.DataFrame, pd.DataFrame]: Tuple of (raw_supervision_df, bacb_df)
        """
        self.logger.info("="*50)
        self.logger.info("Phase 1: Data Pulls")
        self.logger.info("="*50)
        
        # Execute main supervision SQL query
        self.logger.info("Pulling main supervision hours data...")
        df = self.execute_sql_query(start_date)
        
        # Execute BACB supervision query
        self.logger.info("Pulling BACB supervision data...")
        bacb_df = self.execute_bacb_query(start_date)
        
        self.logger.info(f"Data pull completed. Supervision: {len(df)} rows, BACB: {len(bacb_df)} rows")
        return df, bacb_df
    
    def transform_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Phase 2: Transform the raw supervision data into the required format.
        
        Args:
            df (pd.DataFrame): Raw data from SQL query
            
        Returns:
            pd.DataFrame: Transformed data with Clinic, DirectProviderId, DirectProviderName, 
                         DirectHours, SupervisionHours, PctOfDirectHoursSupervised
        """
        self.logger.info("="*50)
        self.logger.info("Phase 2: Data Transformation")
        self.logger.info("="*50)
        
        # Create direct provider name mapping
        direct_dict = {}
        for _, row in df.iterrows():
            if pd.notna(row['DirectFirstName']) and pd.notna(row['DirectLastName']):
                direct_dict[row['DirectProviderId']] = f"{row['DirectFirstName']} {row['DirectLastName']}"
        
        # Group and aggregate data
        transformed_df = df.groupby([
            'DirectProviderId',
            'DirectServiceLocationName']).agg({
            'DirectHours': 'sum',
            'SupervisionHours': 'sum'
        }).reset_index()
        
        # Calculate percentage of direct hours supervised
        transformed_df['PctOfDirectHoursSupervised'] = round(
            100 * (transformed_df['SupervisionHours'] / transformed_df['DirectHours']), 2
        )
        
        # Filter for ORGANIZATION locations and clean clinic names
        transformed_df = transformed_df[transformed_df['DirectServiceLocationName'].str.contains('ORGANIZATION')].reset_index(drop=True)
        transformed_df['Clinic'] = [val.split('ORGANIZATION: ')[1] for val in transformed_df['DirectServiceLocationName']]
        transformed_df['Clinic'] = [val.split('Clinic')[0] for val in transformed_df['Clinic']]
        transformed_df['Clinic'] = [val[:-1] for val in transformed_df['Clinic']]
        transformed_df['Clinic'] = [val.replace(" 8528 Unive", "") for val in transformed_df['Clinic']]
        
        # Drop the original location column and add provider names
        transformed_df.drop(columns=['DirectServiceLocationName'], inplace=True)
        transformed_df['DirectProviderName'] = transformed_df['DirectProviderId'].map(direct_dict)
        
        # Reorder columns and sort
        transformed_df = transformed_df[[
            'Clinic', 'DirectProviderId', 'DirectProviderName', 
            'DirectHours', 'SupervisionHours', 'PctOfDirectHoursSupervised'
        ]]
        transformed_df = transformed_df.sort_values(by=['Clinic', 'DirectProviderName'], ascending=True)
        
        self.logger.info(f"Data transformation completed. {len(transformed_df)} rows in transformed dataset.")
        return transformed_df
    
    def merge_data(self, transformed_df: pd.DataFrame, bacb_df: pd.DataFrame) -> pd.DataFrame:
        """
        Phase 3: Merge BACB supervision data with transformed supervision data.
        
        Joins BACB data onto the transformed DataFrame using DirectProviderId.
        Handles missing values for providers without BACB supervision.
        
        Args:
            transformed_df (pd.DataFrame): Transformed supervision data
            bacb_df (pd.DataFrame): BACB supervision data with ProviderContactId
            
        Returns:
            pd.DataFrame: Final merged DataFrame with all columns including BACB data
        """
        self.logger.info("="*50)
        self.logger.info("Phase 3: Data Merge")
        self.logger.info("="*50)
        
        # Join BACB data to transformed DataFrame
        self.logger.info("Merging BACB supervision data...")
        merged_df = transformed_df.merge(
            bacb_df,
            left_on='DirectProviderId',
            right_on='ProviderContactId',
            how='left'
        )
        
        # Fill NaN values for BACB columns (providers without BACB supervision)
        merged_df['BACBSupervisionCodes_binary'] = merged_df['BACBSupervisionCodes_binary'].fillna(0).astype(int)
        merged_df['BACBSupervisionHours'] = merged_df['BACBSupervisionHours'].fillna(0.0)
        
        # Drop the ProviderContactId column since we're using DirectProviderId
        merged_df.drop(columns=['ProviderContactId'], inplace=True, errors='ignore')
        
        # Reorder columns to include BACB data
        column_order = [
            'Clinic', 'DirectProviderId', 'DirectProviderName', 
            'DirectHours', 'SupervisionHours', 'PctOfDirectHoursSupervised',
            'BACBSupervisionCodes_binary', 'BACBSupervisionHours'
        ]
        # Only include columns that exist
        column_order = [col for col in column_order if col in merged_df.columns]
        merged_df = merged_df[column_order]
        
        self.logger.info(f"Data merge completed. Final dataset has {len(merged_df)} rows with BACB columns.")
        return merged_df
    
    def archive_local_files(self) -> None:
        """Archive existing local files before saving new ones."""
        # Ensure archive folder exists
        os.makedirs(self.archive_folder, exist_ok=True)
        
        # Check for existing CSV files in the main folder
        existing_files = [f for f in os.listdir(self.transformed_folder) 
                         if f.endswith('.csv') and f != self.transformed_filename]
        
        # Move existing files to archive folder
        for file in existing_files:
            source_path = os.path.join(self.transformed_folder, file)
            archive_path = os.path.join(self.archive_folder, file)
            
            # If file already exists in archive, add timestamp to avoid conflicts
            if os.path.exists(archive_path):
                name, ext = os.path.splitext(file)
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                archive_path = os.path.join(self.archive_folder, f'{name}_{timestamp}{ext}')
            
            shutil.move(source_path, archive_path)
            self.logger.info(f"Moved {file} to archive folder")
    
    def save_local_files(self, df: pd.DataFrame, transformed_df: pd.DataFrame) -> None:
        """
        Save both raw and transformed data locally.
        
        Args:
            df (pd.DataFrame): Raw data
            transformed_df (pd.DataFrame): Transformed data
        """
        # Archive previous files
        self.archive_local_files()
        
        # Save raw data
        os.makedirs(self.raw_folder, exist_ok=True)
        df.to_csv(f'{self.raw_folder}/{self.raw_filename}', index=False)
        self.logger.info(f"Saved raw data: {self.raw_filename}")
        
        # Save transformed data
        os.makedirs(self.transformed_folder, exist_ok=True)
        transformed_df.to_csv(f'{self.transformed_folder}/{self.transformed_filename}', index=False)
        self.logger.info(f"Saved transformed data: {self.transformed_filename}")
    
    def upload_to_google_drive(self, df: pd.DataFrame, transformed_df: pd.DataFrame) -> None:
        """
        Upload both raw and transformed data to Google Drive.
        
        Args:
            df (pd.DataFrame): Raw data
            transformed_df (pd.DataFrame): Transformed data
        """
        try:
            # Get Google Drive service
            service = self.get_oauth_service()
            
            # Upload raw file
            self.logger.info("Uploading raw file to Google Drive...")
            raw_file_info = self.upload_dataframe_create_only(df, self.raw_folder_id, self.raw_filename, service)
            self.logger.info(f"Created raw file: {raw_file_info['name']}")
            
            # Archive existing files in transformed folder before uploading new one
            self.logger.info("Archiving existing files in transformed folder...")
            archived_files = self.archive_existing_files_in_folder(
                service, 
                self.transformed_folder_id, 
                self.transformed_archive_id, 
                self.transformed_filename
            )
            
            if archived_files:
                self.logger.info(f"Archived {len(archived_files)} files: {', '.join(archived_files)}")
            else:
                self.logger.info("No files to archive")
            
            # Upload new transformed file
            self.logger.info(f"Uploading new transformed file: {self.transformed_filename}")
            transformed_file_info = self.upload_dataframe_create_only(
                transformed_df, self.transformed_folder_id, self.transformed_filename, service
            )
            self.logger.info(f"Created transformed file: {transformed_file_info['name']}")
            
        except Exception as e:
            self.logger.error(f"Error uploading to Google Drive: {e}")
            raise
    
    def run(self, start_date: str = None) -> None:
        """
        Main execution method.
        
        Args:
            start_date (str, optional): Start date for data extraction. 
                                      If None, will use latest date from existing files or 7 days ago.
        """
        try:
            # Determine start date
            if start_date is None:
                # Try to get the latest date from existing files
                latest_file_date = self._get_latest_date_from_files()
                if latest_file_date:
                    start_date = latest_file_date
                    self.logger.info(f"Using latest date from existing files: {start_date}")
                else:
                    # Fallback to 7 days ago if no files found
                    start_date = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
                    self.logger.info(f"No existing files found, using default date: {start_date}")
            
            self.logger.info("="*50)
            self.logger.info("Starting Weekly Supervision Hours Pull")
            self.logger.info(f"Start date: {start_date}")
            self.logger.info("="*50)
            
            # Phase 1: Pull all data from database (using separate script)
            self.logger.info("Calling pull_data.py...")
            raw_df, bacb_df = pull_data_main(start_date=start_date, save_files=True)
            
            # Phase 2: Transform raw supervision data (using separate script)
            self.logger.info("Calling transform_data.py...")
            transformed_df = transform_data_main(df=raw_df, save_file=True)
            
            # Phase 3: Merge BACB data with transformed data (using separate script)
            self.logger.info("Calling merge_data.py...")
            final_df = merge_data_main(transformed_df=transformed_df, bacb_df=bacb_df, save_file=True)
            
            # Save files locally
            self.save_local_files(raw_df, final_df)
            
            # Upload to Google Drive
            self.upload_to_google_drive(raw_df, final_df)
            
            self.logger.info("="*50)
            self.logger.info("Weekly Supervision Hours Pull completed successfully!")
            self.logger.info("="*50)
            
        except Exception as e:
            self.logger.error(f"Error in main execution: {e}")
            raise


def main():
    """Main function to run the weekly supervision pull."""
    try:
        # Initialize the pull processor
        pull_processor = WeeklySupervisionPull()
        
        # Run the process
        pull_processor.run()
        
    except Exception as e:
        print(f"Fatal error: {e}")
        return 1
    
    return 0


if __name__ == "__main__":
    exit(main())
