#!/usr/bin/env python3
"""
Phase 2: Data Transformation Script

This script reads raw supervision hours data, transforms it into the required format,
and saves the transformed data as CSV for downstream processing.

Usage:
    python transform_data.py [--input PATH] [--output PATH]
"""

import pandas as pd
import os
import logging
import argparse
from datetime import datetime


def setup_logging(log_dir: str = None) -> logging.Logger:
    """Set up logging configuration."""
    # Use root logs directory if not specified
    if log_dir is None:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        # Go up from scripts_notebooks/prod to project root
        project_root = os.path.dirname(os.path.dirname(script_dir))
        log_dir = os.path.join(project_root, 'logs')
    
    # Ensure logs directory exists
    os.makedirs(log_dir, exist_ok=True)
    
    # Create log file path
    log_file = os.path.join(log_dir, 'transform_data.log')
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)


def transform_data(df: pd.DataFrame, logger: logging.Logger) -> pd.DataFrame:
    """
    Transform the raw supervision data into the required format.
    
    Args:
        df (pd.DataFrame): Raw data from SQL query
        logger: Logger instance
        
    Returns:
        pd.DataFrame: Transformed data with Clinic, DirectProviderId, DirectProviderName, 
                     DirectHours, SupervisionHours
    """
    logger.info("="*50)
    logger.info("Phase 2: Data Transformation")
    logger.info("="*50)
    
    # Identify all supervisors (people who appear in Supervisor columns)
    supervisors = set()
    for _, row in df.iterrows():
        if pd.notna(row['SupervisorFirstName']) and pd.notna(row['SupervisorLastName']):
            supervisor_name = (row['SupervisorFirstName'], row['SupervisorLastName'])
            supervisors.add(supervisor_name)
    
    logger.info(f"Found {len(supervisors)} unique supervisors in the data")
    
    # Remove rows where direct provider matches any supervisor
    initial_count = len(df)
    df = df[~(
        df['DirectFirstName'].notna() & 
        df['DirectLastName'].notna() & 
        df.apply(lambda row: (row['DirectFirstName'], row['DirectLastName']) in supervisors, axis=1)
    )].reset_index(drop=True)
    removed_count = initial_count - len(df)
    
    if removed_count > 0:
        logger.info(f"Removed {removed_count} rows where direct provider also appears as supervisor")
    else:
        logger.info("No rows removed (no direct providers matched supervisors)")
    
    # Create direct provider name mapping
    direct_dict = {}
    for _, row in df.iterrows():
        if pd.notna(row['DirectFirstName']) and pd.notna(row['DirectLastName']):
            direct_dict[row['DirectProviderId']] = f"{row['DirectFirstName']} {row['DirectLastName']}"
    
    # Group and aggregate data
    # Include DirectServiceLocationName in groupby to keep it for later use
    transformed_df = df.groupby([
        'DirectProviderId',
        'ClientOfficeLocationName',
        'DirectServiceLocationName']).agg({
        'DirectHours': 'sum',
        'SupervisionHours': 'sum'
    }).reset_index()
    
    # Clean clinic names from ClientOfficeLocationName
    # Check if it contains 'ORGANIZATION' format, otherwise use as-is
    if transformed_df['ClientOfficeLocationName'].str.contains('ORGANIZATION', na=False).any():
        # Filter for ORGANIZATION locations and clean clinic names
        transformed_df = transformed_df[transformed_df['ClientOfficeLocationName'].str.contains('ORGANIZATION', na=False)].reset_index(drop=True)
        
        # Handle both "ORGANIZATION: " and "ORGANIZATION_" formats
        def extract_clinic_name(val):
            val_str = str(val)
            # Try "ORGANIZATION: " first (colon and space)
            if 'ORGANIZATION: ' in val_str:
                return val_str.split('ORGANIZATION: ')[1]
            # Then try "ORGANIZATION_" (underscore)
            elif 'ORGANIZATION_' in val_str:
                return val_str.split('ORGANIZATION_')[1]
            # Fallback to original value
            else:
                return val_str
        
        transformed_df['Clinic'] = transformed_df['ClientOfficeLocationName'].apply(extract_clinic_name)
        # Strip leading whitespace that might result from the split
        transformed_df['Clinic'] = transformed_df['Clinic'].str.strip()
        transformed_df['Clinic'] = [val.split('Clinic')[0] for val in transformed_df['Clinic']]
        transformed_df['Clinic'] = [val[:-1] if val.endswith(' ') else val for val in transformed_df['Clinic']]
        transformed_df['Clinic'] = [val.replace(" 8528 Unive", "") for val in transformed_df['Clinic']]
    else:
        # Use ClientOfficeLocationName directly as Clinic name
        transformed_df['Clinic'] = transformed_df['ClientOfficeLocationName']
    
    # Replace "Diagnostics" in Clinic with DirectServiceLocationName
    # Only replace if DirectServiceLocationName is not null/empty
    diagnostics_mask = (
        transformed_df['Clinic'].str.contains('Diagnostics', na=False, case=False) &
        transformed_df['DirectServiceLocationName'].notna() &
        (transformed_df['DirectServiceLocationName'].astype(str).str.strip() != '')
    )
    if diagnostics_mask.any():
        transformed_df.loc[diagnostics_mask, 'Clinic'] = transformed_df.loc[diagnostics_mask, 'DirectServiceLocationName']
        logger.info(f"Replaced 'Diagnostics' in Clinic column with DirectServiceLocationName for {diagnostics_mask.sum()} rows")
    
    # Drop the original location columns and add provider names
    transformed_df.drop(columns=['ClientOfficeLocationName', 'DirectServiceLocationName'], inplace=True)
    transformed_df['DirectProviderName'] = transformed_df['DirectProviderId'].map(direct_dict)
    
    # Reorder columns and sort
    transformed_df = transformed_df[[
        'Clinic', 'DirectProviderId', 'DirectProviderName', 
        'DirectHours', 'SupervisionHours'
    ]]
    transformed_df = transformed_df.sort_values(by=['Clinic', 'DirectProviderName'], ascending=True)
    
    logger.info(f"Data transformation completed. {len(transformed_df)} rows in transformed dataset.")
    return transformed_df


def transform_data_main(df: pd.DataFrame = None, input_file: str = None, save_file: bool = True) -> pd.DataFrame:
    """
    Main function to transform data.
    
    Args:
        df (pd.DataFrame, optional): Input DataFrame. If None, will read from input_file.
        input_file (str, optional): Input CSV file path. Used if df is None.
        save_file (bool): Whether to save file to disk. Default True.
        
    Returns:
        pd.DataFrame: Transformed DataFrame
    """
    # Set up logging
    logger = setup_logging()
    
    # Get input data
    if df is None:
        if input_file is None:
            # Default to today's file
            today = datetime.now().strftime('%Y-%m-%d')
            input_file = f'../../data/raw_pulls/daily_supervision_hours_{today}.csv'
        
        if not os.path.exists(input_file):
            logger.error(f"Input file not found: {input_file}")
            raise FileNotFoundError(f"Input file not found: {input_file}")
        
        logger.info(f"Reading raw data from: {input_file}")
        df = pd.read_csv(input_file)
        logger.info(f"Loaded {len(df)} rows from input file")
    else:
        logger.info(f"Using provided DataFrame with {len(df)} rows")
    
    # Transform data
    transformed_df = transform_data(df, logger)
    
    if save_file:
        # Save transformed data
        today = datetime.now().strftime('%Y-%m-%d')
        output_file = f'../../data/transformed_supervision_daily/daily_supervision_hours_transformed_{today}.csv'
        os.makedirs(os.path.dirname(output_file), exist_ok=True)
        transformed_df.to_csv(output_file, index=False)
        logger.info(f"Saved transformed data to: {output_file}")
    
    logger.info("="*50)
    logger.info("Data transformation completed successfully!")
    logger.info("="*50)
    
    return transformed_df


def main():
    """CLI entry point for transform_data.py"""
    parser = argparse.ArgumentParser(description='Transform raw supervision data')
    parser.add_argument('--input', type=str, 
                       default='../../data/raw_pulls/daily_supervision_hours_{date}.csv',
                       help='Input CSV file path (use {date} placeholder for today)')
    parser.add_argument('--output', type=str,
                       default='../../data/transformed_supervision_daily/daily_supervision_hours_transformed_{date}.csv',
                       help='Output CSV file path (use {date} placeholder for today)')
    
    args = parser.parse_args()
    
    try:
        today = datetime.now().strftime('%Y-%m-%d')
        input_file = args.input.format(date=today)
        transform_data_main(input_file=input_file, save_file=True)
        return 0
    except Exception as e:
        logging.error(f"Error in data transformation: {e}")
        raise


if __name__ == "__main__":
    exit(main())