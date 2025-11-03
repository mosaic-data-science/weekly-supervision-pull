#!/usr/bin/env python3
"""
Phase 3: Data Merge Script

This script merges BACB supervision data with transformed supervision data,
creates the final output file with all columns including BACB data.

Usage:
    python merge_data.py [--transformed-input PATH] [--bacb-input PATH] [--output PATH]

Author: Generated from weekly_supervision_pull.py
Date: 2025-01-27
"""

import pandas as pd
import os
import shutil
import logging
import argparse
from datetime import datetime


def setup_logging(log_dir: str = 'logs') -> logging.Logger:
    """Set up logging configuration."""
    # Ensure logs directory exists
    os.makedirs(log_dir, exist_ok=True)
    
    # Create log file path
    log_file = os.path.join(log_dir, 'merge_data.log')
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)


def merge_data(transformed_df: pd.DataFrame, bacb_df: pd.DataFrame, logger: logging.Logger) -> pd.DataFrame:
    """
    Merge BACB supervision data with transformed supervision data.
    
    Joins BACB data onto the transformed DataFrame using DirectProviderId.
    Handles missing values for providers without BACB supervision.
    
    Args:
        transformed_df (pd.DataFrame): Transformed supervision data
        bacb_df (pd.DataFrame): BACB supervision data with ProviderContactId
        logger: Logger instance
        
    Returns:
        pd.DataFrame: Final merged DataFrame with all columns including BACB data
    """
    logger.info("="*50)
    logger.info("Phase 3: Data Merge")
    logger.info("="*50)
    
    # Join BACB data to transformed DataFrame
    logger.info("Merging BACB supervision data...")
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
    
    logger.info(f"Data merge completed. Final dataset has {len(merged_df)} rows with BACB columns.")
    return merged_df


def merge_data_main(transformed_df: pd.DataFrame = None, bacb_df: pd.DataFrame = None,
                   transformed_file: str = None, bacb_file: str = None, save_file: bool = True) -> pd.DataFrame:
    """
    Main function to merge data.
    
    Args:
        transformed_df (pd.DataFrame, optional): Transformed DataFrame. If None, will read from transformed_file.
        bacb_df (pd.DataFrame, optional): BACB DataFrame. If None, will read from bacb_file.
        transformed_file (str, optional): Transformed CSV file path. Used if transformed_df is None.
        bacb_file (str, optional): BACB CSV file path. Used if bacb_df is None.
        save_file (bool): Whether to save file to disk. Default True.
        
    Returns:
        pd.DataFrame: Final merged DataFrame
    """
    # Set up logging
    logger = setup_logging()
    
    # Get transformed data
    if transformed_df is None:
        if transformed_file is None:
            today = datetime.now().strftime('%Y-%m-%d')
            transformed_file = f'../../data/transformed_supervision_weekly/weekly_supervision_hours_transformed_{today}.csv'
        
        if not os.path.exists(transformed_file):
            logger.error(f"Transformed input file not found: {transformed_file}")
            raise FileNotFoundError(f"Transformed input file not found: {transformed_file}")
        
        logger.info(f"Reading transformed data from: {transformed_file}")
        transformed_df = pd.read_csv(transformed_file)
        logger.info(f"Loaded {len(transformed_df)} rows from transformed file")
    else:
        logger.info(f"Using provided transformed DataFrame with {len(transformed_df)} rows")
    
    # Get BACB data
    if bacb_df is None:
        if bacb_file is None:
            today = datetime.now().strftime('%Y-%m-%d')
            bacb_file = f'../../data/raw_pulls/bacb_supervision_hours_{today}.csv'
        
        if not os.path.exists(bacb_file):
            logger.error(f"BACB input file not found: {bacb_file}")
            raise FileNotFoundError(f"BACB input file not found: {bacb_file}")
        
        logger.info(f"Reading BACB data from: {bacb_file}")
        bacb_df = pd.read_csv(bacb_file)
        logger.info(f"Loaded {len(bacb_df)} rows from BACB file")
    else:
        logger.info(f"Using provided BACB DataFrame with {len(bacb_df)} rows")
    
    # Merge data
    final_df = merge_data(transformed_df, bacb_df, logger)
    
    if save_file:
        # Archive existing files before saving new one
        today = datetime.now().strftime('%Y-%m-%d')
        output_file = f'../../data/transformed_supervision_weekly/weekly_supervision_hours_transformed_{today}.csv'
        archive_folder = f'../../data/transformed_supervision_weekly/archived'
        
        # Ensure directories exist
        os.makedirs(os.path.dirname(output_file), exist_ok=True)
        os.makedirs(archive_folder, exist_ok=True)
        
        # Archive existing CSV files (excluding the one we're about to create)
        output_filename = os.path.basename(output_file)
        if os.path.exists(os.path.dirname(output_file)):
            existing_files = [f for f in os.listdir(os.path.dirname(output_file)) 
                            if f.endswith('.csv') and f != output_filename]
            
            for file in existing_files:
                source_path = os.path.join(os.path.dirname(output_file), file)
                archive_path = os.path.join(archive_folder, file)
                
                # If file already exists in archive, add timestamp to avoid conflicts
                if os.path.exists(archive_path):
                    name, ext = os.path.splitext(file)
                    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                    archive_path = os.path.join(archive_folder, f'{name}_{timestamp}{ext}')
                
                shutil.move(source_path, archive_path)
                logger.info(f"Archived existing file: {file}")
        
        # Save final data
        final_df.to_csv(output_file, index=False)
        logger.info(f"Saved final merged data to: {output_file}")
    
    logger.info("="*50)
    logger.info("Data merge completed successfully!")
    logger.info("="*50)
    
    return final_df


def main():
    """CLI entry point for merge_data.py"""
    parser = argparse.ArgumentParser(description='Merge transformed and BACB supervision data')
    parser.add_argument('--transformed-input', type=str,
                       default='../../data/transformed_supervision_weekly/weekly_supervision_hours_transformed_{date}.csv',
                       help='Input CSV file path for transformed data (use {date} placeholder)')
    parser.add_argument('--bacb-input', type=str,
                       default='../../data/raw_pulls/bacb_supervision_hours_{date}.csv',
                       help='Input CSV file path for BACB data (use {date} placeholder)')
    parser.add_argument('--output', type=str,
                       default='../../data/transformed_supervision_weekly/weekly_supervision_hours_transformed_{date}.csv',
                       help='Output CSV file path (use {date} placeholder)')
    
    args = parser.parse_args()
    
    try:
        today = datetime.now().strftime('%Y-%m-%d')
        transformed_file = args.transformed_input.format(date=today)
        bacb_file = args.bacb_input.format(date=today)
        merge_data_main(transformed_file=transformed_file, bacb_file=bacb_file, save_file=True)
        return 0
    except Exception as e:
        logging.error(f"Error in data merge: {e}")
        raise


if __name__ == "__main__":
    exit(main())