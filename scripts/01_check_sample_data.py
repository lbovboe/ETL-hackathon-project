#!/usr/bin/env python3
"""
ETL Pipeline - Sample Data Checker
===================================
This script reads and validates the daily spending sample data from parquet files
using chunking techniques for memory-efficient processing.

Features:
- Memory-efficient chunking with pyarrow
- Data quality checks and validation
- Summary statistics and insights
- Handles large datasets gracefully

Author: ETL Hackathon Team
Date: 2025-10-18
"""

import os
import sys
from pathlib import Path
from typing import Dict, Any, Optional
from decimal import Decimal
import logging

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import numpy as np

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class ParquetDataChecker:
    """
    A class to check and validate parquet files using chunking techniques.
    
    Attributes:
        file_path (Path): Path to the parquet file
        chunk_size (int): Number of rows to process per chunk
    """
    
    def __init__(self, file_path: str, chunk_size: int = 10000):
        """
        Initialize the ParquetDataChecker.
        
        Args:
            file_path (str): Path to the parquet file
            chunk_size (int): Number of rows to process per chunk (default: 10000)
        """
        self.file_path = Path(file_path)
        self.chunk_size = chunk_size
        self.parquet_file = None
        
        if not self.file_path.exists():
            raise FileNotFoundError(f"Parquet file not found: {self.file_path}")
        
        logger.info(f"Initialized ParquetDataChecker for: {self.file_path}")
    
    def get_file_metadata(self) -> Dict[str, Any]:
        """
        Get metadata information about the parquet file.
        
        Returns:
            Dict containing file metadata
        """
        logger.info("Reading parquet file metadata...")
        
        parquet_file = pq.ParquetFile(self.file_path)
        
        metadata = {
            'file_path': str(self.file_path),
            'file_size_mb': self.file_path.stat().st_size / (1024 * 1024),
            'num_row_groups': parquet_file.num_row_groups,
            'total_rows': parquet_file.metadata.num_rows,
            'num_columns': parquet_file.metadata.num_columns,
            'schema': parquet_file.schema,
            'column_names': parquet_file.schema.names,
        }
        
        logger.info(f"File contains {metadata['total_rows']:,} rows and {metadata['num_columns']} columns")
        logger.info(f"File size: {metadata['file_size_mb']:.2f} MB")
        
        return metadata
    
    def read_in_chunks(self, columns: Optional[list] = None):
        """
        Generator to read parquet file in chunks using pyarrow.
        
        Args:
            columns (list, optional): List of columns to read. If None, reads all columns.
            
        Yields:
            pd.DataFrame: Chunk of data
        """
        logger.info(f"Reading parquet file in chunks of {self.chunk_size:,} rows...")
        
        parquet_file = pq.ParquetFile(self.file_path)
        
        for batch in parquet_file.iter_batches(batch_size=self.chunk_size, columns=columns):
            # Convert pyarrow Table to pandas DataFrame
            df_chunk = batch.to_pandas()
            yield df_chunk
    
    def validate_data_quality(self) -> Dict[str, Any]:
        """
        Perform data quality checks on the parquet file using chunking.
        
        Returns:
            Dict containing data quality metrics
        """
        logger.info("Starting data quality validation...")
        
        # Initialize counters
        total_rows = 0
        null_counts = {}
        column_types = {}
        numeric_stats = {}
        
        # Process data in chunks
        for chunk_num, df_chunk in enumerate(self.read_in_chunks(), start=1):
            total_rows += len(df_chunk)
            
            # Track null values
            for column in df_chunk.columns:
                if column not in null_counts:
                    null_counts[column] = 0
                    column_types[column] = str(df_chunk[column].dtype)
                
                null_counts[column] += df_chunk[column].isnull().sum()
                
                # Calculate numeric statistics
                if pd.api.types.is_numeric_dtype(df_chunk[column]):
                    if column not in numeric_stats:
                        numeric_stats[column] = {
                            'min': float('inf'),
                            'max': float('-inf'),
                            'sum': 0,
                            'count': 0
                        }
                    
                    valid_values = df_chunk[column].dropna()
                    if len(valid_values) > 0:
                        numeric_stats[column]['min'] = min(
                            numeric_stats[column]['min'], 
                            valid_values.min()
                        )
                        numeric_stats[column]['max'] = max(
                            numeric_stats[column]['max'], 
                            valid_values.max()
                        )
                        numeric_stats[column]['sum'] += valid_values.sum()
                        numeric_stats[column]['count'] += len(valid_values)
            
            if chunk_num % 10 == 0:
                logger.info(f"Processed {chunk_num} chunks ({total_rows:,} rows)...")
        
        # Calculate final statistics
        null_percentages = {
            col: (count / total_rows * 100) if total_rows > 0 else 0
            for col, count in null_counts.items()
        }
        
        for column in numeric_stats:
            if numeric_stats[column]['count'] > 0:
                numeric_stats[column]['mean'] = (
                    numeric_stats[column]['sum'] / numeric_stats[column]['count']
                )
        
        quality_report = {
            'total_rows': total_rows,
            'total_columns': len(column_types),
            'column_types': column_types,
            'null_counts': null_counts,
            'null_percentages': null_percentages,
            'numeric_statistics': numeric_stats
        }
        
        logger.info(f"Data quality validation complete. Total rows processed: {total_rows:,}")
        
        return quality_report
    
    def display_sample_data(self, n_rows: int = 10):
        """
        Display sample rows from the parquet file.
        
        Args:
            n_rows (int): Number of rows to display (default: 10)
        """
        logger.info(f"Reading first {n_rows} rows...")
        
        parquet_file = pq.ParquetFile(self.file_path)
        
        # Read only the first batch
        first_batch = next(parquet_file.iter_batches(batch_size=n_rows))
        df_sample = first_batch.to_pandas()
        
        print("\n" + "="*80)
        print(f"SAMPLE DATA (First {len(df_sample)} rows)")
        print("="*80)
        print(df_sample)
        print("="*80 + "\n")
        
        return df_sample
    
    def print_quality_report(self, quality_report: Dict[str, Any]):
        """
        Print a formatted data quality report.
        
        Args:
            quality_report (Dict): Quality report dictionary
        """
        print("\n" + "="*80)
        print("DATA QUALITY REPORT")
        print("="*80)
        print(f"Total Rows: {quality_report['total_rows']:,}")
        print(f"Total Columns: {quality_report['total_columns']}")
        print("\n" + "-"*80)
        print("COLUMN INFORMATION")
        print("-"*80)
        
        for col, dtype in quality_report['column_types'].items():
            null_count = quality_report['null_counts'][col]
            null_pct = quality_report['null_percentages'][col]
            print(f"  {col:30s} | Type: {dtype:15s} | Nulls: {null_count:8,} ({null_pct:5.2f}%)")
        
        if quality_report['numeric_statistics']:
            print("\n" + "-"*80)
            print("NUMERIC COLUMN STATISTICS")
            print("-"*80)
            
            for col, stats in quality_report['numeric_statistics'].items():
                print(f"\n  {col}:")
                print(f"    Min:   {stats['min']:,.2f}")
                print(f"    Max:   {stats['max']:,.2f}")
                print(f"    Mean:  {stats['mean']:,.2f}")
                print(f"    Count: {stats['count']:,}")
        
        print("\n" + "="*80 + "\n")
    
    def check_data_integrity(self) -> Dict[str, Any]:
        """
        Perform specific data integrity checks for financial data.
        
        Returns:
            Dict containing integrity check results
        """
        logger.info("Performing data integrity checks...")
        
        integrity_issues = {
            'negative_amounts': 0,
            'duplicate_rows': 0,
            'invalid_dates': 0,
            'outliers': []
        }
        
        seen_rows = set()
        
        for df_chunk in self.read_in_chunks():
            # Check for negative amounts (if amount columns exist)
            amount_columns = [col for col in df_chunk.columns if 'amount' in col.lower()]
            for col in amount_columns:
                if pd.api.types.is_numeric_dtype(df_chunk[col]):
                    negative_count = (df_chunk[col] < 0).sum()
                    integrity_issues['negative_amounts'] += negative_count
            
            # Check for duplicates (using hash of row values)
            for idx, row in df_chunk.iterrows():
                row_hash = hash(tuple(row))
                if row_hash in seen_rows:
                    integrity_issues['duplicate_rows'] += 1
                else:
                    seen_rows.add(row_hash)
            
            # Check for invalid dates
            date_columns = [col for col in df_chunk.columns if 'date' in col.lower()]
            for col in date_columns:
                if pd.api.types.is_datetime64_any_dtype(df_chunk[col]):
                    invalid_dates = df_chunk[col].isnull().sum()
                    integrity_issues['invalid_dates'] += invalid_dates
        
        logger.info("Data integrity checks complete.")
        
        return integrity_issues


def main():
    """
    Main function to demonstrate the ParquetDataChecker functionality.
    """
    # Define paths
    project_root = Path(__file__).parent.parent
    data_dir = project_root / "data"
    parquet_file = data_dir / "daily_spending_sample.parquet"
    
    logger.info("="*80)
    logger.info("ETL Pipeline - Sample Data Checker")
    logger.info("="*80)
    
    try:
        # Initialize checker
        checker = ParquetDataChecker(str(parquet_file), chunk_size=10000)
        
        # Get and display file metadata
        print("\n" + "="*80)
        print("FILE METADATA")
        print("="*80)
        metadata = checker.get_file_metadata()
        print(f"File Path: {metadata['file_path']}")
        print(f"File Size: {metadata['file_size_mb']:.2f} MB")
        print(f"Total Rows: {metadata['total_rows']:,}")
        print(f"Total Columns: {metadata['num_columns']}")
        print(f"Row Groups: {metadata['num_row_groups']}")
        print("\nColumn Names:")
        for i, col in enumerate(metadata['column_names'], start=1):
            print(f"  {i}. {col}")
        print("\nSchema:")
        print(metadata['schema'])
        print("="*80)
        
        # Display sample data
        checker.display_sample_data(n_rows=5)
        
        # Perform data quality validation
        quality_report = checker.validate_data_quality()
        checker.print_quality_report(quality_report)
        
        # Perform data integrity checks
        integrity_issues = checker.check_data_integrity()
        print("\n" + "="*80)
        print("DATA INTEGRITY CHECKS")
        print("="*80)
        print(f"Negative amounts found: {integrity_issues['negative_amounts']:,}")
        print(f"Duplicate rows found: {integrity_issues['duplicate_rows']:,}")
        print(f"Invalid dates found: {integrity_issues['invalid_dates']:,}")
        print("="*80 + "\n")
        
        logger.info("Data check completed successfully!")
        
    except FileNotFoundError as e:
        logger.error(f"Error: {e}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"An error occurred: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()

