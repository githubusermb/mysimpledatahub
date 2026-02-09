#!/usr/bin/env python3
"""
Script to generate sample CSV data for testing the data ingestion pipeline.
The CSV file will have the following columns: seriesid,aod,rssdid,submissionts,key,value
"""

import os
import csv
import time
import argparse
from datetime import datetime

def generate_sample_data(output_dir, timestamp=None):
    """
    Generate sample CSV data with the specified structure:
    - First 1000 records: Same values for seriesid-submissionts, distinct key and value
    - Next 1000 records: Same values for seriesid, aod, rssdid, and key as first set,
                         but different values for submissionts and value columns
    - Next 1000 records: Different seriesid, key, and value, but same aod, rssdid, submissionts
    
    Args:
        output_dir (str): Directory to save the CSV file
        timestamp (str): Optional timestamp to use for the folder name
    """
    if timestamp is None:
        timestamp = int(time.time())
    
    # Create directory if it doesn't exist
    ingest_dir = os.path.join(output_dir, f"ingest_ts={timestamp}")
    os.makedirs(ingest_dir, exist_ok=True)
    
    output_file = os.path.join(ingest_dir, f"sample_data_{timestamp}.csv")
    
    with open(output_file, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        # Write header
        writer.writerow(['seriesid', 'aod', 'rssdid', 'submissionts', 'key', 'value'])
        
        # First 1000 records: Same values for seriesid-submissionts, distinct key and value
        for i in range(1, 1001):
            writer.writerow([
                'seriesid_set1',
                'aod_set1',
                'rssdid_set1',
                'submissionts_set1',
                f'key_{i}',
                f'value_{i}'
            ])
        
        # Next 1000 records: Change data for submissionts and value columns only
        # Keep the same values for seriesid, aod, rssdid, and key as the first 1000 records
        for i in range(1, 1001):
            writer.writerow([
                'seriesid_set1',
                'aod_set1',
                'rssdid_set1',
                'submissionts_set2',  # Changed submissionts
                f'key_{i}',      # Same key as first set
                f'value_changed_{i}'  # Changed value
            ])
        
        # Next 1000 records: Different seriesid, key, value
        for i in range(1001, 2001):
            writer.writerow([
                'seriesid_set2',  # Changed seriesid
                'aod_set1',
                'rssdid_set1',
                'submissionts_set1',
                f'key_set2_{i}',  # Changed key
                f'value_set2_{i}'  # Changed value
            ])
    
    print(f"Generated sample data at: {output_file}")
    return output_file

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Generate sample CSV data for testing')
    parser.add_argument('--output-dir', default='/tmp/data', help='Directory to save the CSV file')
    parser.add_argument('--timestamp', help='Optional timestamp to use for the folder name')
    
    args = parser.parse_args()
    generate_sample_data(args.output_dir, args.timestamp)
