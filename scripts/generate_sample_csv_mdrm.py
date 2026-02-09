#!/usr/bin/env python3
"""
Script to generate sample CSV data for MDRM testing.
The CSV file will have the following columns: seriesid,aod,rssdid,submissionts,key,value
"""

import os
import csv
import time
import random
import argparse
from datetime import datetime

def read_keys_from_file(filepath):
    """
    Read comma-separated keys from a file and return as a list.
    
    Args:
        filepath (str): Path to the file containing comma-separated keys
        
    Returns:
        list: List of distinct keys
    """
    with open(filepath, 'r') as f:
        content = f.read().strip()
        keys = [key.strip() for key in content.split(',')]
        return list(set(keys))  # Return distinct keys

def generate_mdrm_data(output_dir, timestamp=None):
    """
    Generate sample CSV data with MDRM structure:
    - First 1000 records: seriesid=FRY9C, aod=20230131, rssdid=1234567, keys from output_fry9c.txt
    - Next 1000 records: seriesid=FRY9C, aod=20231231, rssdid=2345678, keys from output_fry9c.txt
    - Next 1000 records: seriesid=FRY15, aod=20241231, rssdid=2345678, keys from output_fry15.txt
    
    Args:
        output_dir (str): Directory to save the CSV file
        timestamp (str): Optional timestamp to use for the folder name
    """
    if timestamp is None:
        timestamp = int(time.time())
    
    # Create directory if it doesn't exist
    ingest_dir = os.path.join(output_dir, f"ingest_ts={timestamp}")
    os.makedirs(ingest_dir, exist_ok=True)
    
    output_file = os.path.join(ingest_dir, f"mdrm_data_{timestamp}.csv")
    
    # Read keys from files
    fry9c_keys = read_keys_from_file('mdrm/output_fry9c.txt')
    fry15_keys = read_keys_from_file('mdrm/output_fry15.txt')
    
    print(f"Loaded {len(fry9c_keys)} distinct keys from output_fry9c.txt")
    print(f"Loaded {len(fry15_keys)} distinct keys from output_fry15.txt")
    
    with open(output_file, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        # Write header
        writer.writerow(['seriesid', 'aod', 'rssdid', 'submissionts', 'key', 'value'])
        
        # First 1000 records: FRY9C, 20230131, 1234567
        print("Generating first 1000 records (FRY9C, 20230131, 1234567)...")
        for i in range(1000):
            key = fry9c_keys[i % len(fry9c_keys)]
            value = random.randint(1000, 999999)
            writer.writerow([
                'FRY9C',
                '20230131',
                '1234567',
                timestamp,
                key,
                value
            ])
        
        # Next 1000 records: FRY9C, 20231231, 2345678
        print("Generating next 1000 records (FRY9C, 20231231, 2345678)...")
        for i in range(1000):
            key = fry9c_keys[i % len(fry9c_keys)]
            value = random.randint(1000, 999999)
            writer.writerow([
                'FRY9C',
                '20231231',
                '2345678',
                timestamp,
                key,
                value
            ])
        
        # Next 1000 records: FRY15, 20241231, 2345678
        print("Generating next 1000 records (FRY15, 20241231, 2345678)...")
        for i in range(1000):
            key = fry15_keys[i % len(fry15_keys)]
            value = random.randint(1000, 999999)
            writer.writerow([
                'FRY15',
                '20241231',
                '2345678',
                timestamp,
                key,
                value
            ])
    
    print(f"Generated 3000 records at: {output_file}")
    return output_file

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Generate MDRM sample CSV data for testing')
    parser.add_argument('--output-dir', default='/tmp/data', help='Directory to save the CSV file')
    parser.add_argument('--timestamp', help='Optional timestamp to use for the folder name')
    
    args = parser.parse_args()
    generate_mdrm_data(args.output_dir, args.timestamp)
