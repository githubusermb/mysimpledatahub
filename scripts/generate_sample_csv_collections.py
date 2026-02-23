#!/usr/bin/env python3
"""
Script to generate sample CSV data for collections with MDRM structure.
The CSV file will have columns: seriesid,aod,rssdid,submission_ts,item_mdrm,item_desc,
item_value,context_level1_mdrm,context_level1_desc,context_level1_value,context_level2_mdrm,
context_level2_desc,context_level2_value,context_level3_mdrm,context_level3_desc,context_level3_value
"""

import os
import csv
import time
import random
import argparse
from datetime import datetime

def read_mdrm_map(filepath):
    """
    Read MDRM map CSV file and return list of records.
    
    Args:
        filepath (str): Path to the MDRM map CSV file
        
    Returns:
        list: List of dictionaries with MDRM data
    """
    records = []
    with open(filepath, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            records.append({
                'item_mdrm': row['item_mdrm'],
                'item_desc': row['item_desc'],
                'context_level1_mdrm': row['context_level1_mdrm'],
                'context_level1_desc': row['context_level1_desc'],
                'context_level1_value': row['context_level1_value']
            })
    return records

def generate_collections_data(output_dir, timestamp=None):
    """
    Generate sample CSV data with collections structure:
    - First 1000 records: seriesid=FRY9C, aod=20230131, rssdid=1234567, MDRM from fry9c_mdrm_map_output.csv
    - Next 1000 records: seriesid=FRY9C, aod=20231231, rssdid=2345678, MDRM from fry9c_mdrm_map_output.csv
    - Next 1000 records: seriesid=FRY15, aod=20241231, rssdid=2345678, MDRM from fry15_mdrm_map_output.csv
    - Next 1000 records: seriesid=FR2004A, aod=20241231, rssdid=2345678, MDRM from fr2004a_mdrm_map_output.csv
    
    Args:
        output_dir (str): Directory to save the CSV file
        timestamp (str): Optional timestamp to use for the folder name
    """
    if timestamp is None:
        timestamp = int(time.time())
    
    # Create directory if it doesn't exist
    ingest_dir = os.path.join(output_dir, f"ingest_ts={timestamp}")
    os.makedirs(ingest_dir, exist_ok=True)
    
    output_file = os.path.join(ingest_dir, f"collections_data_{timestamp}.csv")
    
    # Read MDRM map files
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(script_dir)
    
    fry9c_file = os.path.join(project_root, 'mdrm', 'fry9c_mdrm_map_output.csv')
    fry15_file = os.path.join(project_root, 'mdrm', 'fry15_mdrm_map_output.csv')
    fr2004a_file = os.path.join(project_root, 'mdrm', 'fr2004a_mdrm_map_output.csv')
    
    print(f"Reading MDRM map files...")
    fry9c_records = read_mdrm_map(fry9c_file)
    fry15_records = read_mdrm_map(fry15_file)
    fr2004a_records = read_mdrm_map(fr2004a_file)
    
    print(f"Loaded {len(fry9c_records)} records from fry9c_mdrm_map_output.csv")
    print(f"Loaded {len(fry15_records)} records from fry15_mdrm_map_output.csv")
    print(f"Loaded {len(fr2004a_records)} records from fr2004a_mdrm_map_output.csv")
    
    with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        # Write header
        writer.writerow([
            'seriesid', 'aod', 'rssdid', 'submission_ts',
            'item_mdrm', 'item_desc', 'item_value',
            'context_level1_mdrm', 'context_level1_desc', 'context_level1_value',
            'context_level2_mdrm', 'context_level2_desc', 'context_level2_value',
            'context_level3_mdrm', 'context_level3_desc', 'context_level3_value'
        ])
        
        # First 1000 records: FRY9C, 20230131, 1234567
        print("Generating first 1000 records (FRY9C, 20230131, 1234567)...")
        for i in range(1000):
            mdrm_record = fry9c_records[i % len(fry9c_records)]
            item_value = random.randint(1000, 9999999)
            writer.writerow([
                'FRY9C',
                '20230131',
                '1234567',
                timestamp,
                mdrm_record['item_mdrm'],
                mdrm_record['item_desc'],
                item_value,
                mdrm_record['context_level1_mdrm'],
                mdrm_record['context_level1_desc'],
                mdrm_record['context_level1_value'],
                '',  # context_level2_mdrm
                '',  # context_level2_desc
                '',  # context_level2_value
                '',  # context_level3_mdrm
                '',  # context_level3_desc
                ''   # context_level3_value
            ])
        
        # Next 1000 records: FRY9C, 20231231, 2345678
        print("Generating next 1000 records (FRY9C, 20231231, 2345678)...")
        for i in range(1000):
            mdrm_record = fry9c_records[i % len(fry9c_records)]
            item_value = random.randint(1000, 9999999)
            writer.writerow([
                'FRY9C',
                '20231231',
                '2345678',
                timestamp,
                mdrm_record['item_mdrm'],
                mdrm_record['item_desc'],
                item_value,
                mdrm_record['context_level1_mdrm'],
                mdrm_record['context_level1_desc'],
                mdrm_record['context_level1_value'],
                '',
                '',
                '',
                '',
                '',
                ''
            ])
        
        # Next 1000 records: FRY15, 20241231, 2345678
        print("Generating next 1000 records (FRY15, 20241231, 2345678)...")
        for i in range(1000):
            mdrm_record = fry15_records[i % len(fry15_records)]
            item_value = random.randint(1000, 9999999)
            writer.writerow([
                'FRY15',
                '20241231',
                '2345678',
                timestamp,
                mdrm_record['item_mdrm'],
                mdrm_record['item_desc'],
                item_value,
                mdrm_record['context_level1_mdrm'],
                mdrm_record['context_level1_desc'],
                mdrm_record['context_level1_value'],
                '',
                '',
                '',
                '',
                '',
                ''
            ])
        
        # Next 1000 records: FR2004A, 20241231, 2345678
        print("Generating next 1000 records (FR2004A, 20241231, 2345678)...")
        for i in range(1000):
            mdrm_record = fr2004a_records[i % len(fr2004a_records)]
            item_value = random.randint(1000, 9999999)
            writer.writerow([
                'FR2004A',
                '20241231',
                '2345678',
                timestamp,
                mdrm_record['item_mdrm'],
                mdrm_record['item_desc'],
                item_value,
                mdrm_record['context_level1_mdrm'],
                mdrm_record['context_level1_desc'],
                mdrm_record['context_level1_value'],
                '',
                '',
                '',
                '',
                '',
                ''
            ])
    
    print(f"Generated 4000 records at: {output_file}")
    print(f"  - 1000 records: FRY9C, 20230131, rssdid=1234567")
    print(f"  - 1000 records: FRY9C, 20231231, rssdid=2345678")
    print(f"  - 1000 records: FRY15, 20241231, rssdid=2345678")
    print(f"  - 1000 records: FR2004A, 20241231, rssdid=2345678")
    print(f"\nIngest timestamp: {timestamp}")
    return output_file

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Generate collections sample CSV data with MDRM structure')
    parser.add_argument('--output-dir', default='data', help='Directory to save the CSV file')
    parser.add_argument('--timestamp', help='Optional timestamp to use for the folder name')
    
    args = parser.parse_args()
    generate_collections_data(args.output_dir, args.timestamp)
