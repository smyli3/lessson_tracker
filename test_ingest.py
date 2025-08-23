#!/usr/bin/env python3
"""
Quick test script to validate CSV ingestion
"""

import polars as pl
from pathlib import Path

def test_csv_reading():
    """Test reading the CSV files with proper handling."""
    
    files_to_test = ["All_Tasks_June.csv", "All_Tasks_July.csv"]
    
    for file_name in files_to_test:
        if not Path(file_name).exists():
            print(f"❌ {file_name} not found")
            continue
            
        print(f"\n📁 Testing {file_name}...")
        
        try:
            # Detect header row
            header_row = 0
            with open(file_name, 'r', encoding='utf-8') as f:
                for i, line in enumerate(f):
                    if 'Date (YYYY/MM/DD)' in line:
                        header_row = i
                        break
            
            print(f"   Headers found at row {header_row + 1}")
            
            # Read with simple settings
            df = pl.read_csv(
                file_name,
                skip_rows=header_row,
                infer_schema_length=0
            )
            
            print(f"   ✅ Successfully read {len(df)} rows")
            print(f"   📊 Columns: {len(df.columns)}")
            
            # Check for Staff ID column
            if 'Staff ID' in df.columns:
                staff_ids = df['Staff ID'].unique().to_list()[:5]
                print(f"   👤 Sample Staff IDs: {staff_ids}")
            
            # Check for problematic values
            if 'Staff ID' in df.columns:
                non_numeric = df.filter(
                    ~pl.col('Staff ID').str.contains(r'^\d+$').fill_null(False)
                )['Staff ID'].unique().to_list()
                if non_numeric:
                    print(f"   ⚠️  Non-numeric Staff IDs found: {non_numeric[:5]}")
                else:
                    print(f"   ✅ All Staff IDs are numeric")
            
        except Exception as e:
            print(f"   ❌ Error: {str(e)}")

if __name__ == '__main__':
    test_csv_reading()
