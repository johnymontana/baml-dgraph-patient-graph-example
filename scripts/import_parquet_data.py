#!/usr/bin/env python3
"""
Import FLIR Parquet Data into DGraph

This script reads the FLIR parquet data file, extracts structured medical data
from the clinical notes using BAML, and then imports the extracted data into DGraph.
"""

import json
import pandas as pd
import sys
import asyncio
from pathlib import Path

# Add the parent directory to the Python path to import baml_client and dgraph_importer
sys.path.insert(0, str(Path(__file__).parent.parent))

from baml_client import b
from scripts.dgraph_importer import DGraphMedicalImporter

def pydantic_to_dict(obj):
    """Convert Pydantic models to dictionaries recursively."""
    if hasattr(obj, 'model_dump'):
        # Pydantic v2
        return obj.model_dump()
    elif hasattr(obj, 'dict'):
        # Pydantic v1
        return obj.dict()
    elif isinstance(obj, list):
        return [pydantic_to_dict(item) for item in obj]
    elif isinstance(obj, dict):
        return {key: pydantic_to_dict(value) for key, value in obj.items()}
    else:
        return obj

async def process_parquet_data_with_baml():
    """Process the parquet data using BAML to extract structured medical data."""
    print("📊 Processing FLIR Parquet Data with BAML")
    print("=" * 60)
    
    # Read the parquet file
    parquet_path = Path("data/flir-data.parquet")
    if not parquet_path.exists():
        print(f"❌ Parquet file not found: {parquet_path}")
        return None
    
    try:
        df = pd.read_parquet(parquet_path)
        print(f"✅ Loaded parquet file with {len(df)} records")
        print(f"📋 Columns: {', '.join(df.columns)}")
        
        # Process the data using BAML
        extracted_records = []
        
        for idx, row in df.iterrows():
            if idx % 100 == 0:
                print(f"   Processing record {idx + 1}/{len(df)}")
            
            # Get the clinical note
            clinical_note = row.get('note', '')
            
            if not clinical_note or clinical_note.strip() == '':
                continue
            
            try:
                # Use BAML to extract structured medical data from the note
                extracted_data = await b.ExtractMedicalData(clinical_note)
                
                # Convert to dictionary for easier handling
                record = pydantic_to_dict(extracted_data)
                
                # Add metadata about the source
                if 'metadata' not in record:
                    record['metadata'] = {}
                record['metadata']['source'] = 'flir-parquet'
                record['metadata']['record_id'] = idx
                record['metadata']['import_timestamp'] = pd.Timestamp.now().isoformat()
                
                extracted_records.append(record)
                
                if idx % 100 == 0:
                    print(f"     ✅ Extracted data for record {idx + 1}")
                
            except Exception as e:
                print(f"     ⚠️  Warning: Failed to extract data from record {idx}: {e}")
                continue
        
        print(f"✅ Successfully extracted data from {len(extracted_records)} records")
        return extracted_records
        
    except Exception as e:
        print(f"❌ Error processing parquet file: {e}")
        return None

def import_to_dgraph(records):
    """Import the extracted records into DGraph."""
    if not records:
        print("❌ No records to import")
        return
    
    print("\n🗃️  Importing Extracted Data to DGraph")
    print("=" * 60)
    
    try:
        # Initialize DGraph importer
        importer = DGraphMedicalImporter()
        
        # Import records
        successful_imports = 0
        for i, record in enumerate(records):
            if i % 100 == 0:
                print(f"📥 Importing record {i + 1}/{len(records)}")
            
            try:
                importer.import_medical_record(record)
                successful_imports += 1
            except Exception as e:
                print(f"⚠️  Warning: Failed to import record {i}: {e}")
                continue
        
        print(f"✅ Successfully imported {successful_imports}/{len(records)} records to DGraph")
        
        # Clean up
        importer.close()
        
    except Exception as e:
        print(f"❌ Error importing to DGraph: {e}")

async def main():
    """Main function to process and import parquet data."""
    print("🏥 FLIR Parquet Data Import to DGraph (via BAML)")
    print("=" * 70)
    
    # Process the parquet data using BAML
    records = await process_parquet_data_with_baml()
    
    if records:
        # Import to DGraph
        import_to_dgraph(records)
        
        print("\n🎉 FLIR Parquet Data Import Complete!")
        print(f"📊 Total records processed: {len(records)}")
    else:
        print("\n❌ Failed to process parquet data")

if __name__ == "__main__":
    asyncio.run(main())
