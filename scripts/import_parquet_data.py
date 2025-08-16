#!/usr/bin/env python3
"""
Import FLIR Parquet Data into DGraph

This script reads the FLIR parquet data file, extracts structured medical data
from the clinical notes using BAML, and immediately imports each extracted record
into DGraph for better memory efficiency and real-time feedback.
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

async def process_and_import_parquet_data():
    """Process the parquet data using BAML and immediately import to DGraph."""
    print("📊 Processing FLIR Parquet Data with BAML and Importing to DGraph")
    print("=" * 70)
    
    # Read the parquet file
    parquet_path = Path("data/flir-data.parquet")
    if not parquet_path.exists():
        print(f"❌ Parquet file not found: {parquet_path}")
        return None
    
    try:
        df = pd.read_parquet(parquet_path)
        print(f"✅ Loaded parquet file with {len(df)} records")
        print(f"📋 Columns: {', '.join(df.columns)}")
        
        # Initialize DGraph importer once
        print("\n🔌 Initializing DGraph connection...")
        importer = DGraphMedicalImporter()
        
        # Process and import records one by one
        successful_extractions = 0
        successful_imports = 0
        failed_extractions = 0
        failed_imports = 0
        
        print(f"\n🔄 Processing and importing {len(df)} records...")
        print("-" * 50)
        
        for idx, row in df.iterrows():
            if idx % 100 == 0:
                print(f"📊 Progress: {idx + 1}/{len(df)} records processed")
                print(f"   ✅ Extracted: {successful_extractions}, ❌ Failed: {failed_extractions}")
                print(f"   ✅ Imported: {successful_imports}, ❌ Failed: {failed_imports}")
                print("-" * 50)
            
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
                
                successful_extractions += 1
                
                # Immediately import to DGraph
                try:
                    importer.import_medical_record(record)
                    successful_imports += 1
                    
                    if idx % 100 == 0:
                        print(f"   📥 Record {idx + 1}: ✅ Extracted and ✅ Imported")
                    
                except Exception as import_error:
                    failed_imports += 1
                    print(f"   📥 Record {idx + 1}: ✅ Extracted but ❌ Import failed: {import_error}")
                
            except Exception as extraction_error:
                failed_extractions += 1
                print(f"   🔍 Record {idx + 1}: ❌ Extraction failed: {extraction_error}")
                continue
        
        # Final summary
        print("\n" + "=" * 70)
        print("📊 FINAL PROCESSING SUMMARY")
        print("=" * 70)
        print(f"📋 Total records in parquet: {len(df)}")
        print(f"✅ Successful extractions: {successful_extractions}")
        print(f"❌ Failed extractions: {failed_extractions}")
        print(f"✅ Successful imports: {successful_imports}")
        print(f"❌ Failed imports: {failed_imports}")
        print(f"📈 Success rate (extraction): {successful_extractions/(successful_extractions+failed_extractions)*100:.1f}%")
        print(f"📈 Success rate (import): {successful_imports/(successful_imports+failed_imports)*100:.1f}%")
        
        # Clean up
        importer.close()
        
        return {
            'total_records': len(df),
            'successful_extractions': successful_extractions,
            'failed_extractions': failed_extractions,
            'successful_imports': successful_imports,
            'failed_imports': failed_imports
        }
        
    except Exception as e:
        print(f"❌ Error processing parquet file: {e}")
        return None

async def main():
    """Main function to process and import parquet data."""
    print("🏥 FLIR Parquet Data Import to DGraph (via BAML)")
    print("=" * 70)
    print("🔄 Processing and importing records in real-time...")
    print("💡 Each record is extracted and immediately imported to DGraph")
    print("💾 This approach provides better memory efficiency and real-time feedback")
    print("=" * 70)
    
    # Process and import the parquet data
    results = await process_and_import_parquet_data()
    
    if results:
        print("\n🎉 FLIR Parquet Data Import Complete!")
        print(f"📊 Final results: {results['successful_imports']} records successfully imported to DGraph")
    else:
        print("\n❌ Failed to process parquet data")

if __name__ == "__main__":
    asyncio.run(main())
