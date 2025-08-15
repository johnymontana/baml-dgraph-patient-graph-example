#!/usr/bin/env python3
"""
Test FLIR Parquet Data Import (Limited Records)

This script tests the BAML parsing pipeline on a small subset of the parquet data
to verify the functionality works correctly.
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

async def test_parquet_data_with_baml(limit=5):
    """Test processing a limited number of parquet records using BAML."""
    print(f"🧪 Testing FLIR Parquet Data with BAML (First {limit} records)")
    print("=" * 70)
    
    # Read the parquet file
    parquet_path = Path("data/flir-data.parquet")
    if not parquet_path.exists():
        print(f"❌ Parquet file not found: {parquet_path}")
        return None
    
    try:
        df = pd.read_parquet(parquet_path)
        print(f"✅ Loaded parquet file with {len(df)} total records")
        print(f"📋 Columns: {', '.join(df.columns)}")
        
        # Process only the first few records for testing
        test_df = df.head(limit)
        print(f"🧪 Testing with first {len(test_df)} records")
        
        # Process the data using BAML
        extracted_records = []
        
        for idx, row in test_df.iterrows():
            print(f"\n📄 Processing record {idx + 1}/{len(test_df)}")
            
            # Get the clinical note
            clinical_note = row.get('note', '')
            
            if not clinical_note or clinical_note.strip() == '':
                print("   ⚠️  Empty note, skipping")
                continue
            
            # Show a preview of the note
            note_preview = clinical_note[:200] + "..." if len(clinical_note) > 200 else clinical_note
            print(f"   📝 Note preview: {note_preview}")
            
            try:
                # Use BAML to extract structured medical data from the note
                print("   🔍 Extracting data with BAML...")
                extracted_data = await b.ExtractMedicalData(clinical_note)
                
                # Convert to dictionary for easier handling
                record = pydantic_to_dict(extracted_data)
                
                # Add metadata about the source
                if 'metadata' not in record:
                    record['metadata'] = {}
                record['metadata']['source'] = 'flir-parquet-test'
                record['metadata']['record_id'] = idx
                record['metadata']['import_timestamp'] = pd.Timestamp.now().isoformat()
                
                extracted_records.append(record)
                
                # Show extracted data summary
                patient_name = record.get('patient', {}).get('name', 'Unknown')
                visits_count = len(record.get('visits', []))
                allergies_count = len(record.get('allergies', []))
                print(f"   ✅ Extracted: {patient_name} - {visits_count} visits, {allergies_count} allergies")
                
            except Exception as e:
                print(f"   ❌ Failed to extract data: {e}")
                continue
        
        print(f"\n✅ Successfully extracted data from {len(extracted_records)} test records")
        return extracted_records
        
    except Exception as e:
        print(f"❌ Error processing parquet file: {e}")
        return None

def test_dgraph_import(records):
    """Test importing the extracted records into DGraph."""
    if not records:
        print("❌ No records to import")
        return
    
    print(f"\n🗃️  Testing DGraph Import ({len(records)} records)")
    print("=" * 60)
    
    try:
        # Initialize DGraph importer
        importer = DGraphMedicalImporter()
        
        # Import records
        successful_imports = 0
        for i, record in enumerate(records):
            print(f"📥 Testing import of record {i + 1}/{len(records)}")
            
            try:
                importer.import_medical_record(record)
                successful_imports += 1
                print(f"   ✅ Successfully imported")
            except Exception as e:
                print(f"   ❌ Failed to import: {e}")
                continue
        
        print(f"✅ Successfully imported {successful_imports}/{len(records)} test records to DGraph")
        
        # Clean up
        importer.close()
        
    except Exception as e:
        print(f"❌ Error importing to DGraph: {e}")

async def main():
    """Main function to test the parquet data processing."""
    print("🧪 FLIR Parquet Data Import Test (via BAML)")
    print("=" * 70)
    
    # Test processing a few parquet records using BAML
    records = await test_parquet_data_with_baml(limit=3)
    
    if records:
        # Test DGraph import
        test_dgraph_import(records)
        
        print("\n🎉 Test Complete!")
        print(f"📊 Test records processed: {len(records)}")
        print("\n💡 To run the full import, use: make full-import")
    else:
        print("\n❌ Test failed")

if __name__ == "__main__":
    asyncio.run(main())
