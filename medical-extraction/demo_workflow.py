#!/usr/bin/env python3
"""
Medical Data Extraction and DGraph Import - Complete Workflow Demo

This script demonstrates the complete end-to-end workflow:
1. BAML extraction of medical data from text
2. DGraph import of the extracted data
3. Querying the imported data
"""

import asyncio
import json
import sys
from pathlib import Path

# Add the parent directory to the Python path to import baml_client
sys.path.insert(0, str(Path(__file__).parent))

from baml_client import b
from scripts.sample_medical_text import SAMPLE_MEDICAL_TEXT, ADDITIONAL_SAMPLE
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

async def demo_workflow():
    """Demonstrate the complete workflow."""
    print("🏥 Medical Data Extraction and DGraph Import - Complete Workflow Demo")
    print("=" * 80)
    
    # Step 1: BAML Extraction
    print("\n📋 Step 1: BAML Medical Data Extraction")
    print("-" * 50)
    
    try:
        print("🔍 Extracting medical data from sample text...")
        extracted_data = await b.ExtractMedicalData(SAMPLE_MEDICAL_TEXT)
        
        # Convert to dictionary for easier handling
        record = pydantic_to_dict(extracted_data)
        
        print(f"✅ Successfully extracted data for patient: {record['patient']['name']}")
        print(f"   📊 Patient details: {record['patient']['age']} years old, {record['patient']['gender']}")
        print(f"   ⚠️  Allergies: {len(record['allergies'])} found")
        print(f"   🏥 Visits: {len(record['visits'])} recorded")
        
        # Handle provider_facility vs providers field mismatch
        if 'provider_facility' in record and record['provider_facility']:
            print(f"   🏥 Provider Facility: {record['provider_facility']['city']}, {record['provider_facility']['state']}")
        elif 'providers' in record:
            print(f"   👨‍⚕️  Providers: {len(record['providers'])} listed")
        else:
            print(f"   👨‍⚕️  Providers: None listed")
        
    except Exception as e:
        print(f"❌ BAML extraction failed: {e}")
        return
    
    # Step 2: DGraph Import
    print("\n🗃️  Step 2: DGraph Data Import")
    print("-" * 50)
    
    try:
        print("🔌 Connecting to DGraph...")
        importer = DGraphMedicalImporter()
        
        print("📥 Importing extracted medical record...")
        result = importer.import_medical_record(record)
        
        print("✅ Successfully imported medical record to DGraph!")
        
        # Step 3: Query the imported data
        print("\n🔍 Step 3: Querying Imported Data")
        print("-" * 50)
        
        print(f"🔍 Querying data for: {record['patient']['name']}")
        query_result = importer.query_patient_data(record['patient']['name'])
        
        if query_result and 'data' in query_result:
            patient_data = query_result['data']
            if patient_data:
                patient = patient_data[0]
                print(f"   👤 Patient ID: {patient.get('patient_id', 'N/A')}")
                print(f"   📅 Visits: {len(patient.get('has_visit', []))}")
                print(f"   ⚠️  Allergies: {len(patient.get('has_allergy', []))}")
                print(f"   🏥 Providers: {len(patient.get('treated_at', []))}")
            else:
                print("   ℹ️  No patient data found in query result")
        else:
            print("   ℹ️  Query result structure different than expected")
        
        # Clean up
        importer.close()
        
    except Exception as e:
        print(f"❌ DGraph import/query failed: {e}")
        return
    
    print("\n🎉 Complete Workflow Demo Finished Successfully!")
    print("=" * 80)

if __name__ == "__main__":
    asyncio.run(demo_workflow())
