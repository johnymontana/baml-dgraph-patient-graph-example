import asyncio
import json
import sys
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv

# Add the parent directory to the Python path to import baml_client
sys.path.insert(0, str(Path(__file__).parent.parent))

from baml_client import b
from sample_medical_text import SAMPLE_MEDICAL_TEXT, ADDITIONAL_SAMPLE

load_dotenv()

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

async def extract_medical_data(text: str, source_id: str = None):
    """Extract structured medical data from unstructured text using BAML."""
    try:
        print(f"Processing medical text...")
        print(f"Text preview: {text[:100]}...")
        
        # Extract full medical record
        medical_record = await b.ExtractMedicalData(text)
        
        # Add metadata
        extraction_metadata = {
            "source_id": source_id or f"extract_{datetime.now().isoformat()}",
            "extracted_at": datetime.now().isoformat(),
            "text_length": len(text),
            "extraction_version": "1.0"
        }
        
        # Convert Pydantic models to dictionaries
        record_dict = {
            "metadata": extraction_metadata,
            "patient": pydantic_to_dict(medical_record.patient),
            "visits": [pydantic_to_dict(visit) for visit in medical_record.visits],
            "allergies": [pydantic_to_dict(allergy) for allergy in medical_record.allergies],
            "provider_facility": pydantic_to_dict(medical_record.provider_facility) if medical_record.provider_facility else None,
            "extracted_entities": medical_record.extracted_entities
        }
        
        print("✅ Successfully extracted medical data")
        print(f"Patient: {medical_record.patient.name}")
        print(f"Visits: {len(medical_record.visits)}")
        print(f"Allergies: {len(medical_record.allergies)}")
        
        return record_dict
        
    except Exception as e:
        print(f"❌ Error extracting medical data: {e}")
        return None

async def main():
    """Test medical data extraction with sample texts."""
    print("🏥 Medical Data Extraction with BAML")
    print("=" * 50)
    
    # Process sample texts
    samples = [
        (SAMPLE_MEDICAL_TEXT, "sample_1_breitenberg"),
        (ADDITIONAL_SAMPLE, "sample_2_rodriguez")
    ]
    
    extracted_records = []
    
    for text, source_id in samples:
        print(f"\n📄 Processing {source_id}")
        print("-" * 30)
        
        record = await extract_medical_data(text, source_id)
        if record:
            extracted_records.append(record)
            
            # Save individual record
            filename = f"extracted_{source_id}.json"
            with open(filename, 'w') as f:
                json.dump(record, f, indent=2)
            print(f"💾 Saved to {filename}")
    
    # Save all records to a single file
    with open("data/all_medical_records.json", 'w') as f:
        json.dump(extracted_records, f, indent=2, default=str)
    print(f"\n💾 Saved all {len(extracted_records)} records to data/all_medical_records.json")
    
    return extracted_records

if __name__ == "__main__":
    records = asyncio.run(main())
