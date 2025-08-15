# Medical Data Extraction with BAML and DGraph Integration

## Project Structure
```
medical-extraction/
├── baml_src/
│   ├── main.baml
│   ├── medical_models.baml
│   └── clients.baml
├── scripts/
│   ├── extract_medical_data.py
│   ├── dgraph_importer.py
│   └── sample_medical_text.py
├── requirements.txt
├── .env
└── README.md
```

## 1. BAML Schema Definition (baml_src/medical_models.baml)

```baml
// Medical data models for structured extraction

class Address {
  street string
  city string
  state string
  zip_code string
  country string
}

class MedicalProvider {
  name string
  provider_id string?
  specialty string?
  address Address?
}

class Allergy {
  allergen string
  severity string?  // mild, moderate, severe
  reaction_type string?
  confirmed_date string?
  notes string?
}

class MedicalVisit {
  visit_type string
  start_time string
  end_time string?
  timezone string?
  location string?
  provider MedicalProvider?
  notes string?
}

class Patient {
  name string
  patient_id string?
  marital_status string?
  age int?
  gender string?
  date_of_birth string?
}

class MedicalRecord {
  patient Patient
  visits MedicalVisit[]
  allergies Allergy[]
  provider_facility Address?
  extracted_entities string[]  // Any other important entities found
}
```

## 2. BAML Function Definition (baml_src/main.baml)

```baml
function ExtractMedicalData(medical_text: string) -> MedicalRecord {
  client GPT4Mini
  prompt #"
    Extract structured medical information from the following clinical text.
    
    Medical Text:
    ---
    {{ medical_text }}
    ---
    
    Instructions:
    1. Extract patient information including name, marital status, and any demographic data
    2. Identify all medical visits with dates, times, types, and providers
    3. Extract allergy information including allergens, dates discovered, and severity
    4. Identify healthcare facility locations and addresses
    5. Extract provider information including names and IDs
    6. Parse all dates and times with timezone information when available
    7. Include any other medically relevant entities in the extracted_entities field
    
    Be thorough and accurate. If information is not explicitly stated, leave those fields null.
    If dates/times are provided, preserve the original format and timezone information.
    
    {{ ctx.output_format }}
  "#
}

function ExtractPatientSummary(medical_text: string) -> Patient {
  client GPT4Mini
  prompt #"
    Extract only patient demographic information from this medical text:
    
    {{ medical_text }}
    
    Focus on: name, patient ID, age, gender, marital status, date of birth.
    
    {{ ctx.output_format }}
  "#
}
```

## 3. BAML Client Configuration (baml_src/clients.baml)

```baml
client<llm> GPT4Mini {
  provider openai
  options {
    model "gpt-4o-mini"
    api_key env.OPENAI_API_KEY
    max_tokens 2000
    temperature 0.1
  }
}

client<llm> Claude {
  provider anthropic
  options {
    model "claude-3-haiku-20240307"
    api_key env.ANTHROPIC_API_KEY
    max_tokens 2000
    temperature 0.1
  }
}
```

## 4. Requirements (requirements.txt)

```
baml-py>=0.60.0
pydgraph>=23.0.0
python-dotenv>=1.0.0
asyncio
json
datetime
uuid
```

## 5. Sample Medical Text (scripts/sample_medical_text.py)

```python
SAMPLE_MEDICAL_TEXT = """
This is about Mr. Fernando Amos Breitenberg. He's married. Now let's talk 
about his medical encounters.

The main one is a well child visit. This 
happened at the clinic. The visit started on December 23, 1992, at 01:08:42 
and ended on the same day at 01:23:42, with timezone being +01:00. Mr. 
Breitenberg was looked after by Dr. Trent Krajcik. He was the one who did 
the whole procedure.

Moving on, Mr. Breitenberg has an allergy. It's 
active and confirmed. He's allergic to shellfish. We've known this since 
April 2, 1994, at 12:08:42, with timezone as +02:00.

Finally, all this 
happened at our healthcare provider in Quincy. It's located at 
300 CONGRESS ST STE 203, Quincy, MA, 021690907, US.
"""

ADDITIONAL_SAMPLE = """
Patient: Sarah Michelle Rodriguez, ID: SMR-9901234
DOB: March 15, 1985, Female, Single

Emergency Department Visit - January 15, 2025, 14:30:00 EST
Chief Complaint: Severe headache and nausea
Attending: Dr. Emily Watson, MD (Provider ID: EW-7799)
Discharge: January 15, 2025, 18:45:00 EST

Known Allergies:
- Penicillin (severe - anaphylaxis, discovered June 10, 2018)
- Latex (moderate - contact dermatitis)

Location: Metropolitan General Hospital
Emergency Department, 1500 Medical Center Drive, Boston, MA 02115, USA
"""
```

## 6. Medical Data Extraction Script (scripts/extract_medical_data.py)

```python
import asyncio
import json
from datetime import datetime
from dotenv import load_dotenv
from baml_client import b
from sample_medical_text import SAMPLE_MEDICAL_TEXT, ADDITIONAL_SAMPLE

load_dotenv()

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
        
        # Convert to dict for easier handling
        record_dict = {
            "metadata": extraction_metadata,
            "patient": medical_record.patient.__dict__,
            "visits": [visit.__dict__ for visit in medical_record.visits],
            "allergies": [allergy.__dict__ for allergy in medical_record.allergies],
            "provider_facility": medical_record.provider_facility.__dict__ if medical_record.provider_facility else None,
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
    
    # Save all records
    if extracted_records:
        with open("all_medical_records.json", 'w') as f:
            json.dump(extracted_records, f, indent=2)
        print(f"\n💾 Saved all {len(extracted_records)} records to all_medical_records.json")
    
    return extracted_records

if __name__ == "__main__":
    records = asyncio.run(main())
```

## 7. DGraph Schema and Import Script (scripts/dgraph_importer.py)

```python
import json
import uuid
from datetime import datetime
from typing import Dict, List, Any
import pydgraph

class DGraphMedicalImporter:
    def __init__(self, dgraph_host: str = "localhost:9080"):
        """Initialize DGraph client and setup schema."""
        self.client_stub = pydgraph.DgraphClientStub(dgraph_host)
        self.client = pydgraph.DgraphClient(self.client_stub)
        self.setup_schema()
    
    def setup_schema(self):
        """Setup DGraph schema for medical data."""
        schema = """
        # Patient node
        type Patient {
            name: string
            patient_id: string
            marital_status: string
            age: int
            gender: string
            date_of_birth: string
            has_visit: [uid]
            has_allergy: [uid]
            treated_at: [uid]
        }
        
        # Medical Visit node  
        type MedicalVisit {
            visit_type: string
            start_time: string
            end_time: string
            timezone: string
            location: string
            notes: string
            conducted_by: [uid]
            visit_of: [uid]
        }
        
        # Medical Provider node
        type MedicalProvider {
            name: string
            provider_id: string
            specialty: string
            conducts_visit: [uid]
            works_at: [uid]
        }
        
        # Allergy node
        type Allergy {
            allergen: string
            severity: string
            reaction_type: string
            confirmed_date: string
            notes: string
            allergy_of: [uid]
        }
        
        # Address/Facility node
        type Address {
            street: string
            city: string
            state: string
            zip_code: string
            country: string
            hosts_provider: [uid]
            hosts_visit: [uid]
        }
        
        # Extraction metadata
        type ExtractionRecord {
            source_id: string
            extracted_at: string
            text_length: int
            extraction_version: string
            contains_patient: [uid]
        }
        
        # Predicates
        name: string @index(term, fulltext) .
        patient_id: string @index(exact) .
        provider_id: string @index(exact) .
        source_id: string @index(exact) .
        marital_status: string @index(exact) .
        age: int @index(int) .
        gender: string @index(exact) .
        date_of_birth: string @index(exact) .
        visit_type: string @index(exact) .
        start_time: string @index(exact) .
        end_time: string @index(exact) .
        timezone: string @index(exact) .
        location: string @index(term, fulltext) .
        specialty: string @index(exact) .
        allergen: string @index(term, fulltext) .
        severity: string @index(exact) .
        reaction_type: string @index(exact) .
        confirmed_date: string @index(exact) .
        notes: string @index(fulltext) .
        street: string @index(fulltext) .
        city: string @index(exact) .
        state: string @index(exact) .
        zip_code: string @index(exact) .
        country: string @index(exact) .
        extracted_at: string @index(exact) .
        text_length: int @index(int) .
        extraction_version: string @index(exact) .
        
        # Relationships
        has_visit: [uid] @reverse .
        has_allergy: [uid] @reverse .
        treated_at: [uid] @reverse .
        conducted_by: [uid] @reverse .
        visit_of: [uid] @reverse .
        conducts_visit: [uid] @reverse .
        works_at: [uid] @reverse .
        allergy_of: [uid] @reverse .
        hosts_provider: [uid] @reverse .
        hosts_visit: [uid] @reverse .
        contains_patient: [uid] @reverse .
        """
        
        print("Setting up DGraph schema...")
        op = pydgraph.Operation(schema=schema)
        self.client.alter(op)
        print("✅ Schema setup complete")
    
    def generate_uid(self) -> str:
        """Generate a unique identifier."""
        return f"_:{uuid.uuid4().hex}"
    
    def import_medical_record(self, record: Dict[str, Any]) -> str:
        """Import a single medical record into DGraph."""
        txn = self.client.txn()
        
        try:
            # Create nodes with UIDs
            patient_uid = self.generate_uid()
            extraction_uid = self.generate_uid()
            
            # Build mutation object
            mutation_data = {}
            
            # Patient node
            patient_data = {
                "uid": patient_uid,
                "dgraph.type": "Patient",
                **{k: v for k, v in record["patient"].items() if v is not None}
            }
            mutation_data[patient_uid] = patient_data
            
            # Extraction metadata
            extraction_data = {
                "uid": extraction_uid,
                "dgraph.type": "ExtractionRecord",
                **record["metadata"],
                "contains_patient": [{"uid": patient_uid}]
            }
            mutation_data[extraction_uid] = extraction_data
            
            # Process visits
            visit_uids = []
            for visit in record["visits"]:
                visit_uid = self.generate_uid()
                visit_uids.append({"uid": visit_uid})
                
                visit_data = {
                    "uid": visit_uid,
                    "dgraph.type": "MedicalVisit",
                    "visit_of": [{"uid": patient_uid}],
                    **{k: v for k, v in visit.items() if v is not None and k != "provider"}
                }
                
                # Handle provider if present
                if visit.get("provider"):
                    provider_uid = self.generate_uid()
                    provider_data = {
                        "uid": provider_uid,
                        "dgraph.type": "MedicalProvider",
                        "conducts_visit": [{"uid": visit_uid}],
                        **{k: v for k, v in visit["provider"].items() if v is not None and k != "address"}
                    }
                    
                    # Handle provider address
                    if visit["provider"].get("address"):
                        address_uid = self.generate_uid()
                        address_data = {
                            "uid": address_uid,
                            "dgraph.type": "Address",
                            "hosts_provider": [{"uid": provider_uid}],
                            **visit["provider"]["address"]
                        }
                        mutation_data[address_uid] = address_data
                        provider_data["works_at"] = [{"uid": address_uid}]
                    
                    mutation_data[provider_uid] = provider_data
                    visit_data["conducted_by"] = [{"uid": provider_uid}]
                
                mutation_data[visit_uid] = visit_data
            
            # Add visits to patient
            if visit_uids:
                patient_data["has_visit"] = visit_uids
            
            # Process allergies
            allergy_uids = []
            for allergy in record["allergies"]:
                allergy_uid = self.generate_uid()
                allergy_uids.append({"uid": allergy_uid})
                
                allergy_data = {
                    "uid": allergy_uid,
                    "dgraph.type": "Allergy",
                    "allergy_of": [{"uid": patient_uid}],
                    **{k: v for k, v in allergy.items() if v is not None}
                }
                mutation_data[allergy_uid] = allergy_data
            
            # Add allergies to patient
            if allergy_uids:
                patient_data["has_allergy"] = allergy_uids
            
            # Process facility address
            if record.get("provider_facility"):
                facility_uid = self.generate_uid()
                facility_data = {
                    "uid": facility_uid,
                    "dgraph.type": "Address",
                    **record["provider_facility"]
                }
                mutation_data[facility_uid] = facility_data
                patient_data["treated_at"] = [{"uid": facility_uid}]
            
            # Execute mutation
            mutation = pydgraph.Mutation(set_obj=list(mutation_data.values()))
            result = txn.mutate(mutation)
            txn.commit()
            
            print(f"✅ Successfully imported medical record for patient: {record['patient']['name']}")
            return result
            
        except Exception as e:
            print(f"❌ Error importing medical record: {e}")
            txn.discard()
            raise
        finally:
            txn.discard()
    
    def query_patient_data(self, patient_name: str) -> Dict:
        """Query comprehensive patient data from DGraph."""
        query = f"""
        {{
            patient(func: eq(name, "{patient_name}")) {{
                uid
                name
                patient_id
                marital_status
                age
                gender
                date_of_birth
                
                has_visit {{
                    uid
                    visit_type
                    start_time
                    end_time
                    location
                    conducted_by {{
                        name
                        provider_id
                        specialty
                    }}
                }}
                
                has_allergy {{
                    uid
                    allergen
                    severity
                    confirmed_date
                    notes
                }}
                
                treated_at {{
                    uid
                    street
                    city
                    state
                    zip_code
                    country
                }}
            }}
        }}
        """
        
        txn = self.client.txn(read_only=True)
        try:
            result = txn.query(query)
            return json.loads(result.json)
        finally:
            txn.discard()
    
    def close(self):
        """Close DGraph connection."""
        self.client_stub.close()

def main():
    """Import extracted medical records into DGraph."""
    print("🗃️  DGraph Medical Data Import")
    print("=" * 40)
    
    # Initialize DGraph importer
    importer = DGraphMedicalImporter()
    
    try:
        # Load extracted medical records
        with open("all_medical_records.json", 'r') as f:
            records = json.load(f)
        
        print(f"Found {len(records)} medical records to import")
        
        # Import each record
        for i, record in enumerate(records, 1):
            print(f"\n📥 Importing record {i}/{len(records)}")
            importer.import_medical_record(record)
        
        # Test queries
        print("\n🔍 Testing Queries")
        print("-" * 20)
        
        # Query patients
        for record in records:
            patient_name = record["patient"]["name"]
            print(f"\nQuerying data for: {patient_name}")
            
            result = importer.query_patient_data(patient_name)
            if result.get("patient"):
                patient = result["patient"][0]
                print(f"  👤 Patient ID: {patient.get('patient_id', 'N/A')}")
                print(f"  📅 Visits: {len(patient.get('has_visit', []))}")
                print(f"  ⚠️  Allergies: {len(patient.get('has_allergy', []))}")
    
    except FileNotFoundError:
        print("❌ No medical records found. Run extract_medical_data.py first.")
    except Exception as e:
        print(f"❌ Error: {e}")
    finally:
        importer.close()

if __name__ == "__main__":
    main()
```

## 8. Environment Configuration (.env)

```bash
# OpenAI API Key for BAML
OPENAI_API_KEY=your_openai_api_key_here

# Anthropic API Key (alternative)
ANTHROPIC_API_KEY=your_anthropic_api_key_here

# DGraph Configuration
DGRAPH_HOST=localhost:9080
DGRAPH_ALPHA_HOST=localhost:8080
```

## 9. Setup and Usage Instructions

### Installation

```bash
# 1. Install BAML CLI
npm install -g @boundaryml/baml

# 2. Create project directory
mkdir medical-extraction && cd medical-extraction

# 3. Initialize BAML project
baml init

# 4. Install Python dependencies
pip install -r requirements.txt

# 5. Setup environment variables
cp .env.example .env
# Edit .env with your API keys
```

### Running the Pipeline

```bash
# 1. Generate BAML client
baml generate

# 2. Extract medical data using BAML
python scripts/extract_medical_data.py

# 3. Start DGraph (if not running)
docker run -it -p 8080:8080 -p 9080:9080 dgraph/standalone:latest

# 4. Import data into DGraph
python scripts/dgraph_importer.py
```

### Sample Output

The system will extract structured data like:

```json
{
  "patient": {
    "name": "Mr. Fernando Amos Breitenberg",
    "marital_status": "married"
  },
  "visits": [{
    "visit_type": "well child visit",
    "start_time": "December 23, 1992, at 01:08:42",
    "end_time": "01:23:42",
    "timezone": "+01:00",
    "provider": {
      "name": "Dr. Trent Krajcik"
    }
  }],
  "allergies": [{
    "allergen": "shellfish",
    "confirmed_date": "April 2, 1994, at 12:08:42"
  }]
}
```

This complete pipeline demonstrates how BAML can extract structured medical data and integrate it with a graph database for advanced querying and relationship analysis.
