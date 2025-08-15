import json
import uuid
import os
from datetime import datetime
from typing import Dict, List, Any
from dotenv import load_dotenv
import pydgraph

load_dotenv()

class DGraphMedicalImporter:
    def __init__(self):
        """Initialize DGraph client and setup schema."""
        # Get connection string from environment variable
        connection_string = os.getenv('DGRAPH_CONNECTION_STRING', 'dgraph://localhost:9080')
        
        print(f"Connecting to DGraph using: {connection_string}")
        
        # Use pydgraph.open() to handle the connection string automatically
        self.client = pydgraph.open(connection_string)
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
        try:
            op = pydgraph.Operation(schema=schema)
            self.client.alter(op)
            print("✅ Schema setup complete")
        except Exception as e:
            print(f"⚠️  Schema setup warning (may already exist): {e}")
    
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
            
            # Build N-Quad mutations for better control
            nquads = []
            
            # Patient node
            patient_data = record["patient"]
            for key, value in patient_data.items():
                if value is not None:
                    nquads.append(f'{patient_uid} <{key}> "{value}" .')
            
            # Add patient type
            nquads.append(f'{patient_uid} <dgraph.type> "Patient" .')
            
            # Extraction metadata
            metadata = record.get("metadata", {})
            for key, value in metadata.items():
                if value is not None:
                    nquads.append(f'{extraction_uid} <{key}> "{value}" .')
            
            # Add extraction type
            nquads.append(f'{extraction_uid} <dgraph.type> "MedicalRecord" .')
            
            # Add relationships
            if "allergies" in record and record["allergies"]:
                for i, allergy in enumerate(record["allergies"]):
                    allergen = allergy.get("allergen")
                    if allergen:
                        # Only check for duplicates if there's existing data
                        existing_allergy_uid = None
                        if self.has_existing_data():
                            existing_allergy_uid = self.find_existing_allergy(allergen)
                            print(f"   🔍 Checking for existing allergy '{allergen}': {existing_allergy_uid}")
                        
                        if existing_allergy_uid and existing_allergy_uid != "0x0" and existing_allergy_uid != "0" and self.is_valid_uid(existing_allergy_uid):
                            # Use existing allergy node
                            print(f"   🔗 Connecting to existing allergy: {allergen}")
                            nquads.append(f'{patient_uid} <has_allergy> {existing_allergy_uid} .')
                        else:
                            # Create new allergy node (either no existing node or invalid UID)
                            if existing_allergy_uid and not self.is_valid_uid(existing_allergy_uid):
                                print(f"   ⚠️  Found existing allergy '{allergen}' but UID '{existing_allergy_uid}' is invalid, creating new node")
                            
                            allergy_uid = self.generate_uid()
                            print(f"   🆕 Creating new allergy: {allergen}")
                            
                            # Allergy data
                            for key, value in allergy.items():
                                if value is not None:
                                    nquads.append(f'{allergy_uid} <{key}> "{value}" .')
                            
                            # Allergy type
                            nquads.append(f'{allergy_uid} <dgraph.type> "Allergy" .')
                            
                            # Link patient to allergy
                            nquads.append(f'{patient_uid} <has_allergy> {allergy_uid} .')
                    else:
                        # Create new allergy node if no allergen name
                        allergy_uid = self.generate_uid()
                        
                        # Allergy data
                        for key, value in allergy.items():
                            if value is not None:
                                nquads.append(f'{allergy_uid} <{key}> "{value}" .')
                        
                        # Allergy type
                        nquads.append(f'{allergy_uid} <dgraph.type> "Allergy" .')
                        
                        # Link patient to allergy
                        nquads.append(f'{patient_uid} <has_allergy> {allergy_uid} .')
            
            # Handle provider_facility (address)
            if "provider_facility" in record and record["provider_facility"]:
                facility = record["provider_facility"]
                street = facility.get("street")
                city = facility.get("city")
                state = facility.get("state")
                
                if street and city and state:
                    # Check if address already exists
                    existing_address_uid = self.find_existing_address(street, city, state)
                    
                    # FIXME: This is a test to debug error when saving duplicate facility
                    if False:
                    #if existing_address_uid:
                        # Use existing address node
                        print(f"   🔗 Connecting to existing facility: {city}, {state}")
                        nquads.append(f'{patient_uid} <treated_at> {existing_address_uid} .')
                    else:
                        # Create new address node
                        address_uid = self.generate_uid()
                        print(f"   🆕 Creating new facility: {city}, {state}")
                        
                        # Address data
                        for key, value in facility.items():
                            if value is not None:
                                nquads.append(f'{address_uid} <{key}> "{value}" .')
                        
                        # Address type
                        nquads.append(f'{address_uid} <dgraph.type> "Address" .')
                        
                        # Link patient to address
                        nquads.append(f'{patient_uid} <treated_at> {address_uid} .')
                else:
                    # Create new address node if missing required fields
                    address_uid = self.generate_uid()
                    
                    # Address data
                    for key, value in facility.items():
                        if value is not None:
                            nquads.append(f'{address_uid} <{key}> "{value}" .')
                    
                    # Address type
                    nquads.append(f'{address_uid} <dgraph.type> "Address" .')
                    
                    # Link patient to address
                    nquads.append(f'{patient_uid} <treated_at> {address_uid} .')
            
            if "visits" in record and record["visits"]:
                for i, visit in enumerate(record["visits"]):
                    visit_uid = self.generate_uid()
                    
                    # Visit data
                    for key, value in visit.items():
                        if value is not None:
                            nquads.append(f'{visit_uid} <{key}> "{value}" .')
                    
                    # Visit type
                    nquads.append(f'{visit_uid} <dgraph.type> "MedicalVisit" .')
                    
                    # Link patient to visit
                    nquads.append(f'{patient_uid} <has_visit> {visit_uid} .')
            
            if "providers" in record and record["providers"]:
                for i, provider in enumerate(record["providers"]):
                    provider_name = provider.get("name")
                    if provider_name:
                        # Check if provider already exists
                        existing_provider_uid = self.find_existing_provider(provider_name)
                        
                        if existing_provider_uid:
                            # Use existing provider node
                            print(f"   🔗 Connecting to existing provider: {provider_name}")
                            nquads.append(f'{patient_uid} <treated_at> {existing_provider_uid} .')
                        else:
                            # Create new provider node
                            provider_uid = self.generate_uid()
                            print(f"   🆕 Creating new provider: {provider_name}")
                            
                            # Provider data
                            for key, value in provider.items():
                                if value is not None:
                                    nquads.append(f'{provider_uid} <{key}> "{value}" .')
                            
                            # Provider type
                            nquads.append(f'{provider_uid} <dgraph.type> "MedicalProvider" .')
                            
                            # Link patient to provider
                            nquads.append(f'{patient_uid} <treated_at> {provider_uid} .')
                    else:
                        # Create new provider node if no name
                        provider_uid = self.generate_uid()
                        
                        # Provider data
                        for key, value in provider.items():
                            if value is not None:
                                nquads.append(f'{provider_uid} <{key}> "{value}" .')
                        
                        # Provider type
                        nquads.append(f'{provider_uid} <dgraph.type> "MedicalProvider" .')
                        
                        # Link patient to provider
                        nquads.append(f'{patient_uid} <treated_at> {provider_uid} .')
            
            # Execute mutation using set_nquads
            result = txn.mutate(set_nquads='\n'.join(nquads))
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
    
    def has_existing_data(self) -> bool:
        """Check if there are any existing nodes in the database."""
        query = """
        {
            count(func: has(dgraph.type)) {
                count(uid)
            }
        }
        """
        
        txn = self.client.txn(read_only=True)
        try:
            result = txn.query(query)
            data = json.loads(result.json)
            if data.get("count") and len(data["count"]) > 0:
                count = data["count"][0].get("count", 0)
                return count > 0
            return False
        finally:
            txn.discard()
    
    def is_valid_uid(self, uid: str) -> bool:
        """Check if a UID is valid for N-Quad syntax."""
        if not uid:
            return False
        
        # UID should start with 0x and be at least 8 characters long (0x + 6 hex chars)
        if not uid.startswith("0x"):
            return False
        
        # Remove 0x prefix and check hex length
        hex_part = uid[2:]
        if len(hex_part) < 6:
            return False
        
        # Check if it's valid hex
        try:
            int(hex_part, 16)
            return True
        except ValueError:
            return False
    
    def find_existing_allergy(self, allergen: str) -> str:
        """Find existing allergy node by allergen name, return UID if found, None otherwise."""
        query = f"""
        {{
            allergy(func: eq(allergen, "{allergen}")) @filter(eq(dgraph.type, "Allergy")) {{
                uid
                allergen
            }}
        }}
        """
        
        txn = self.client.txn(read_only=True)
        try:
            result = txn.query(query)
            data = json.loads(result.json)
            if data.get("allergy") and len(data["allergy"]) > 0:
                uid = data["allergy"][0]["uid"]
                # Convert UID to proper format for N-Quad syntax
                if uid.startswith("0x"):
                    return uid
                else:
                    return f"0x{uid}"
            return None
        finally:
            txn.discard()
    
    def find_existing_provider(self, provider_name: str) -> str:
        """Find existing provider node by name, return UID if found, None otherwise."""
        query = f"""
        {{
            provider(func: eq(name, "{provider_name}")) @filter(eq(dgraph.type, "MedicalProvider")) {{
                uid
                name
            }}
        }}
        """
        
        txn = self.client.txn(read_only=True)
        try:
            result = txn.query(query)
            data = json.loads(result.json)
            if data.get("provider") and len(data["provider"]) > 0:
                uid = data["provider"][0]["uid"]
                # Convert UID to proper format for N-Quad syntax
                if uid.startswith("0x"):
                    return uid
                else:
                    return f"0x{uid}"
            return None
        finally:
            txn.discard()
    
    def find_existing_address(self, street: str, city: str, state: str) -> str:
        """Find existing address node by street, city, and state, return UID if found, None otherwise."""
        query = f"""
        {{
            address(func: eq(street, "{street}")) @filter(eq(dgraph.type, "Address") AND eq(city, "{city}") AND eq(state, "{state}")) {{
                uid
                street
                city
                state
            }}
        }}
        """
        
        txn = self.client.txn(read_only=True)
        try:
            result = txn.query(query)
            data = json.loads(result.json)
            if data.get("address") and len(data["address"]) > 0:
                uid = data["address"][0]["uid"]
                # Convert UID to proper format for N-Quad syntax
                if uid.startswith("0x"):
                    return uid
                else:
                    return f"0x{uid}"
            return None
        finally:
            txn.discard()
    
    def close(self):
        """Close DGraph connection."""
        self.client.close()

def main():
    """Import extracted medical records into DGraph."""
    print("🗃️  DGraph Medical Data Import")
    print("=" * 40)
    
    # Check if DGraph connection string is set
    if not os.getenv('DGRAPH_CONNECTION_STRING'):
        print("❌ DGRAPH_CONNECTION_STRING environment variable not set!")
        print("Please set it in your .env file (e.g., DGRAPH_CONNECTION_STRING=dgraph://localhost:9080)")
        return
    
    # Initialize DGraph importer
    importer = DGraphMedicalImporter()
    
    try:
        # Load extracted medical records
        try:
            with open("data/all_medical_records.json", 'r') as f:
                records = json.load(f)
        except FileNotFoundError:
            print("❌ No medical records found. Run extract_medical_data.py first.")
            return
        
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
    
    except Exception as e:
        print(f"❌ Error: {e}")
    finally:
        importer.close()

if __name__ == "__main__":
    main()
