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
        """Setup DGraph schema for medical data with upsert support."""
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
        
        # Predicates with upsert support for duplicate prevention
        name: string @index(term, fulltext) @upsert .
        patient_id: string @index(exact) @upsert .
        provider_id: string @index(exact) @upsert .
        source_id: string @index(exact) @upsert .
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
        allergen: string @index(term, fulltext) @upsert .
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
    
    def import_medical_record(self, record: Dict[str, Any]) -> Any:
        """Import a medical record into DGraph using proper upsert pattern for duplicate prevention."""
        txn = self.client.txn()
        try:
            # First, query for existing nodes to implement proper upsert
            query = self._build_upsert_query(record)
            print(f"   🔍 Query: {query}")
            
            # Execute query to find existing nodes
            query_result = txn.query(query)
            query_data = json.loads(query_result.json)
            print(f"   🔍 Query result: {json.dumps(query_data, indent=2)}")
            
            # Build the mutation using proper upsert logic
            nquads = self._build_upsert_mutation(record, query_data)
            print(f"   📝 N-Quads: {nquads}")
            
            # Create mutation from N-Quads
            mutation = txn.create_mutation(set_nquads=nquads)
            
            # Execute the mutation
            result = txn.mutate(mutation)
            txn.commit()
            
            print(f"✅ Successfully imported medical record for patient: {record['patient']['name']}")
            return result
            
        except Exception as e:
            print(f"❌ Error importing medical record: {e}")
            import traceback
            traceback.print_exc()
            txn.discard()
            raise
        finally:
            txn.discard()
    
    def _build_upsert_query(self, record: Dict[str, Any]) -> str:
        """Build the query to check for existing nodes for upsert logic."""
        query_parts = []
        
        # Check for existing patient by patient_id
        query_parts.append(f'patient(func: eq(patient_id, "{record["patient"]["patient_id"]}")) {{ uid }}')
        
        # Check for existing allergies by allergen name
        if "allergies" in record and record["allergies"]:
            for i, allergy in enumerate(record["allergies"]):
                allergen = allergy.get("allergen")
                if allergen:
                    query_parts.append(f'allergy_{i}(func: eq(allergen, "{allergen}")) @filter(eq(dgraph.type, "Allergy")) {{ uid }}')
        
        # Check for existing providers by provider_id
        if "providers" in record and record["providers"]:
            for i, provider in enumerate(record["providers"]):
                provider_id = provider.get("provider_id")
                if provider_id:
                    query_parts.append(f'provider_{i}(func: eq(provider_id, "{provider_id}")) @filter(eq(dgraph.type, "MedicalProvider")) {{ uid }}')
        
        # Check for existing addresses by street, city, state combination
        if "provider_facility" in record and record["provider_facility"]:
            facility = record["provider_facility"]
            street = facility.get("street")
            city = facility.get("city")
            state = facility.get("state")
            if street and city and state:
                query_parts.append(f'address(func: eq(street, "{street}")) @filter(eq(dgraph.type, "Address") AND eq(city, "{city}") AND eq(state, "{state}")) {{ uid }}')
        
        return "{\n  " + "\n  ".join(query_parts) + "\n}"
    
    def _build_upsert_mutation(self, record: Dict[str, Any], query_data: Dict) -> str:
        """Build the mutation using proper upsert logic based on query results."""
        nquads = []
        
        # Check what nodes exist from the query
        patient_exists = 'patient' in query_data and len(query_data['patient']) > 0
        allergies_exist = {}
        for i in range(len(record.get("allergies", []))):
            allergies_exist[i] = f'allergy_{i}' in query_data and len(query_data[f'allergy_{i}']) > 0
        
        # Handle patient - use existing if found, otherwise create new
        if patient_exists:
            patient_uid = query_data['patient'][0]['uid']
        else:
            patient_uid = self.generate_uid()
            # Add patient data since we're creating a new one
            for key, value in record["patient"].items():
                if value is not None:
                    nquads.append(f'{patient_uid} <{key}> "{value}" .')
            nquads.append(f'{patient_uid} <dgraph.type> "Patient" .')
        
        # Handle extraction metadata
        if "metadata" in record and record["metadata"]:
            extraction_uid = self.generate_uid()
            for key, value in record["metadata"].items():
                if value is not None:
                    nquads.append(f'{extraction_uid} <{key}> "{value}" .')
            nquads.append(f'{extraction_uid} <extracted_at> "{datetime.now().isoformat()}" .')
            nquads.append(f'{extraction_uid} <extraction_version> "1.0" .')
            nquads.append(f'{extraction_uid} <dgraph.type> "ExtractionRecord" .')
        
        # Handle allergies with proper upsert logic
        if "allergies" in record and record["allergies"]:
            for i, allergy in enumerate(record["allergies"]):
                allergen = allergy.get("allergen")
                if allergen:
                    if allergies_exist.get(i, False):
                        # Use existing allergy - just link patient to it
                        existing_allergy_uid = query_data[f'allergy_{i}'][0]['uid']
                        nquads.append(f'{patient_uid} <has_allergy> <{existing_allergy_uid}> .')
                        # Add reverse relationship
                        nquads.append(f'<{existing_allergy_uid}> <allergy_of> {patient_uid} .')
                    else:
                        # Create new allergy node
                        allergy_uid = self.generate_uid()
                        for key, value in allergy.items():
                            if value is not None:
                                nquads.append(f'{allergy_uid} <{key}> "{value}" .')
                        nquads.append(f'{allergy_uid} <dgraph.type> "Allergy" .')
                        
                        # Link patient to allergy (bidirectional)
                        nquads.append(f'{patient_uid} <has_allergy> {allergy_uid} .')
                        nquads.append(f'{allergy_uid} <allergy_of> {patient_uid} .')
        
        # Handle provider_facility (address)
        if "provider_facility" in record and record["provider_facility"]:
            facility = record["provider_facility"]
            street = facility.get("street")
            city = facility.get("city")
            state = facility.get("state")
            
            if street and city and state:
                address_exists = 'address' in query_data and len(query_data['address']) > 0
                if address_exists:
                    # Use existing address - just link patient to it
                    existing_address_uid = query_data['address'][0]['uid']
                    nquads.append(f'{patient_uid} <treated_at> <{existing_address_uid}> .')
                else:
                    # Create new address node
                    address_uid = self.generate_uid()
                    for key, value in facility.items():
                        if value is not None:
                            nquads.append(f'{address_uid} <{key}> "{value}" .')
                    nquads.append(f'{address_uid} <dgraph.type> "Address" .')
                    nquads.append(f'{patient_uid} <treated_at> {address_uid} .')
        
        # Handle visits
        if "visits" in record and record["visits"]:
            for visit in record["visits"]:
                visit_uid = self.generate_uid()
                for key, value in visit.items():
                    if value is not None:
                        nquads.append(f'{visit_uid} <{key}> "{value}" .')
                nquads.append(f'{visit_uid} <dgraph.type> "MedicalVisit" .')
                nquads.append(f'{patient_uid} <has_visit> {visit_uid} .')
                nquads.append(f'{visit_uid} <visit_of> {patient_uid} .')
        
        # Handle providers
        if "providers" in record and record["providers"]:
            for i, provider in enumerate(record["providers"]):
                provider_id = provider.get("provider_id")
                provider_exists = f'provider_{i}' in query_data and len(query_data[f'provider_{i}']) > 0
                if provider_id and provider_exists:
                    # Use existing provider - just link patient to it
                    existing_provider_uid = query_data[f'provider_{i}'][0]['uid']
                    nquads.append(f'{patient_uid} <treated_at> <{existing_provider_uid}> .')
                else:
                    # Create new provider node
                    provider_uid = self.generate_uid()
                    for key, value in provider.items():
                        if value is not None:
                            nquads.append(f'{provider_uid} <{key}> "{value}" .')
                    nquads.append(f'{provider_uid} <dgraph.type> "MedicalProvider" .')
                    nquads.append(f'{patient_uid} <treated_at> {provider_uid} .')
        
        # Return N-Quads string
        return '\n'.join(nquads)
    
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
                # Ensure UID is in proper format for N-Quad syntax
                if uid and uid != "0x0" and uid != "0":
                    return uid
            return None
        except Exception as e:
            print(f"   ⚠️  Error querying for existing allergy '{allergen}': {e}")
            return None
        finally:
            txn.discard()
    
    def find_existing_provider(self, provider_id: str) -> str:
        """Find existing provider node by provider_id, return UID if found, None otherwise."""
        query = f"""
        {{
            provider(func: eq(provider_id, "{provider_id}")) @filter(eq(dgraph.type, "MedicalProvider")) {{
                uid
                provider_id
            }}
        }}
        """
        
        txn = self.client.txn(read_only=True)
        try:
            result = txn.query(query)
            data = json.loads(result.json)
            if data.get("provider") and len(data["provider"]) > 0:
                uid = data["provider"][0]["uid"]
                # Ensure UID is in proper format for N-Quad syntax
                if uid and uid != "0x0" and uid != "0":
                    return uid
            return None
        except Exception as e:
            print(f"   ⚠️  Error querying for existing provider '{provider_id}': {e}")
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
                # Ensure UID is in proper format for N-Quad syntax
                if uid and uid != "0x0" and uid != "0":
                    return uid
            return None
        except Exception as e:
            print(f"   ⚠️  Error querying for existing address '{street}, {city}, {state}': {e}")
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
