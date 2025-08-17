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
        """Setup DGraph schema for enhanced medical knowledge graph with upsert support."""
        schema = """
        # Simplified Medical Knowledge Graph Schema
        # Focused on essential types and predicates for current data
        
        # Patient node - Central entity
        type Patient {
            name: string
            patient_id: string
            marital_status: string
            age: int
            gender: string
            date_of_birth: string
            multiple_birth: bool
            primary_language: string
            
            # Relationships to other entities
            has_visit: [uid]
            has_allergy: [uid]
            has_immunization: [uid]
            has_condition: [uid]
            has_observation: [uid]
            has_procedure: [uid]
            lives_in: [uid]
            treated_by: [uid]
            has_contact_info: [uid]
            has_social_history: [uid]
        }
        
        # Address/Facility node
        type Address {
            street: string
            city: string
            state: string
            zip_code: string
            country: string
            timezone: string
            latitude: float
            longitude: float
            geocoded: bool
            
            # Relationships
            location_of_patient: [uid]
            location_of_provider: [uid]
            location_of_organization: [uid]
            location_of_visit: [uid]
            location_of_procedure: [uid]
        }
        
        # Medical Visit node
        type MedicalVisit {
            visit_type: string
            start_time: string
            end_time: string
            timezone: string
            status: string
            classification: string
            reason: string
            notes: string
            
            # Relationships
            visit_of: [uid]
            includes_procedure: [uid]
            includes_observation: [uid]
            includes_immunization: [uid]
        }
        
        # Basic predicates with proper indexing
        name: string @index(term, fulltext) @upsert .
        patient_id: string @index(exact) @upsert .
        marital_status: string @index(exact) .
        age: int @index(int) .
        gender: string @index(exact) .
        date_of_birth: string @index(exact) .
        multiple_birth: bool @index(bool) .
        primary_language: string @index(exact) .
        visit_type: string @index(exact) .
        start_time: string @index(exact) .
        end_time: string @index(exact) .
        timezone: string @index(exact) .
        status: string @index(exact) .
        classification: string @index(exact) .
        reason: string @index(exact) .
        notes: string @index(fulltext) .
        street: string @index(exact) .
        city: string @index(exact) .
        state: string @index(exact) .
        zip_code: string @index(exact) .
        country: string @index(exact) .
        latitude: float @index(float) .
        longitude: float @index(float) .
        geocoded: bool @index(bool) .
        location: geo @index(geo) .
        
        # Additional predicates needed for current data
        condition_name: string @index(exact) .
        onset_date: string @index(exact) .
        vaccine_name: string @index(exact) .
        vaccine_type: string @index(exact) .
        administration_date: string @index(exact) .
        procedure_type: string @index(exact) .
        observation_type: string @index(exact) .
        value: string @index(fulltext) .
        unit: string @index(exact) .
        effective_time: string @index(exact) .
        issued_time: string @index(exact) .
        category: string @index(exact) .
        code: string @index(exact) .
        interpretation: string @index(exact) .
        source: string @index(exact) .
        record_id: string @index(exact) .
        import_timestamp: string @index(exact) .
        extracted_at: string @index(exact) .
        extraction_version: string @index(exact) .
        
        # Vector embedding predicates for AI-powered search
        embedding: float32vector @index(hnsw(metric:"cosine",exponent:"4")) .
        embedding_model: string @index(exact) .
        embedding_text: string @index(fulltext) .
        embedding_dimensions: string @index(exact) .
        
        # Basic relationships
        has_visit: [uid] .
        has_allergy: [uid] .
        has_immunization: [uid] .
        has_condition: [uid] .
        has_observation: [uid] .
        has_procedure: [uid] .
        lives_in: [uid] .
        treated_by: [uid] .
        has_contact_info: [uid] .
        has_social_history: [uid] .
        visit_of: [uid] .
        includes_procedure: [uid] .
        includes_observation: [uid] .
        includes_immunization: [uid] .
        location_of_patient: [uid] .
        location_of_provider: [uid] .
        location_of_organization: [uid] .
        location_of_visit: [uid] .
        location_of_procedure: [uid] .
        """
        
        print("Setting up DGraph schema...")
        try:
            op = pydgraph.Operation(schema=schema)
            self.client.alter(op)
            print("✅ Schema setup complete")
        except Exception as e:
            print(f"❌ Schema setup failed: {e}")
            print("🔄 Attempting to drop and recreate schema...")
            try:
                # Drop all data and schema
                drop_op = pydgraph.Operation(drop_all=True)
                self.client.alter(drop_op)
                print("✅ Dropped existing schema and data")
                
                # Reapply schema
                op = pydgraph.Operation(schema=schema)
                self.client.alter(op)
                print("✅ Schema recreated successfully")
            except Exception as e2:
                print(f"❌ Schema recreation failed: {e2}")
                raise e2
    
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
        if record["patient"].get("patient_id"):
            query_parts.append(f'patient(func: eq(patient_id, "{record["patient"]["patient_id"]}")) {{ uid }}')
        
        # Check for existing allergies by allergen name
        if "allergies" in record and record["allergies"]:
            for i, allergy in enumerate(record["allergies"]):
                allergen = allergy.get("allergen")
                if allergen:
                    query_parts.append(f'allergy_{i}(func: eq(allergen, "{allergen}")) @filter(eq(dgraph.type, "Allergy")) {{ uid }}')
        
        # Check for existing immunizations by vaccine name
        if "immunizations" in record and record["immunizations"]:
            for i, immunization in enumerate(record["immunizations"]):
                vaccine_name = immunization.get("vaccine_name")
                if vaccine_name:
                    query_parts.append(f'immunization_{i}(func: eq(vaccine_name, "{vaccine_name}")) @filter(eq(dgraph.type, "Immunization")) {{ uid }}')
        
        # Check for existing substances by name
        if "substances" in record and record["substances"]:
            for i, substance in enumerate(record["substances"]):
                substance_name = substance.get("name")
                if substance_name:
                    query_parts.append(f'substance_{i}(func: eq(substance_name, "{substance_name}")) @filter(eq(dgraph.type, "Substance")) {{ uid }}')
        
        # Check for existing conditions by condition name
        if "conditions" in record and record["conditions"]:
            for i, condition in enumerate(record["conditions"]):
                condition_name = condition.get("condition_name")
                if condition_name:
                    query_parts.append(f'condition_{i}(func: eq(condition_name, "{condition_name}")) @filter(eq(dgraph.type, "MedicalCondition")) {{ uid }}')
        
        # Check for existing providers by provider_id
        if "providers" in record and record["providers"]:
            for i, provider in enumerate(record["providers"]):
                provider_id = provider.get("provider_id")
                if provider_id:
                    query_parts.append(f'provider_{i}(func: eq(provider_id, "{provider_id}")) @filter(eq(dgraph.type, "MedicalProvider")) {{ uid }}')
        
        # Check for existing organizations by organization_id
        if "organizations" in record and record["organizations"]:
            for i, organization in enumerate(record["organizations"]):
                organization_id = organization.get("organization_id")
                if organization_id:
                    query_parts.append(f'organization_{i}(func: eq(organization_id, "{organization_id}")) @filter(eq(dgraph.type, "Organization")) {{ uid }}')
        
        # Check for existing addresses by street, city, state combination
        if "provider_facility" in record and record["provider_facility"]:
            facility = record["provider_facility"]
            street = facility.get("street")
            city = facility.get("city")
            state = facility.get("state")
            if street and city and state:
                query_parts.append(f'address(func: eq(street, "{street}")) @filter(eq(dgraph.type, "Address") AND eq(city, "{city}") AND eq(state, "{state}")) {{ uid }}')
        
        # Check for existing addresses from patient address
        if "patient" in record and record["patient"].get("address"):
            patient_address = record["patient"]["address"]
            street = patient_address.get("street")
            city = patient_address.get("city")
            state = patient_address.get("state")
            if street and city and state:
                query_parts.append(f'patient_address(func: eq(street, "{street}")) @filter(eq(dgraph.type, "Address") AND eq(city, "{city}") AND eq(state, "{state}")) {{ uid }}')
        
        # If no query parts, return a minimal valid query
        if not query_parts:
            return "{\n  _empty(func: uid(0x1)) {\n    uid\n  }\n}"
        
        return "{\n  " + "\n  ".join(query_parts) + "\n}"
    
    def _build_upsert_mutation(self, record: Dict[str, Any], query_data: Dict) -> str:
        """Build the mutation using proper upsert logic based on query results."""
        nquads = []
        
        # Check what nodes exist from the query
        patient_exists = 'patient' in query_data and len(query_data['patient']) > 0
        allergies_exist = {}
        for i in range(len(record.get("allergies", []))):
            allergies_exist[i] = f'allergy_{i}' in query_data and len(query_data[f'allergy_{i}']) > 0
        immunizations_exist = {}
        for i in range(len(record.get("immunizations", []))):
            immunizations_exist[i] = f'immunization_{i}' in query_data and len(query_data[f'immunization_{i}']) > 0
        substances_exist = {}
        for i in range(len(record.get("substances", []))):
            substances_exist[i] = f'substance_{i}' in query_data and len(query_data[f'substance_{i}']) > 0
        conditions_exist = {}
        for i in range(len(record.get("conditions", []))):
            conditions_exist[i] = f'condition_{i}' in query_data and len(query_data[f'condition_{i}']) > 0
        
        # Handle patient - use existing if found, otherwise create new
        if patient_exists:
            patient_uid = query_data['patient'][0]['uid']
        else:
            patient_uid = self.generate_uid()
            # Add patient data since we're creating a new one
            for key, value in record["patient"].items():
                if value is not None and key != "address":  # Handle address separately
                    nquads.append(f'{patient_uid} <{key}> "{value}" .')
            nquads.append(f'{patient_uid} <dgraph.type> "Patient" .')
        
        # Handle patient address
        if "patient" in record and record["patient"].get("address"):
            patient_address = record["patient"]["address"]
            street = patient_address.get("street")
            city = patient_address.get("city")
            state = patient_address.get("state")
            
            if street and city and state:
                patient_address_exists = 'patient_address' in query_data and len(query_data['patient_address']) > 0
                if patient_address_exists:
                    # Use existing address - just link patient to it
                    existing_address_uid = query_data['patient_address'][0]['uid']
                    nquads.append(f'{patient_uid} <lives_in> <{existing_address_uid}> .')
                else:
                    # Create new address node
                    address_uid = self.generate_uid()
                    for key, value in patient_address.items():
                        if value is not None:
                            # Handle geospatial fields as geo type
                            if key == 'location':
                                nquads.append(f'{address_uid} <{key}> "{value}"^^<geo:geojson> .')
                            else:
                                nquads.append(f'{address_uid} <{key}> "{value}" .')
                    nquads.append(f'{address_uid} <dgraph.type> "Address" .')
                    nquads.append(f'{patient_uid} <lives_in> {address_uid} .')
                    nquads.append(f'{address_uid} <location_of_patient> {patient_uid} .')
        
        # Handle extraction metadata
        if "metadata" in record and record["metadata"]:
            extraction_uid = self.generate_uid()
            for key, value in record["metadata"].items():
                if value is not None:
                    nquads.append(f'{extraction_uid} <{key}> "{value}" .')
            nquads.append(f'{extraction_uid} <extracted_at> "{datetime.now().isoformat()}" .')
            nquads.append(f'{extraction_uid} <extraction_version> "1.0" .')
            nquads.append(f'{extraction_uid} <dgraph.type> "ExtractionRecord" .')
            # Use proper UID format for existing patient
            if patient_exists:
                nquads.append(f'{extraction_uid} <contains_patient> <{patient_uid}> .')
            else:
                nquads.append(f'{extraction_uid} <contains_patient> {patient_uid} .')
        
        # Handle allergies with proper upsert logic
        if "allergies" in record and record["allergies"]:
            for i, allergy in enumerate(record["allergies"]):
                allergen = allergy.get("allergen")
                if allergen:
                    if allergies_exist.get(i, False):
                        # Use existing allergy - just link patient to it
                        existing_allergy_uid = query_data[f'allergy_{i}'][0]['uid']
                        if patient_exists:
                            nquads.append(f'<{patient_uid}> <has_allergy> <{existing_allergy_uid}> .')
                            # Add reverse relationship
                            nquads.append(f'<{existing_allergy_uid}> <allergy_of> <{patient_uid}> .')
                        else:
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
                        if patient_exists:
                            nquads.append(f'<{patient_uid}> <has_allergy> {allergy_uid} .')
                            nquads.append(f'{allergy_uid} <allergy_of> <{patient_uid}> .')
                        else:
                            nquads.append(f'{patient_uid} <has_allergy> {allergy_uid} .')
                            nquads.append(f'{allergy_uid} <allergy_of> {patient_uid} .')
        
        # Handle immunizations with proper upsert logic
        if "immunizations" in record and record["immunizations"]:
            for i, immunization in enumerate(record["immunizations"]):
                vaccine_name = immunization.get("vaccine_name")
                if vaccine_name:
                    if immunizations_exist.get(i, False):
                        # Use existing immunization - just link patient to it
                        existing_immunization_uid = query_data[f'immunization_{i}'][0]['uid']
                        nquads.append(f'{patient_uid} <has_immunization> <{existing_immunization_uid}> .')
                        nquads.append(f'<{existing_immunization_uid}> <immunization_of> {patient_uid} .')
                    else:
                        # Create new immunization node
                        immunization_uid = self.generate_uid()
                        for key, value in immunization.items():
                            if value is not None:
                                nquads.append(f'{immunization_uid} <{key}> "{value}" .')
                        nquads.append(f'{immunization_uid} <dgraph.type> "Immunization" .')
                        
                        # Link patient to immunization (bidirectional)
                        nquads.append(f'{patient_uid} <has_immunization> {immunization_uid} .')
                        nquads.append(f'{immunization_uid} <immunization_of> {patient_uid} .')
        
        # Handle substances with proper upsert logic
        if "substances" in record and record["substances"]:
            for i, substance in enumerate(record["substances"]):
                substance_name = substance.get("name")
                if substance_name:
                    if substances_exist.get(i, False):
                        # Use existing substance - just link to it
                        existing_substance_uid = query_data[f'substance_{i}'][0]['uid']
                        # Link substance to allergies if applicable
                        if "allergies" in record and record["allergies"]:
                            for allergy in record["allergies"]:
                                if allergy.get("allergen") == substance_name:
                                    nquads.append(f'<{existing_substance_uid}> <causes_allergy> {allergy_uid} .')
                    else:
                        # Create new substance node
                        substance_uid = self.generate_uid()
                        for key, value in substance.items():
                            if value is not None:
                                nquads.append(f'{substance_uid} <{key}> "{value}" .')
                        nquads.append(f'{substance_uid} <dgraph.type> "Substance" .')
                        
                        # Link substance to allergies if applicable
                        if "allergies" in record and record["allergies"]:
                            for allergy in record["allergies"]:
                                if allergy.get("allergen") == substance_name:
                                    nquads.append(f'{substance_uid} <causes_allergy> {allergy_uid} .')
                                    nquads.append(f'{allergy_uid} <caused_by> {substance_uid} .')
        
        # Handle conditions with proper upsert logic
        if "conditions" in record and record["conditions"]:
            for i, condition in enumerate(record["conditions"]):
                condition_name = condition.get("condition_name")
                if condition_name:
                    if conditions_exist.get(i, False):
                        # Use existing condition - just link patient to it
                        existing_condition_uid = query_data[f'condition_{i}'][0]['uid']
                        nquads.append(f'{patient_uid} <has_condition> <{existing_condition_uid}> .')
                        nquads.append(f'<{existing_condition_uid}> <condition_of> {patient_uid} .')
                    else:
                        # Create new condition node
                        condition_uid = self.generate_uid()
                        for key, value in condition.items():
                            if value is not None:
                                nquads.append(f'{condition_uid} <{key}> "{value}" .')
                        nquads.append(f'{condition_uid} <dgraph.type> "MedicalCondition" .')
                        
                        # Link patient to condition (bidirectional)
                        nquads.append(f'{patient_uid} <has_condition> {condition_uid} .')
                        nquads.append(f'{condition_uid} <condition_of> {patient_uid} .')
        
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
                            # Handle geospatial fields as geo type
                            if key == 'location':
                                nquads.append(f'{address_uid} <{key}> "{value}"^^<geo:geojson> .')
                            else:
                                nquads.append(f'{address_uid} <{key}> "{value}" .')
                    nquads.append(f'{address_uid} <dgraph.type> "Address" .')
                    nquads.append(f'{patient_uid} <treated_at> {address_uid} .')
        
        # Handle visits
        if "visits" in record and record["visits"]:
            for visit in record["visits"]:
                visit_uid = self.generate_uid()
                for key, value in visit.items():
                    if value is not None and key != "provider" and key != "location":
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
                    nquads.append(f'{patient_uid} <treated_by> <{existing_provider_uid}> .')
                else:
                    # Create new provider node
                    provider_uid = self.generate_uid()
                    for key, value in provider.items():
                        if value is not None and key != "address":
                            nquads.append(f'{provider_uid} <{key}> "{value}" .')
                    nquads.append(f'{provider_uid} <dgraph.type> "MedicalProvider" .')
                    nquads.append(f'{patient_uid} <treated_by> {provider_uid} .')
                    nquads.append(f'{provider_uid} <treats> {patient_uid} .')
        
        # Handle organizations
        if "organizations" in record and record["organizations"]:
            for i, organization in enumerate(record["organizations"]):
                organization_id = organization.get("organization_id")
                organization_exists = f'organization_{i}' in query_data and len(query_data[f'organization_{i}']) > 0
                if organization_id and organization_exists:
                    # Use existing organization - just link provider to it
                    existing_organization_uid = query_data[f'organization_{i}'][0]['uid']
                    # Only create provider-organization links if we have a provider
                    if "providers" in record and record["providers"] and i < len(record["providers"]):
                        provider = record["providers"][i]
                        if provider.get("name"):  # Only link if provider has a name
                            provider_uid = self.generate_uid()
                            nquads.append(f'{provider_uid} <name> "{provider["name"]}" .')
                            nquads.append(f'{provider_uid} <dgraph.type> "MedicalProvider" .')
                            nquads.append(f'{provider_uid} <works_at> <{existing_organization_uid}> .')
                            nquads.append(f'<{existing_organization_uid}> <employs_provider> {provider_uid} .')
                else:
                    # Create new organization node
                    organization_uid = self.generate_uid()
                    for key, value in organization.items():
                        if value is not None and key != "address":
                            nquads.append(f'{organization_uid} <{key}> "{value}" .')
                    nquads.append(f'{organization_uid} <dgraph.type> "Organization" .')
                    # Only create provider-organization links if we have a provider
                    if "providers" in record and record["providers"] and i < len(record["providers"]):
                        provider = record["providers"][i]
                        if provider.get("name"):  # Only link if provider has a name
                            provider_uid = self.generate_uid()
                            nquads.append(f'{provider_uid} <name> "{provider["name"]}" .')
                            nquads.append(f'{provider_uid} <dgraph.type> "MedicalProvider" .')
                            nquads.append(f'{provider_uid} <works_at> {organization_uid} .')
                            nquads.append(f'{organization_uid} <employs_provider> {provider_uid} .')
        
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
                multiple_birth
                primary_language
                
                has_visit {{
                    uid
                    visit_type
                    start_time
                    end_time
                    timezone
                    status
                    classification
                    reason
                    notes
                    conducted_by {{
                        name
                        provider_id
                        specialty
                        role
                        organization
                    }}
                    conducted_at {{
                        street
                        city
                        state
                        zip_code
                        country
                    }}
                }}
                
                has_allergy {{
                    uid
                    allergen
                    severity
                    reaction_type
                    confirmed_date
                    status
                    notes
                    caused_by {{
                        name
                        type
                        description
                    }}
                }}
                
                has_immunization {{
                    uid
                    vaccine_name
                    vaccine_type
                    administration_date
                    status
                    lot_number
                    manufacturer
                    notes
                    administered_by {{
                        name
                        provider_id
                        specialty
                    }}
                    administered_at {{
                        street
                        city
                        state
                        zip_code
                        country
                    }}
                }}
                
                has_condition {{
                    uid
                    condition_name
                    status
                    onset_date
                    severity
                    category
                    notes
                    diagnosed_by {{
                        name
                        provider_id
                        specialty
                    }}
                }}
                
                has_observation {{
                    uid
                    observation_type
                    value
                    unit
                    status
                    effective_time
                    issued_time
                    category
                    code
                    interpretation
                    notes
                    performed_by {{
                        name
                        provider_id
                        specialty
                    }}
                }}
                
                has_procedure {{
                    uid
                    procedure_type
                    status
                    start_time
                    end_time
                    duration
                    reason
                    outcome
                    notes
                    performed_by {{
                        name
                        provider_id
                        specialty
                    }}
                    performed_at {{
                        street
                        city
                        state
                        zip_code
                        country
                    }}
                }}
                
                lives_in {{
                    uid
                    street
                    city
                    state
                    zip_code
                    country
                    timezone
                }}
                
                treated_by {{
                    uid
                    name
                    provider_id
                    specialty
                    role
                    organization
                    works_at {{
                        name
                        organization_id
                        type
                        phone
                        website
                        specialties
                        located_at {{
                            street
                            city
                            state
                            zip_code
                            country
                        }}
                    }}
                }}
                
                has_contact_info {{
                    uid
                    phone_number
                    email
                    preferred_language
                    communication_preferences
                }}
                
                has_social_history {{
                    uid
                    employment_status
                    education_level
                    insurance_provider
                    annual_income
                    housing_status
                    transportation_access
                    social_support_level
                    risk_factors
                    lifestyle_factors
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
                print(f"  💉 Immunizations: {len(patient.get('has_immunization', []))}")
                print(f"  🏥 Conditions: {len(patient.get('has_condition', []))}")
                print(f"  🔬 Observations: {len(patient.get('has_observation', []))}")
                print(f"  🏥 Procedures: {len(patient.get('has_procedure', []))}")
                print(f"  🏠 Address: {'Yes' if patient.get('lives_in') else 'No'}")
                print(f"  👨‍⚕️ Providers: {len(patient.get('treated_by', []))}")
                print(f"  📞 Contact Info: {'Yes' if patient.get('has_contact_info') else 'No'}")
                print(f"  📊 Social History: {'Yes' if patient.get('has_social_history') else 'No'}")
    
    except Exception as e:
        print(f"❌ Error: {e}")
    finally:
        importer.close()

if __name__ == "__main__":
    main()
