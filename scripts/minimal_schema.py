#!/usr/bin/env python3
"""
Minimal DGraph schema for testing.
"""

import os
import pydgraph
from dotenv import load_dotenv

def test_minimal_schema():
    """Test a minimal schema to isolate the issue."""
    
    # Load environment variables
    load_dotenv(verbose=False)
    
    # Get DGraph connection
    dgraph_url = os.getenv('DGRAPH_CONNECTION_STRING', 'dgraph://localhost:9080')
    print(f"🔗 Testing minimal schema with: {dgraph_url}")
    
    try:
        # Connect to DGraph
        client = pydgraph.open(dgraph_url)
        print("✅ Connected to DGraph")
        
        # Minimal schema with just essential types and predicates
        minimal_schema = """
        # Minimal Medical Schema
        
        # Basic types
        type Patient {
            name: string
            age: int
            gender: string
        }
        
        type Address {
            street: string
            city: string
            state: string
            zip_code: string
            country: string
            latitude: float
            longitude: float
        }
        
        type MedicalVisit {
            visit_type: string
            start_time: string
            status: string
        }
        
        # Basic predicates
        name: string @index(term) .
        age: int @index(int) .
        gender: string @index(exact) .
        street: string @index(exact) .
        city: string @index(exact) .
        state: string @index(exact) .
        zip_code: string @index(exact) .
        country: string @index(exact) .
        latitude: float @index(float) .
        longitude: float @index(float) .
        visit_type: string @index(exact) .
        start_time: string @index(exact) .
        status: string @index(exact) .
        
        # Basic relationships
        has_visit: [uid] .
        lives_in: [uid] .
        visit_of: [uid] .
        location_of_patient: [uid] .
        """
        
        print("🔧 Testing minimal schema...")
        op = pydgraph.Operation(schema=minimal_schema)
        client.alter(op)
        print("✅ Minimal schema applied successfully")
        
        return True
        
    except Exception as e:
        print(f"❌ Minimal schema failed: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    test_minimal_schema()
