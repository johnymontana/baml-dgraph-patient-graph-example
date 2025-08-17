#!/usr/bin/env python3
"""
Simple script to test DGraph schema setup in isolation.
"""

import os
import pydgraph
from dotenv import load_dotenv

def test_schema_setup():
    """Test just the schema setup to isolate issues."""
    
    # Load environment variables
    load_dotenv(verbose=False)
    
    # Get DGraph connection
    dgraph_url = os.getenv('DGRAPH_CONNECTION_STRING', 'dgraph://localhost:9080')
    print(f"🔗 Testing schema setup with: {dgraph_url}")
    
    try:
        # Connect to DGraph
        client = pydgraph.open(dgraph_url)
        print("✅ Connected to DGraph")
        
        # Test basic schema
        simple_schema = """
        # Test schema
        name: string @index(term) .
        age: int @index(int) .
        """
        
        print("🔧 Testing simple schema...")
        op = pydgraph.Operation(schema=simple_schema)
        client.alter(op)
        print("✅ Simple schema applied successfully")
        
        # Test the actual schema from dgraph_importer
        print("\n🔧 Testing full medical schema...")
        
        # Import the schema from dgraph_importer
        import sys
        sys.path.append('.')
        from scripts.dgraph_importer import DGraphMedicalImporter
        
        importer = DGraphMedicalImporter()
        print("✅ Full schema setup completed")
        
    except Exception as e:
        print(f"❌ Schema setup failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_schema_setup()
