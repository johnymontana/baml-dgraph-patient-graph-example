#!/usr/bin/env python3
"""
Simple script to test DGraph schema setup.
"""

import os
import pydgraph
from pydgraph import DgraphClient

def test_schema():
    """Test if the DGraph schema can be set up correctly."""
    
    # Get connection string from environment
    dgraph_url = os.getenv('DGRAPH_CONNECTION_STRING')
    if not dgraph_url:
        print("❌ DGRAPH_CONNECTION_STRING not set in environment")
        return False
    
    try:
        print(f"🔗 Connecting to DGraph: {dgraph_url}")
        client = DgraphClient(dgraph_url)
        
        # Test basic connection
        print("✅ Connected to DGraph successfully")
        
        # Test schema setup
        schema = """
        # Test schema with basic predicates
        name: string @index(term) .
        age: int @index(int) .
        latitude: float @index(float) .
        longitude: float @index(float) .
        geo: geo @index(geo) .
        """
        
        print("🔧 Testing schema setup...")
        op = pydgraph.Operation(schema=schema)
        client.alter(op)
        print("✅ Schema setup successful!")
        
        return True
        
    except Exception as e:
        print(f"❌ Schema test failed: {e}")
        return False

if __name__ == "__main__":
    test_schema()
