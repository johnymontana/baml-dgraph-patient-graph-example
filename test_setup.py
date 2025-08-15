#!/usr/bin/env python3
"""
Test script to verify the medical data extraction project setup
"""

import os
import sys
from pathlib import Path
from dotenv import load_dotenv

def test_imports():
    """Test if all required modules can be imported."""
    print("🔍 Testing imports...")
    
    try:
        import pydgraph
        print("✅ pydgraph imported successfully")
    except ImportError as e:
        print(f"❌ Failed to import pydgraph: {e}")
        return False
    
    try:
        from dotenv import load_dotenv
        print("✅ python-dotenv imported successfully")
    except ImportError as e:
        print(f"❌ Failed to import python-dotenv: {e}")
        return False
    
    try:
        # Try to import BAML client (may not exist yet)
        import baml_client
        print("✅ baml_client imported successfully")
    except ImportError:
        print("⚠️  baml_client not found (run 'baml generate' first)")
    
    return True

def test_environment():
    """Test environment variables."""
    print("\n🔍 Testing environment...")
    
    load_dotenv()
    
    # Check required environment variables
    required_vars = ['OPENAI_API_KEY', 'DGRAPH_CONNECTION_STRING']
    missing_vars = []
    
    for var in required_vars:
        if not os.getenv(var):
            missing_vars.append(var)
    
    if missing_vars:
        print(f"❌ Missing environment variables: {', '.join(missing_vars)}")
        print("Please set these in your .env file")
        return False
    
    print("✅ Environment variables configured")
    
    # Test DGraph connection string format
    dgraph_conn = os.getenv('DGRAPH_CONNECTION_STRING')
    if dgraph_conn.startswith('dgraph://'):
        print(f"✅ DGraph connection string format: {dgraph_conn}")
    else:
        print(f"⚠️  DGraph connection string format: {dgraph_conn}")
    
    return True

def test_file_structure():
    """Test if all required files exist."""
    print("\n🔍 Testing file structure...")
    
    required_files = [
        'baml_src/main.baml',
        'baml_src/medical_models.baml',
        'baml_src/clients.baml',
        'scripts/extract_medical_data.py',
        'scripts/dgraph_importer.py',
        'scripts/sample_medical_text.py',
        'pyproject.toml',
        'env.example',
        'README.md'
    ]
    
    missing_files = []
    
    for file_path in required_files:
        if not Path(file_path).exists():
            missing_files.append(file_path)
    
    if missing_files:
        print(f"❌ Missing files: {', '.join(missing_files)}")
        return False
    
    print("✅ All required files present")
    return True

def test_uv_environment():
    """Test uv virtual environment."""
    print("\n🔍 Testing uv environment...")
    
    # Check if .venv directory exists
    venv_path = Path(".venv")
    if not venv_path.exists():
        print("❌ Virtual environment not found. Run 'uv sync' first.")
        return False
    
    # Check if uv.lock exists
    lock_path = Path("uv.lock")
    if not lock_path.exists():
        print("❌ uv.lock not found. Run 'uv sync' first.")
        return False
    
    print("✅ uv virtual environment configured")
    return True

def test_dgraph_connection():
    """Test DGraph connection."""
    print("\n🔍 Testing DGraph connection...")
    
    load_dotenv()
    dgraph_conn = os.getenv('DGRAPH_CONNECTION_STRING')
    
    if not dgraph_conn:
        print("❌ DGRAPH_CONNECTION_STRING not set in environment")
        return False
    
    try:
        import pydgraph
        
        # Use pydgraph.open() directly with the connection string
        # This matches the working approach from dgraph_importer.py
        print(f"🔌 Testing connection to: {dgraph_conn}")
        
        client = pydgraph.open(dgraph_conn)
        
        # Try a simple query to test connection
        txn = client.txn(read_only=True)
        try:
            # Use a valid DQL query - query for any node with dgraph.type
            result = txn.query("""
            {
                test(func: has(dgraph.type)) {
                    count(uid)
                }
            }
            """)
            print("✅ DGraph connection successful")
            return True
        finally:
            txn.discard()
            
    except Exception as e:
        print(f"❌ DGraph connection failed: {e}")
        print("Make sure DGraph is running and accessible")
        print("Note: Connection string format should be: dgraph://host:port or dgraph://host:port?params")
        return False

def main():
    """Run all tests."""
    print("🧪 Medical Data Extraction Project - Setup Test")
    print("=" * 50)
    
    tests = [
        ("Import Tests", test_imports),
        ("Environment Tests", test_environment),
        ("File Structure Tests", test_file_structure),
        ("UV Environment Tests", test_uv_environment),
        ("DGraph Connection Tests", test_dgraph_connection)
    ]
    
    passed = 0
    total = len(tests)
    
    for test_name, test_func in tests:
        print(f"\n📋 Running {test_name}")
        print("-" * 30)
        
        try:
            if test_func():
                passed += 1
            else:
                print(f"❌ {test_name} failed")
        except Exception as e:
            print(f"❌ {test_name} failed with error: {e}")
    
    print(f"\n📊 Test Results: {passed}/{total} tests passed")
    
    if passed == total:
        print("🎉 All tests passed! Project is ready to use.")
        print("\nNext steps:")
        print("1. Run extraction: uv run python scripts/extract_medical_data.py")
        print("2. Import to DGraph: uv run python scripts/dgraph_importer.py")
    else:
        print("⚠️  Some tests failed. Please check the errors above.")
        print("\nCommon solutions:")
        print("- Install missing dependencies: uv sync")
        print("- Set up environment variables in .env file")
        print("- Start DGraph: docker run -it -p 8080:8080 -p 9080:9080 dgraph/standalone:latest")
        print("- Generate BAML client: baml generate")
    
    return passed == total

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
