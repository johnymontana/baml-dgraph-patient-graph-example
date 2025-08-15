#!/usr/bin/env python3
"""
Setup script for Medical Data Extraction with BAML and DGraph
"""

import os
import subprocess
import sys
from pathlib import Path

def run_command(command, description):
    """Run a shell command and handle errors."""
    print(f"🔄 {description}...")
    try:
        result = subprocess.run(command, shell=True, check=True, capture_output=True, text=True)
        print(f"✅ {description} completed successfully")
        return True
    except subprocess.CalledProcessError as e:
        print(f"❌ {description} failed: {e}")
        if e.stdout:
            print(f"STDOUT: {e.stdout}")
        if e.stderr:
            print(f"STDERR: {e.stderr}")
        return False

def check_prerequisites():
    """Check if required tools are installed."""
    print("🔍 Checking prerequisites...")
    
    # Check Python version
    if sys.version_info < (3, 8):
        print("❌ Python 3.8+ is required")
        return False
    
    # Check if uv is installed
    try:
        subprocess.run(["uv", "--version"], check=True, capture_output=True)
        print("✅ uv found")
    except (subprocess.CalledProcessError, FileNotFoundError):
        print("❌ uv not found. Please install with: curl -LsSf https://astral.sh/uv/install.sh | sh")
        return False
    
    # Check if BAML CLI is installed
    try:
        subprocess.run(["baml-cli", "--version"], check=True, capture_output=True)
        print("✅ BAML CLI found")
    except (subprocess.CalledProcessError, FileNotFoundError):
        print("❌ BAML CLI not found. Please install with: npm install -g @boundaryml/baml")
        return False
    
    return True

def setup_python_dependencies():
    """Install Python dependencies using uv."""
    print("📦 Installing Python dependencies with uv...")
    
    # Create virtual environment and install dependencies
    if not run_command("uv sync", "Installing dependencies with uv sync"):
        return False
    
    return True

def setup_baml_project():
    """Initialize BAML project and generate client."""
    print("🚀 Setting up BAML project...")
    
    # Check if baml_src directory exists
    if not Path("baml_src").exists():
        print("❌ baml_src directory not found")
        return False
    
    # Initialize BAML project
    if not run_command("baml-cli init", "Initializing BAML project"):
        return False
    
    # Generate Python client
    if not run_command("baml-cli generate", "Generating BAML client"):
        return False
    
    return True

def create_env_file():
    """Create .env file from template if it doesn't exist."""
    env_file = Path(".env")
    env_example = Path("env.example")
    
    if env_file.exists():
        print("✅ .env file already exists")
        return True
    
    if not env_example.exists():
        print("❌ env.example not found")
        return False
    
    try:
        with open(env_example, 'r') as f:
            content = f.read()
        
        with open(env_file, 'w') as f:
            f.write(content)
        
        print("✅ Created .env file from template")
        print("⚠️  Please edit .env file with your actual API keys and DGraph connection string")
        return True
    except Exception as e:
        print(f"❌ Failed to create .env file: {e}")
        return False

def main():
    """Main setup function."""
    print("🏥 Medical Data Extraction Setup")
    print("=" * 40)
    
    # Check prerequisites
    if not check_prerequisites():
        print("\n❌ Setup failed: Prerequisites not met")
        sys.exit(1)
    
    # Setup Python dependencies
    if not setup_python_dependencies():
        print("\n❌ Setup failed: Python dependencies installation failed")
        sys.exit(1)
    
    # Setup BAML project
    if not setup_baml_project():
        print("\n❌ Setup failed: BAML project setup failed")
        sys.exit(1)
    
    # Create .env file
    if not create_env_file():
        print("\n❌ Setup failed: Environment file creation failed")
        sys.exit(1)
    
    print("\n🎉 Setup completed successfully!")
    print("\nNext steps:")
    print("1. Edit .env file with your API keys and DGraph connection string")
    print("2. Start DGraph (if not running): docker run -it -p 8080:8080 -p 9080:9080 dgraph/standalone:latest")
    print("3. Activate virtual environment: source .venv/bin/activate (or uv run)")
    print("4. Run extraction: uv run python scripts/extract_medical_data.py")
    print("5. Import to DGraph: uv run python scripts/dgraph_importer.py")
    print("\nFor more information, see README.md")

if __name__ == "__main__":
    main()
