#!/usr/bin/env python3
"""
Helper script to demonstrate uv usage for the medical extraction project.
This script can be run directly with: uv run python scripts/run_with_uv.py
"""

import sys
import subprocess
from pathlib import Path

def run_command(command, description):
    """Run a shell command and handle errors."""
    print(f"🔄 {description}...")
    try:
        result = subprocess.run(command, shell=True, check=True, capture_output=True, text=True)
        print(f"✅ {description} completed successfully")
        if result.stdout.strip():
            print(f"Output: {result.stdout.strip()}")
        return True
    except subprocess.CalledProcessError as e:
        print(f"❌ {description} failed: {e}")
        if e.stdout:
            print(f"STDOUT: {e.stdout}")
        if e.stderr:
            print(f"STDERR: {e.stderr}")
        return False

def check_uv_installation():
    """Check if uv is properly installed."""
    print("🔍 Checking uv installation...")
    
    try:
        result = subprocess.run(["uv", "--version"], capture_output=True, text=True, check=True)
        print(f"✅ uv version: {result.stdout.strip()}")
        return True
    except (subprocess.CalledProcessError, FileNotFoundError):
        print("❌ uv not found. Please install with: curl -LsSf https://astral.sh/uv/install.sh | sh")
        return False

def check_virtual_environment():
    """Check if virtual environment exists."""
    print("\n🔍 Checking virtual environment...")
    
    venv_path = Path(".venv")
    if not venv_path.exists():
        print("❌ Virtual environment not found. Run 'uv sync' first.")
        return False
    
    print("✅ Virtual environment found at .venv/")
    return True

def check_dependencies():
    """Check if dependencies are installed."""
    print("\n🔍 Checking dependencies...")
    
    try:
        import pydgraph
        print("✅ pydgraph available")
    except ImportError:
        print("❌ pydgraph not available. Run 'uv sync' first.")
        return False
    
    try:
        from dotenv import load_dotenv
        print("✅ python-dotenv available")
    except ImportError:
        print("❌ python-dotenv not available. Run 'uv sync' first.")
        return False
    
    return True

def demonstrate_uv_commands():
    """Demonstrate common uv commands."""
    print("\n🚀 Demonstrating uv commands...")
    
    commands = [
        ("uv --version", "Check uv version"),
        ("uv sync", "Sync dependencies"),
        ("uv run python -c 'import pydgraph; print(\"pydgraph imported successfully\")'", "Test dependency import"),
        ("uv add --help", "Show add command help"),
        ("uv remove --help", "Show remove command help"),
    ]
    
    for command, description in commands:
        print(f"\n📋 {description}")
        print(f"Command: {command}")
        if not run_command(command, description):
            print("⚠️  Command failed, continuing...")

def main():
    """Main function to demonstrate uv usage."""
    print("🐍 UV Package Manager Demonstration")
    print("=" * 40)
    
    # Check prerequisites
    if not check_uv_installation():
        print("\n❌ uv is not properly installed. Please install it first.")
        return False
    
    # Check virtual environment
    if not check_virtual_environment():
        print("\n⚠️  Virtual environment not found. Run 'uv sync' to create it.")
        print("Example: uv sync")
        return False
    
    # Check dependencies
    if not check_dependencies():
        print("\n⚠️  Dependencies not installed. Run 'uv sync' to install them.")
        print("Example: uv sync")
        return False
    
    # Demonstrate commands
    demonstrate_uv_commands()
    
    print("\n🎉 UV demonstration completed!")
    print("\nCommon uv commands for this project:")
    print("  uv sync                    - Install dependencies")
    print("  uv sync --extra dev        - Install dev dependencies")
    print("  uv run python script.py    - Run script in virtual environment")
    print("  uv add package_name        - Add new dependency")
    print("  uv remove package_name     - Remove dependency")
    print("  source .venv/bin/activate  - Activate virtual environment")
    
    return True

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
