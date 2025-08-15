#!/bin/bash
# Quick start script for Medical Data Extraction with BAML and DGraph
# Uses uv for fast dependency management

set -e

echo "🏥 Medical Data Extraction - Quick Start"
echo "========================================"

# Check if uv is installed
if ! command -v uv &> /dev/null; then
    echo "❌ uv not found. Installing uv..."
    curl -LsSf https://astral.sh/uv/install.sh | sh
    echo "✅ uv installed. Please restart your terminal or run: source ~/.bashrc"
    exit 1
fi

echo "✅ uv found: $(uv --version)"

# Check if BAML CLI is installed
if ! command -v baml-cli &> /dev/null; then
    echo "❌ BAML CLI not found. Installing BAML CLI..."
    npm install -g @boundaryml/baml
fi

echo "✅ BAML CLI found: $(baml-cli --version)"

# Install dependencies
echo "📦 Installing dependencies with uv..."
uv sync

# Initialize BAML project
echo "🚀 Initializing BAML project..."
baml-cli init
baml-cli generate

# Create .env file if it doesn't exist
if [ ! -f .env ]; then
    echo "📝 Creating .env file from template..."
    cp env.example .env
    echo "⚠️  Please edit .env file with your API keys and DGraph connection string"
else
    echo "✅ .env file already exists"
fi

# Run tests
echo "🧪 Running setup tests..."
uv run python test_setup.py

echo ""
echo "🎉 Quick start completed!"
echo ""
echo "Next steps:"
echo "1. Edit .env file with your API keys and DGraph connection string"
echo "2. Start DGraph: make start-dgraph"
echo "3. Run extraction: make extract"
echo "4. Import to DGraph: make import"
echo ""
echo "Or run the complete pipeline: make pipeline"
echo ""
echo "For help: make help"
