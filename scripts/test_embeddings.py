#!/usr/bin/env python3
"""
Test script to demonstrate vector embedding functionality.
This script shows how text would be converted to embeddings using Ollama.
"""

import json
import requests
import time

def test_ollama_connection(ollama_url: str = "http://localhost:11434"):
    """Test Ollama connection and available models."""
    print("🤖 Testing Ollama Connection")
    print("=" * 40)
    
    try:
        # Check if Ollama is running
        response = requests.get(f"{ollama_url}/api/tags")
        if response.status_code == 200:
            models = response.json().get('models', [])
            print(f"✅ Ollama is running at {ollama_url}")
            print(f"📚 Available models: {len(models)}")
            
            for model in models:
                name = model.get('name', 'Unknown')
                size = model.get('size', 0)
                size_mb = size / (1024 * 1024) if size > 0 else 0
                print(f"  📖 {name} ({size_mb:.1f} MB)")
            
            return True
        else:
            print(f"❌ Ollama API error: {response.status_code}")
            return False
            
    except Exception as e:
        print(f"❌ Connection failed: {e}")
        print(f"   Make sure Ollama is running at {ollama_url}")
        return False

def test_embedding_generation(ollama_url: str = "http://localhost:11434", model: str = "nomic-embed-text"):
    """Test embedding generation with sample medical text."""
    print(f"\n🧠 Testing Embedding Generation with {model}")
    print("=" * 50)
    
    # Sample medical text for testing
    test_texts = [
        "Patient: John Smith, Age: 45, Gender: Male, Marital Status: Married",
        "Medical Visit: Annual Checkup, Status: Completed, Reason: Preventive Care",
        "Allergy: Peanuts, Severity: Severe, Reaction: Anaphylaxis, Status: Active",
        "Immunization: Influenza Vaccine, Type: Injectable, Status: Administered",
        "Condition: Hypertension, Status: Chronic, Severity: Moderate"
    ]
    
    for i, text in enumerate(test_texts, 1):
        print(f"\n📝 Test {i}: {text[:60]}...")
        
        try:
            # Generate embedding
            payload = {
                "model": model,
                "prompt": text
            }
            
            response = requests.post(f"{ollama_url}/api/embeddings", json=payload)
            
            if response.status_code == 200:
                result = response.json()
                embedding = result.get('embedding', [])
                
                if embedding:
                    print(f"  ✅ Generated embedding: {len(embedding)} dimensions")
                    print(f"  📊 First 5 values: {embedding[:5]}")
                    print(f"  📊 Last 5 values: {embedding[-5:]}")
                else:
                    print(f"  ❌ No embedding returned")
                    
            else:
                print(f"  ❌ API error: {response.status_code}")
                
        except Exception as e:
            print(f"  ❌ Error: {e}")
        
        # Small delay between requests
        time.sleep(0.5)

def test_vector_similarity():
    """Demonstrate how vector similarity would work."""
    print(f"\n🔍 Vector Similarity Concepts")
    print("=" * 40)
    
    print("📚 How Vector Embeddings Enable Semantic Search:")
    print("  1. Text is converted to high-dimensional vectors")
    print("  2. Similar concepts have similar vector representations")
    print("  3. Distance between vectors indicates semantic similarity")
    print("  4. Enables finding related medical concepts")
    
    print("\n💡 Medical Use Cases:")
    print("  • Find patients with similar symptoms")
    print("  • Discover related medical conditions")
    print("  • Identify similar treatment patterns")
    print("  • Semantic search across medical notes")
    print("  • Find relevant medical literature")
    
    print("\n🔬 Technical Benefits:")
    print("  • Handles synonyms and related terms")
    print("  • Language-agnostic (works with medical jargon)")
    print("  • Scalable to large medical datasets")
    print("  • Enables advanced AI-powered medical insights")

def main():
    """Main test function."""
    print("🧠 Vector Embedding Test Suite")
    print("=" * 50)
    
    # Test 1: Ollama connection
    if not test_ollama_connection():
        print("\n⚠️ Ollama connection failed. Please:")
        print("   1. Install Ollama: https://ollama.ai/")
        print("   2. Start Ollama: ollama serve")
        print("   3. Pull a model: ollama pull nomic-embed-text")
        return
    
    # Test 2: Embedding generation
    test_embedding_generation()
    
    # Test 3: Vector similarity concepts
    test_vector_similarity()
    
    print(f"\n🎉 All tests completed!")
    print(f"\n💡 Next steps:")
    print(f"   • Run: make add-embeddings")
    print(f"   • Query embeddings in DGraph")
    print(f"   • Build semantic search applications")

if __name__ == "__main__":
    main()
