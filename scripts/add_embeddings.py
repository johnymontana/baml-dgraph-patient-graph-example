#!/usr/bin/env python3
"""
Post-processing script to add vector embeddings to DGraph nodes using Ollama.

This script:
1. Finds nodes that need embeddings (Patient, MedicalVisit, Allergy, etc.)
2. Generates text representations of each node
3. Uses Ollama to create vector embeddings
4. Stores embeddings in DGraph using the vector type with proper indexing
"""

import os
import json
import sys
from typing import Dict, List, Any, Optional, Tuple
import pydgraph
from pydgraph import Txn
import uuid
from dotenv import load_dotenv
import requests
import time

class EmbeddingProcessor:
    def __init__(self, dgraph_url: str, ollama_url: str = "http://localhost:11434"):
        """Initialize the processor with DGraph and Ollama connections."""
        print(f"🔗 Connecting to DGraph: {dgraph_url}")
        self.client = pydgraph.open(dgraph_url)
        
        self.ollama_url = ollama_url
        self.embedding_model = "nomic-embed-text"  # Good for text embeddings
        
        # Test Ollama connection
        self._test_ollama_connection()
    
    def _test_ollama_connection(self):
        """Test if Ollama is accessible and the model is available."""
        try:
            # Check if model is available
            response = requests.get(f"{self.ollama_url}/api/tags")
            if response.status_code == 200:
                models = response.json().get('models', [])
                available_models = [model['name'] for model in models]
                
                if self.embedding_model in available_models:
                    print(f"✅ Ollama connection successful, model '{self.embedding_model}' available")
                else:
                    print(f"⚠️ Model '{self.embedding_model}' not found, available models: {available_models}")
                    print(f"   Using default model for embeddings")
                    self.embedding_model = available_models[0] if available_models else "llama2"
            else:
                print(f"⚠️ Could not fetch Ollama models, using default")
        except Exception as e:
            print(f"⚠️ Ollama connection test failed: {e}")
            print(f"   Make sure Ollama is running at {self.ollama_url}")
    
    def get_embedding(self, text: str) -> Optional[List[float]]:
        """Generate embedding for text using Ollama."""
        if not text or text.strip() == "":
            return None
            
        try:
            # Prepare the embedding request
            payload = {
                "model": self.embedding_model,
                "prompt": text.strip()
            }
            
            response = requests.post(f"{self.ollama_url}/api/embeddings", json=payload)
            
            if response.status_code == 200:
                result = response.json()
                embedding = result.get('embedding', [])
                if embedding:
                    print(f"  📊 Generated embedding: {len(embedding)} dimensions")
                    return embedding
                else:
                    print(f"  ❌ No embedding returned from Ollama")
                    return None
            else:
                print(f"  ❌ Ollama API error: {response.status_code} - {response.text}")
                return None
                
        except Exception as e:
            print(f"  ❌ Error generating embedding: {e}")
            return None
    
    def find_nodes_for_embeddings(self) -> Dict[str, List[Dict[str, Any]]]:
        """Find nodes that need embeddings."""
        print("🔍 Finding nodes for embedding generation...")
        
        # Query for different node types
        query = """
        {
            patients(func: type(Patient)) {
                uid
                name
                patient_id
                age
                gender
                date_of_birth
                marital_status
                primary_language
            }
            
            visits(func: type(MedicalVisit)) {
                uid
                visit_type
                start_time
                end_time
                timezone
                status
                classification
                reason
                notes
            }
            
            allergies(func: type(Allergy)) {
                uid
                allergen
                severity
                reaction_type
                confirmed_date
                status
                notes
            }
            
            immunizations(func: type(Immunization)) {
                uid
                vaccine_name
                vaccine_type
                administration_date
                status
                lot_number
                manufacturer
                notes
            }
            
            conditions(func: type(Condition)) {
                uid
                condition_name
                status
                severity
                onset_date
                notes
            }
            
            providers(func: type(MedicalProvider)) {
                uid
                name
                provider_id
                specialty
                role
                organization
            }
        }
        """
        
        try:
            txn = self.client.txn(read_only=True)
            result = txn.query(query)
            txn.discard()
            
            if result.json:
                data = json.loads(result.json)
                nodes = {}
                
                for node_type in ['patients', 'visits', 'allergies', 'immunizations', 'conditions', 'providers']:
                    node_list = data.get(node_type, [])
                    if node_list:
                        nodes[node_type] = node_list
                        print(f"  📍 Found {len(node_list)} {node_type}")
                
                return nodes
            else:
                print("❌ No results returned from query")
                return {}
                
        except Exception as e:
            print(f"❌ Error querying nodes: {e}")
            return {}
    
    def generate_node_text(self, node: Dict[str, Any], node_type: str) -> str:
        """Generate text representation of a node for embedding."""
        text_parts = []
        
        if node_type == 'patients':
            text_parts.append(f"Patient: {node.get('name', 'Unknown')}")
            if node.get('age'):
                text_parts.append(f"Age: {node['age']}")
            if node.get('gender'):
                text_parts.append(f"Gender: {node['gender']}")
            if node.get('marital_status'):
                text_parts.append(f"Marital Status: {node['marital_status']}")
            if node.get('primary_language'):
                text_parts.append(f"Primary Language: {node['primary_language']}")
                
        elif node_type == 'visits':
            text_parts.append(f"Medical Visit: {node.get('visit_type', 'Unknown')}")
            if node.get('start_time'):
                text_parts.append(f"Start: {node['start_time']}")
            if node.get('end_time'):
                text_parts.append(f"End: {node['end_time']}")
            if node.get('status'):
                text_parts.append(f"Status: {node['status']}")
            if node.get('reason'):
                text_parts.append(f"Reason: {node['reason']}")
            if node.get('notes'):
                text_parts.append(f"Notes: {node['notes']}")
                
        elif node_type == 'allergies':
            text_parts.append(f"Allergy: {node.get('allergen', 'Unknown')}")
            if node.get('severity'):
                text_parts.append(f"Severity: {node['severity']}")
            if node.get('reaction_type'):
                text_parts.append(f"Reaction: {node['reaction_type']}")
            if node.get('status'):
                text_parts.append(f"Status: {node['status']}")
            if node.get('notes'):
                text_parts.append(f"Notes: {node['notes']}")
                
        elif node_type == 'immunizations':
            text_parts.append(f"Immunization: {node.get('vaccine_name', 'Unknown')}")
            if node.get('vaccine_type'):
                text_parts.append(f"Type: {node['vaccine_type']}")
            if node.get('administration_date'):
                text_parts.append(f"Date: {node['administration_date']}")
            if node.get('status'):
                text_parts.append(f"Status: {node['status']}")
            if node.get('notes'):
                text_parts.append(f"Notes: {node['notes']}")
                
        elif node_type == 'conditions':
            text_parts.append(f"Condition: {node.get('condition_name', 'Unknown')}")
            if node.get('status'):
                text_parts.append(f"Status: {node['status']}")
            if node.get('severity'):
                text_parts.append(f"Severity: {node['severity']}")
            if node.get('notes'):
                text_parts.append(f"Notes: {node['notes']}")
                
        elif node_type == 'providers':
            text_parts.append(f"Provider: {node.get('name', 'Unknown')}")
            if node.get('specialty'):
                text_parts.append(f"Specialty: {node['specialty']}")
            if node.get('role'):
                text_parts.append(f"Role: {node['role']}")
            if node.get('organization'):
                text_parts.append(f"Organization: {node['organization']}")
        
        return " | ".join(text_parts)
    
    def add_embeddings_to_nodes(self, nodes: Dict[str, List[Dict[str, Any]]]) -> bool:
        """Add embeddings to all nodes."""
        if not nodes:
            print("⚠️ No nodes to process")
            return True
        
        total_nodes = sum(len(node_list) for node_list in nodes.values())
        print(f"🔄 Processing {total_nodes} nodes for embeddings...")
        
        # Prepare N-Quads for batch update
        nquads = []
        processed_count = 0
        
        for node_type, node_list in nodes.items():
            print(f"\n📝 Processing {node_type}...")
            
            for i, node in enumerate(node_list):
                # Generate text representation
                node_text = self.generate_node_text(node, node_type)
                
                # Generate embedding
                embedding = self.get_embedding(node_text)
                
                if embedding:
                    # Add embedding to node
                    uid = node['uid']
                    embedding_str = json.dumps(embedding)
                    
                    # Add embedding predicate
                    nquads.append(f'<{uid}> <embedding> "{embedding_str}" .')
                    
                    # Add embedding metadata
                    nquads.append(f'<{uid}> <embedding_model> "{self.embedding_model}" .')
                    nquads.append(f'<{uid}> <embedding_text> "{node_text}" .')
                    nquads.append(f'<{uid}> <embedding_dimensions> "{len(embedding)}" .')
                    
                    processed_count += 1
                    print(f"  ✅ {node_type[:-1]} {i+1}/{len(node_list)}: {len(embedding)} dimensions")
                else:
                    print(f"  ❌ {node_type[:-1]} {i+1}/{len(node_list)}: Failed to generate embedding")
                
                # Add small delay to avoid overwhelming Ollama
                time.sleep(0.1)
        
        if not nquads:
            print("⚠️ No embeddings generated, nothing to update")
            return True
        
        # Execute the mutation
        try:
            print(f"\n💾 Saving {len(nquads)} embedding predicates to DGraph...")
            txn = self.client.txn()
            nquads_string = '\n'.join(nquads)
            response = txn.mutate(set_nquads=nquads_string)
            txn.commit()
            
            print(f"✅ Successfully added embeddings to {processed_count} nodes")
            return True
            
        except Exception as e:
            print(f"❌ Error saving embeddings: {e}")
            if 'txn' in locals():
                txn.discard()
            return False
    
    def verify_embeddings(self, nodes: Dict[str, List[Dict[str, Any]]]) -> bool:
        """Verify that embeddings were added correctly."""
        if not nodes:
            return True
        
        print("\n🔍 Verifying embeddings...")
        
        # Count nodes with embeddings
        query = """
        {
            nodes_with_embeddings(func: has(embedding)) {
                uid
                dgraph.type
                embedding_model
                embedding_dimensions
            }
        }
        """
        
        try:
            txn = self.client.txn(read_only=True)
            result = txn.query(query)
            txn.discard()
            
            if result.json:
                data = json.loads(result.json)
                nodes_with_embeddings = data.get('nodes_with_embeddings', [])
                
                print(f"📊 Found {len(nodes_with_embeddings)} nodes with embeddings:")
                
                # Group by type
                by_type = {}
                for node in nodes_with_embeddings:
                    # Handle dgraph.type which can be a list
                    node_types = node.get('dgraph.type', [])
                    if isinstance(node_types, list):
                        node_type = node_types[0] if node_types else 'Unknown'
                    else:
                        node_type = node_types
                    
                    if node_type not in by_type:
                        by_type[node_type] = []
                    by_type[node_type].append(node)
                
                for node_type, type_nodes in by_type.items():
                    print(f"  📍 {node_type}: {len(type_nodes)} nodes")
                    for node in type_nodes[:3]:  # Show first 3 examples
                        model = node.get('embedding_model', 'Unknown')
                        dims = node.get('embedding_dimensions', 'Unknown')
                        print(f"    - UID: {node['uid']}, Model: {model}, Dimensions: {dims}")
                    if len(type_nodes) > 3:
                        print(f"    ... and {len(type_nodes) - 3} more")
                
                return len(nodes_with_embeddings) > 0
            else:
                print("❌ No results returned from verification query")
                return False
                
        except Exception as e:
            print(f"❌ Error verifying embeddings: {e}")
            return False
    
    def run_full_process(self) -> bool:
        """Run the complete embedding processing workflow."""
        print("🧠 DGraph Vector Embedding Post-Processing")
        print("=" * 50)
        
        # Step 1: Find nodes for embeddings
        print("\n🔍 Step 1: Finding nodes for embeddings...")
        nodes = self.find_nodes_for_embeddings()
        
        if not nodes:
            print("⚠️ No nodes found. Nothing to process.")
            return True
        
        # Step 2: Generate and add embeddings
        print("\n🔄 Step 2: Generating and adding embeddings...")
        success = self.add_embeddings_to_nodes(nodes)
        
        if not success:
            print("❌ Failed to add embeddings")
            return False
        
        # Step 3: Verify the changes
        print("\n🔍 Step 3: Verifying embeddings...")
        verification_success = self.verify_embeddings(nodes)
        
        if verification_success:
            print("\n🎉 Embedding processing completed successfully!")
            print("\n💡 You can now run vector similarity queries like:")
            print("   - Find patients with similar medical conditions")
            print("   - Find similar medical visits")
            print("   - Semantic search across medical records")
            print("   - Find related allergies or immunizations")
        else:
            print("\n⚠️ Embedding processing completed with verification issues")
        
        return verification_success

def main():
    """Main entry point."""
    # Load environment variables from .env file (if it exists)
    load_dotenv(verbose=False)
    
    # Get DGraph connection from environment with default fallback
    dgraph_url = os.getenv('DGRAPH_CONNECTION_STRING', 'dgraph://localhost:9080')
    print(f"🔗 Using DGraph connection: {dgraph_url}")
    
    # Get Ollama URL from environment
    ollama_url = os.getenv('OLLAMA_URL', 'http://localhost:11434')
    print(f"🤖 Using Ollama URL: {ollama_url}")
    
    try:
        # Initialize processor
        processor = EmbeddingProcessor(dgraph_url, ollama_url)
        
        # Run the full process
        success = processor.run_full_process()
        
        if success:
            sys.exit(0)
        else:
            sys.exit(1)
            
    except KeyboardInterrupt:
        print("\n⚠️ Process interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
