#!/usr/bin/env python3
"""
Cleanup script to remove invalid UIDs from DGraph database
"""

import json
import sys
from pathlib import Path

# Add the parent directory to the Python path to import dgraph_importer
sys.path.insert(0, str(Path(__file__).parent.parent))

from scripts.dgraph_importer import DGraphMedicalImporter

def cleanup_invalid_uids():
    """Clean up invalid UIDs from the database."""
    print("🧹 Cleaning up invalid UIDs from DGraph")
    print("=" * 50)
    
    # Initialize DGraph importer
    importer = DGraphMedicalImporter()
    
    try:
        # First, find all nodes with invalid UIDs
        query = """
        {
            all_nodes(func: has(dgraph.type)) {
                uid
                dgraph.type
            }
        }
        """
        
        txn = importer.client.txn(read_only=True)
        try:
            result = txn.query(query)
            data = json.loads(result.json)
            
            if data.get("all_nodes"):
                nodes = data["all_nodes"]
                print(f"📊 Found {len(nodes)} total nodes in database")
                
                # Identify nodes with invalid UIDs
                invalid_nodes = []
                valid_nodes = []
                
                for node in nodes:
                    uid = node.get("uid", "")
                    node_type = node.get("dgraph.type", "Unknown")
                    
                    if importer.is_valid_uid(uid):
                        valid_nodes.append((uid, node_type))
                    else:
                        invalid_nodes.append((uid, node_type))
                
                print(f"✅ Valid nodes: {len(valid_nodes)}")
                print(f"❌ Invalid nodes: {len(invalid_nodes)}")
                
                if invalid_nodes:
                    print("\n📋 Invalid nodes to be removed:")
                    for uid, node_type in invalid_nodes:
                        print(f"   {node_type}: {uid}")
                    
                    # Ask for confirmation
                    print(f"\n⚠️  This will permanently delete {len(invalid_nodes)} nodes with invalid UIDs.")
                    print("   This is recommended to fix the duplicate prevention functionality.")
                    
                    # For safety, we'll just show what would be deleted
                    # In production, you might want to add user confirmation here
                    print("\n💡 To actually delete these nodes, you would need to:")
                    print("   1. Manually confirm the deletion")
                    print("   2. Use DGraph's delete mutation")
                    print("   3. Re-run the import to create properly formatted nodes")
                    
                    print(f"\n🔍 Summary of invalid UIDs:")
                    uid_lengths = []
                    for uid, _ in invalid_nodes:
                        if uid.startswith("0x"):
                            hex_part = uid[2:]
                            uid_lengths.append(len(hex_part))
                    
                    if uid_lengths:
                        print(f"   Shortest UID hex length: {min(uid_lengths)}")
                        print(f"   Longest UID hex length: {max(uid_lengths)}")
                        print(f"   All should be ≥6 characters")
                else:
                    print("🎉 No invalid UIDs found! Database is clean.")
                    
            else:
                print("📊 No nodes found in database")
                
        finally:
            txn.discard()
        
    except Exception as e:
        print(f"❌ Error during cleanup analysis: {e}")
    finally:
        importer.close()
    
    print("\n🎉 Cleanup Analysis Complete!")
    print("\n💡 Next steps:")
    print("   1. The pipeline is working (creating new nodes with valid UIDs)")
    print("   2. Consider cleaning up the database to remove invalid UIDs")
    print("   3. Re-run the import to create a clean database with proper duplicate prevention")

if __name__ == "__main__":
    cleanup_invalid_uids()
