#!/usr/bin/env python3
"""
Debug script to investigate UID issues in DGraph
"""

import json
import sys
from pathlib import Path

# Add the parent directory to the Python path to import dgraph_importer
sys.path.insert(0, str(Path(__file__).parent.parent))

from scripts.dgraph_importer import DGraphMedicalImporter

def debug_uid_issue():
    """Debug the UID issue by querying the database."""
    print("🔍 Debugging UID Issue in DGraph")
    print("=" * 50)
    
    # Initialize DGraph importer
    importer = DGraphMedicalImporter()
    
    try:
        # Query for all allergies to see what UIDs we have
        query = """
        {
            allergies(func: has(dgraph.type)) @filter(eq(dgraph.type, "Allergy")) {
                uid
                allergen
                severity
                reaction_type
                confirmed_date
                notes
            }
        }
        """
        
        txn = importer.client.txn(read_only=True)
        try:
            result = txn.query(query)
            data = json.loads(result.json)
            
            if data.get("allergies"):
                allergies = data["allergies"]
                print(f"📊 Found {len(allergies)} allergy nodes in database:")
                
                for allergy in allergies:
                    uid = allergy.get("uid", "N/A")
                    allergen = allergy.get("allergen", "N/A")
                    severity = allergy.get("severity", "N/A")
                    
                    print(f"\n   🏷️  Allergy: {allergen}")
                    print(f"      UID: {uid}")
                    print(f"      Severity: {severity}")
                    
                    # Check if UID is valid
                    if importer.is_valid_uid(uid):
                        print(f"      ✅ UID is valid")
                    else:
                        print(f"      ❌ UID is invalid (too short or malformed)")
                        
                        # Show UID details
                        if uid.startswith("0x"):
                            hex_part = uid[2:]
                            print(f"      📏 Hex length: {len(hex_part)} (minimum: 6)")
                            print(f"      🔢 Hex value: {hex_part}")
                        else:
                            print(f"      📏 Format: {uid} (should start with 0x)")
            else:
                print("📊 No allergy nodes found in database")
                
        finally:
            txn.discard()
        
        # Also check for any other node types
        query_all = """
        {
            all_nodes(func: has(dgraph.type)) {
                uid
                dgraph.type
            }
        }
        """
        
        txn = importer.client.txn(read_only=True)
        try:
            result = txn.query(query_all)
            data = json.loads(result.json)
            
            if data.get("all_nodes"):
                nodes = data["all_nodes"]
                print(f"\n📊 Total nodes in database: {len(nodes)}")
                
                # Group by type
                type_counts = {}
                uid_lengths = []
                
                for node in nodes:
                    node_type = node.get("dgraph.type", "Unknown")
                    uid = node.get("uid", "")
                    
                    if node_type not in type_counts:
                        type_counts[node_type] = 0
                    type_counts[node_type] += 1
                    
                    if uid.startswith("0x"):
                        hex_part = uid[2:]
                        uid_lengths.append(len(hex_part))
                
                print("\n📋 Node types and counts:")
                for node_type, count in type_counts.items():
                    print(f"   {node_type}: {count}")
                
                if uid_lengths:
                    print(f"\n📏 UID hex lengths:")
                    print(f"   Min: {min(uid_lengths)}")
                    print(f"   Max: {max(uid_lengths)}")
                    print(f"   Most common: {max(set(uid_lengths), key=uid_lengths.count)}")
                    
                    # Check for short UIDs
                    short_uids = [l for l in uid_lengths if l < 6]
                    if short_uids:
                        print(f"   ⚠️  Found {len(short_uids)} UIDs with hex length < 6 (invalid)")
                    else:
                        print(f"   ✅ All UIDs have valid hex length (≥6)")
            else:
                print("📊 No nodes found in database")
                
        finally:
            txn.discard()
        
    except Exception as e:
        print(f"❌ Error during debug: {e}")
    finally:
        importer.close()
    
    print("\n🎉 UID Debug Complete!")

if __name__ == "__main__":
    debug_uid_issue()
