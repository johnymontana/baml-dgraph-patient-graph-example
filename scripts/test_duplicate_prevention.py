#!/usr/bin/env python3
"""
Test Duplicate Prevention in DGraph Import

This script tests the duplicate prevention functionality by trying to import
the same allergy (shellfish) multiple times to ensure it creates only one node.
"""

import json
import sys
from pathlib import Path

# Add the parent directory to the Python path to import dgraph_importer
sys.path.insert(0, str(Path(__file__).parent.parent))

from scripts.dgraph_importer import DGraphMedicalImporter

def test_duplicate_allergy_prevention():
    """Test that the same allergy is not duplicated."""
    print("🧪 Testing Duplicate Prevention in DGraph Import")
    print("=" * 60)
    
    # Create test records with the same allergy
    test_records = [
        {
            "patient": {
                "name": "Test Patient 1",
                "patient_id": "TP001",
                "marital_status": "single",
                "age": 30,
                "gender": "male",
                "date_of_birth": "1993-01-01"
            },
            "allergies": [
                {
                    "allergen": "shellfish",
                    "severity": "severe",
                    "reaction_type": "anaphylaxis",
                    "confirmed_date": "2020-01-01",
                    "notes": "First patient with shellfish allergy"
                }
            ],
            "visits": [],
            "providers": [],
            "metadata": {
                "source": "test-duplicate",
                "test_id": 1
            }
        },
        {
            "patient": {
                "name": "Test Patient 2",
                "patient_id": "TP002",
                "marital_status": "married",
                "age": 35,
                "gender": "female",
                "date_of_birth": "1988-01-01"
            },
            "allergies": [
                {
                    "allergen": "shellfish",
                    "severity": "moderate",
                    "reaction_type": "hives",
                    "confirmed_date": "2021-01-01",
                    "notes": "Second patient with shellfish allergy"
                }
            ],
            "visits": [],
            "providers": [],
            "metadata": {
                "source": "test-duplicate",
                "test_id": 2
            }
        },
        {
            "patient": {
                "name": "Test Patient 3",
                "patient_id": "TP003",
                "marital_status": "divorced",
                "age": 40,
                "gender": "male",
                "date_of_birth": "1983-01-01"
            },
            "allergies": [
                {
                    "allergen": "shellfish",
                    "severity": "mild",
                    "reaction_type": "stomach upset",
                    "confirmed_date": "2022-01-01",
                    "notes": "Third patient with shellfish allergy"
                }
            ],
            "visits": [],
            "providers": [],
            "metadata": {
                "source": "test-duplicate",
                "test_id": 3
            }
        }
    ]
    
    print(f"📋 Created {len(test_records)} test records, all with 'shellfish' allergy")
    print("🎯 Expected: Only 1 allergy node should be created, 3 patients should link to it")
    
    # Initialize DGraph importer
    importer = DGraphMedicalImporter()
    
    try:
        # Import each record
        for i, record in enumerate(test_records, 1):
            print(f"\n📥 Importing test record {i}/{len(test_records)}")
            print(f"   Patient: {record['patient']['name']}")
            print(f"   Allergy: {record['allergies'][0]['allergen']}")
            
            importer.import_medical_record(record)
            print(f"   ✅ Imported successfully")
        
        # Query to verify the results
        print("\n🔍 Verifying Results")
        print("-" * 40)
        
        # Query for all shellfish allergies
        query = """
        {
            shellfish_allergies(func: eq(allergen, "shellfish")) @filter(eq(dgraph.type, "Allergy")) {
                uid
                allergen
                severity
                reaction_type
                confirmed_date
                notes
                allergy_of {
                    name
                    patient_id
                }
            }
        }
        """
        
        txn = importer.client.txn(read_only=True)
        try:
            result = txn.query(query)
            data = json.loads(result.json)
            
            if data.get("shellfish_allergies"):
                allergies = data["shellfish_allergies"]
                print(f"📊 Found {len(allergies)} shellfish allergy node(s)")
                
                for allergy in allergies:
                    print(f"\n   🏷️  Allergy Node:")
                    print(f"      UID: {allergy['uid']}")
                    print(f"      Allergen: {allergy['allergen']}")
                    print(f"      Severity: {allergy.get('severity', 'N/A')}")
                    print(f"      Reaction: {allergy.get('reaction_type', 'N/A')}")
                    print(f"      Patients with this allergy: {len(allergy.get('allergy_of', []))}")
                    
                    for patient in allergy.get('allergy_of', []):
                        print(f"         👤 {patient.get('name', 'Unknown')} (ID: {patient.get('patient_id', 'N/A')})")
                
                if len(allergies) == 1:
                    print(f"\n✅ SUCCESS: Only 1 allergy node created as expected!")
                    print(f"   {len(allergies[0].get('allergy_of', []))} patients are linked to this single node")
                else:
                    print(f"\n❌ FAILED: {len(allergies)} allergy nodes created (expected 1)")
            else:
                print("❌ No shellfish allergies found")
                
        finally:
            txn.discard()
        
        # Clean up test data
        print("\n🧹 Cleaning up test data...")
        # Note: In a real scenario, you might want to delete test data
        # For now, we'll just note that it exists
        
    except Exception as e:
        print(f"❌ Error during test: {e}")
    finally:
        importer.close()
    
    print("\n🎉 Duplicate Prevention Test Complete!")

if __name__ == "__main__":
    test_duplicate_allergy_prevention()
