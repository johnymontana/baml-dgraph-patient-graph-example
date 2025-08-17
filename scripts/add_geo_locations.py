#!/usr/bin/env python3
"""
Post-processing script to add geo location predicates to DGraph nodes.

This script:
1. Finds all Address nodes with latitude and longitude values
2. Converts the coordinates to GeoJSON Point format
3. Adds a new 'location' predicate with geo type for geospatial indexing
4. Preserves the existing latitude/longitude fields
"""

import os
import json
import sys
from typing import Dict, List, Any, Optional, Tuple
import pydgraph
from pydgraph import Txn
import uuid
from dotenv import load_dotenv

class GeoLocationProcessor:
    def __init__(self, dgraph_url: str):
        """Initialize the processor with DGraph connection."""
        print(f"🔗 Connecting to DGraph: {dgraph_url}")
        # Use pydgraph.open() to handle the connection string automatically
        self.client = pydgraph.open(dgraph_url)
        
    def find_addresses_with_coordinates(self) -> List[Dict[str, Any]]:
        """Find all Address nodes that have both latitude and longitude values."""
        query = """
        {
            addresses(func: type(Address)) @filter(has(latitude) AND has(longitude)) {
                uid
                latitude
                longitude
                street
                city
                state
                zip_code
                country
                geocoded
            }
        }
        """
        
        try:
            txn = self.client.txn(read_only=True)
            result = txn.query(query)
            txn.discard()
            
            if result.json:
                data = json.loads(result.json)
                addresses = data.get('addresses', [])
                print(f"📍 Found {len(addresses)} addresses with coordinates")
                return addresses
            else:
                print("❌ No results returned from query")
                return []
                
        except Exception as e:
            print(f"❌ Error querying addresses: {e}")
            return []
    
    def coordinates_to_geojson(self, latitude: float, longitude: float) -> str:
        """Convert latitude/longitude to GeoJSON Point format."""
        # DGraph expects proper GeoJSON format, not WKT
        # GeoJSON Point format: {"type": "Point", "coordinates": [longitude, latitude]}
        geojson = {
            "type": "Point",
            "coordinates": [longitude, latitude]
        }
        return json.dumps(geojson)
    
    def add_geo_location_predicate(self, addresses: List[Dict[str, Any]]) -> bool:
        """Add geo location predicate to all addresses with coordinates."""
        if not addresses:
            print("⚠️ No addresses to process")
            return True
        
        print(f"🔄 Processing {len(addresses)} addresses...")
        
        # Prepare N-Quads for batch update
        nquads = []
        
        for addr in addresses:
            uid = addr['uid']
            lat = addr['latitude']
            lon = addr['longitude']
            
            # Convert to GeoJSON
            geojson = self.coordinates_to_geojson(lat, lon)
            
            # Add location predicate - escape the JSON string properly
            escaped_geojson = geojson.replace('"', '\\"')
            nquads.append(f'<{uid}> <location> "{escaped_geojson}"^^<geo:geojson> .')
            
            # Log the conversion
            location_str = f"{addr.get('street', '')}, {addr.get('city', '')}, {addr.get('state', '')}"
            print(f"  📍 {location_str} -> {geojson}")
        
        # Execute the mutation
        try:
            txn = self.client.txn()
            # Join N-Quads into a single string
            nquads_string = '\n'.join(nquads)
            response = txn.mutate(set_nquads=nquads_string)
            txn.commit()
            
            print(f"✅ Successfully added geo location to {len(addresses)} addresses")
            # Handle different response types
            if hasattr(response, 'txn_id'):
                print(f"   Transaction ID: {response.txn_id}")
            elif hasattr(response, 'uids'):
                print(f"   UIDs processed: {len(response.uids)}")
            return True
            
        except Exception as e:
            print(f"❌ Error adding geo locations: {e}")
            if 'txn' in locals():
                txn.discard()
            return False
    
    def verify_geo_locations(self, addresses: List[Dict[str, Any]]) -> bool:
        """Verify that geo locations were added correctly."""
        if not addresses:
            return True
        
        print("🔍 Verifying geo locations...")
        
        # Simple query to count addresses with location predicates
        query = """
        {
            addresses_with_geo(func: type(Address)) @filter(has(location)) {
                uid
                street
                city
                state
                location
            }
        }
        """
        
        try:
            txn = self.client.txn(read_only=True)
            result = txn.query(query)
            txn.discard()
            
            if result.json:
                data = json.loads(result.json)
                verified_addresses = data.get('addresses_with_geo', [])
                
                print(f"📊 Found {len(verified_addresses)} addresses with geo locations:")
                for addr in verified_addresses:
                    location_str = f"{addr.get('street', '')}, {addr.get('city', '')}, {addr.get('state', '')}"
                    print(f"  ✅ {location_str}: {addr['location']}")
                
                # Check if we have the expected number
                expected_count = len(addresses)
                actual_count = len(verified_addresses)
                
                if actual_count >= expected_count:
                    print(f"🎉 Success! All {expected_count} addresses now have geo locations")
                    return True
                else:
                    print(f"⚠️ Expected {expected_count} addresses with geo locations, found {actual_count}")
                    return False
            else:
                print("❌ No results returned from verification query")
                return False
                
        except Exception as e:
            print(f"❌ Error verifying geo locations: {e}")
            return False
    
    def run_full_process(self) -> bool:
        """Run the complete geo location processing workflow."""
        print("🌍 DGraph Geo Location Post-Processing")
        print("=" * 50)
        
        # Step 1: Find addresses with coordinates
        print("\n🔍 Step 1: Finding addresses with coordinates...")
        addresses = self.find_addresses_with_coordinates()
        
        if not addresses:
            print("⚠️ No addresses found with coordinates. Nothing to process.")
            return True
        
        # Step 2: Add geo location predicates
        print("\n🔄 Step 2: Adding geo location predicates...")
        success = self.add_geo_location_predicate(addresses)
        
        if not success:
            print("❌ Failed to add geo locations")
            return False
        
        # Step 3: Verify the changes
        print("\n🔍 Step 3: Verifying geo locations...")
        verification_success = self.verify_geo_locations(addresses)
        
        if verification_success:
            print("\n🎉 Geo location processing completed successfully!")
            print(f"   Added geo locations to {len(addresses)} addresses")
            print("\n💡 You can now run geospatial queries like:")
            print("   - Find addresses within 10km of a point")
            print("   - Find the nearest medical facilities")
            print("   - Analyze geographic distribution of patients")
        else:
            print("\n⚠️ Geo location processing completed with verification issues")
        
        return verification_success

def main():
    """Main entry point."""
    # Load environment variables from .env file (if it exists)
    load_dotenv(verbose=False)
    
    # Get DGraph connection from environment with default fallback
    dgraph_url = os.getenv('DGRAPH_CONNECTION_STRING', 'dgraph://localhost:9080')
    print(f"🔗 Using DGraph connection: {dgraph_url}")
    
    try:
        # Initialize processor
        processor = GeoLocationProcessor(dgraph_url)
        
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
