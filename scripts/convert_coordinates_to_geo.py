#!/usr/bin/env python3
"""
Post-processing script to convert DGraph nodes with latitude/longitude coordinates
to use geo predicates for geospatial indexing and querying.

This script:
1. Finds all Address nodes with latitude and longitude values
2. Converts them to GeoJSON Point format
3. Adds a geo predicate with proper geospatial indexing
4. Removes the old latitude/longitude predicates
"""

import json
import sys
from typing import Dict, List, Any, Optional
import pydgraph
from pydgraph import DgraphClient, Txn
import uuid

class CoordinateToGeoConverter:
    def __init__(self, dgraph_url: str):
        """Initialize the converter with DGraph connection."""
        self.client = DgraphClient(dgraph_url)
        
    def find_addresses_with_coordinates(self) -> List[Dict[str, Any]]:
        """Find all Address nodes that have latitude and longitude values."""
        query = """
        {
            addresses(func: type(Address)) @filter(has(latitude) AND has(longitude)) {
                uid
                street
                city
                state
                zip_code
                country
                latitude
                longitude
                geocoded
            }
        }
        """
        
        txn = self.client.txn()
        try:
            result = txn.query(query)
            data = json.loads(result.json)
            return data.get('addresses', [])
        except Exception as e:
            print(f"❌ Error querying addresses: {e}")
            return []
        finally:
            txn.discard()
    
    def convert_coordinates_to_geojson(self, latitude: float, longitude: float) -> str:
        """Convert latitude/longitude to GeoJSON Point format."""
        # GeoJSON Point format: "POINT(longitude latitude)"
        # Note: GeoJSON uses longitude first, then latitude
        return f"POINT({longitude} {latitude})"
    
    def update_address_with_geo(self, address_uid: str, latitude: float, longitude: float) -> bool:
        """Update an address node to use geo predicate instead of lat/lng."""
        try:
            # Convert coordinates to GeoJSON
            geo_point = self.convert_coordinates_to_geojson(latitude, longitude)
            
            # Create mutation to:
            # 1. Add geo predicate
            # 2. Remove latitude and longitude predicates
            mutation = f"""
            <{address_uid}> <geo> "{geo_point}"^^<geo:geojson> .
            <{address_uid}> <latitude> * .
            <{address_uid}> <longitude> * .
            """
            
            txn = self.client.txn()
            try:
                # Execute mutation
                result = txn.mutate(set_nquads=mutation)
                txn.commit()
                print(f"✅ Updated address {address_uid}: {geo_point}")
                return True
            except Exception as e:
                print(f"❌ Error updating address {address_uid}: {e}")
                txn.discard()
                return False
                
        except Exception as e:
            print(f"❌ Error processing address {address_uid}: {e}")
            return False
    
    def process_all_addresses(self) -> Dict[str, int]:
        """Process all addresses with coordinates and convert them to geo predicates."""
        print("🔍 Finding addresses with coordinates...")
        addresses = self.find_addresses_with_coordinates()
        
        if not addresses:
            print("ℹ️  No addresses with coordinates found.")
            return {"total": 0, "success": 0, "failed": 0}
        
        print(f"📍 Found {len(addresses)} addresses with coordinates")
        
        success_count = 0
        failed_count = 0
        
        for address in addresses:
            uid = address['uid']
            latitude = address.get('latitude')
            longitude = address.get('longitude')
            
            if latitude is not None and longitude is not None:
                print(f"🔄 Processing address {uid}: {address.get('street', 'Unknown')}, {address.get('city', 'Unknown')}")
                
                if self.update_address_with_geo(uid, latitude, longitude):
                    success_count += 1
                else:
                    failed_count += 1
            else:
                print(f"⚠️  Address {uid} missing coordinates: lat={latitude}, lng={longitude}")
                failed_count += 1
        
        return {
            "total": len(addresses),
            "success": success_count,
            "failed": failed_count
        }
    
    def verify_geo_conversion(self) -> Dict[str, int]:
        """Verify that addresses now have geo predicates and no lat/lng predicates."""
        query = """
        {
            with_geo: addresses(func: type(Address)) @filter(has(geo)) {
                uid
                street
                city
                geo
            }
            with_lat_lng: addresses(func: type(Address)) @filter(has(latitude) OR has(longitude)) {
                uid
                street
                city
                latitude
                longitude
            }
        }
        """
        
        txn = self.client.txn()
        try:
            result = txn.query(query)
            data = json.loads(result.json)
            
            with_geo = len(data.get('with_geo', []))
            with_lat_lng = len(data.get('with_lat_lng', []))
            
            print(f"📊 Verification Results:")
            print(f"   Addresses with geo predicates: {with_geo}")
            print(f"   Addresses still with lat/lng: {with_lat_lng}")
            
            return {
                "with_geo": with_geo,
                "with_lat_lng": with_lat_lng
            }
        except Exception as e:
            print(f"❌ Error during verification: {e}")
            return {"with_geo": 0, "with_lat_lng": 0}
        finally:
            txn.discard()

def main():
    """Main function to run the coordinate conversion."""
    if len(sys.argv) != 2:
        print("Usage: python convert_coordinates_to_geo.py <dgraph_url>")
        print("Example: python convert_coordinates_to_geo.py dgraph://localhost:8080")
        sys.exit(1)
    
    dgraph_url = sys.argv[1]
    
    print("🌍 DGraph Coordinate to Geo Converter")
    print("=" * 50)
    print(f"🔗 Connecting to: {dgraph_url}")
    
    try:
        converter = CoordinateToGeoConverter(dgraph_url)
        
        # Process all addresses
        print("\n🔄 Starting coordinate conversion...")
        results = converter.process_all_addresses()
        
        print(f"\n📈 Conversion Results:")
        print(f"   Total addresses processed: {results['total']}")
        print(f"   Successfully converted: {results['success']}")
        print(f"   Failed conversions: {results['failed']}")
        
        # Verify the conversion
        print(f"\n🔍 Verifying conversion...")
        verification = converter.verify_geo_conversion()
        
        if verification['with_lat_lng'] == 0:
            print("✅ All addresses successfully converted to geo predicates!")
        else:
            print(f"⚠️  {verification['with_lat_lng']} addresses still have lat/lng predicates")
            
    except Exception as e:
        print(f"❌ Fatal error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
