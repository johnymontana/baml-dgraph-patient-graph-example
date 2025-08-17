#!/usr/bin/env python3
"""
Test script to demonstrate geo location conversion functionality.
This script shows how latitude/longitude coordinates are converted to GeoJSON.
"""

def coordinates_to_geojson(latitude: float, longitude: float) -> str:
    """Convert latitude/longitude to GeoJSON Point format."""
    # GeoJSON Point format: "POINT(longitude latitude)"
    # Note: GeoJSON uses longitude first, then latitude
    return f"POINT({longitude} {latitude})"

def main():
    """Demonstrate coordinate conversion with sample data."""
    print("🌍 Coordinate to GeoJSON Conversion Test")
    print("=" * 50)
    
    # Sample coordinates from our test data
    test_coordinates = [
        ("Easthampton, MA", 42.2602, -72.6654),
        ("Boston, MA", 42.3601, -71.0589),
        ("New York, NY", 40.7128, -74.0060),
        ("Los Angeles, CA", 34.0522, -118.2437),
        ("Chicago, IL", 41.8781, -87.6298)
    ]
    
    print("Converting coordinates to GeoJSON Point format:")
    print("Format: POINT(longitude latitude)")
    print()
    
    for city, lat, lon in test_coordinates:
        geojson = coordinates_to_geojson(lat, lon)
        print(f"📍 {city}")
        print(f"   Latitude: {lat}")
        print(f"   Longitude: {lon}")
        print(f"   GeoJSON: {geojson}")
        print()
    
    print("💡 Key Points:")
    print("   - GeoJSON uses longitude first, then latitude")
    print("   - This follows the GeoJSON specification (RFC 7946)")
    print("   - DGraph's geo type expects this exact format")
    print("   - The format enables geospatial queries and indexing")

if __name__ == "__main__":
    main()
