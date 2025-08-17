#!/usr/bin/env python3
"""
Test script to demonstrate coordinate conversion functionality.
This script shows how latitude/longitude coordinates would be converted to GeoJSON.
"""

def convert_coordinates_to_geojson(latitude: float, longitude: float) -> str:
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
    ]
    
    print("📍 Converting sample coordinates to GeoJSON:")
    print()
    
    for location, lat, lng in test_coordinates:
        geo_point = convert_coordinates_to_geojson(lat, lng)
        print(f"   {location}:")
        print(f"     Latitude: {lat}")
        print(f"     Longitude: {lng}")
        print(f"     GeoJSON: {geo_point}")
        print()
    
    print("✅ Conversion complete!")
    print()
    print("💡 This demonstrates how the post-processing script would work:")
    print("   1. Extract latitude/longitude from DGraph Address nodes")
    print("   2. Convert to GeoJSON Point format")
    print("   3. Add geo predicate with geospatial indexing")
    print("   4. Remove old latitude/longitude predicates")

if __name__ == "__main__":
    main()
