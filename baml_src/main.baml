generator python_client {
  output_type "python/pydantic"
  output_dir "../"
  version "0.205.0"
}

function ExtractMedicalData(medical_text: string) -> MedicalRecord {
  client GPT4Mini
  prompt #"
    Extract structured medical information from the following clinical text.
    
    Medical Text:
    ---
    {{ medical_text }}
    ---
    
    Instructions:
    1. Extract patient information including name, marital status, and any demographic data
    2. Identify all medical visits with dates, times, types, and providers
    3. Extract allergy information including allergens, dates discovered, and severity
    4. Identify healthcare facility locations and addresses
    5. Extract provider information including names and IDs
    6. Parse all dates and times with timezone information when available
    7. Include any other medically relevant entities in the extracted_entities field
    8. **GEOCODING**: For any addresses found (patient addresses, provider addresses, facility addresses), attempt to geocode them to coordinates. Use the most specific address available (street + city + state + zip + country) to generate coordinates. If geocoding is successful, set geocoded=true and provide latitude and longitude as decimal degrees (e.g., latitude: 42.3601, longitude: -71.0589 for Boston). If geocoding fails or address is incomplete, set geocoded=false and leave latitude and longitude as null.
    
    Be thorough and accurate. If information is not explicitly stated, leave those fields null.
    If dates/times are provided, preserve the original format and timezone information.
    For geocoding, use standard coordinate systems (WGS84) and provide coordinates to 6 decimal places for precision. Always use latitude and longitude as separate decimal values.
    
    **IMPORTANT**: When you successfully geocode an address, you MUST provide both latitude and longitude fields. Do not leave them as null when geocoded=true.
    
    {{ ctx.output_format }}
  "#
}

function ExtractPatientSummary(medical_text: string) -> Patient {
  client GPT4Mini
  prompt #"
    Extract only patient demographic information from this medical text:
    
    {{ medical_text }}
    
    Focus on: name, patient ID, age, gender, marital status, date of birth.
    
    {{ ctx.output_format }}
  "#
}
