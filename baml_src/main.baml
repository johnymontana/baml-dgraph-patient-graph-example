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
    
    Be thorough and accurate. If information is not explicitly stated, leave those fields null.
    If dates/times are provided, preserve the original format and timezone information.
    
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
