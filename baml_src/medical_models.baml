// Enhanced Medical data models for structured extraction
// Aligned with knowledge graph structure and additional medical entities

class Address {
  street string
  city string
  state string
  zip_code string
  country string
  timezone string?  // Timezone information for the location
  latitude float?  // Latitude coordinate when geocoded
  longitude float?  // Longitude coordinate when geocoded
  geocoded bool  // Whether the address was successfully geocoded
}

class ContactInfo {
  phone_number string?
  email string?
  preferred_language string?
  communication_preferences string?
}

class MedicalProvider {
  name string
  provider_id string?
  specialty string?
  address Address?
  organization string?  // Healthcare organization they work for
  role string?  // Primary performer, consultant, etc.
}

class Substance {
  name string
  type string?  // medication, environmental, food, chemical
  description string?
  active_ingredients string[]?
  dosage string?
  frequency string?
}

class Allergy {
  allergen string
  severity string?  // mild, moderate, severe, low criticality
  reaction_type string?  // skin reaction, angioedema, itching, diarrhea, etc.
  confirmed_date string?
  status string?  // active, resolved, confirmed, unknown
  notes string?
  substance Substance?  // Link to specific substance
}

class Immunization {
  vaccine_name string
  vaccine_type string?  // seasonal, preservative-free, etc.
  administration_date string
  status string?  // completed, scheduled, unknown
  lot_number string?
  manufacturer string?
  provider MedicalProvider?
  location Address?
  notes string?
}

class ClinicalObservation {
  observation_type string  // lab result, vital sign, measurement
  value string?
  unit string?
  status string?  // final, preliminary, unknown
  effective_time string?
  issued_time string?
  category string?  // laboratory, vital-signs, survey, etc.
  code string?  // LOINC or other coding system
  interpretation string?  // normal, high, low, etc.
  notes string?
}

class MedicalProcedure {
  procedure_type string
  status string?  // completed, in-progress, unknown
  start_time string?
  end_time string?
  duration string?
  location Address?
  provider MedicalProvider?
  reason string?
  outcome string?
  notes string?
}

class MedicalCondition {
  condition_name string
  status string?  // active, resolved, chronic, acute
  onset_date string?
  severity string?
  category string?  // diagnosis, symptom, finding
  notes string?
}

class MedicalVisit {
  visit_type string
  start_time string
  end_time string?
  timezone string?
  location Address?
  provider MedicalProvider?
  status string?  // completed, in-progress, unknown
  classification string?  // ambulatory, inpatient, emergency
  reason string?
  notes string?
  procedures MedicalProcedure[]?
  observations ClinicalObservation[]?
  immunizations Immunization[]?
}

class Organization {
  name string
  organization_id string?
  type string?  // hospital, clinic, laboratory, pharmacy
  address Address?
  phone string?
  website string?
  specialties string[]?
}

class SocialHistory {
  employment_status string?
  education_level string?
  insurance_provider string?
  annual_income string?
  housing_status string?
  transportation_access bool?
  social_support_level string?
  risk_factors string[]?
  lifestyle_factors string[]?
}

class Patient {
  name string
  patient_id string?
  marital_status string?
  age int?
  gender string?
  date_of_birth string?
  contact_info ContactInfo?
  address Address?
  multiple_birth bool?
  primary_language string?
  social_history SocialHistory?
}

class MedicalRecord {
  patient Patient
  visits MedicalVisit[]
  allergies Allergy[]
  immunizations Immunization[]
  conditions MedicalCondition[]
  observations ClinicalObservation[]
  procedures MedicalProcedure[]
  provider_facility Address?
  organizations Organization[]
  substances Substance[]
  extracted_entities string[]  // Any other important entities found
  record_metadata RecordMetadata?
}

class RecordMetadata {
  source string
  record_id string?
  import_timestamp string
  extraction_version string
  fhir_resource_type string?
  clinical_note_length int?
  extraction_confidence float?
}
