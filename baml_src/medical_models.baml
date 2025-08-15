// Medical data models for structured extraction

class Address {
  street string
  city string
  state string
  zip_code string
  country string
}

class MedicalProvider {
  name string
  provider_id string?
  specialty string?
  address Address?
}

class Allergy {
  allergen string
  severity string?  // mild, moderate, severe
  reaction_type string?
  confirmed_date string?
  notes string?
}

class MedicalVisit {
  visit_type string
  start_time string
  end_time string?
  timezone string?
  location string?
  provider MedicalProvider?
  notes string?
}

class Patient {
  name string
  patient_id string?
  marital_status string?
  age int?
  gender string?
  date_of_birth string?
}

class MedicalRecord {
  patient Patient
  visits MedicalVisit[]
  allergies Allergy[]
  provider_facility Address?
  extracted_entities string[]  // Any other important entities found
}
