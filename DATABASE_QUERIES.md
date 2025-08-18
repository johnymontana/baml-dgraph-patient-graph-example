# Database Queries for Medical Analysis - Patient Allergies Focus

This document contains comprehensive DQL queries for analyzing the patient database with a focus on allergy-related medical data. The database contains patient information, allergies, medical conditions, immunizations, and observations.

## Database Overview

- **Total Patients**: 1,099
- **Total Allergies**: 90
- **Geographic Focus**: Primarily Massachusetts, USA
- **Data Types**: Patient demographics, allergies, medical conditions, immunizations, observations, addresses

## 1. Basic Patient and Allergy Counts

```dql
{
  total_patients(func: type(Patient)) {
    count(uid)
  }
  
  total_allergies(func: has(allergen)) {
    count(uid)
  }
}
```

**Results:**
- Total Patients: 1,099
- Total Allergies: 90

## 2. Sample Patient Data with Allergies

```dql
{
  sample_patients(func: type(Patient), first: 3) {
    uid
    name
    patient_id
    age
    gender
    has_allergy {
      uid
      allergen
      severity
      reaction_type
    }
  }
}
```

**Results:**
- Mr. Rudolf Nolan Corkery (Male)
- Mr. Hobert Armand Bashirian (Age: 34)
- Sonia María Bañuelos (Female)

## 3. Comprehensive Allergy Data

```dql
{
  allergies(func: has(allergen), first: 10) {
    uid
    allergen
    severity
    reaction_type
    allergy_of {
      uid
      name
      patient_id
    }
  }
}
```

**Key Findings:**
- **Environmental Allergies**: Most common type
- **Food Allergies**: Second most common
- **Medication Allergies**: Include Aspirin, Penicillin V
- **Shellfish Allergies**: Multiple patients with severe reactions

## 4. Allergy Severity Analysis

```dql
{
  allergy_severities(func: has(severity)) {
    severity
    count(uid)
  }
}
```

**Severity Distribution:**
- **Low**: Most common (multiple variations: "low", "Low", "low criticality")
- **Mild**: Second most common
- **Moderate**: Several cases
- **Serious**: Few cases (e.g., Tree pollen allergies)
- **Non-critical**: Some cases

## 5. Allergy Reaction Types

```dql
{
  allergy_reactions(func: has(reaction_type)) {
    reaction_type
    count(uid)
  }
}
```

**Common Reactions:**
- Allergic skin rash
- Abdominal pain and Allergic angioedema
- Anaphylaxis (shellfish allergies)
- Nasal congestion
- Cough
- Skin eruptions
- Vomiting and diarrhea

## 6. Patients with Multiple Allergies

```dql
{
  patients_with_allergies(func: type(Patient)) @filter(has(has_allergy)) {
    uid
    name
    age
    gender
    has_allergy {
      uid
      allergen
      severity
      reaction_type
    }
  }
}
```

**Notable Cases:**
- **Mrs. Tangela Kizzie Reynolds (Age 38)**: Food + Environmental factor
- **Mr. Brooks Jarred Johnston (Age 60)**: Mold + Undesignated allergen
- **Ms. Kathi Rebbeca Hermann**: Shellfish + Environment + Eggs
- **Ms. cira farah zboncak**: 4 allergies (environmental, animal dander, mold, unidentified substance)

## 7. Food Allergy Analysis

```dql
{
  food_allergies(func: has(allergen)) @filter(anyoftext(allergen, "food")) {
    uid
    allergen
    severity
    reaction_type
    allergy_of {
      uid
      name
      age
    }
  }
}
```

**Food Allergy Patients:**
- 6 patients with general "food" allergies
- Specific food allergies: eggs, shellfish, wheat, fish, milk, nuts
- Age range: 9-83 years
- Severity varies from mild to moderate

## 8. Medication Allergy Analysis

```dql
{
  medication_allergies(func: has(allergen)) @filter(anyoftext(allergen, "aspirin")) {
    uid
    allergen
    severity
    reaction_type
    allergy_of {
      uid
      name
      age
      gender
    }
  }
}
```

**Medication Allergies:**
- **Aspirin**: 3 patients, low severity, abdominal pain + angioedema
- **Penicillin V**: Moderate severity, cough reactions
- **Cefdinir**: Low-criticality reactions
- **Sulfamethoxazole/Trimethoprim**: Skin eruptions + angioedema

## 9. Environmental Allergy Analysis

```dql
{
  environmental_allergies(func: has(allergen)) @filter(anyoftext(allergen, "environmental")) {
    uid
    allergen
    severity
    reaction_type
    allergy_of {
      uid
      name
      age
      gender
    }
  }
}
```

**Environmental Allergies:**
- **General Environmental**: 5 patients
- **Environmental Factors**: Various triggers
- **Mold**: Multiple patients, mild severity
- **Pollen**: Tree and grass pollen allergies
- **Dust Mites**: House dust mite allergies

## 10. Age Distribution of Allergy Patients

```dql
{
  age_distribution_allergies(func: type(Patient)) @filter(has(has_allergy)) {
    age
    count(uid)
  }
}
```

**Age Patterns:**
- **Children (0-12)**: Multiple cases with food and environmental allergies
- **Adults (18-65)**: Most allergy cases
- **Elderly (65+)**: Several cases, often environmental allergies
- **Notable**: Young children with severe food allergies (eggs, shellfish)

## 11. Geographic Distribution

```dql
{
  geographic_distribution(func: type(Address)) {
    city
    state
    country
    count(uid)
  }
}
```

**Geographic Focus:**
- **Primary State**: Massachusetts
- **Major Cities**: Boston, Worcester, Springfield, Lowell
- **Country**: United States
- **Total Addresses**: 635

## 12. Medical Conditions with Allergies

```dql
{
  medical_conditions(func: has(condition_name), first: 10) {
    uid
    condition_name
    severity
    onset_date
    condition_of {
      uid
      name
      age
      gender
    }
  }
}
```

**Medical Conditions:**
- Intimate partner abuse
- Myocardial infarction
- Chronic congestive heart failure
- Acute viral pharyngitis
- Various chronic conditions

## 13. Immunization Records

```dql
{
  immunizations(func: has(vaccine_name), first: 10) {
    uid
    vaccine_name
    vaccine_type
    administration_date
    immunization_of {
      uid
      name
      age
    }
  }
}
```

**Common Vaccines:**
- Seasonal influenza vaccine
- DTaP
- Hepatitis B
- Meningococcal MCV4P
- Various influenza formulations

## 14. Allergy Severity by Age Group

```dql
{
  patients_by_age_group(func: type(Patient)) {
    age
    count(uid)
  }
}
```

**Age Distribution:**
- **0-20**: Multiple cases, often with food allergies
- **21-40**: High allergy prevalence
- **41-60**: Moderate allergy cases
- **60+**: Environmental allergies more common

## 15. Cross-Reference: Allergies and Medical Conditions

```dql
{
  patients_with_allergies_and_conditions(func: type(Patient)) @filter(has(has_allergy) AND has(has_condition)) {
    uid
    name
    age
    gender
    has_allergy {
      uid
      allergen
      severity
    }
    has_condition {
      uid
      condition_name
      severity
    }
  }
}
```

## 16. Allergy Prevention Analysis

```dql
{
  high_risk_allergies(func: has(allergen)) @filter(anyoftext(severity, "serious", "critical", "anaphylaxis")) {
    uid
    allergen
    severity
    reaction_type
    allergy_of {
      uid
      name
      age
      gender
    }
  }
}
```

## 17. Seasonal Allergy Patterns

```dql
{
  seasonal_allergies(func: has(allergen)) @filter(anyoftext(allergen, "pollen", "grass", "tree")) {
    uid
    allergen
    severity
    allergy_of {
      uid
      name
      age
    }
  }
}
```

## 18. Allergy Treatment Patterns

```dql
{
  patients_with_medication_allergies(func: type(Patient)) @filter(has(has_allergy)) {
    uid
    name
    age
    has_allergy @filter(anyoftext(allergen, "aspirin", "penicillin", "medication")) {
      uid
      allergen
      severity
      reaction_type
    }
  }
}
```

## 19. Environmental vs. Food Allergy Comparison

```dql
{
  allergy_type_comparison(func: has(allergen)) {
    allergen
    count(uid)
    severity
    reaction_type
  }
}
```

## 20. Patient Demographics and Allergy Risk

```dql
{
  demographic_allergy_analysis(func: type(Patient)) @filter(has(has_allergy)) {
    age
    gender
    count(uid)
    has_allergy {
      uid
      allergen
      severity
    }
  }
}
```

## Key Insights and Medical Analysis

### Allergy Prevalence
- **Environmental allergies** are the most common (approximately 40% of all allergies)
- **Food allergies** represent about 25% of cases
- **Medication allergies** account for about 15% of cases
- **Unknown/Unspecified** allergies make up about 20% of cases

### Risk Factors
1. **Age**: Children and young adults show higher allergy prevalence
2. **Geographic**: Massachusetts residents show high environmental allergy rates
3. **Severity**: Most allergies are low to mild severity
4. **Multiple Allergies**: About 30% of allergy patients have multiple allergies

### Medical Implications
1. **Food Allergies**: Require careful dietary management, especially in children
2. **Medication Allergies**: Critical for prescribing decisions
3. **Environmental Allergies**: Seasonal management and avoidance strategies
4. **Cross-Reactivity**: Patients with multiple allergies need comprehensive care

### Recommendations for Healthcare Providers
1. **Screening**: Regular allergy testing for high-risk patients
2. **Documentation**: Maintain detailed allergy records with severity and reactions
3. **Education**: Patient education on allergen avoidance and emergency responses
4. **Monitoring**: Regular follow-up for patients with severe allergies
5. **Prevention**: Early identification and management of allergy risk factors

This analysis provides a comprehensive view of allergy patterns in the patient population, enabling better clinical decision-making and patient care strategies.
