# Medical Data Extraction with BAML and DGraph Integration

This project demonstrates how to use BAML (Boundary ML) to extract structured medical data from unstructured clinical text and then import that data into DGraph for advanced graph-based querying and relationship analysis.

![](img/fhir-patient-graph.png)

## Project Structure

```
baml-dgraph-patient-graph-example/
├── baml_src/
│   ├── main.baml              # BAML function definitions
│   ├── medical_models.baml    # Data models and schemas
│   └── clients.baml           # LLM client configurations
├── scripts/
│   ├── extract_medical_data.py    # BAML extraction script
│   ├── dgraph_importer.py         # DGraph import script
│   ├── sample_medical_text.py     # Sample medical text data
│   └── run_with_uv.py             # UV environment verification
├── data/                           # Data directory
│   ├── all_medical_records.json   # Combined extracted data
│   ├── extracted_sample_*.json    # Individual extraction results
│   └── flir-data.parquet          # Additional sample data
├── baml_client/                    # Generated BAML Python client
├── pyproject.toml                  # Project configuration and dependencies
├── uv.lock                         # Locked dependency versions
├── .env                            # Environment variables (create from env.example)
├── env.example                     # Environment variables template
├── setup.py                        # Automated setup script
├── test_setup.py                   # Setup verification script
├── demo_workflow.py                # Complete workflow demonstration
├── Makefile                        # Common commands
├── quick_start.sh                  # Quick setup automation
└── README.md                       # This file
```

## Features

- **Structured Medical Data Extraction**: Uses BAML with GPT-4 to extract patient information, medical visits, allergies, and provider details
- **Graph Database Integration**: Imports extracted data into DGraph with proper relationships and indexing
- **Comprehensive Schema**: Full medical data model with patient, visit, provider, allergy, and address entities
- **Sample Data**: Includes realistic medical text examples for testing
- **Environment Configuration**: Secure API key management via environment variables
- **Modern Python Tooling**: Uses uv for fast dependency management and virtual environments

## Prerequisites

- Python 3.9+
- uv package manager
- Node.js (for BAML CLI)
- DGraph instance running (local or remote)
- OpenAI API key (or Anthropic API key as alternative)

## Installation

### 1. Install uv

```bash
# Install uv package manager
curl -LsSf https://astral.sh/uv/install.sh | sh

# Or using pip
pip install uv
```

### 2. Install BAML CLI

```bash
npm install -g @boundaryml/baml
```

### 3. Clone and Setup Project

```bash
# Navigate to project directory
cd baml-dgraph-patient-graph-example

# Install dependencies and create virtual environment
uv sync

# Copy environment template
cp env.example .env

# Edit .env with your API keys and DGraph connection
# DGRAPH_CONNECTION_STRING=dgraph://your-dgraph-host:9080
```

### Quick Start (Automated)

For a fully automated setup, use the provided script:

```bash
# Make the script executable and run it
chmod +x quick_start.sh
./quick_start.sh
```

This script will:
- Check for required tools (uv, baml-cli)
- Install them if missing
- Set up the project environment
- Generate BAML client
- Create .env file
- Run setup verification

### 4. Initialize BAML Project

```bash
# Initialize BAML project
baml-cli init

# Generate Python client
baml-cli generate
```

## Configuration

### Environment Variables

Create a `.env` file with the following variables:

```bash
# OpenAI API Key for BAML
OPENAI_API_KEY=your_openai_api_key_here

# Anthropic API Key (alternative)
ANTHROPIC_API_KEY=your_anthropic_api_key_here

# DGraph Configuration
DGRAPH_CONNECTION_STRING=dgraph://localhost:9080
```

### DGraph Connection

The system supports the `dgraph://` connection string format:
- `dgraph://localhost:9080` - Local DGraph instance
- `dgraph://your-dgraph-host.com:9080` - Remote DGraph instance
- `localhost:9080` - Direct host:port format (fallback)

## Usage

### 1. Start DGraph (if not running)

```bash
# Using Docker
docker run -it -p 8080:8080 -p 9080:9080 dgraph/standalone:latest

# Or connect to existing DGraph instance
```

### 2. FLIR Parquet Data Import

The project now supports importing large-scale medical data from FLIR parquet files:

```bash
# Test import with first 3 records
make test-full-import

# Full import of all records (2,726 records)
make full-import
```

This will:
- Load the FLIR parquet file with medical records
- Extract structured data using BAML and GPT-4
- Import each record into DGraph with proper relationships
- Show real-time progress and import status

### 3. Extract Medical Data

```bash
# Run BAML extraction on sample texts
uv run python scripts/extract_medical_data.py
```

This will:
- Process sample medical texts using BAML
- Extract structured data (patient info, visits, allergies, etc.)
- Save results to JSON files
- Create `all_medical_records.json` for DGraph import

### 4. Import to DGraph

```bash
# Import extracted data into DGraph
uv run python scripts/dgraph_importer.py
```

This will:
- Setup DGraph schema for medical data
- Import all extracted records
- Create relationships between entities
- Run test queries to verify import

### 5. Run Complete Demo Workflow

```bash
# Run the complete end-to-end demonstration
uv run python demo_workflow.py
```

This demonstrates the full pipeline:
- BAML extraction from medical text
- DGraph import with schema setup
- Data querying and verification

## Data Organization

The project organizes data files in a dedicated `data/` directory:

- **`data/all_medical_records.json`**: Combined extracted data for DGraph import
- **`data/extracted_sample_*.json`**: Individual extraction results
- **`data/flir-data.parquet`**: Additional sample data files

This organization keeps the root directory clean and separates code from data files.

##  Sample Notes Input

```text
	
### Instruction:
Patient Information:

Mr. Colton Tracey McCullough, a male, born on the 5th of December, 1967, is an English (United States) speaking individual.

Clinical Encounter:

Drawing back to his medical history, Mr. McCullough had an eventful medical encounter on the 6th of November, 2018. The encounter took place at Fitchburg Outpatient Clinic, from 3:28:31 AM to 4:28:31 AM, Central European Time. Dr. Ted Reilly took the responsibility to manage and guide him through this medical encounter, where Mr. McCullough was identified as his prominent patient. The purpose of this encounter was to address prevailing health issues; primary among them was chronic congestive heart failure. This ambitious act marked a significant point in his diagnosis.

The Outpatient Clinic:

The encounter occurred at the esteemed medical institute, Fitchburg Outpatient Clinic, located at 881 Main Street, Fitchburg, MA 01420, United States of America. Being a sturdy pillar in the healthcare field, the Clinic serves as a highly respected healthcare provider in the community.

Medical Procedure:

Adding to his long medical chronicle, Mr. McCullough underwent a medical procedure on the same day of his encounter. This meticulous procedure, which was successfully completed, comprehensibly transpired from 3:28:31 AM to 03:43:31 AM, Central European Time.
It's noteworthy that these aforementioned details cohesively form the medical history of Mr. McCullough in concordance with his clinical journey at the Fitchburg Outpatient Clinic.
```

## Sample Output

The system extracts structured data like:

```json
{
  "patient": {
    "name": "Mr. Hobert Armand Bashirian",
    "patient_id": null,
    "marital_status": "widowed",
    "age": 34,
    "gender": null,
    "date_of_birth": "1988-11-12"
  },
  "visits": [
    {
      "visit_type": "health questionnaire",
      "start_time": "2023-02-11T02:48:00+01:00",
      "end_time": null,
      "timezone": "+01:00",
      "location": null,
      "provider": null,
      "notes": "Evaluated wellbeing"
    }
  ],
  "allergies": [],
  "provider_facility": null,
  "extracted_entities": [
    "intimate partner abuse",
    "condition",
    "health questionnaire"
  ]
}
```

### Complex Medical Record Example

```json
{
  "patient": {
    "name": "Mrs. Shanon Wolff",
    "marital_status": null,
    "age": 31,
    "gender": "Female",
    "date_of_birth": "January 22, 1992"
  },
  "visits": [
    {
      "visit_type": "Triglycerides Measurement",
      "start_time": "April 5, 2023, 14:14:00",
      "end_time": null,
      "timezone": "GMT+2",
      "location": null,
      "provider": null,
      "notes": null
    },
    {
      "visit_type": "Tobacco smoking status assessment",
      "start_time": "April 5, 2023, 14:14:00",
      "end_time": null,
      "timezone": "GMT+2",
      "location": null,
      "provider": null,
      "notes": null
    }
  ],
  "allergies": [],
  "provider_facility": null,
  "extracted_entities": []
}
```

## DGraph Schema

The system creates a comprehensive graph schema with the following entity types and relationships:

### Core Entities

- **Patient nodes**: Core patient information (name, age, gender, marital status, date of birth)
- **Medical Visit nodes**: Visit details with timestamps, types, and notes
- **Provider nodes**: Healthcare provider information (name, ID, specialty, role)
- **Allergy nodes**: Patient allergy records (allergen, severity, reaction type, confirmed date)
- **Address nodes**: Facility and provider locations (street, city, state, zip, country)
- **Organization nodes**: Healthcare facilities, clinics, and labs
- **Substance nodes**: Medications, environmental factors, and other substances
- **Medical Condition nodes**: Patient conditions and diagnoses
- **Immunization nodes**: Vaccine records and administration details
- **Clinical Observation nodes**: Lab results, vital signs, and measurements
- **Medical Procedure nodes**: Medical procedures and interventions
- **Contact Information nodes**: Patient contact details and preferences
- **Social History nodes**: Employment, education, insurance, and lifestyle factors
- **Extraction Record nodes**: Metadata about data extraction process

### Key Relationships

- `has_visit` / `visit_of`: Links patients to medical visits
- `has_allergy` / `allergy_of`: Links patients to allergy records
- `has_immunization` / `immunization_of`: Links patients to immunization records
- `has_condition` / `condition_of`: Links patients to medical conditions
- `lives_in` / `location_of_patient`: Links patients to addresses
- `treated_by` / `treats`: Links patients to healthcare providers
- `works_at` / `employs_provider`: Links providers to organizations
- `contains_patient`: Links extraction records to patients

### Schema Features

- **Full-text indexing** on key fields like names, notes, and descriptions
- **Exact indexing** on identifiers, dates, and categorical fields
- **Bidirectional relationships** for efficient querying in both directions
- **Proper data types** (string, int, bool, float) with appropriate indexing
- **Upsert support** for duplicate prevention and data consistency

## Querying Data

After import, you can query the graph using DGraph's DQL syntax. Here are some practical examples focused on medical analysis and patient allergies:

### Basic Patient Information Query

```dql
{
  patient(func: eq(name, "Mr. Hobert Armand Bashirian")) {
    name
    age
    marital_status
    date_of_birth
    has_visit {
      visit_type
      start_time
      timezone
      notes
    }
    has_allergy {
      allergen
      severity
      reaction_type
      confirmed_date
    }
  }
}
```

### Allergy-Focused Queries

#### Find Patients with Multiple Allergies
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

#### Allergy Analysis by Severity
```dql
{
  allergy_severities(func: has(severity)) {
    severity
    count(uid)
  }
}
```

#### Food Allergy Analysis
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

#### Medication Allergy Analysis
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

### Complex Relationship Queries

#### Find Patients with Multiple Visit Types
```dql
{
  patients(func: has(has_visit)) @filter(gt(count(has_visit), 1)) {
    name
    age
    has_visit {
      visit_type
      start_time
      notes
    }
  }
}
```

#### Temporal Analysis of Visits
```dql
{
  visits(func: eq(dgraph.type, "MedicalVisit")) @filter(ge(start_time, "2023-01-01")) {
    visit_type
    start_time
    timezone
    visit_of {
      name
      age
    }
  }
}
```

#### Provider and Organization Relationships
```dql
{
  providers(func: eq(dgraph.type, "MedicalProvider")) {
    name
    specialty
    treats {
      name
      age
    }
    works_at {
      name
      type
    }
  }
}
```

### Advanced Analytics Queries

#### Patient Demographics Analysis
```dql
{
  patients(func: eq(dgraph.type, "Patient")) {
    name
    age
    gender
    marital_status
    count(has_visit)
    count(has_allergy)
  }
}
```

#### Visit Type Distribution
```dql
{
  visit_types(func: eq(dgraph.type, "MedicalVisit")) {
    visit_type
    count(visit_of)
  }
}
```

### Comprehensive Query Collection

For a complete collection of 20+ medical analysis queries including allergy patterns, risk assessment, and clinical decision support, see **[DATABASE_QUERIES.md](DATABASE_QUERIES.md)**.

The full query collection includes:
- **Allergy Prevalence Analysis** - Environmental, food, and medication allergies
- **Risk Stratification** - High-risk patients and prevention strategies  
- **Clinical Decision Support** - Cross-referencing allergies with medical conditions
- **Geographic Analysis** - Location-based allergy patterns
- **Temporal Trends** - Seasonal and historical allergy data
- **Quality Metrics** - Documentation completeness and consistency analysis

## Development

### Using uv Commands

```bash
# Install dependencies
uv sync

# Install development dependencies
uv sync --extra dev

# Add new dependency
uv add package_name

# Add development dependency
uv add --dev package_name

# Remove dependency
uv remove package_name

# Run Python script in virtual environment
uv run python script.py

# Activate virtual environment
source .venv/bin/activate
```

### Using Make Commands

```bash
# Show all available commands
make help

# Install dependencies
make install

# Install development dependencies
make install-dev

# Run tests
make test

# Run complete pipeline
make pipeline

# Clean up
make clean
```

The `clean` target removes generated files from both the root directory and the `data/` directory.

## Customization

### Adding New Medical Text

1. Add your medical text to `scripts/sample_medical_text.py`
2. Update the `samples` list in `extract_medical_data.py`
3. Run the extraction script

### Modifying Data Models

1. Update `baml_src/medical_models.baml` with new fields
2. Regenerate BAML client: `baml-cli generate`
3. Update DGraph schema in `dgraph_importer.py`
4. Re-run the pipeline

### Using Different LLMs

Modify `baml_src/clients.baml` to use different models or providers.

## Troubleshooting

### Common Issues

1. **BAML Client Not Found**: Run `baml-cli generate` after setup
2. **DGraph Connection Failed**: Check your connection string and ensure DGraph is running
3. **API Key Errors**: Verify your `.env` file has correct API keys
4. **Schema Errors**: DGraph schema setup warnings are usually safe to ignore
5. **uv Not Found**: Install uv with `curl -LsSf https://astral.sh/uv/install.sh | sh`

### Debug Mode

Enable verbose logging by setting environment variables:
```bash
export PYTHONPATH="${PYTHONPATH}:$(pwd)"
export BAML_LOG_LEVEL=DEBUG
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Test with sample data
5. Submit a pull request

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Support

For issues related to:
- **BAML**: Check [BAML documentation](https://docs.boundaryml.com/)
- **DGraph**: Check [DGraph documentation](https://dgraph.io/docs/)
- **uv**: Check [uv documentation](https://docs.astral.sh/uv/)
- **This Project**: Open an issue in the repository

## Graph RAG Enabled Medical Analysis Sample Topics

### Patient Demographics & Patterns
#### Age and Visit Analysis:
- What is the age distribution of patients and how does it correlate with visit frequency?
- Do older patients have more medical visits or different types of visits?
- What's the average time between visits for patients with multiple encounters?

#### Gender-Based Healthcare Utilization:
- Are there differences in visit types between male and female patients?
- Do certain medical procedures or assessments show gender-based patterns?
- How do gender differences affect allergy patterns and visit frequency?

### Allergy Management & Safety
#### Allergy Severity Analysis:
- What's the distribution of allergy severities across the patient population?
- Which allergens are associated with the most severe reactions?
- How many patients have multiple allergies, and what are common combinations?

#### Allergy Documentation Quality:
- Which allergy records have complete documentation (confirmed dates, severity, reaction types)?
- Are there gaps in allergy documentation that could impact patient safety?
- Which patients need allergy documentation updates?

### Visit Analysis & Care Coordination
#### Visit Type Patterns:
- What are the most common types of medical visits (immunizations, observations, procedures)?
- Which patients have the most diverse visit types (indicating complex care needs)?
- Are there seasonal patterns in immunizations or other visit types?

#### Care Continuity & Timing:
- Which patients have had visits spanning multiple years?
- Are there patients with gaps in care that might need follow-up?
- What's the typical duration and frequency of different visit types?

### Temporal Healthcare Trends
#### Historical Care Patterns:
- How has the frequency of different visit types changed over time?
- Are there patients with very old allergy confirmations that might need updating?
- What's the timeline of care for patients with multiple visits?

#### Seasonal & Time-Based Analysis:
- Are there patterns in visit timing (morning vs. afternoon, weekdays vs. weekends)?
- Do certain visit types cluster around specific times of year?

### Data Quality & Completeness
#### Missing Information Analysis:
- Which patients lack complete demographic information?
- How many patients have visits but no associated provider information?
- Are there patients missing critical allergy or contact information?

#### Data Consistency Checks:
- Which patients have inconsistent information across visits?
- Are there duplicate or conflicting allergy records?

### Geographic Healthcare Access
#### Location-Based Care:
- Are patients clustered around certain medical facilities?
- What's the geographic distribution of different visit types?
- Are there areas with limited healthcare access based on patient addresses?

#### Facility Utilization:
- Which healthcare facilities serve the most diverse patient populations?
- Are there geographic patterns in visit types or provider specialties?

### Risk Stratification & Clinical Decision Support
#### High-Risk Patient Identification:
- Which patients have multiple severe allergies that require special monitoring?
- Are there patients with concerning gaps between visits who might need outreach?
- Which patients have complex medical histories (multiple visit types + allergies)?

#### Medication & Allergy Safety:
- Are there patients with drug allergies who had medication-related visits?
- Which patients might benefit from allergy testing based on their visit patterns?
- Are there potential drug-allergy interactions that need attention?

### Advanced Analytics Opportunities
#### Population Health Insights:
- What are the most common health conditions in different age groups?
- How do visit patterns correlate with patient demographics?
- Which patient populations have the highest healthcare utilization?

#### Quality Metrics:
- What's the completeness of documentation across different visit types?
- How do provider specialties correlate with visit outcomes?
- Are there patterns in follow-up care after certain visit types?

### Sample Queries You Could Run:

#### Patient Safety Queries:
- **"Find patients with multiple severe allergies and their most recent visit"**
- **"Identify patients who haven't had visits in over 2 years"**
- **"Find patients with shellfish allergies who had visits but no allergy warnings documented"**

#### Care Quality Queries:
- **"Analyze the relationship between patient age and number of allergies"**
- **"Find all immunization visits and check if patients have documented allergies"**
- **"Identify patients with incomplete allergy documentation"**

#### Complex Care Analysis:
- **"Find patients with the most complex care patterns (multiple visit types)"**
- **"Which patients have visits spanning multiple years with different providers?"**
- **"Analyze care continuity for patients with chronic conditions"**

#### Clinical Decision Support:
- **"Which patients might benefit from allergy testing based on their visit patterns?"**
- **"Are there medication-related visits for patients with drug allergies?"**
- **"Find patients with gaps in preventive care (immunizations, screenings)"**

## Geospatial Capabilities

The system includes comprehensive geospatial capabilities for medical addresses:

### BAML Geocoding
- **Automatic Geocoding**: BAML automatically geocodes addresses during extraction
- **Coordinate Storage**: Locations are stored as separate `latitude` and `longitude` float fields
- **Precision**: Coordinates use WGS84 standard with 6 decimal places for accuracy
- **Geocoding Status**: Each address includes a `geocoded` boolean flag

### DGraph Schema
- **Latitude/Longitude**: Stored as `float` types with `@index(float)` for efficient querying
- **Geo Predicate**: Additional `geo` predicate with `@index(geo)` for advanced geospatial operations
- **Hybrid Approach**: Maintains both coordinate types for flexibility

### Post-Processing Conversion
A separate Python script (`scripts/add_geo_locations.py`) converts latitude/longitude coordinates to GeoJSON format:
- Finds all Address nodes with coordinates
- Converts to GeoJSON Point format: `POINT(longitude latitude)`
- Adds `location` predicate with `geo` type and geospatial indexing
- Preserves existing `latitude`/`longitude` fields for backward compatibility

### Usage Examples
```bash
# Add geo location predicates to existing nodes
make add-geo-locations

# Test coordinate conversion functionality
make test-geo-conversion

# Manual execution
uv run python scripts/add_geo_locations.py
uv run python scripts/test_geo_conversion.py
```

### Sample Geospatial Queries
```dql
# Find patients within 50km of a specific location
{
  nearby_patients(func: type(Address)) @filter(ge(location, "POINT(-71.0589 42.3601)", 50)) {
    uid
    street
    city
    state
    ~location_of_patient {
      name
      age
    }
  }
}

# Find healthcare facilities in a specific region
{
  facilities(func: type(Organization)) @filter(ge(location, "POINT(-74.006 40.7128)", 100)) {
    name
    type
    specialties
  }
}

# Find addresses within a bounding box
{
  region_addresses(func: type(Address)) @filter(ge(location, "POINT(-74.006 40.7128)", 100)) {
    uid
    street
    city
    state
    latitude
    longitude
    location
  }
}
```

## Vector Embedding Capabilities

The system includes advanced vector embedding capabilities for semantic search and similarity analysis:

### Ollama Setup and Installation

#### 1. Install Ollama
```bash
# macOS/Linux
curl -fsSL https://ollama.ai/install.sh | sh

# Windows
# Download from: https://ollama.ai/download

# Docker
docker run -d -v ollama:/root/.ollama -p 11434:11434 --name ollama ollama/ollama
```

#### 2. Start Ollama Service
```bash
# Start the Ollama service
ollama serve

# Or run in background
ollama serve &
```

#### 3. Pull Required Models
```bash
# Pull the recommended embedding model (small, fast, accurate)
ollama pull nomic-embed-text

# Alternative models (larger, more powerful)
ollama pull llama2
ollama pull qwen2:7b
```

#### 4. Verify Installation
```bash
# Test Ollama connection
make test-embeddings

# Or manually test
curl http://localhost:11434/api/tags
```

#### 5. Environment Configuration
```bash
# Add to your .env file (optional, defaults to localhost:11434)
OLLAMA_URL=http://localhost:11434

# Or set environment variable
export OLLAMA_URL=http://localhost:11434
```

### Ollama Integration
- **Local Embedding Generation**: Uses Ollama for local, privacy-preserving embedding generation
- **Multiple Model Support**: Automatically detects available models (nomic-embed-text, llama2, etc.)
- **Configurable Endpoint**: Supports custom Ollama URLs via `OLLAMA_URL` environment variable

### DGraph Vector Storage
- **Vector Type**: Uses DGraph's `vector` type with `@index(vector)` for efficient similarity search
- **Metadata Storage**: Stores embedding model, source text, and dimensions for traceability
- **Batch Processing**: Efficiently processes multiple nodes in single transactions

### Supported Node Types
- **Patients**: Demographics, medical history, and personal information
- **Medical Visits**: Visit types, reasons, status, and clinical notes
- **Allergies**: Allergens, severity, reactions, and clinical status
- **Immunizations**: Vaccine details, administration, and clinical notes
- **Conditions**: Medical conditions, severity, and clinical status
- **Providers**: Medical staff information, specialties, and roles

### Usage Examples
```bash
# Test embedding functionality
make test-embeddings

# Add embeddings to all nodes
make add-embeddings

# Manual execution
uv run python scripts/test_embeddings.py
uv run python scripts/add_embeddings.py
```

### Sample Vector Similarity Queries
```dql
# Find patients with similar medical conditions
{
  similar_patients(func: type(Patient)) @filter(ge(embedding, "query_embedding_vector", 0.8)) {
    uid
    name
    age
    ~has_condition {
      condition_name
      severity
    }
  }
}

# Find similar medical visits
{
  similar_visits(func: type(MedicalVisit)) @filter(ge(embedding, "visit_embedding_vector", 0.8)) {
    uid
    visit_type
    reason
    notes
    embedding_model
    embedding_dimensions
  }
}

# Semantic search across all medical entities
{
  semantic_results(func: has(embedding)) @filter(ge(embedding, "search_query_vector", 0.7)) {
    uid
    dgraph.type
    embedding_model
    embedding_dimensions
  }
}
```

### Benefits for Medical Analysis
- **Semantic Understanding**: Finds related concepts even with different terminology
- **Clinical Decision Support**: Identifies similar patient cases and treatment patterns
- **Research Insights**: Discovers hidden relationships in medical data
- **Natural Language Search**: Enables conversational queries across medical records
- **Scalable AI**: Foundation for advanced medical AI applications

### Troubleshooting Ollama Issues

#### Common Problems and Solutions

**1. Ollama Service Not Running**
```bash
# Check if Ollama is running
ps aux | grep ollama

# Start Ollama if not running
ollama serve
```

**2. Model Not Found**
```bash
# List available models
ollama list

# Pull missing model
ollama pull nomic-embed-text
```

**3. Port Already in Use**
```bash
# Check what's using port 11434
lsof -i :11434

# Kill existing process or use different port
export OLLAMA_URL=http://localhost:11435
ollama serve -p 11435
```

**4. Memory Issues with Large Models**
```bash
# Use smaller models for embeddings
ollama pull nomic-embed-text  # ~260MB
ollama pull llama2:7b         # ~4GB

# Check system resources
free -h  # Linux
top       # macOS
```

**5. Test Connection Manually**
```bash
# Test basic connectivity
curl http://localhost:11434/api/tags

# Test embedding generation
curl -X POST http://localhost:11434/api/embeddings \
  -H "Content-Type: application/json" \
  -d '{"model": "nomic-embed-text", "prompt": "test"}'
```

#### Performance Optimization
- **Use SSD storage** for faster model loading
- **Allocate sufficient RAM** (at least 8GB for larger models)
- **Consider GPU acceleration** if available (CUDA support)
- **Batch processing** for large datasets (already implemented in the script)