# Intro to Spark Declarative Pipelines Tutorial

This repository contains a hands-on tutorial for building data pipelines using Spark Declarative Pipelines (SDP), a framework that enables data engineers to define data pipelines in simple, high-level terms while allowing Spark to handle complex task orchestration and optimizations behind the scenes.

## Prerequisites

### System Requirements
- Python 3.8 or higher
- Java 17 or higher
- At least 4GB of available RAM

### Required Software
- **PySpark 4.1.0 or later** with Spark Declarative Pipelines support
- A code editor (VS Code, PyCharm, etc.)
- Terminal/Command Prompt access

## Setup Instructions

### 1. Create a Python Virtual Environment

First, create an isolated Python environment to avoid conflicts with other projects:

```bash
# Create a new virtual environment
python -m venv .venv

# Activate the virtual environment
# On macOS/Linux:
source .venv/bin/activate

# On Windows:
.venv\Scripts\activate
```

Verify your virtual environment is active - you should see `(.venv)` in your terminal prompt.

### 2. Install PySpark 4.1.0

Since PySpark 4.1.0 with Spark Declarative Pipelines is still in development, you'll need to install a development build:

#### Install Pre-release Version (if available)
```bash
pip install pyspark==4.1.0-dev
```

## Running the Tutorial

### Step 1: Generate Test Data

Before running the pipeline, you need to generate mock web traffic log data:

```bash
python log_generator.py
```

#### Expected Output:
- A new directory called `web_traffic_logs` will be created
- 5 JSON files will be generated: `web_logs_0.json` through `web_logs_4.json`

#### Verify Data Generation:
```bash
# Check that the directory was created
ls -la web_traffic_logs/

# Expected output:
# web_logs_0.json
# web_logs_1.json  
# web_logs_2.json
# web_logs_3.json
# web_logs_4.json

# Inspect the structure of one file
head -20 web_traffic_logs/web_logs_0.json
```

Each JSON file should contain a CloudWatch-like structure with log events including timestamps, IP addresses, HTTP methods, page URLs, status codes, and user agents.

### Step 2: Run the Declarative Pipeline

Execute the main pipeline script:

```bash
python main.py
```

#### Expected Pipeline Output:

The pipeline will display timestamped events as it processes the data. You should see output similar to:

```
[2025-01-XX XX:XX:XX] Pipeline started for dataflow graph: <graph_id>
[2025-01-XX XX:XX:XX] Registering streaming table: raw_traffic_logs
[2025-01-XX XX:XX:XX] Registering flow: ingest_raw_traffic_logs -> raw_traffic_logs  
[2025-01-XX XX:XX:XX] Registering streaming table: augmented_traffic_logs
[2025-01-XX XX:XX:XX] Registering flow: augment_web_traffic_logs -> augmented_traffic_logs
[2025-01-XX XX:XX:XX] Starting pipeline execution...
[2025-01-XX XX:XX:XX] Processing batch 1 for raw_traffic_logs
[2025-01-XX XX:XX:XX] Processing batch 1 for augmented_traffic_logs
[2025-01-XX XX:XX:XX] Pipeline running successfully...
```

#### Verify Pipeline Success:

1. **No Error Messages**: The pipeline should run without throwing exceptions
2. **Continuous Processing**: You should see periodic batch processing messages
3. **Data Flow**: Both `raw_traffic_logs` and `augmented_traffic_logs` should show processing activity

### Step 3: Understanding the Pipeline Output

The tutorial pipeline creates two streaming tables under the `spark-warehouse` directory:

1. **raw_traffic_logs**: Contains the original JSON log data from the generated files
2. **augmented_traffic_logs**: Contains enriched data with calculated columns:
   - `product_id`: Extracted from product page URLs
   - `is_error`: Boolean flag for 4xx/5xx status codes  
   - `is_mobile`: Mobile device detection based on user agent
   - `page_type`: Categorization (home, product, cart, checkout, other)
   - `payload_size_kb`: Mock payload size based on response time

## Troubleshooting

### Common Issues

#### "ModuleNotFoundError: No module named 'pyspark.pipelines'"
- Ensure you're using PySpark 4.1.0 or later with SDP support
- Verify your virtual environment is activated
- Try rebuilding PySpark from source following the Part 1 tutorial

#### "FileNotFoundError" when running main.py
- Make sure you ran `python log_generator.py` first
- Verify the `web_traffic_logs` directory exists and contains JSON files

#### Pipeline stops immediately or shows no output
- Check that your Spark session is properly initialized
- Ensure the JSON schema in the pipeline matches the generated data structure
- Try running with `dry=True` in the `start_run` function to test the query plan

#### Java/Memory Issues
- Ensure you have Java 17 installed
- Increase available memory if needed: `export SPARK_DRIVER_MEMORY=4g`

## Project Structure

```
spark-declarative-pipelines-tutorial/
├── main.py                 # Main pipeline implementation
├── log_generator.py        # Test data generator
├── README.md              # This file
└── web_traffic_logs/      # Generated test data (created after running log_generator.py)
    ├── web_logs_0.json
    ├── web_logs_1.json
    ├── web_logs_2.json
    ├── web_logs_3.json
    └── web_logs_4.json
```

## Cleanup

To clean up your environment:

```bash
# Deactivate the virtual environment
deactivate

# Remove the virtual environment (optional)
rm -rf .venv

# Remove generated test data (optional)
rm -rf web_traffic_logs/
```
