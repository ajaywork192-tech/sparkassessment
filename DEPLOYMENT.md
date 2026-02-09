# NYC Jobs Data Engineering Pipeline - Deployment Guide

## Overview
This document provides comprehensive deployment strategies for the NYC Jobs Data Engineering pipeline across different environments.

## Architecture

```
Raw Data (CSV)
    ↓
[Data Exploration & Profiling]
    ↓
[Data Cleaning & Feature Selection]
    ↓
[Feature Engineering]
    ↓
[KPI Computation]
    ↓
[Output: Parquet + CSV Reports]
```

---

## Deployment Scenarios

### 1. Local Development (Standalone Spark)

#### Prerequisites
```bash
# Install Python dependencies
pip install pyspark pytest pytest-ipytest matplotlib pandas

# Verify Spark installation
spark-submit --version
```

#### Execution
```bash
# Run the pipeline locally
python nyc_jobs_pipeline.py

# Run with Docker (optional)
docker run -v $(pwd):/app pyspark:latest python /app/nyc_jobs_pipeline.py
```

#### Expected Output
- Console logs showing each pipeline phase
- Profiling report with data statistics
- KPI analysis results (10 results per KPI)
- CSV exports in `./output/` directory
- Parquet file: `./output/processed_jobs_data/`

---

### 2. Spark Cluster Deployment (Production)

#### Prerequisites
- Spark 2.4.5+ cluster (3+ nodes recommended)
- Hadoop HDFS or S3 for data storage
- Python 3.7+ on all nodes

#### Configuration
Update `get_spark()` function for your cluster:

```python
def get_spark():
    """Configure for production cluster"""
    return (
        SparkSession.builder
        .appName("NYC_Job_DE_Challenge")
        .master("spark://master:7077")  # Your cluster master
        .config("spark.executor.cores", "8")
        .config("spark.executor.memory", "16g")
        .config("spark.driver.memory", "4g")
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )
```

#### Execution
```bash
# Submit to cluster
spark-submit \
  --master spark://master:7077 \
  --deploy-mode cluster \
  --executor-cores 8 \
  --executor-memory 16g \
  --driver-memory 4g \
  nyc_jobs_pipeline.py

# Monitor job
# Access Spark UI: http://master:4040
```

---

### 3. Cloud Deployment (AWS EMR / Databricks)

#### AWS EMR
```bash
# Create cluster
aws emr create-cluster \
  --name nyc-jobs-pipeline \
  --release-label emr-6.10.0 \
  --applications Name=Spark \
  --instance-count 5 \
  --instance-type m5.xlarge \
  --log-uri s3://your-bucket/logs/

# Submit job
aws emr add-steps \
  --cluster-id j-XXXXX \
  --steps Type=Spark,Name="NYC Jobs Pipeline",\
  SparkSubmitParameters="--class org.apache.spark.deploy.SparkSubmit \
  s3://your-bucket/nyc_jobs_pipeline.py"
```

#### Databricks
```python
# Upload to Databricks workspace
# Create job with:
# - Cluster type: All-purpose
# - Python version: 3.9+
# - Libraries: pytest, ipytest, matplotlib

# Run via Databricks API or UI
```

---

### 4. Containerized Deployment (Docker/Kubernetes)

#### Dockerfile
```dockerfile
FROM spark:3.3-python3

WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt

COPY nyc_jobs_pipeline.py .
COPY dataset/nyc-jobs.csv /dataset/

CMD ["spark-submit", "nyc_jobs_pipeline.py"]
```

#### Build & Run
```bash
docker build -t nyc-jobs-pipeline:latest .
docker run -v $(pwd)/output:/app/output nyc-jobs-pipeline:latest
```

#### Kubernetes
```yaml
apiVersion: batch/v1
kind: Job
metadata:
  name: nyc-jobs-pipeline
spec:
  template:
    spec:
      containers:
      - name: pipeline
        image: nyc-jobs-pipeline:latest
        volumeMounts:
        - name: output
          mountPath: /app/output
      restartPolicy: Never
      volumes:
      - name: output
        persistentVolumeClaim:
          claimName: pipeline-output
```

---

## Data Flow

### Input Requirements
- **File**: `nyc-jobs.csv` (CSV format)
- **Size**: ~2-3MB typical
- **Format**: CSV with headers
- **Location**: `/dataset/nyc-jobs.csv` (local) or S3 path

### Output Artifacts
1. **Processed Data** (Parquet)
   - Location: `./output/processed_jobs_data/`
   - Size: ~1-2MB (compressed)
   - Contains: All records with engineered features

2. **KPI Reports** (CSV)
   - `job_stats/` - Top 10 job categories
   - `salary_distribution/` - Salary stats per category
   - `education_correlation/` - Education-salary correlation
   - `highest_salary_by_agency/` - Best paid roles
   - `2yr_avg_salary/` - Agency salary trends
   - `top_skills/` - Highest paid skills in US

---

## Performance Tuning

### For Large Datasets (>1GB)

```python
# Increase parallelism
spark.conf.set("spark.sql.shuffle.partitions", "200")

# Enable Adaptive Query Execution
spark.conf.set("spark.sql.adaptive.enabled", "true")

# Cache intermediate results
df.cache()

# Monitor partitioning
df.rdd.getNumPartitions()
```

### Memory Optimization
- Use Parquet instead of CSV for intermediate data
- Persist only hot dataframes
- Call `unpersist()` when done with large DFs
- Monitor GC with: `--conf spark.driver.extraJavaOptions=-Xmx4g`

---

## Monitoring & Logging

### Pipeline Phases
The pipeline outputs status for each phase:
1. **Data Exploration** - Schema, nulls, statistics
2. **Data Cleaning** - Record counts, validation
3. **Feature Engineering** - Feature details
4. **KPI Analysis** - Results for all 6 KPIs
5. **Data Persistence** - Export confirmations

### Log Levels
```python
# In production, control log verbosity
spark.sparkContext.setLogLevel("WARN")

# Only critical logs printed
# Adjust to INFO for debugging
```

### Metrics to Monitor
- **Execution Time**: Each phase duration
- **Records Processed**: Input vs output counts
- **Failed Records**: Invalid salary ranges, null values
- **Output Size**: Parquet compression ratio

---

## Triggering the Pipeline

### Option 1: Manual Execution
```bash
python nyc_jobs_pipeline.py
```

### Option 2: Scheduled (Cron)
```bash
# Run daily at 2 AM
0 2 * * * cd /home/user/project && python nyc_jobs_pipeline.py >> logs/pipeline.log 2>&1
```

### Option 3: Airflow DAG
```python
from airflow import DAG
from airflow.operators.bash import BashOperator

dag = DAG('nyc_jobs_pipeline', default_view='graph')

run_pipeline = BashOperator(
    task_id='run_pipeline',
    bash_command='spark-submit /path/to/nyc_jobs_pipeline.py',
    dag=dag
)
```

### Option 4: Jenkins Pipeline
```groovy
pipeline {
    agent any
    stages {
        stage('Run Pipeline') {
            steps {
                sh 'spark-submit nyc_jobs_pipeline.py'
            }
        }
        stage('Verify Output') {
            steps {
                sh 'ls -lh output/'
            }
        }
    }
}
```

---

## Testing Strategy

### Unit Tests
```bash
# Run pytest
pytest nyc_jobs_pipeline.py -v

# Expected: 4 tests passing
# - test_normalize_columns
# - test_clean_data_filters_invalid_salary
# - test_feature_engineering_avg_salary
# - test_top_skills_us_filters_non_us
```

### Integration Tests
```bash
# Verify end-to-end pipeline
python nyc_jobs_pipeline.py

# Check outputs exist
[ -d "output/processed_jobs_data" ] && echo "✓ Parquet output exists"
[ -d "output/job_stats" ] && echo "✓ CSV reports exist"
```

### Data Quality Checks
```python
# Validate output
def validate_output():
    df = spark.read.parquet("output/processed_jobs_data")
    assert df.count() > 0, "No records in output"
    assert "edu_rank" in df.columns, "Missing engineered feature"
    assert df.filter(col("avg_salary").isNull()).count() == 0, "Nulls in avg_salary"
```

---

## Troubleshooting

### Common Issues

**Issue**: "CSV file not found"
```
Solution: Ensure /dataset/nyc-jobs.csv exists or update path in code
```

**Issue**: "OutOfMemory exception"
```
Solution: Increase executor memory
spark-submit --executor-memory 32g nyc_jobs_pipeline.py
```

**Issue**: "Slow KPI computation"
```
Solution: Enable adaptive query execution and shuffle optimization
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.shuffle.partitions", "200")
```

**Issue**: "Feature engineering produces nulls"
```
Solution: Validate minimum_qual_requirements column is not empty
df.filter(col("minimum_qual_requirements").isNotNull()).show()
```

---

## Performance Benchmarks

Expected performance on standard hardware (4 cores, 8GB RAM):
- **Data Load**: ~2 seconds
- **Profiling**: ~1 second
- **Cleaning**: ~3 seconds
- **Feature Engineering**: ~2 seconds
- **KPI Computation**: ~5 seconds
- **Export**: ~3 seconds
- **Total**: ~16 seconds

For production clusters, expect 10x faster execution with proper resource allocation.

---

## Future Enhancements

1. **Real-time Processing**: Stream job postings via Kafka
2. **ML Pipeline**: Predict salary based on job features
3. **Dashboard**: Tableau/Looker integration for KPIs
4. **API Endpoint**: REST service for KPI queries
5. **AutoML**: Feature selection optimization
6. **Data Validation**: Great Expectations integration

---

## Support & Maintenance

- **Code Reviews**: All changes require test coverage
- **Documentation**: Keep this guide updated
- **Monitoring**: Set up alerts for pipeline failures
- **Versioning**: Tag releases in git
- **Backups**: Archive processed data monthly

