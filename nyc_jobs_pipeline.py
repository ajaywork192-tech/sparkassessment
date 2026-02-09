"""
NYC Job Data Engineering Challenge

This is a Spark pipeline for analyzing NYC job postings. We're basically
loading a CSV, cleaning it up, doing some feature engineering, computing
a few business metrics (KPIs), and exporting the results.

Built with:
- PySpark for the heavy lifting
- pytest for unit tests
- Parquet for data storage (it's faster than CSV)

Kept the code straightforward so it's easy to follow.
"""

import os
import pytest
import ipytest

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, lower, when, to_date,
    add_months, max as spark_max,
    split, explode, trim, avg, round, length, regexp_replace
)
from pyspark import StorageLevel

ipytest.autoconfig()

# Spark Session
def get_spark():
    """Creates a SparkSession connected to a Spark cluster."""
    return (
        SparkSession.builder
        .appName("NYC_Job_DE_Challenge")
        .master("spark://master:7077")
        .getOrCreate()
    )

# Data Exploration
def profile_data(df: DataFrame) -> None:
    """Quick data profiling - shows schema, nulls, stats. Helps us understand what we're working with."""
    print("Data profile:")
    
    print(f"Records: {df.count():,} | Columns: {len(df.columns)}")
    print("\nSchema:")
    df.printSchema()
    
    print("\nMissing values:")
    for col_name in df.columns:
        null_count = df.filter(col(col_name).isNull()).count()
        null_pct = (null_count / df.count()) * 100
        if null_pct > 0:
            print(f"  {col_name}: {null_pct:.1f}%")


def select_features_for_analysis(df: DataFrame) -> DataFrame:
    """Drop columns we don't need and explain why each was removed.

    This prints a short human-friendly summary of dropped columns and the
    rationale for removing them (when known). The function keeps a focused
    set of columns that are used by downstream analysis and KPIs.
    """
    cols_to_keep = [
        "agency", "business_title", "job_category", "posting_date", "post_until",
        "salary_range_from", "salary_range_to", "work_location",
        "minimum_qual_requirements", "preferred_skills", "salary_frequency",
        "posting_type"
    ]

    # Use only columns that actually exist in the incoming DataFrame
    available_cols = [c for c in cols_to_keep if c in df.columns]

    # Determine which columns we will drop and provide short reasons
    dropped = [c for c in df.columns if c not in available_cols]
    if dropped:
        reasons = {
            "recruitment_contact": "too sparse",
            "division_work_unit": "redundant with agency",
            "additional_information": "unstructured free text",
            "posting_updated": "duplicate of posting_date",
            "process_date": "not relevant to market analysis",
            "to_apply": "external link / not analytical",
            "job_id": "identifier, not used in analysis",
            "work_location_1": "duplicate location field"
        }

        print("Dropping columns not used for analysis:")
        for col_name in dropped:
            reason = reasons.get(col_name, "not required for this analysis")
            print(f" - {col_name}: {reason}")

    return df.select(*available_cols)

# Data Cleaning
def normalize_columns(df: DataFrame) -> DataFrame:
    """Convert column names to lowercase with underscores. Prevents SQL query issues."""
    new_cols = [
        c.lower().replace(" ", "_").replace("/", "_").replace("-", "_")
        for c in df.columns
    ]
    return df.toDF(*new_cols)


def clean_data(df: DataFrame) -> DataFrame:
    """Remove duplicates, fix dates, and filter out bad records (like salary_to < salary_from)."""
    return (
        df.dropDuplicates()
        .withColumn("posting_date", to_date("posting_date"))
        .withColumn("post_until", to_date("post_until"))
        .filter(col("posting_date").isNotNull())
        .filter(col("salary_range_from").isNotNull())
        .filter(col("salary_range_to").isNotNull())
        .filter(col("salary_range_to") >= col("salary_range_from"))
    )


def apply_feature_engineering(df: DataFrame) -> DataFrame:
    """Add useful features: avg_salary (middle of range), education_level (category), edu_rank (0-6 scale)."""
    return (
        df.withColumn(
            "avg_salary",
            (col("salary_range_from") + col("salary_range_to")) / 2
        )
        .withColumn(
            "education_level",
            when(lower(col("minimum_qual_requirements")).contains("master"), "Masters")
            .when(lower(col("minimum_qual_requirements")).contains("graduate"), "Graduate")
            .when(lower(col("minimum_qual_requirements")).contains("phd"), "PhD")
            .when(lower(col("minimum_qual_requirements")).contains("doctorate"), "Doctorate")
            .when(lower(col("minimum_qual_requirements")).contains("bachelor"), "Bachelors")
            .when(lower(col("minimum_qual_requirements")).contains("associate"), "Associates")
            .when(lower(col("minimum_qual_requirements")).contains("vocational|trade school"), "Vocational")
            .when(lower(col("minimum_qual_requirements")).contains("high school|ged|diploma"), "HighSchool")
            .otherwise("Other")
        )
        .withColumn(
            "edu_rank",
            when(col("education_level") == "PhD", 6)
            .when(col("education_level") == "Doctorate", 6)
            .when(col("education_level") == "Masters", 5)
            .when(col("education_level") == "Graduate", 5)
            .when(col("education_level") == "Bachelors", 4)
            .when(col("education_level") == "Associates", 2)
            .when(col("education_level") == "Vocational", 1)
            .when(col("education_level") == "HighSchool", 1)
            .otherwise(0)
        )
    )

# Business Metrics
def compute_kpis(df: DataFrame):
    """Compute 6 KPIs: job volume, salaries, education correlation, etc."""
    df.createOrReplaceTempView("jobs")
    spark = df.sql_ctx.sparkSession

    max_date_val = df.select(spark_max("posting_date")).collect()[0][0]

    # KPI 1: Job volume & average salary by category
    stats = spark.sql("""
        SELECT job_category,
               COUNT(*) AS job_count,
               ROUND(AVG(avg_salary), 2) AS avg_sal
        FROM jobs
        GROUP BY job_category
        ORDER BY job_count DESC
        LIMIT 10
    """)

    # KPI 2: Salary distribution by job category
    salary_distribution = spark.sql("""
        SELECT job_category,
               MIN(avg_salary) AS min_salary,
               MAX(avg_salary) AS max_salary,
               ROUND(AVG(avg_salary), 2) AS avg_salary,
               percentile_approx(avg_salary, 0.5) AS median_salary
        FROM jobs
        GROUP BY job_category
        ORDER BY avg_salary DESC
    """)

    # KPI 3: Correlation between education level and salary
    correlation = spark.sql("""
        SELECT corr(edu_rank, avg_salary) AS edu_salary_corr
        FROM jobs
        WHERE edu_rank > 0
    """)

    # KPI 4: Highest paying role per agency
    highest_sal_agency = spark.sql("""
        SELECT agency, business_title, avg_salary
        FROM (
            SELECT agency,
                   business_title,
                   avg_salary,
                   ROW_NUMBER() OVER (
                       PARTITION BY agency
                       ORDER BY avg_salary DESC
                   ) AS rnk
            FROM jobs
        )
        WHERE rnk = 1
        ORDER BY avg_salary DESC
    """)

    # KPI 5: Rolling 2-year average salary per agency
    avg_sal_2yr = spark.sql(f"""
        SELECT agency,
               ROUND(AVG(avg_salary), 2) AS rolling_avg
        FROM jobs
        WHERE posting_date >= add_months(
            CAST('{max_date_val}' AS DATE), -24
        )
        GROUP BY agency
        ORDER BY rolling_avg DESC
    """)

    # KPI 6: Top paid skills in US market (handles multiple delimiters)
    # More robust skill extraction:
    # - split only on explicit delimiters (commas, semicolons, slashes, pipes)
    # - trim tokens, drop empty or very short tokens, remove numeric-only tokens
    # - drop common stopwords that appear in free-text skill descriptions
    stopwords = [
        "and", "or", "with", "experience", "years", "year", "skills",
        "knowledge", "ability", "including", "etc", "preferred", "required",
        "strong", "excellent", "good", "demonstrated", "responsibilities"
    ]

    top_skills_us = (
        df
        .filter(col("work_location").isNotNull())
        .filter(
            lower(col("work_location")).rlike(
                "united states|\\busa\\b|\\bny\\b|new york|brooklyn|queens|bronx|manhattan|staten"
            )
        )
        # split only on delimiter characters, not whitespace
        .withColumn("skill", explode(split(lower(col("preferred_skills")), "[,;/|]+")))
        .withColumn("skill", trim(col("skill")))
        # remove punctuation that sometimes remains (smart quotes, bullets)
        .withColumn("skill", regexp_replace(col("skill"), "[\u2018\u2019\u201c\u201d\u2013\u2014,\\.\\(\\)\[\]:]", ""))
        .filter(col("skill").isNotNull())
        .filter(col("skill") != "")
        .filter(length(col("skill")) > 1)
        # keep tokens that start with a letter (avoid pure numbers like '7-10')
        .filter(col("skill").rlike("^[a-zA-Z].*$"))
        .filter(~col("skill").isin(*stopwords))
        .groupBy("skill")
        .agg(round(avg(col("avg_salary").cast("double")), 2).alias("skill_val"))
        .orderBy(col("skill_val").desc())
        .limit(10)
    )

    return (
        stats,
        salary_distribution,
        correlation,
        highest_sal_agency,
        avg_sal_2yr,
        top_skills_us
    )

# Export Results
def save_processed_data(df: DataFrame, output_path: str) -> None:
    """Save processed data to Parquet (faster & smaller than CSV)."""
    try:
        df.write.mode("overwrite").parquet(output_path)
        print(f"Processed data saved to: {output_path}")
    except Exception as e:
        print(f"Error saving processed data: {e}")


def save_kpi_results(stats, salary_dist, corr, high_agency, avg_2yr, skills_us, output_dir: str) -> None:
    """Export KPI results as CSVs for Excel/BI tools."""
    try:
        stats.coalesce(1).write.csv(f"{output_dir}/job_stats", header=True, mode="overwrite")
        salary_dist.coalesce(1).write.csv(f"{output_dir}/salary_distribution", header=True, mode="overwrite")
        corr.coalesce(1).write.csv(f"{output_dir}/education_correlation", header=True, mode="overwrite")
        high_agency.coalesce(1).write.csv(f"{output_dir}/highest_salary_by_agency", header=True, mode="overwrite")
        avg_2yr.coalesce(1).write.csv(f"{output_dir}/2yr_avg_salary", header=True, mode="overwrite")
        skills_us.coalesce(1).write.csv(f"{output_dir}/top_skills", header=True, mode="overwrite")
        print(f"All KPI results exported to: {output_dir}/")
    except Exception as e:
        print(f"Error exporting KPI results: {e}")

# Main Pipeline
def main():
    """Run the full pipeline: load, clean, feature engineer, compute KPIs, export."""
    spark = None
    try:
        spark = get_spark()
        print(f"Spark UI: {spark.sparkContext.uiWebUrl}")
        path = "/dataset/nyc-jobs.csv"
        if not os.path.exists(path):
            raise FileNotFoundError(f"{path} not found")

        print("Loading data.")
        raw_df = spark.read.option("header", True).option("inferSchema", True).csv(path)

        print("Normalizing column names and cleaning data.")
        df = normalize_columns(raw_df)
        df = select_features_for_analysis(df)
        df = clean_data(df)
        profile_data(df)
        print(f"Cleaned data: {df.count():,} records (removed {raw_df.count() - df.count():,}).")

        print("Adding features.")
        df = apply_feature_engineering(df)
        print("Added features: avg_salary, education_level, edu_rank.")
        df.persist(StorageLevel.MEMORY_AND_DISK)

        print("Computing KPIs.")
        stats, salary_dist, corr, high_agency, avg_2yr, skills_us = compute_kpis(df)

        print("KPI 1: Job Category Stats (Top 10)")
        stats.show(10)
        print("KPI 2: Salary Distribution by Category")
        salary_dist.show(10)
        print("KPI 3: Education-Salary Correlation")
        corr.show()
        print("KPI 4: Highest Paying Role per Agency")
        high_agency.show(10)
        print("KPI 5: 2-Year Average Salary per Agency")
        avg_2yr.show(10)
        print("KPI 6: Top Paid Skills in US Market")
        skills_us.show(10)

        output_dir = "./output"
        os.makedirs(output_dir, exist_ok=True)
        print(f"Saving results to {output_dir}/")
        save_processed_data(df, f"{output_dir}/processed_jobs_data")
        save_kpi_results(stats, salary_dist, corr, high_agency, avg_2yr, skills_us, output_dir)

        df.unpersist()
        print("\nDone!")

    except Exception as e:
        print(f"Error: {str(e)}")
        raise
    finally:
        if spark:
            spark.stop()

# Tests
@pytest.fixture(scope="session")
def spark_session():
    return get_spark()


@pytest.fixture
def base_df(spark_session):
    data = [
        ("IT", "Agency1", "Engineer", "2023-01-01", "2023-12-31",
         60000, 80000, "Bachelor degree required", "python, spark", "New York"),
        ("IT", "Agency1", "Senior Engineer", "2024-01-01", "2024-12-31",
         90000, 120000, "Master degree preferred", "spark; kafka", "Brooklyn"),
        ("HR", "Agency2", "HR Manager", "2023-06-01", "2023-12-31",
         50000, 70000, "Bachelor degree", "communication/leadership", "London")
    ]

    cols = [
        "job_category", "agency", "business_title",
        "posting_date", "post_until",
        "salary_range_from", "salary_range_to",
        "minimum_qual_requirements", "preferred_skills",
        "work_location"
    ]

    return spark_session.createDataFrame(data, cols)


def test_normalize_columns():
    """Check that column names are properly normalized (lowercase, underscores)."""
    spark = get_spark()
    df = spark.createDataFrame([(1,)], ["Job ID"])
    result = normalize_columns(df)
    assert "job_id" in result.columns


def test_clean_data_filters_invalid_salary(base_df):
    """Make sure we remove records with salary_to < salary_from."""
    df = base_df.union(
        base_df.limit(1).withColumn("salary_range_to", col("salary_range_from") - 1)
    )
    cleaned = clean_data(df)
    assert cleaned.count() == 3


def test_feature_engineering_avg_salary(base_df):
    """Check that avg_salary is calculated correctly as (from + to) / 2."""
    df = apply_feature_engineering(clean_data(base_df))
    row = df.filter(col("business_title") == "Engineer").collect()[0]
    assert row.avg_salary == 70000


def test_top_skills_us_filters_non_us(base_df):
    """Verify US-only filtering and that multiple delimiters (comma, semicolon, slash) work."""
    df = apply_feature_engineering(clean_data(base_df))
    stats, _, _, _, _, skills_us = compute_kpis(df)
    skills = {r.skill for r in skills_us.collect()}
    assert "spark" in skills  # from NY and Brooklyn
    assert "python" in skills
    assert "kafka" in skills
    assert "communication" not in skills  # London jobs filtered out

if __name__ == "__main__":
    # Run the pipeline. To run tests, use: pytest -q
    main()
    print("To run unit tests, run: pytest -q")
