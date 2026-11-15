import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

# Define a local Spark Session for testing
@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder \
        .appName("pytest-pyspark-testing") \
        .master("local[2]") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

def test_bronze_metadata_enrichment(spark):
    """Tests if the Bronze layer correctly appends audit metadata."""
    
    # 1. Arrange: Create mock input landing data
    schema = StructType([
        StructField("work_order_id", StringType(), True),
        StructField("city", StringType(), True)
    ])
    raw_data = [("123", "Mumbai"), ("124", "Bengaluru")]
    df_raw = spark.createDataFrame(raw_data, schema)
    
    # Register as a temporary view to mimic the Unity Catalog source table
    df_raw.createOrReplaceTempView("mock_landing")
    
    # 2. Act: Import and run your transformation function
    # Note: Adjust the import path based on how you convert your .ipynb to modules for testing
    from src.01_bronze_ingestion import enrich_with_bronze_metadata
    df_result = enrich_with_bronze_metadata(spark, "mock_landing", "test_system")
    
    # 3. Assert: Validate the business logic
    columns = df_result.columns
    assert "ingested_at" in columns
    assert "source_system" in columns
    assert df_result.select("source_system").first()[0] == "test_system"
    assert df_result.count() == 2