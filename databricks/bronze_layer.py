"""
Bronze Layer: Raw Data Ingestion
Ingests raw scraped listings into bronze.raw_listings Delta table
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
import sys
sys.path.append('/Workspace/Repos/bejmafei@gmail.com/iasi-housing-market-analysis/pipeline/scraper')
from scraper import scrape_page

def get_spark():
    """Get or create Spark session"""
    return SparkSession.builder.appName("Bronze Layer - Raw Listings").getOrCreate()

def ingest_raw_listings(spark, page_number: int):
    """
    Scrape data from Storia.ro and write to Bronze layer
    
    Args:
        spark: SparkSession
        page_number: Page number to scrape
    """
    print(f"Scraping page {page_number}...")
    records = scrape_page(page_number)
    
    if not records:
        print(f"No records found on page {page_number}")
        return
    
    # Convert to DataFrame
    df = spark.createDataFrame(records)
    
    # Add metadata columns
    df = df.withColumn("ingestion_timestamp", current_timestamp()) \
           .withColumn("source", lit("storia.ro")) \
           .withColumn("page_number", lit(page_number))
    
    print(f"Writing {df.count()} records to bronze.raw_listings...")
    
    # Write to Bronze Delta table (append mode for incremental loads)
    df.write \
      .format("delta") \
      .mode("append") \
      .option("mergeSchema", "true") \
      .saveAsTable("bronze.raw_listings")
    
    print("Bronze layer ingestion complete!")

def ingest_multiple_pages(spark, start_page: int = 1, end_page: int = 5):
    """
    Scrape multiple pages and ingest to Bronze layer
    
    Args:
        spark: SparkSession
        start_page: Starting page number
        end_page: Ending page number (inclusive)
    """
    for page in range(start_page, end_page + 1):
        try:
            ingest_raw_listings(spark, page)
        except Exception as e:
            print(f"Error processing page {page}: {e}")
            continue

if __name__ == "__main__":
    spark = get_spark()
    
    # Create bronze schema if it doesn't exist
    spark.sql("CREATE SCHEMA IF NOT EXISTS bronze")
    
    # Ingest data from pages 1-5 (adjust as needed)
    ingest_multiple_pages(spark, start_page=1, end_page=5)
