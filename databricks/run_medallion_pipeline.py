"""
Medallion Pipeline Orchestrator
Runs Bronze -> Silver -> Gold pipeline for Iasi housing market data
"""
from pyspark.sql import SparkSession
from datetime import datetime
import sys

# Import layer modules
sys.path.append('/Workspace/Repos/bejmafei@gmail.com/iasi-housing-market-analysis/databricks')
from bronze_layer import ingest_multiple_pages, get_spark as get_bronze_spark
from silver_layer import transform_bronze_to_silver, get_spark as get_silver_spark
from gold_layer import build_all_gold_tables, get_spark as get_gold_spark

def run_full_pipeline(start_page: int = 1, end_page: int = 5):
    """
    Execute the complete medallion pipeline
    
    Args:
        start_page: Starting page for scraping
        end_page: Ending page for scraping (inclusive)
    """
    print("=" * 80)
    print(f"MEDALLION PIPELINE STARTED - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80)
    
    try:
        # BRONZE LAYER: Ingest raw data
        print("\n🥉 BRONZE LAYER: Ingesting raw data from Storia.ro...")
        spark = get_bronze_spark()
        spark.sql("CREATE SCHEMA IF NOT EXISTS bronze")
        ingest_multiple_pages(spark, start_page=start_page, end_page=end_page)
        print("✓ Bronze layer complete\n")
        
        # SILVER LAYER: Transform and clean data
        print("🥈 SILVER LAYER: Transforming and cleaning data...")
        spark = get_silver_spark()
        spark.sql("CREATE SCHEMA IF NOT EXISTS silver")
        transform_bronze_to_silver(spark)
        print("✓ Silver layer complete\n")
        
        # GOLD LAYER: Create BI-ready aggregations
        print("🥇 GOLD LAYER: Creating BI-ready analytics...")
        spark = get_gold_spark()
        spark.sql("CREATE SCHEMA IF NOT EXISTS gold")
        build_all_gold_tables(spark)
        print("✓ Gold layer complete\n")
        
        # Summary
        print("=" * 80)
        print("PIPELINE COMPLETED SUCCESSFULLY!")
        print("=" * 80)
        print("\nCreated Tables:")
        print("  📊 bronze.raw_listings")
        print("  📊 silver.clean_listings")
        print("  📊 gold.price_by_zone")
        print("  📊 gold.price_by_rooms")
        print("  📊 gold.price_by_year")
        print("  📊 gold.zone_type_summary")
        print("  📊 gold.market_overview")
        print("\nYou can now query these tables or build dashboards on the Gold layer!")
        
    except Exception as e:
        print(f"\n❌ PIPELINE FAILED: {e}")
        raise
    
if __name__ == "__main__":
    # Default: scrape pages 1-10
    # Adjust these values as needed
    START_PAGE = 1
    END_PAGE = 10
    
    print(f"Configuration: Scraping pages {START_PAGE} to {END_PAGE}")
    run_full_pipeline(start_page=START_PAGE, end_page=END_PAGE)
