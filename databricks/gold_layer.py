"""
Gold Layer: BI-Ready Analytics
Creates aggregated views and metrics for dashboards and reporting
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, avg, count, min, max, stddev, percentile_approx,
    round as spark_round, current_timestamp, sum as spark_sum
)

def get_spark():
    """Get or create Spark session"""
    return SparkSession.builder.appName("Gold Layer - Analytics").getOrCreate()

def create_price_by_zone(spark):
    """
    Create aggregated price statistics by zone
    """
    print("Creating gold.price_by_zone...")
    
    df = spark.table("silver.clean_listings") \
        .groupBy("zona", "tip_zona") \
        .agg(
            spark_round(avg("pret"), 2).alias("pret_mediu"),
            spark_round(avg("pret_mp"), 2).alias("pret_mp_mediu"),
            spark_round(min("pret"), 2).alias("pret_minim"),
            spark_round(max("pret"), 2).alias("pret_maxim"),
            spark_round(stddev("pret"), 2).alias("deviatia_standard_pret"),
            count("*").alias("numar_anunturi"),
            spark_round(avg("suprafata_utila"), 2).alias("suprafata_medie")
        ) \
        .withColumn("update_timestamp", current_timestamp()) \
        .orderBy(col("pret_mediu").desc())
    
    df.write \
      .format("delta") \
      .mode("overwrite") \
      .option("overwriteSchema", "true") \
      .saveAsTable("gold.price_by_zone")
    
    print(f"Created gold.price_by_zone with {df.count()} zones")

def create_price_by_rooms(spark):
    """
    Create aggregated price statistics by number of rooms
    """
    print("Creating gold.price_by_rooms...")
    
    df = spark.table("silver.clean_listings") \
        .filter(col("camere").isNotNull()) \
        .groupBy("camere") \
        .agg(
            spark_round(avg("pret"), 2).alias("pret_mediu"),
            spark_round(avg("pret_mp"), 2).alias("pret_mp_mediu"),
            spark_round(avg("suprafata_utila"), 2).alias("suprafata_medie"),
            spark_round(min("pret"), 2).alias("pret_minim"),
            spark_round(max("pret"), 2).alias("pret_maxim"),
            count("*").alias("numar_anunturi")
        ) \
        .withColumn("update_timestamp", current_timestamp()) \
        .orderBy("camere")
    
    df.write \
      .format("delta") \
      .mode("overwrite") \
      .option("overwriteSchema", "true") \
      .saveAsTable("gold.price_by_rooms")
    
    print(f"Created gold.price_by_rooms with {df.count()} room types")

def create_price_by_year(spark):
    """
    Create aggregated price statistics by construction year
    """
    print("Creating gold.price_by_year...")
    
    df = spark.table("silver.clean_listings") \
        .filter(col("an_constructie").isNotNull()) \
        .groupBy("an_constructie") \
        .agg(
            spark_round(avg("pret"), 2).alias("pret_mediu"),
            spark_round(avg("pret_mp"), 2).alias("pret_mp_mediu"),
            spark_round(avg("suprafata_utila"), 2).alias("suprafata_medie"),
            count("*").alias("numar_anunturi")
        ) \
        .withColumn("update_timestamp", current_timestamp()) \
        .orderBy(col("an_constructie").desc())
    
    df.write \
      .format("delta") \
      .mode("overwrite") \
      .option("overwriteSchema", "true") \
      .saveAsTable("gold.price_by_year")
    
    print(f"Created gold.price_by_year with {df.count()} years")

def create_zone_type_summary(spark):
    """
    Create high-level summary by zone type
    """
    print("Creating gold.zone_type_summary...")
    
    df = spark.table("silver.clean_listings") \
        .groupBy("tip_zona") \
        .agg(
            spark_round(avg("pret"), 2).alias("pret_mediu"),
            spark_round(avg("pret_mp"), 2).alias("pret_mp_mediu"),
            spark_round(min("pret_mp"), 2).alias("pret_mp_minim"),
            spark_round(max("pret_mp"), 2).alias("pret_mp_maxim"),
            count("*").alias("numar_anunturi"),
            spark_round(avg("suprafata_utila"), 2).alias("suprafata_medie")
        ) \
        .withColumn("update_timestamp", current_timestamp()) \
        .orderBy(col("pret_mp_mediu").desc())
    
    df.write \
      .format("delta") \
      .mode("overwrite") \
      .option("overwriteSchema", "true") \
      .saveAsTable("gold.zone_type_summary")
    
    print(f"Created gold.zone_type_summary with {df.count()} zone types")

def create_market_overview(spark):
    """
    Create overall market statistics
    """
    print("Creating gold.market_overview...")
    
    df = spark.table("silver.clean_listings") \
        .agg(
            count("*").alias("total_listings"),
            spark_round(avg("pret"), 2).alias("avg_price"),
            spark_round(avg("pret_mp"), 2).alias("avg_price_per_sqm"),
            spark_round(min("pret"), 2).alias("min_price"),
            spark_round(max("pret"), 2).alias("max_price"),
            spark_round(avg("suprafata_utila"), 2).alias("avg_area"),
            spark_round(avg("camere"), 2).alias("avg_rooms")
        ) \
        .withColumn("update_timestamp", current_timestamp())
    
    df.write \
      .format("delta") \
      .mode("overwrite") \
      .option("overwriteSchema", "true") \
      .saveAsTable("gold.market_overview")
    
    print("Created gold.market_overview")

def build_all_gold_tables(spark):
    """
    Build all Gold layer tables
    """
    print("=" * 60)
    print("Building Gold Layer Tables")
    print("=" * 60)
    
    create_price_by_zone(spark)
    create_price_by_rooms(spark)
    create_price_by_year(spark)
    create_zone_type_summary(spark)
    create_market_overview(spark)
    
    print("=" * 60)
    print("Gold layer build complete!")
    print("=" * 60)

if __name__ == "__main__":
    spark = get_spark()
    
    # Create gold schema if it doesn't exist
    spark.sql("CREATE SCHEMA IF NOT EXISTS gold")
    
    # Build all gold tables
    build_all_gold_tables(spark)
