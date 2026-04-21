"""
Silver Layer: Data Transformation
Cleans and transforms raw listings from Bronze into structured data
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, regexp_extract, regexp_replace, when, current_timestamp,
    udf, trim, lower, round as spark_round
)
from pyspark.sql.types import FloatType, IntegerType, StringType
import re

def get_spark():
    """Get or create Spark session"""
    return SparkSession.builder.appName("Silver Layer - Transform Listings").getOrCreate()

def extrage_suprafata(text):
    """Extract square meters from text"""
    if not text:
        return None
    match = re.search(r'(\d+(?:\.\d+)?)\s*(?:m²|mp|m)', str(text).lower())
    if match:
        return float(match.group(1))
    return None

def extrage_camere(text):
    """Extract number of rooms from text"""
    if not text:
        return None
    match = re.search(r'(\d+)\s*camer', str(text).lower())
    if match:
        return int(match.group(1))
    return None

def extrage_an(text):
    """Extract construction year from text"""
    if not text:
        return None
    match = re.search(r'(?:19|20)\d{2}', str(text))
    if match:
        an = int(match.group(0))
        if an >= 1950 and an <= 2026:
            return an
    return None

def extrage_zona(text):
    """Extract neighborhood zone from text"""
    if not isinstance(text, str):
        return "Necunoscut"
    
    parts = text.split('|')
    for part in parts:
        if 'iasi' in part.lower() and ',' in part:
            zona = part.split(',')[0].strip()
            if len(zona) < 30 and zona.lower() != "iasi":
                return zona
    return "Iasi (General)"

MAPPING_ZONE = {
    'Nicolina-CUG': ['Nicolina 1', 'Nicolina 2', 'CUG', 'Hlincea', 'Tudor Neculai', 'Soseaua Nicolina', 'Poitiers', 'Manta Rosie'],
    'Centru-Civic': ['Centru', 'Palas', 'Independentei', 'Academiei', 'Ion Creanga', 'Carol I', 'Anastasie Panu', 'Cuza Voda', 'Arcu', 'Smardan', 'Podu de Fier'],
    'Podu-Ros-Cantemir': ['Podu Ros', 'Cantemir', 'Tesatura'],
    'Tatarasi-Tudor': ['Tatarasi Sud', 'Tatarasi Nord', 'Vasile Lupu', 'Oancea', 'Tudor Vladimirescu', 'Baza 3'],
    'Pacurari-Canta': ['Pacurari', 'Canta', 'Moara de Foc'],
    'Copou-Saras': ['Copou', 'Agronomie', 'Sadoveanu', 'Agronomilor', 'Moara de Vant', 'Ticau'],
    'Alexandru-Dacia': ['Alexandru Cel Bun', 'Dacia', 'Mircea cel Batran', 'Bularga', 'Decebal'],
    'Bucium': ['Bucium', 'Visan', 'Barnova'],
    'Galata-Frumoasa': ['Galata', 'Frumoasa', 'Ciurea', 'Bisericii'],
    'Periferie-Metropolitana': ['Miroslava', 'Rediu', 'Dancu', 'Aroneanu', 'Valea Lupului', 'Voinesti']
}

def clean_location(zona_raw):
    """Map raw zone to standardized categories"""
    if not zona_raw:
        return "Necunoscut"
    
    zona_raw_lower = zona_raw.lower()
    for categoria, cuvinte_cheie in MAPPING_ZONE.items():
        for cuvant in cuvinte_cheie:
            if cuvant.lower() in zona_raw_lower:
                return categoria
    return "Necunoscut"

def tip_de_zona(zona):
    """Categorize zone type"""
    if zona in ['Copou-Saras', 'Centru-Civic']:
        return 'Premium'
    elif zona in ['Tatarasi-Tudor', 'Podu-Ros-Cantemir', 'Pacurari-Canta', 'Alexandru-Dacia']:
        return 'Standard/Urban'
    elif zona in ['Nicolina-CUG', 'Galata-Frumoasa']:
        return 'Accesibil/Rezidential'
    else:
        return 'Periferie'

def extrage_pret(text):
    """Extract price from text"""
    if not text:
        return None
    text = str(text).replace('€', '').strip()
    text = re.sub(r'[^\d]', '', text)
    return float(text) if text else None

# Register UDFs
extrage_suprafata_udf = udf(extrage_suprafata, FloatType())
extrage_camere_udf = udf(extrage_camere, IntegerType())
extrage_an_udf = udf(extrage_an, IntegerType())
extrage_zona_udf = udf(extrage_zona, StringType())
clean_location_udf = udf(clean_location, StringType())
tip_de_zona_udf = udf(tip_de_zona, StringType())
extrage_pret_udf = udf(extrage_pret, FloatType())

def transform_bronze_to_silver(spark):
    """
    Transform Bronze layer data to Silver layer
    Applies cleaning, parsing, and enrichment
    """
    print("Reading from bronze.raw_listings...")
    bronze_df = spark.table("bronze.raw_listings")
    
    print("Applying transformations...")
    silver_df = bronze_df.select(
        # Extract price
        extrage_pret_udf(col("price_raw")).alias("pret"),
        
        # Extract area
        extrage_suprafata_udf(col("description_raw")).alias("suprafata_utila"),
        
        # Extract rooms
        extrage_camere_udf(col("description_raw")).alias("camere"),
        
        # Extract construction year
        extrage_an_udf(col("description_raw")).alias("an_constructie"),
        
        # Extract and clean zone
        extrage_zona_udf(col("description_raw")).alias("zona_raw")
    ) \
    .withColumn("zona", clean_location_udf(col("zona_raw"))) \
    .withColumn("tip_zona", tip_de_zona_udf(col("zona"))) \
    .withColumn(
        "pret_mp",
        when(
            (col("pret").isNotNull()) & (col("suprafata_utila").isNotNull()) & (col("suprafata_utila") > 0),
            spark_round(col("pret") / col("suprafata_utila"), 2)
        ).otherwise(None)
    ) \
    .withColumn("transformation_timestamp", current_timestamp())
    
    # Filter out records with missing essential data
    silver_df = silver_df.filter(
        col("pret").isNotNull() & col("suprafata_utila").isNotNull()
    )
    
    # Select final columns
    silver_df = silver_df.select(
        "pret",
        "suprafata_utila",
        "camere",
        "pret_mp",
        "zona",
        "zona_raw",
        "an_constructie",
        "tip_zona",
        "transformation_timestamp"
    )
    
    print(f"Transformed {silver_df.count()} records...")
    
    # Write to Silver Delta table (overwrite for full refresh)
    print("Writing to silver.clean_listings...")
    silver_df.write \
      .format("delta") \
      .mode("overwrite") \
      .option("overwriteSchema", "true") \
      .saveAsTable("silver.clean_listings")
    
    print("Silver layer transformation complete!")
    return silver_df

if __name__ == "__main__":
    spark = get_spark()
    
    # Create silver schema if it doesn't exist
    spark.sql("CREATE SCHEMA IF NOT EXISTS silver")
    
    # Transform data
    transform_bronze_to_silver(spark)
