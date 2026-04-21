# Iasi Housing Market - Databricks Medallion Architecture

This project implements a **Medallion Architecture** for analyzing housing market data from Storia.ro (Iasi, Romania) using Databricks Delta Lake.

## Architecture Overview

```
┌─────────────┐      ┌─────────────┐      ┌─────────────┐
│   BRONZE    │ ───> │   SILVER    │ ───> │    GOLD     │
│  Raw Data   │      │  Cleaned    │      │ BI-Ready    │
└─────────────┘      └─────────────┘      └─────────────┘
```

### Bronze Layer
**Purpose**: Store raw, unprocessed data  
**Table**: `bronze.raw_listings`  
**Source**: Web scraping from Storia.ro

**Columns**:
* `title_raw` - Raw listing title
* `price_raw` - Raw price string
* `description_raw` - Raw description text
* `url` - Listing URL
* `scraped_at` - Timestamp of scraping
* `ingestion_timestamp` - When data was loaded to Databricks
* `source` - Data source identifier
* `page_number` - Page number scraped

### Silver Layer
**Purpose**: Cleaned, structured, validated data  
**Table**: `silver.clean_listings`  
**Transformations**:
* Extract numeric price from raw text
* Parse square meters from description
* Extract number of rooms
* Parse construction year
* Categorize zones/neighborhoods
* Calculate price per square meter
* Filter out incomplete records

**Columns**:
* `pret` - Price in EUR
* `suprafata_utila` - Usable area in m²
* `camere` - Number of rooms
* `pret_mp` - Price per square meter
* `zona` - Standardized neighborhood category
* `zona_raw` - Original zone name
* `an_constructie` - Construction year
* `tip_zona` - Zone type (Premium, Standard/Urban, etc.)
* `transformation_timestamp` - When transformation occurred

### Gold Layer
**Purpose**: Aggregated, BI-ready analytics  
**Tables**:

1. **`gold.price_by_zone`**
   * Average price, price/m², min, max by neighborhood
   * Standard deviation and listing counts

2. **`gold.price_by_rooms`**
   * Price statistics grouped by number of rooms

3. **`gold.price_by_year`**
   * Price trends by construction year

4. **`gold.zone_type_summary`**
   * High-level metrics by zone type (Premium, Standard, etc.)

5. **`gold.market_overview`**
   * Overall market statistics (single row)

## Usage

### Option 1: Run Full Pipeline
```python
%run /Users/bejmafei@gmail.com/databricks/run_medallion_pipeline.py
```

This will:
1. Scrape pages 1-10 from Storia.ro
2. Transform raw data into clean records
3. Build all Gold layer aggregations

### Option 2: Run Individual Layers

**Bronze Layer**:
```python
from bronze_layer import ingest_multiple_pages, get_spark

spark = get_spark()
spark.sql("CREATE SCHEMA IF NOT EXISTS bronze")
ingest_multiple_pages(spark, start_page=1, end_page=5)
```

**Silver Layer**:
```python
from silver_layer import transform_bronze_to_silver, get_spark

spark = get_spark()
spark.sql("CREATE SCHEMA IF NOT EXISTS silver")
transform_bronze_to_silver(spark)
```

**Gold Layer**:
```python
from gold_layer import build_all_gold_tables, get_spark

spark = get_spark()
spark.sql("CREATE SCHEMA IF NOT EXISTS gold")
build_all_gold_tables(spark)
```

## Example Queries

### Query Silver Layer
```sql
SELECT zona, AVG(pret_mp) as avg_price_per_sqm
FROM silver.clean_listings
GROUP BY zona
ORDER BY avg_price_per_sqm DESC
LIMIT 10
```

### Query Gold Layer
```sql
-- Most affordable neighborhoods
SELECT zona, pret_mediu, numar_anunturi
FROM gold.price_by_zone
WHERE numar_anunturi > 5
ORDER BY pret_mediu ASC
LIMIT 10
```

## Incremental Updates

* **Bronze**: Uses `append` mode for incremental scraping
* **Silver**: Uses `overwrite` mode (full refresh from Bronze)
* **Gold**: Uses `overwrite` mode (full refresh from Silver)

For production, consider:
* Adding a `load_date` partition column
* Implementing merge/upsert logic for Silver layer
* Using Delta Lake time travel for auditing

## Dependencies

Ensure these packages are installed in your Databricks cluster:
```
requests
beautifulsoup4
```

The original scraper code is located at:
`/Repos/bejmafei@gmail.com/iasi-housing-market-analysis/pipeline/scraper/scraper.py`
