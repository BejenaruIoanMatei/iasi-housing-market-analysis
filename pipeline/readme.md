### Arhitectura generala

```
Docker Compose
├── postgres        → baza de date (3 scheme)
├── airflow         → orchestrare (webserver + scheduler)
└── scraper         → Python app (rulat ca task în Airflow)
```

### Schema PostgreSQL

```
raw.listings          → dump brut din scraper, neatins
staging.listings      → curățat, tipuri corecte, deduplicat
analytics.prices      → agregări pe timp (săptămânal / lunar)
analytics.snapshots   → "fotografie" a pieței la fiecare run
```

### DAG Airflow

```
scrape_listings
      ↓
load_raw (INSERT în raw.listings cu scraped_at)
      ↓
transform_staging (Python: curățare, categorii zone)
      ↓
build_analytics (SQL: agregări time series)
```

### Layere Concrete

```
raw.listings          → exact ce vine din scraper, nimic atins
                         + scraped_at TIMESTAMP adăugat automat

staging.listings      → curățat în SQL:
                         - prețuri parsate (string → numeric)
                         - construction_year imputat
                         - outlieri eliminați
                         - zona categorizată (Premium/Standard/Accessible)

analytics.avg_price_by_zone_month   → time series agregat
analytics.market_snapshot           → starea pieței per run
```

### Cu ce difera fata de restul proiectului

-   Restul proiectului -> ETL

```
Scraper → Python curăță → PostgreSQL (date curate)
```

-   Ce fac acum -> ELT

```
Scraper → PostgreSQL raw (dump brut) → SQL transformă → analytics
```