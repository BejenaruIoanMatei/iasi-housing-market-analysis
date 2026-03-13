### Arhitectura generala

```
Docker Compose
├── postgres        → baza de date (2 scheme)
├── airflow         → orchestrare (webserver + scheduler)
└── scraper         → Python app (rulat ca task în Airflow)
└── transformer     → Python app (rulat ca task în Airflow) 
```

### Schema PostgreSQL

```
dev.raw_istings          → dump brut din scraper, neatins
dev.staging_listings      → curățat, tipuri corecte, deduplicat
```

### DAG Airflow

```
scrape_listings
      ↓
load_raw (INSERT INTO raw_listings)
      ↓
transform_staging (Python: feature engineering, clearing data, categorii, zone)
      ↓
load_staging (INSERT INTO staging_listings)
```

Datele urmeaza a fi prelucrate ulterior de echipa de Analytics.

### Cu ce difera fata de restul proiectului

-   Restul proiectului -> ETL

```
Scraper → Python curăță → PostgreSQL (date curate)
```

-   Ce fac acum -> ELT

```
Scraper → PostgreSQL raw (dump brut) → SQL transformă → analytics
```