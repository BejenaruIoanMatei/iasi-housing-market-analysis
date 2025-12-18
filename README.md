# Iași Real Estate Market Analysis & ETL Pipeline

![Python](https://img.shields.io/badge/Python-3.10%2B-blue)
![Pandas](https://img.shields.io/badge/Library-Pandas-150458)

## Project Overview
This project focuses on analyzing the residential real estate market in **Iași, Romania**. 
Unlike standard datasets, this project involved building an end-to-end data pipeline: **scraping** raw data from real estate listings, **cleaning and transforming** the dataset (ETL), and performing **statistical analysis** to identify key price drivers.

The goal was to determine how factors like location (neighborhood), surface area, and building age influence the final listing price.

## Tech Stack & Skills
* **Language:** Python
* **Data Collection:** BeautifulSoup / Requests (Web Scraping)
* **Data Engineering:** Pandas, NumPy (Data Cleaning, Feature Engineering)
* **Visualization:** Matplotlib, Seaborn
* **Statistical Analysis:** Scikit-learn, Statsmodels (Regression Analysis, Hypothesis Testing)

## The Pipeline (ETL Process)

### 1. Data Extraction (Scraping)
* Automated extraction of real estate listings from major Romanian aggregators.
* Collected raw data points: Price, Location (Zone), Surface Area, Construction Year, Number of Rooms.

### 2. Data Cleaning & Transformation
* **Handling Missing Values:** Imputed missing construction years based on neighborhood averages.
* **Outlier Detection:** Removed pricing anomalies using the IQR (Interquartile Range) method to ensure statistical relevance.
* **Feature Engineering:** * Created a categorical variable `Building_Era` based on `Construction_Year` (e.g., *Pre-1977 Earthquake*, *Post-Revolution*, *Modern-Residential*).
    * Parsed and standardized numeric strings (e.g., converting "55.000 EUR" to integer format).

### 3. Analysis & Insights
* **Correlation:** Identified a strong positive correlation ($R^2 > 0.8$) between Surface Area and Price.
* **Regression Model:** Built a Linear Regression model to predict apartment prices based on surface area and location.
* **Hypothesis Testing:** Validated significant price differences between premium areas (e.g., Copou) and residential hubs (e.g., Nicolina).

## How to Run
1. Clone the repository:
   ```bash
   git clone [https://github.com/yourusername/iasi-real-estate-etl-pipeline.git](https://github.com/yourusername/iasi-real-estate-etl-pipeline.git)
