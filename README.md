# Iasi Real Estate Market Analysis: ETL Pipeline & Hybrid Statistical Modeling

## Project Overview
This project provides a comprehensive analysis of the residential real estate market in Iași, Romania. It features an end-to-end workflow: from automated web scraping in Python to advanced econometric modeling in R.

-   Data pipeline: **scraping** raw data from real estate listings, **cleaning and transforming** the dataset (ETL), and performing **statistical analysis** to identify key price drivers.

The goal was to determine how factors like location (neighborhood), surface area, and building age influence the final listing price.

## Key Insights

-   **Location beats Age**: Welch’s T-test revealed no significant difference in price per squared meter between new buildings (Post-2000) and old buildings (Pre-1977). **This suggests that the premium location and infrastructure of older districts (e.g., Copou, City Center) fully compensate for the building's age.**

-   **Diminishing Returns on Surface Area**: There is a negative correlation between Surface Area and Price per squared meter. **This confirms that smaller units (studios) command a higher unit price due to concentrated fixed costs and high investment demand.**

-   **Price Drivers**: Surface area is the primary predictor, but **Location Premium adds a significant value of around 11.250 euros for premium zones** compared to accessible ones, regardless of the size.

-   **Renovation Value**: The high price of older apartments is also driven by secondary market renovations; many "old" listings are renovated to modern luxury standards, effectively matching the price per squared meter of new apartments.

## Tech Stack & Skills
* **Data Engineering (Python):** Pandas, NumPy (Data Cleaning, Feature Engineering)
* **Visualization:** Matplotlib, Seaborn
* **Statistical Analysis (Python):** Scikit-learn, Statsmodels (Regression Analysis, Hypothesis Testing)
* **Statistical Analysis (R**): Tidyverse, Car, Corrplot (Hypothesis Testing, Multiple Regression, Residual Diagnostics)

## The Pipeline (ETL Process)

### 1. Data Extraction (Scraping)
* Automated extraction of real estate listings from Storia
* Collected raw data points: Price, Location (Zone), Surface Area, Construction Year, Number of Rooms
  -   Example of raw data extracted from Storia:
  ```
      Titlu Necunoscut,154 000 €,https://www.storia.ro/ro/oferta/apartament-de-vanzare-3-camere-loc-de-parcare-gradina-valea-lupului-IDFTIw,"Adăugat astăzi | ... | ... | ... | ... | ... | ... | ... | ... | ... | 1 |  /  | 12 | 154 000 € | 2567 €/m² | Apartament de vanzare, 3 camere, loc de parcare+gradina, Valea Lupului | Pacurari, Iasi, Iasi | Numărul de camere | 3 camere | Prețul pe metru pătrat | 60 m² | Etaj | parter | Vezi descrierea anunțului | Apartament 3 camere cu gradina si parcare &ndash; Valea LupuluiApartament cu 3 camere de vanzare, situat in Valea Lupului, intr-o zona linistita si apreciata pentru locuit, ideal pentru familie sau in... | Evo Imobiliare | Agenție imobiliară",
      
### 2. Data Cleaning & Transformation
* **Handling Missing Values:** Imputed missing construction years based on neighborhood averages.
* **Outlier Detection:** Removed pricing anomalies using the IQR (Interquartile Range) method to ensure statistical relevance.
* **Feature Engineering:** Categorization of neighborhoods into Premium, Standard, and Accessible zones.

### 3. Analysis & Insights
* **Correlation:** Identified a strong positive correlation between Surface Area and Price.
* **Regression Model:** Built a Linear Regression model to predict apartment prices based on surface area and location.
* **Hypothesis Testing:** Validated significant price differences between premium areas (Copou) and residential hubs (Nicolina).

## Academic Context

This repository contains two distinct analytical phases, developed for different academic modules:

* **Python Component:** Developed for the **"Introducere in Python pentru Data Mining"** course, under the guidance of **Prof. Mircea Asandului**. This part focuses on the automated ETL pipeline and initial exploratory data analysis.


* **R Component:** Developed for the **"Introducere în R"** course, under the guidance of **Prof. Viorica Daniela**. This part focuses on advanced econometric modeling, hypothesis testing, and residual diagnostics.
