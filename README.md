====================================================================
                     NEAR-EARTH ASTEROIDS (NEO) PROJECT
            NASA API → PYTHON ETL → AZURE SQL → TABLEAU DASHBOARD
====================================================================

**1. INTRODUCTION**

This project focuses on analyzing Near-Earth Objects (NEOs)—asteroids 
that pass relatively close to Earth. Data is sourced from NASA’s NEO 
Feed API. We use an end-to-end pipeline:

- **Extract** NEO data from NASA’s API via Python.
- **Load** into an Azure SQL database.
- **Refresh** data weekly (scheduled).
- **Export** to CSV for visualization in Tableau on macOS.

**2. ARCHITECTURE OVERVIEW**

1. **NASA NEO API**  
   - Endpoint: https://api.nasa.gov/neo/rest/v1/feed  
   - Provides diameter, velocity, close-approach date, and hazard status.

2. **Python ETL**  
   - **Extract**: Python script calls NASA API (7-day windows).  
   - **Transform**: Convert JSON → Pandas DataFrame; compute average diameter.  
   - **Load**: Insert into Azure SQL’s `NEO_Data` table. Primary key on (id, date).

3. **Azure SQL**  
   - Hosts `NEO_Data` table.  
   - Access controlled by firewall rules (add your IP in Azure Portal).

4. **Scheduling**  
   - On macOS, scheduled via `launchd` every Monday at 11 AM to fetch new NEO data.

5. **CSV Export (Workaround)**  
   - Python script (`export_neo_csv.py`) uses `pymssql` or `pyodbc` (when working) 
     to SELECT all rows from Azure SQL.  
   - Writes `neo_data_export.csv`.

6. **Tableau Dashboard**  
   - Connects to `neo_data_export.csv`.  
   - Includes line charts, scatter plots, histograms, and KPI cards.

**3. IMPLEMENTATION DETAILS**

- **Historic Backfill**  
  - Loops from 2019 to present in 7-day increments to gather a large dataset (~3,800 rows).  
  - Inserted into `NEO_Data` table with upsert logic.  

- **Ongoing Updates**  
  - Weekly script pulls the last 7 days from NASA’s feed.  
  - Duplicates are skipped by primary key constraint.

- **CSV Extraction**  
  - Another script queries Azure SQL with `SELECT * FROM dbo.NEO_Data;`.  
  - Saves to `neo_data_export.csv`.  
  - Tableau loads this CSV for easy Mac usage.

**4. TABLEAU DASHBOARD**
Tableau Link: https://public.tableau.com/app/profile/oluwasegun.adegoke/viz/Near-EarthAsteroids_17395688260880/Near-EarthAsteroidsCloseApproachesPotentialHazards

- **Headline KPIs**: 
  - Total Objects
  - Avg. Diameter
  - Avg. Velocity
  - Hazardous Objects (count)
- **Charts**:
  1. **NEO Approaches by Date** (line chart)
  2. **Velocity vs. Diameter** (scatter, color by hazard)
  3. **Diameter Distribution** (histogram)
- **Interactive Filters**:
  - Date range slider
  - Hazard status dropdown

**5. KEY INSIGHTS & FINDINGS**

- **3,847 total NEOs** in the dataset from 2019–2025.
- **Average velocity** ~43,000 km/h. Some exceed 150,000 km/h.
- **245 hazardous** objects per NASA’s classification.
- The dataset shows spikes in recorded objects around 2024–2025.

**6. FUTURE ENHANCEMENTS**

- **Miss Distance**: Visualize how close they come to Earth.
- **Direct Live Connection**: If macOS driver issues are resolved, connect Tableau directly to Azure.
- **Predictive Modeling**: Use advanced ML to predict hazard status or future close approaches.

**7. CONCLUSION**

This project demonstrates an **end-to-end** data pipeline:
- Automated ingestion of asteroid data from NASA’s feed.
- Cloud storage in Azure SQL.
- Scheduled refresh for continuous updates.
- CSV export for easy compatibility on macOS.
- Final visualization in Tableau, highlighting key metrics and interactive filters.


**REFERENCES & LINKS**

- [NASA NEO API](https://api.nasa.gov/)  
- [Azure SQL](https://azure.microsoft.com/en-us/services/sql-database/)  
- [Tableau]([https://www.tableau.com/](https://public.tableau.com/app/profile/oluwasegun.adegoke/viz/Near-EarthAsteroids_17395688260880/Near-EarthAsteroidsCloseApproachesPotentialHazards))  
- [Python `pymssql`](https://pymssql.readthedocs.io/)

**Author**: Oluwasegun Adegoke  
**Date**: 14 Feb 2025
