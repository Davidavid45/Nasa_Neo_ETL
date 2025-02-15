"""
NASA Near-Earth Object ETL Script
---------------------------------
Pulls the last 7 days of NEO data from NASA's API,
transforms the data, and loads it into an Azure SQL database.
"""


# 1. INSTALL LIBRARIES (via terminal/command line):
#    pip install requests sqlalchemy pyodbc

import requests
import datetime
import json
import pandas as pd
from sqlalchemy import create_engine, text

# 2. SET YOUR NASA API KEY
NASA_API_KEY = "MNOS3cVhOvavxCQwXQk4v8BkeVfOdFhkOv6YDM01"

# 3. AZURE SQL CONNECTION DETAILS

USERNAME = "oadegoke"       
PASSWORD = "Amazonfresh4$"      
SERVER   = "dattaserver.database.windows.net"
DATABASE = "Neodb"
driver_path = "/opt/homebrew/lib/libmsodbcsql.18.dylib"

# Build the connection string for SQLAlchemy
connection_string = (
    f"mssql+pyodbc://{USERNAME}:{PASSWORD}@{SERVER}:1433/"
    f"{DATABASE}?driver={driver_path}&Encrypt=yes&TrustServerCertificate=no"
)

# Create the engine
engine = create_engine(connection_string, fast_executemany=True)

def main():
    try:
        # 4. CREATE TABLE IF NOT EXISTS
        create_table_if_not_exists()

        # 5. DETERMINE THE DATE RANGE (LAST 7 DAYS)
        end_date = datetime.date.today()
        start_date = end_date - datetime.timedelta(days=7)

        # 6. BUILD NASA API URL
        url = (
            f"https://api.nasa.gov/neo/rest/v1/feed?"
            f"start_date={start_date}&end_date={end_date}&api_key={NASA_API_KEY}"
        )

        # 7. REQUEST DATA
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()

        # 8. PARSE AND TRANSFORM DATA INTO A PANDAS DATAFRAME
        df_neo = parse_neo_data(data)

        if df_neo.empty:
            print("No new NEO data found.")
        else:
            # 9. UPSERT / INSERT INTO AZURE SQL
            load_data_to_sql(df_neo)
            print(f"Inserted/updated {len(df_neo)} rows into NEO_Data table.")
    
    except Exception as e:
        print("Error occurred:", e)

def create_table_if_not_exists():
    """
    Creates the NEO_Data table in the Azure SQL database if it does not already exist.
    """
    create_table_sql = text("""
    IF NOT EXISTS (
       SELECT * FROM sys.tables WHERE name = 'NEO_Data'
    )
    BEGIN
        CREATE TABLE dbo.NEO_Data (
            reference_id             VARCHAR(50) NOT NULL,
            name                     VARCHAR(200),
            close_approach_date      DATE,
            estimated_diameter_km    FLOAT,
            velocity_km_h            FLOAT,
            miss_distance_km         FLOAT,
            is_potentially_hazardous BIT,
            CONSTRAINT PK_NEO PRIMARY KEY (reference_id, close_approach_date)
        );
    END
    """)

    with engine.begin() as conn:
        conn.execute(create_table_sql)
    
    print("Table check/creation complete.")

def parse_neo_data(data_json):
    """
    Takes the JSON from NASA's NEO API and returns a Pandas DataFrame
    with the relevant fields.
    """
    # The NEO data is nested under: near_earth_objects -> dates -> list of objects
    near_earth_objects = data_json.get("near_earth_objects", {})

    records = []
    for date_str, neo_list in near_earth_objects.items():
        for neo in neo_list:
            reference_id = neo.get("id")
            name = neo.get("name")
            close_approach = neo.get("close_approach_data", [])
            
          
            # Some NEOs have multiple future approach dates, but for simplicity we handle one.
            if len(close_approach) > 0:
                approach_data = close_approach[0]
                close_approach_date = approach_data.get("close_approach_date", date_str)

                velocity_km_h = None
                if approach_data.get("relative_velocity"):
                    velocity_km_h = float(approach_data["relative_velocity"].get("kilometers_per_hour", 0))

                miss_distance_km = None
                if approach_data.get("miss_distance"):
                    miss_distance_km = float(approach_data["miss_distance"].get("kilometers", 0))

            else:
                close_approach_date = date_str
                velocity_km_h = None
                miss_distance_km = None

            est_diameter = None
            est_diameter_info = neo.get("estimated_diameter", {}).get("kilometers", {})
            estimated_diameter_min = est_diameter_info.get("estimated_diameter_min")
            estimated_diameter_max = est_diameter_info.get("estimated_diameter_max")

            if estimated_diameter_min is not None and estimated_diameter_max is not None:
                est_diameter = (estimated_diameter_min + estimated_diameter_max) / 2.0

            hazardous = neo.get("is_potentially_hazardous_asteroid", False)

            records.append({
                "reference_id": reference_id,
                "name": name,
                "close_approach_date": close_approach_date,
                "estimated_diameter_km": est_diameter,
                "velocity_km_h": velocity_km_h,
                "miss_distance_km": miss_distance_km,
                "is_potentially_hazardous": 1 if hazardous else 0
            })

    df = pd.DataFrame(records)
    df.drop_duplicates(subset=["reference_id", "close_approach_date"], inplace=True)

    df["close_approach_date"] = pd.to_datetime(df["close_approach_date"]).dt.date

    return df

def load_data_to_sql(df):
    """
    Loads the given DataFrame into the dbo.NEO_Data table in Azure SQL.
    If there's a conflict on PRIMARY KEY, we skip or update existing records.
    For demonstration, I'll do a simple 'try insert, if fails on PK, ignore'.
    """
    # So for upsert logic, i might need a custom approach. 
    # But let's do a simple 'append' and rely on the PK constraint to skip duplicates.

    # Using the 'if_exists="append"' approach, but any PK violations will raise an error.
    # I'll wrap that in a try/except to continue.

    try:
        df.to_sql("NEO_Data", con=engine, schema="dbo", if_exists="append", index=False)
    except Exception as exc:
        # If the error is caused by PK constraint violation, we can ignore or handle individually.
        print("Bulk insert encountered duplicates. Attempting row-by-row insertion.")
        insert_row_by_row(df)

def insert_row_by_row(df):
    """
    Inserts each row individually, ignoring duplicates that violate the PK constraint.
    This is slower but ensures we skip already-inserted records.
    """
    insert_sql = text("""
    INSERT INTO dbo.NEO_Data
    (reference_id, name, close_approach_date, estimated_diameter_km,
     velocity_km_h, miss_distance_km, is_potentially_hazardous)
    VALUES
    (:reference_id, :name, :close_approach_date, :estimated_diameter_km,
     :velocity_km_h, :miss_distance_km, :is_potentially_hazardous);
    """)

    with engine.begin() as conn:
        for idx, row in df.iterrows():
            try:
                conn.execute(insert_sql, **row.to_dict())
            except Exception as row_exc:
                # If it’s a primary key violation, we can ignore
                if "PRIMARY KEY" in str(row_exc):
                    pass
                else:
                    # Some other error, raise or print
                    print(f"Row insert error: {row_exc}")

def backfill_historical_data():
    """
    Loops over a longer date range in 7-day increments to populate
    the NEO_Data table with more historical data.
    """
    import datetime
    import time

    # Choose how far back you want to go, e.g., Jan 1, 2021, up to yesterday
    start_date = datetime.date(2019, 1, 1)
    end_date = datetime.date.today() - datetime.timedelta(days=1)

    # NASA feed endpoint only allows up to 7-day windows
    chunk_size = 7

    current = start_date
    while current < end_date:
        chunk_start = current
        chunk_end = current + datetime.timedelta(days=chunk_size - 1)
        if chunk_end > end_date:
            chunk_end = end_date

        # Build the feed URL (same as in main, but for chunk_start, chunk_end)
        url = (
            f"https://api.nasa.gov/neo/rest/v1/feed?"
            f"start_date={chunk_start}&end_date={chunk_end}&api_key={NASA_API_KEY}"
        )
        print(f"Requesting data for {chunk_start} to {chunk_end}...")

        response = requests.get(url)
        response.raise_for_status()
        data = response.json()

        # Parse and insert using your existing functions
        df_neo = parse_neo_data(data)
        if not df_neo.empty:
            load_data_to_sql(df_neo)
            print(f"Inserted {len(df_neo)} rows for {chunk_start} to {chunk_end}.\n")
        else:
            print(f"No data found for {chunk_start} to {chunk_end}.\n")

        # Move current date forward by chunk_size
        current = chunk_end + datetime.timedelta(days=1)

        # Optional short sleep to avoid spamming the API
        time.sleep(1)

    print("Historical data load complete!")

if __name__ == "__main__":
    backfill_historical_data()

