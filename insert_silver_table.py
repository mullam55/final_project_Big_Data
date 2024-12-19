from db_connection import connection_details
from cassandra.query import BatchStatement
import pandas as pd

# Establish session
session = connection_details()

# Query data from the bronze_accident_table
rows = session.execute("SELECT * FROM bronze_accident_table;")

# Convert the query result to a pandas DataFrame
df = pd.DataFrame(rows)

# Replace NaN values with a placeholder (e.g., 'Unknown') for TEXT columns
df.fillna('Unknown', inplace=True)

print("Number of records Before cleaning: ", len(df))

df = df.drop_duplicates(subset='accident_index')

# Cleaning the data before inserting into the silver table
# Drop rows with missing values in required columns
df_cleaned = df.dropna(subset=[
    'accident_index', 'accident_date', 'day_of_week', 'junction_control',
    'accident_severity', 'number_of_casualties', 'number_of_vehicles',
    'speed_limit', 'weather_conditions', 'vehicle_type'
])

print("Number of records After cleaning :", len(df_cleaned))

# Prepare the query for batch processing
insert_query = session.prepare("""
    INSERT INTO silver_accident_table (
        accident_index, accident_date, day_of_week, junction_control,
        accident_severity, number_of_casualties, number_of_vehicles,
        speed_limit, weather_conditions, vehicle_type
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
""")

# Batch size
batch_size = 400  # Adjust this based on your performance requirements
batch = BatchStatement()

# Insert cleaned data into the Silver table in batches
for i, row in df_cleaned.iterrows():
    batch.add(insert_query, (
        row['accident_index'], row['accident_date'], row['day_of_week'],
        row['junction_control'], row['accident_severity'], row['number_of_casualties'],
        row['number_of_vehicles'], row['speed_limit'],
        row['weather_conditions'], row['vehicle_type']
    ))

    # Execute the batch when it reaches the defined batch size
    if (i + 1) % batch_size == 0:
        session.execute(batch)
        batch = BatchStatement()  # Reset batch
        print("Inserting ", (i+1)/batch_size , " batch")


# Execute the remaining records in the final batch
if len(batch) > 0:
    session.execute(batch)

print("Data inserted into silver_accident_table.")

# Count the total number of rows in the table
count_query = "SELECT COUNT(*) FROM silver_accident_table;"
rows = session.execute(count_query)

# Fetch the result
for row in rows:
    print(f"Total records in silver_accident_table: {row[0]}")
