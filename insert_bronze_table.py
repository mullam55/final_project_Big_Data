from db_connection import connection_details
from cassandra.query import BatchStatement
import pandas as pd
import uuid

# Establish session
session = connection_details()

# Load data from CSV using pandas
csv_file_path = 'received_accidents_data.csv'  # Replace with your actual CSV file path
df = pd.read_csv(csv_file_path)

# Replace NaN values with a placeholder (e.g., 'Unknown') for TEXT columns
df.fillna('Unknown', inplace=True)

print(df.shape)



# Prepare the query for batch processing
insert_query = session.prepare("""
    INSERT INTO bronze_accident_table (
        unique_id,accident_index, accident_date, day_of_week, junction_control, junction_detail,
        accident_severity, latitude, light_conditions, local_authority_district,
        carriageway_hazards, longitude, number_of_casualties, number_of_vehicles,
        police_force, road_surface_conditions, road_type, speed_limit, time,
        urban_or_rural_area, weather_conditions, vehicle_type
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
""")

# Batch size
batch_size = 1000  # Adjust based on your performance needs
batch = BatchStatement()

# Insert data in batches
for i, row in df.iterrows():
    batch.add(insert_query, (
        uuid.uuid4(), row['Accident_Index'], row['Accident Date'], row['Day_of_Week'], row['Junction_Control'], row['Junction_Detail'],
        row['Accident_Severity'], row['Latitude'], row['Light_Conditions'], row['Local_Authority_(District)'],
        row['Carriageway_Hazards'], row['Longitude'], row['Number_of_Casualties'], row['Number_of_Vehicles'],
        row['Police_Force'], row['Road_Surface_Conditions'], row['Road_Type'], row['Speed_limit'], row['Time'],
        row['Urban_or_Rural_Area'], row['Weather_Conditions'], row['Vehicle_Type']
    ))

    # Execute batch when batch size is reached
    if (i + 1) % batch_size == 0:
        session.execute(batch)
        batch = BatchStatement()  # Reset batch
        print("Inserting ", (i+1)/batch_size , " batch")

# Execute remaining records in batch
if len(batch) > 0:
    session.execute(batch)

print("Data inserted into bronze_accident_table.")

# Count the total number of rows in the table
count_query = "SELECT COUNT(*) FROM bronze_accident_table;"
rows = session.execute(count_query)

# Fetch the result
for row in rows:
    print(f"Total records in bronze_accident_table: {row[0]}")
