from db_connection import connection_details

session = connection_details()


# Create Bronze Level Table
session.execute("""
    CREATE TABLE IF NOT EXISTS bronze_accident_table (
        unique_id UUID PRIMARY KEY,
        accident_index TEXT,
        accident_date TEXT,
        day_of_week TEXT,
        junction_control TEXT,
        junction_detail TEXT,
        accident_severity TEXT,
        latitude DECIMAL,
        light_conditions TEXT,
        local_authority_district TEXT,
        carriageway_hazards TEXT,
        longitude DECIMAL,
        number_of_casualties INT,
        number_of_vehicles INT,
        police_force TEXT,
        road_surface_conditions TEXT,
        road_type TEXT,
        speed_limit INT,
        time TEXT,
        urban_or_rural_area TEXT,
        weather_conditions TEXT,
        vehicle_type TEXT
    );
""")
print("Bronze Level Accident Table created successfully.")


# Create Silver Level Table
session.execute("""
    CREATE TABLE IF NOT EXISTS silver_accident_table (
        accident_index TEXT PRIMARY KEY,
        accident_date TEXT,
        day_of_week TEXT,
        junction_control TEXT,
        accident_severity TEXT,
        number_of_casualties INT,
        number_of_vehicles INT,
        speed_limit INT,
        weather_conditions TEXT,
        vehicle_type TEXT
    );
""")
print("Silver Level Accident Table created successfully.")

# Create Gold Level Table 1: Accidents by Day of the Week
session.execute("""
    CREATE TABLE IF NOT EXISTS gold_accidents_by_day_of_week (
        day_of_week TEXT PRIMARY KEY,
        total_accidents INT
    );
""")
print("Gold Level Table (Accidents by Day of the Week) created successfully.")

# Create Gold Level Table 2: Accidents by Severity
session.execute("""
    CREATE TABLE IF NOT EXISTS gold_accidents_by_severity (
        accident_severity TEXT PRIMARY KEY,
        total_accidents INT
    );
""")
print("Gold Level Table (Accidents by Severity) created successfully.")

# Create Gold Level Table 3: Accidents by Weather Conditions
session.execute("""
    CREATE TABLE IF NOT EXISTS gold_accidents_by_weather_conditions (
        weather_conditions TEXT PRIMARY KEY,
        total_accidents INT
    );
""")
print("Gold Level Table (Accidents by Weather Conditions) created successfully.")
