from db_connection import connection_details


session = connection_details()

# Fetch data from the silver accident table
rows_silver_table = session.execute("SELECT * FROM silver_accident_table;")

# Initialize dictionaries for aggregating accident data
day_of_week_accidents = {}
severity_accidents = {}
weather_conditions_accidents = {}

# Aggregate data from the silver table
for row in rows_silver_table:
    # Aggregate by Day of the Week
    if row.day_of_week in day_of_week_accidents:
        day_of_week_accidents[row.day_of_week] += 1
    else:
        day_of_week_accidents[row.day_of_week] = 1

    # Aggregate by Accident Severity
    if row.accident_severity in severity_accidents:
        severity_accidents[row.accident_severity] += 1
    else:
        severity_accidents[row.accident_severity] = 1

    # Aggregate by Weather Conditions
    if row.weather_conditions in weather_conditions_accidents:
        weather_conditions_accidents[row.weather_conditions] += 1
    else:
        weather_conditions_accidents[row.weather_conditions] = 1

# Insert aggregated data into Gold Level Table 1: Accidents by Day of the Week
for day_of_week, total_accidents in day_of_week_accidents.items():
    session.execute("""
        INSERT INTO gold_accidents_by_day_of_week (day_of_week, total_accidents)
        VALUES (%s, %s)
    """, (day_of_week, total_accidents))

# Insert aggregated data into Gold Level Table 2: Accidents by Severity
for accident_severity, total_accidents in severity_accidents.items():
    session.execute("""
        INSERT INTO gold_accidents_by_severity (accident_severity, total_accidents)
        VALUES (%s, %s)
    """, (accident_severity, total_accidents))

# Insert aggregated data into Gold Level Table 3: Accidents by Weather Conditions
for weather_conditions, total_accidents in weather_conditions_accidents.items():
    session.execute("""
        INSERT INTO gold_accidents_by_weather_conditions (weather_conditions, total_accidents)
        VALUES (%s, %s)
    """, (weather_conditions, total_accidents))

print("Data inserted into gold tables")

# Count the total number of rows in the table
print("\n")
query = "SELECT * FROM gold_accidents_by_day_of_week;"
rows = session.execute(query)

# Fetch the result
for row in rows:
    print(row)

# Count the total number of rows in the table
print("\n")
query = "SELECT * FROM gold_accidents_by_weather_conditions;"
rows = session.execute(query)

# Fetch the result
for row in rows:
    print(row)

# Count the total number of rows in the table
print("\n")
query = "SELECT * FROM gold_accidents_by_severity;"
rows = session.execute(query)

# Fetch the result
for row in rows:
    print(row)
