import matplotlib.pyplot as plt
from db_connection import connection_details


session = connection_details()

# Querying the gold_accidents_by_day_of_week table
rows_day_of_week = session.execute("SELECT * FROM gold_accidents_by_day_of_week;")
days = []
accidents_day_of_week = []
for row in rows_day_of_week:
    days.append(row.day_of_week)
    accidents_day_of_week.append(row.total_accidents)

# Querying the gold_accidents_by_severity table
rows_severity = session.execute("SELECT * FROM gold_accidents_by_severity;")
severity = []
accidents_severity = []
for row in rows_severity:
    severity.append(row.accident_severity)
    accidents_severity.append(row.total_accidents)

# Querying the gold_accidents_by_weather_conditions table
rows_weather = session.execute("SELECT * FROM gold_accidents_by_weather_conditions;")
weather_conditions = []
accidents_weather = []
for row in rows_weather:
    weather_conditions.append(row.weather_conditions)
    accidents_weather.append(row.total_accidents)

# Accidents by Day of the Week
plt.figure(figsize=(10, 6))
plt.bar(days, accidents_day_of_week, color='skyblue')
plt.title('Accidents by Day of the Week')
plt.xlabel('Day of the Week')
plt.ylabel('Total Accidents')
plt.xticks(rotation=45)  # Rotate x-axis labels for better readability
plt.show()

# Accidents by Severity (Pie Chart)
plt.figure(figsize=(8, 8))
plt.pie(accidents_severity, labels=severity, autopct='%1.1f%%', startangle=140, colors=['lightcoral', 'gold', 'lightgreen'])
plt.title('Accidents by Severity')
plt.show()

# Accidents by Weather Conditions
plt.figure(figsize=(12, 9))
plt.bar(weather_conditions, accidents_weather, color='lightgreen')
plt.title('Accidents by Weather Conditions')
plt.xlabel('Weather Conditions')
plt.ylabel('Total Accidents')
plt.xticks(rotation=25)  # Rotate x-axis labels for better readability
plt.show()
