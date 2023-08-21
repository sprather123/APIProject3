import requests
import pandas as pd
import json

# API endpoint URL
url = "https://api.bls.gov/publicAPI/v2/timeseries/data/"

# Specify the series IDs
series_ids = ["CUUR0000SA0", "LNU04000000"]

# Specify the date range
start_year = 1947
end_year = 2023

# Divide the years range into smaller chunks (e.g., 5-year chunks)
chunk_size = 5
year_chunks = range(start_year, end_year + 1, chunk_size)

# Initialize an empty list to store data
all_data = []

# Iterate through year chunks and make API requests
for chunk_start in year_chunks:
    chunk_end = chunk_start + chunk_size - 1
    payload = {
        "seriesid": series_ids,
        "startyear": str(chunk_start),
        "endyear": str(chunk_end),
        "registrationkey": "3b713b4dcbc14672ba7358a311057ac5"  # Replace with your registration key
    }

    # Send POST request with payload
    response = requests.post(url, json=payload)

    # Check if the request was successful
    if response.status_code == 200:
        try:
            data = json.loads(response.text)
            results = data["Results"]["series"]
            all_data.extend(results)
        except json.JSONDecodeError as e:
            print("Error decoding JSON:", e)
    else:
        print("Request failed with status code:", response.status_code)

# Transform data and create a DataFrame
transformed_data = []
for result in all_data:
    series_id = result["seriesID"]
    series_data = result.get("data", [])
    for entry in series_data:
        entry_data = {
            "seriesID": series_id,
            "year": entry.get("year", ""),
            "period": entry.get("period", ""),
            "periodName": entry.get("periodName", ""),
            "value": entry.get("value", ""),
        }
        transformed_data.append(entry_data)

df = pd.DataFrame(transformed_data)

# Print the DataFrame
print(df)

# Export data to a CSV file
df.to_csv('data.csv', index=False)

# Sort the data by series ID, year, and period
df_sorted = df.sort_values(by=['seriesID', 'year', 'period'])

df_sorted

df_sorted.to_csv('sorted_data.csv', index=False)

# Split data by seriesID
grouped_data = df_sorted.groupby('seriesID')

# Save data to separate files
for series_id, group in grouped_data:
    file_name = f'{series_id}.csv'
    group.to_csv(file_name, index=False)