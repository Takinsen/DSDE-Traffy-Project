import pandas as pd
import requests
import time

# ---------------------------------------------------------
# CONFIGURATION
# ---------------------------------------------------------
# Traffy data spans Aug 2021 - Jan 2025. Let's cover that range.
START_DATE = "2021-08-01"
END_DATE = "2025-01-30" 
OUTPUT_FILE = "bangkok_pm25_history.csv"

# ---------------------------------------------------------
# 1. PREPARE LOCATIONS (Districts)
# ---------------------------------------------------------
# Load your geography file
df_geo = pd.read_csv("../thailand_geography.csv")

# Filter for Bangkok Districts
# Adjust the string 'Bangkok' or 'กรุงเทพ' based on your CSV content
bangkok_districts = df_geo[df_geo['province'].str.contains("Bangkok|กรุงเทพ", case=False, na=False)]

# Create target list
target_locations = []
for index, row in bangkok_districts.iterrows():
    target_locations.append({
        "name": row['district'], 
        "lat": row['latitude'],
        "lon": row['longitude']
    })

print(f"Targeting {len(target_locations)} districts for PM 2.5 data...")

# ---------------------------------------------------------
# 2. THE SCRAPING LOOP
# ---------------------------------------------------------
pm25_data = []

print("Starting Air Quality Data Collection...")

for location in target_locations:
    dist_name = location['name']
    
    # Air Quality API URL (Different from Weather API)
    url = "https://air-quality-api.open-meteo.com/v1/air-quality"
    
    params = {
        "latitude": location['lat'],
        "longitude": location['lon'],
        "start_date": START_DATE,
        "end_date": END_DATE,
        "hourly": "pm2_5",  # We request hourly data
        "timezone": "Asia/Bangkok"
    }
    
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()
        
        # -----------------------------------------------------
        # PROCESS: Convert Hourly -> Daily Average
        # -----------------------------------------------------
        hourly_data = data.get('hourly', {})
        times = hourly_data.get('time', [])
        values = hourly_data.get('pm2_5', [])
        
        # Create a temporary DataFrame for this district
        temp_df = pd.DataFrame({
            'time': pd.to_datetime(times),
            'pm25': values
        })
        
        # Resample to Daily Mean (Average PM 2.5 per day)
        # This reduces 24 rows per day into 1 row per day
        temp_df.set_index('time', inplace=True)
        daily_df = temp_df.resample('D').mean().reset_index()
        
        # Add District Name back
        daily_df['district'] = dist_name

        daily_df['lat'] = location['lat']   # Assigns the latitude to every row
        daily_df['long'] = location['lon']  # Assigns the longitude to every row
        
        # Append to main list
        pm25_data.append(daily_df)
            
        print(f"✓ Fetched & Aggregated data for {dist_name}")
        
    except Exception as e:
        print(f"x Failed to fetch {dist_name}: {e}")
        
    # Be polite to the API
    time.sleep(0.5)

# ---------------------------------------------------------
# 3. EXPORT
# ---------------------------------------------------------
# Combine all districts into one DataFrame
if pm25_data:
    final_df = pd.concat(pm25_data, ignore_index=True)
    
    # Format the date nicely for Power BI (yyyy-mm-dd)
    final_df['date'] = final_df['time'].dt.strftime('%Y-%m-%d')
    
    # Select clean columns
    final_df = final_df[['district', 'date', 'pm25','lat', 'long']]
    
    # Rename for clarity
    final_df.rename(columns={'pm25': 'avg_pm25_ug_m3'}, inplace=True)
    
    final_df.to_csv(OUTPUT_FILE, index=False)
    
    print("------------------------------------------------")
    print(f"Scraping Completed!")
    print(f"Total Records: {len(final_df)}")
    print(f"File saved to: {OUTPUT_FILE}")
else:
    print("No data collected. Check your internet or API status.")