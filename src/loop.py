import boto3
import botocore
import matplotlib.pyplot as plt
import matplotlib.animation as animation
import pyart
from datetime import datetime
import cartopy.crs as ccrs
import threading
import time

# Setup S3 client
session = boto3.Session(
    aws_access_key_id=None,
    aws_secret_access_key=None,
    region_name='us-east-1'
)

s3_client = session.client('s3', config=botocore.client.Config(signature_version=botocore.UNSIGNED))
bucket = 'noaa-nexrad-level2'
station = 'KOKX'
latest_s3_key = None

# Function to retrieve the latest radar file URL for a given station
def get_latest_level_2_url(station, callback):
    cur_time = datetime.utcnow()
    year = str(cur_time.year)
    month = f"{cur_time.month:02}"
    day = f"{cur_time.day:02}"
    
    station_to_get = station.upper().replace(' ', '')
    prefix = f"{year}/{month}/{day}/{station_to_get}/"
    
    try:
        response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
        if 'Contents' in response:
            files = response['Contents']
            if files:
                files.sort(key=lambda x: x['Key'], reverse=True)
                latest_file = files[0]['Key']
                
                if 'MDM' in latest_file:
                    latest_file = files[1]['Key'] if len(files) > 1 else None
                
                if latest_file:
                    s3_url = f"s3://{bucket}/{latest_file}"
                    callback(s3_url) 
                else:
                    print("No valid radar data available.")
            else:
                print("No files found for the specified station and date.")
        else:
            print("No files found for the specified prefix.")
    except Exception as e:
        print(f"Error fetching data: {e}")

# Plot update function for animation
def update_plot(frame):
    global latest_s3_key
    if latest_s3_key is None:
        return
    
    radar_data = latest_s3_key
    if radar_data:
        try:
            radar = pyart.io.read_nexrad_archive(radar_data)
            plt.clf()
            display = pyart.graph.RadarMapDisplay(radar)
            ax = plt.subplot(111, projection=ccrs.PlateCarree())

            display.plot_ppi_map(
                "reflectivity",
                sweep=0,
                ax=ax,
                colorbar_label="Equivalent Reflectivity ($Z_e$) (dBZ)",
                vmin=-20,
                vmax=60,
            )
            plt.draw()
        except Exception as e:
            print(f"Error displaying radar data: {e}")
    else:
        print("No radar data available or error fetching.")

def update_plot_with_new_data(url):
    global latest_s3_key
    latest_s3_key = url
    print(f"Latest radar data S3 URL: {url}")

def check_for_new_data_periodically():
    while True:
        print("Checking for new radar data...")
        get_latest_level_2_url(station, lambda url: update_plot_with_new_data(url))
        time.sleep(300)

# Main function to run the animation
def main():
    fig = plt.figure(figsize=(12, 6))

    ani = animation.FuncAnimation(
        fig,
        update_plot,
        frames=100,
        interval=3000,
        repeat=True
    )

    # Start the thread for periodic checks every 5 minutes
    data_check_thread = threading.Thread(target=check_for_new_data_periodically)
    data_check_thread.daemon = True 
    data_check_thread.start()

    plt.show()

if __name__ == "__main__":
    main()
