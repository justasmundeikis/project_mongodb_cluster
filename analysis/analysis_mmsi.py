import pymongo
import pandas as pd
import matplotlib.pyplot as plt
from mpl_toolkits.basemap import Basemap
import numpy as np

# MongoDB connection setup
client = pymongo.MongoClient("mongodb://localhost:27020")  # Use your mongos or MongoDB instance
db = client.ais_db
collection = db.vesseldata

# Querying the data
def get_ship_data(mmsi):
    """Fetches data for a specific MMSI."""
    return pd.DataFrame(list(collection.find({"mmsi": mmsi}, {"latitude": 1, "longitude": 1, "timestamp": 1, "_id": 0})))

def plot_ship_movements(ship_data):
    """Plots ship movements on a map."""
    # Setting up the map
    plt.figure(figsize=(12, 8))
    m = Basemap(projection='mill',
                llcrnrlat=-60, urcrnrlat=85,
                llcrnrlon=-180, urcrnrlon=180,
                resolution='c')
    m.drawcoastlines()
    m.drawcountries()
    m.fillcontinents(color='coral', lake_color='aqua')
    m.drawmapboundary(fill_color='aqua')

    # Converting lat/lon to x/y for plotting
    x, y = m(list(ship_data["longitude"]), list(ship_data["latitude"]))

    # Plot the data
    m.scatter(x, y, marker='o', color='r', zorder=5)
    plt.title(f'Ship MMSI: {mmsi} Movement Path')
    plt.show()

# Specify the MMSI for the ship you want to track
mmsi = input("Enter the MMSI number: ")  # User input for MMSI

# Fetch and plot data
ship_data = get_ship_data(mmsi)

if not ship_data.empty:
    plot_ship_movements(ship_data)
else:
    print(f"No records found for MMSI: {mmsi}")
