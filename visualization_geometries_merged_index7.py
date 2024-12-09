# -*- coding: utf-8 -*-
"""
Created on Mon Nov 18 12:45:43 2024

@author: yenyi
"""

import pandas as pd
import folium
from shapely import wkt
from shapely.ops import unary_union
from shapely.geometry import mapping
import random
import numpy as np

# Load the CSV file
file_path = r'D:\geoextent_extracts\bbox_results - Copy.csv'
# Save the map to an HTML file
map_path = r'D:/merged_geometries_index17.htm'
data = pd.read_csv(file_path,delimiter=';')



def safe_wkt_loads(geometry):
    # Handle missing or non-string values gracefully
    if isinstance(geometry, str):  # Only process valid strings
        try:
            return wkt.loads(geometry)
        except Exception as e:
            print(f"Invalid WKT: {geometry}, Error: {e}")
            return None
    else:
        return None
    
# translate back into polygon objects
data['geometry'] = data['geometry'].apply(safe_wkt_loads)

# Extract 'index' from the 'filename' column
# Assuming 'filename' starts with the index followed by other details
data['index'] = data['filename'].str.extract(r'^(\d+)').astype(int)

# Initialize a Folium map centered globally
m = folium.Map(location=[0, 0], zoom_start=2)

    
# Loop through each entry and add the merged geometry to the map
for index_value in [np.unique(data['index'].values)[12]]:
    # Convert Shapely geometry to GeoJSON format
    mapping_data = data[data['index'] == index_value]
    #unmerged_geometry = mapping_data['geometry']
    combined_geometry = unary_union(mapping_data['geometry'])
    
    # Debugging: print individual and merged geometries
    
    print("Original Geometries:")
    print(mapping_data['geometry'].tolist())
    print("Merged Geometry:")
    print(combined_geometry)
    print(f"Index: {index_value}")
    #geo_json = mapping(mapping_data['geometry'].values[0])
    geo_json = mapping(combined_geometry)
    
    # Create a popup with detailed information
    popup_html = f"""
    <b>REPOSITORY</b><br>
    <ul>
        <li><b>Filename:</b> {mapping_data['filename'].values[0]}</li>
        <li><b>DOI:</b> {mapping_data['doi_url'].dropna().values[0]}</li>
    </ul>
    """
    popup = folium.Popup(popup_html, max_width=300)

    # Add the geometry to the map as a GeoJson layer with the new style
    folium.GeoJson(
        geo_json,
        style_function=lambda x, color="#{:06x}".format(random.randint(0, 0xFFFFFF)): {
            'fillColor': 'none',
            'color': color,
            'weight': 1.5,
            'fillOpacity': 0
        },
        tooltip=f"Index: {index_value}",
        popup=popup
    ).add_to(m)
 
m.save(map_path)
# Output the path to the saved map
map_path
print(f"Saved merged map at {map_path}")

