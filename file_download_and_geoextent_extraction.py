# -*- coding: utf-8 -*- 
"""
Created on Fri Nov  1 13:30:51 2024

@author: yenyi
"""
import os
import requests
import pickle
import pandas as pd
import geoextent.lib.extent as geoextent
from geoextent.lib.helpfunctions import transform_bbox as transformBbox
from shapely import wkt
import time
import random
# Define the list of geospatial file extensions to check
geospatial_format_list = ['.shp', '.geojson', '.kml', '.gml', '.asc', '.tif', '.tiff', '.img', '.rst', '.gdb']

# remove_csv = os.remove(r"D:\geoextent_extracts\bbox_results.csv")

# Load the data from pickle files
def load_pickle(file_path):
    """Load pickle file and return its data as a list of dictionaries."""
    with open(file_path, 'rb') as f:
        data = pickle.load(f)
    return data

#load the pickle file
zenodo_data = load_pickle(r"C:\Users\yenyi\OneDrive\Uni\1_Zenodo_geospatial_files.pkl")
#specify the output directory for downloaded files 
output_dir = r"D:\output_Zenodo"
os.makedirs(output_dir, exist_ok=True) 
output_csv_path = r"D:\geoextent_extracts\bbox_results.csv"

# Define the checkpoint file path
checkpoint_path = r"D:\geoextent_extracts\checkpoint.pkl"

# Load checkpoint if exists
if os.path.exists(checkpoint_path):
    with open(checkpoint_path, 'rb') as f:
        processed_entries = pickle.load(f)
else:
    processed_entries = set()


# Calculate the total summed size of all 'sum_size' values in zenodo_data
total_sum_size = sum(entry.get('sum_size', 0) for entry in zenodo_data)
print(f"Total summed size of all entries (sum_size): {total_sum_size} bytes")
# Convert total_sum_size from bytes to gigabytes (= 5237.501164991409)
total_sum_size_gb = total_sum_size / (1024 ** 3)
print(f"Total summed size of all entries (sum_size) in gigabytes: {total_sum_size_gb:.2f} GB")


# Iterate over each entry in zenodo_data
for entry_index, entry in enumerate(zenodo_data, start=1):
    
    if entry_index in processed_entries:
        print(f"Skipping already processed entry {entry_index}")
        continue
    
    files = entry.get('files', [])  # Get the 'files' list if it exists in the entry
    doi_url = entry.get('doi_url', 'N/A')  # Retrieve the doi_url from the entry, default to 'N/A' if missing
    # ToDo: random sample  i.e. 10% of all available files from files
    entry_sum_size = entry.get('sum_size', 0)
    target_size = entry_sum_size * 0.10  # Set the target size to 10% of sum_size
    cumulative_size = 0  # Track the cumulative size of downloaded files
    failure_count = 0
    
    #Initialize an empty DataFrame with columns for file name, bbox, and temporal extent
    results_df = pd.DataFrame(columns=["filename", "bbox", "geometry", "doi_url"])
    
    #Shuffle the files list for random sampling
    random.shuffle(files)
    
    #Process each file in the 'files' list unti the 10% of sum_size (target)is reached
    for file_index, file_info in enumerate(files):
        if cumulative_size >= target_size or failure_count >= 5:
            break  # Stop if we've reached or exceeded the 10% target size
        
        file_name = file_info.get('key')  # Use 'key' as file name or default name
        file_size = file_info.get('size', 0)  # Get the file size if it exists
        file_url = file_info.get('links', {}).get('self')  # Get the 'self' link if it exists
        
        # Check if file_name has an extension in geospatial_format_list
        if file_name and any(file_name.lower().endswith(ext) for ext in geospatial_format_list):
            # Replace invalid characters in the file name
            safe_file_name = file_name.replace(':', '_')
            # Construct a new filename with article numbering prefix (e.g., "1_file_name.tif")
            numbered_file_name = f"{entry_index}_{safe_file_name}"
        
            # Only proceed if file_url is found and the accumulated size is less then or equal to target size
            if file_url and cumulative_size + file_size <= target_size:
                response = requests.get(file_url)

                # Check if the request was successful
                if response.status_code == 200:
                    # Construct the file path in the specified output directory
                    file_path = os.path.join(output_dir, numbered_file_name)

                    # Save the downloaded content to a file
                    with open(file_path, "wb") as f:
                        f.write(response.content)
                    
                    # Update cumulative size
                    cumulative_size += file_size
                    print(f"'{numbered_file_name}' ({file_size} bytes); cumulative size: {cumulative_size} bytes downloaded successfully to '{file_path}'")
                    
                    #Attempt to extract bbox through geoextent with error handling
                    try: 
                        result = geoextent.fromFile(file_path, bbox=True, tbox=True)
                        bbox = result.get('bbox')
                        
                    except Exception as e:
                        print(f"Error extracting bouding box from file {file_name}: {e}")
                        failure_count += 1  # Increment failure count
                        #Skip the file if extraction fails
                        os.remove(file_path)
                        print(f"File '{file_path}' deleted after extraction failure.")
                        continue
                    
                    #Attempt to Transform bbox into WKT format using transformBbox with error handling 
                    try:
                        if bbox:
                            bbox_wkt = transformBbox(bbox)
                            # Convert WKT to geometry using wkt.loads
                            geometry = wkt.loads(bbox_wkt)
                        else:
                            raise ValueError("No bounding box found.")
                    except Exception as e:
                        print(f"Error converting bbox to geometry for file {file_name}: {e}")
                        failure_count += 1  # Increment failure count
                        # Skip the file if transformation fails
                        os.remove(file_path)
                        print(f"File '{file_path}' deleted after transformation failure.")
                        continue
                    
                    #Reset failure count after a successful processing (only count consecutive failures)
                    failure_count = 0
                    
                    #Create a DataFrame for the current row and concatenate it
                    row_df = pd.DataFrame({
                              "filename": [numbered_file_name],
                              "bbox": [bbox],               # Original bbox as a list/tuple
                              "geometry": [geometry],        # Converted geometry from WKT
                              "doi_url": [doi_url]           #add the extracted doi as well 
                            })
                    results_df = pd.concat([results_df, row_df], ignore_index=True)
                    # Delete the file after processing
                    os.remove(file_path)
                    print(f"File '{file_path}' deleted after processing.")

                else:
                    print(f"Failed to download '{numbered_file_name}'. Status code: {response.status_code}")
                                    
        else:
            print(f"No 'self' link found for file {file_index} in entry {entry_index}.")
    
    # Save results to a CSV file after processing each entry
    if not results_df.empty:  # Only save if there is data
        results_df.to_csv(output_csv_path,mode='a', header=not os.path.exists(output_csv_path), index=False)
        print(f"Results for entry {entry_index} saved to {output_csv_path}")
        
    # Add to processed entries and save the checkpoint
    processed_entries.add(entry_index)
    with open(checkpoint_path, 'wb') as f:
        pickle.dump(processed_entries, f)   
      
    print(f"Finished processing 10% of entry {entry_index} with cumulative size: {cumulative_size} bytes (target: {target_size} bytes)")
    
    #Break after processing the first two entries
    #if entry_index >= 5: 
       #break
    
 
