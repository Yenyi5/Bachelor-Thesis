""""
Zenodo
- simpler and less prone to server overload
- slower than parallel execution

"""
import requests
import time
import csv
import random
import pickle

# Define key parameters for the script
page_size = 200 # Number of results per page
n_desired_records = 500 # Desired number of geospatial records to retrieve
API_URL =  "https://zenodo.org/api/records" # Zenodo API base URL
API_version = 1

# Define query keywords and geospatial file formats to filter
query_list = ['geospatial', 'gis', 'remote sensing', 'ISO 19115', 'ISO 19119', 'ISO 19139','shapefile', 'geodatabase', 'vector', 'raster']
geospatial_format_list = ['.shp', '.geojson', '.kml','.gml', '.asc', '.tif', '.tiff', '.img','.rst', '.gdb']  # You can add more formats if needed

# Keys to include in the saved records
standard_save_keys = [
    'submitted', 'metadata', 'owners', 'updated', 'doi_url', 
    'state', 'conceptdoi', 'files', 'id', 'links', 
    'status', 'title', 'modified', 'conceptrecid', 
    'revision', 'stats', 'created', 'recid', 'doi'
]

def get_total_pages(zenodo_url, query_string, size, retries=3):
    """Fetches the initial response to determine the total number of records and calculates total pages."""
    for attempt in range(retries):
        try:
            response = requests.get(zenodo_url, params={'size': size, 'page': 1, 'q': query_string}, timeout=30)
            response.raise_for_status()
            data = response.json()
            total_records = data['hits']['total']
            total_pages = (total_records // size) + (1 if total_records % size > 0 else 0)
            print(f"Total records found: {total_records}, Total pages: {total_pages}")
            return total_pages
        except requests.exceptions.Timeout:
            print(f"Timeout occurred on attempt {attempt + 1}. Retrying...")
        except requests.exceptions.RequestException as e:
            print(f"Error fetching total pages: {e}")
            break
        time.sleep(2)
    return 0

def fetch_records(zenodo_url, query_string, page_number, size, retries=3):
    """Fetch records from a specific page number with retry logic and enhanced error handling."""
    backoff_time = 4  # Initial backoff time
    max_backoff = 64  # Maximum backoff time in seconds

    for attempt in range(retries):
        try:
            response = requests.get(zenodo_url, params={'size': size, 'page': page_number, 'q': query_string}, timeout=30)
            response.raise_for_status()  # Raise an error for bad responses
            data = response.json()
            return data['hits']['hits']  # Return the records
        except requests.exceptions.HTTPError as e:
            print(f"HTTPError on page {page_number}: {e}")
            wait_time = int(response.headers.get('Retry-After', backoff_time))
            print(f"Rate limit or server error on page {page_number}. Retrying in {wait_time} seconds...")
            time.sleep(wait_time)
            backoff_time = min(backoff_time * 2, max_backoff)
         
        except requests.exceptions.Timeout:
            print(f"Timeout on page {page_number}. Retrying in {backoff_time} seconds...")
            time.sleep(backoff_time)
            backoff_time = min(backoff_time * 2, max_backoff)
        except requests.exceptions.RequestException as e:
            print(f"Request error on page {page_number}: {e}")
            break
    return []  # Return an empty list if there's an error after retries

def filter_geospatial_files(records,geospatial_format_list,query_key):
    """Filter for geospatial records based on file formats."""

    filtered_records = []
    # Iterate over all records
    for record in records:
        # Check, if key 'files' exists in the record
        if 'files' in record:
            # Check, if one of the files matches a format in the geospatial_format_list 
            for file in record['files']:
                if file['key'].lower().endswith(tuple(geospatial_format_list)):
                    
        
                    # Collect all file names 
                    all_file_names = [file['key'] for file in record['files']]
                    record.update({'file_format': all_file_names})
                    # Add the new key 'query_key' to the record
                    record.update({'query_key': query_key})
                    
                    # Add the new key 'sum_size' to the record, if there are multiple files, sum up their sizes 
                    record.update({'sum_size': sum([ f['size'] for f in record['files'] ])})

                    # Add the updated record to filtered_records
                    filtered_records.append(record)
            
    return filtered_records

def normalize_data(original_dict, key_mapping_dict):
    """Normalize the keys of a dictionary using a key mapping."""
    normalized_dict = {}
    for original_key in original_dict.keys():
        # Check if the original key has a corresponding mapped key
        if original_key in key_mapping_dict:
            # Map the original key to the new key and use the original value
            new_key = key_mapping_dict[original_key]
            normalized_dict[new_key] = original_dict.get(original_key, None)
        else:
            # Keep the key unchanged if no mapping is found
            normalized_dict[original_key] = original_dict.get(original_key, None)

    return normalized_dict

def save_results(records,API_version, filename='geospatial_files.pkl'):
    """Save the collected records to a file in pickle format"""
 
    keys = set()
    for record in records:
        keys.update(record.keys())
    keys = list(keys)
   
    key_mapping_dict = {
    'doi': 'doi',
    'doi_URL': 'doi_URL',
    'resource_type': 'owners',
    'resource_type': 'resource_type',
    'date_publication': 'publication_date',
    'last_update': 'updated',
    'tags': 'keywords',
    'query':'query_key',
    'size(bytes)': 'size'
    # Add more mappings as needed
    }
    # Define filename for saving
    filename = '1_Zenodo_'+ filename
    
    if not records:
        print("No records to save.")
        return
    
    # Normalize each record
    normalized_records = [normalize_data(record, key_mapping_dict) for record in records]

    # Write to read_pickle
    with open(filename, 'wb') as output_file:  # Open file in binary write mode
        pickle.dump(normalized_records, output_file)  # Serialize the data and save it
        
 
def main():
    ### initialisation ### 
    collected_records = []
    failed_pages = []  # Track failed pages for a final retry
    final_failed_pages_dict = dict()

    print("Starting search for geospatial files on Zenodo...")
    
    # Run iteratively for each query_list entry
    for query_string in query_list:
        print(' ')
        print(f"#### searching for query string :  {query_string} ####")
        print(' ')
        
        total_pages = get_total_pages(API_URL, query_string, size=page_size)
        if total_pages == 0:
            print("Failed to get total pages. Skipping this query.")
            continue
        #debug
        #total_pages = 1
        for page in range(1, total_pages + 1):
            try:
                records = fetch_records(API_URL, query_string, page, page_size)
                if records:
                    filtered_records = filter_geospatial_files(records,geospatial_format_list,query_string)
                    collected_records.extend(filtered_records)
                    print(f"Page {page} is valid with {len(filtered_records)} records.")
                else:
                    print(f"Page {page} is invalid or returned no records. Retry at the end.")
                    failed_pages.append(page)

            except Exception as e:
                print(f"Error occurred while processing page {page}: {e}. Retry at the end.")
                failed_pages.append(page)

        # Retry failed pages
        if failed_pages:
            final_failed_pages = list()
            print(f"Retrying {len(failed_pages)} failed pages...")
            for page in failed_pages:
                try:
                    records = fetch_records(API_URL, query_string, page, page_size)
                    if records:
                        filtered_records = filter_geospatial_files(records,geospatial_format_list,query_string)
                        collected_records.extend(filtered_records)
                        print(f"Retry successful for page {page} with {len(filtered_records)} records.")
                    else:
                        final_failed_pages.append(page)
                        print(f"Retry failed or no records for page {page}. Skipping.")
                except Exception as e:
                    print(f"Retry failed for page {page}: {e}")
            failed_pages.clear()  # Clear failed pages after retrying
            # storing failed_pages that failed retry at the end for documentation in dict
            final_failed_pages_dict['query_string'] = final_failed_pages
            
     
    if len(collected_records) >= n_desired_records:
        print(f"{len(collected_records)} records collected -> randomly sample {n_desired_records} records")
        selected_records = random.sample(collected_records, n_desired_records)
    else:
        print(f"{len(collected_records)} records collected -> available records less than desired {n_desired_records} -> choose all available records")
        selected_records = collected_records

    if not selected_records:
        print("No geospatial records found. Exiting.")
        return

    save_results(selected_records,API_version)
    print(f"Saved {len(selected_records)} records to file.")

if __name__ == "__main__":
    main()
