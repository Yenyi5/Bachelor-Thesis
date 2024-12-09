""""
Zenodo
serial implementation of main_05.py script -> simpler and less prone to server overload
However, slower than parallel execution

"""
import requests
import time
import csv
import random
import pickle

### parameters ###
page_size = 200
n_desired_records = 500  # Either random sample the desired records number from all available or choose all available if not enough for random sampling
# URL for API platform
API_URL =  "https://zenodo.org/api/records"
API_version = 1
##################

# query keys that are serially searched for 
query_list = ['geospatial', 'gis', 'remote sensing', 'ISO 19115', 'ISO 19119', 'ISO 19139','shapefile', 'geodatabase', 'vector', 'raster']
#query_list = ['remote sensing']
# file formats for filtering out relevant search results
geospatial_format_list = ['.shp', '.geojson', '.kml','.gml', '.asc', '.tif', '.tiff', '.img','.rst', '.gdb']  # You can add more formats if needed

# Define the standard keys you want to include in your CSV
#this was not used
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
    backoff_time = 4
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
    # Durchlaufe alle records
    for record in records:
        # Prüfe, ob der key 'files' im record existiert
        if 'files' in record:
            # Überprüfe, ob eines der files das Dateiformat in geospatial_format_list hat
            for file in record['files']:
                if file['key'].lower().endswith(tuple(geospatial_format_list)):
                    
        
                    # Collect all file names (not just matching ones)
                    all_file_names = [file['key'] for file in record['files']]
                    record.update({'file_format': all_file_names})
                    # Füge den neuen Key 'query_key' zum record hinzu
                    record.update({'query_key': query_key})
                    
                    # Füge den neuen Key 'sum_size' hinzu, falls mehrere files im record enthalten sind, werden die einzelnen sizes addiert
                    record.update({'sum_size': sum([ f['size'] for f in record['files'] ])})

                    # Füge den aktualisierten record zur Liste der gefilterten records hinzu
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
    """Save the collected records to read_pickle file."""
    # ToDo :
        # key_mapping_dict erweitern und für jede API anpassen. Ziel ist es, dass alles einheitlich ist -> identischen keys
        # überlegen welche Daten und einträge sind relevant für dich!
    # folgender code findet alle keys die in records enthalten sind 
    # Collect all unique keys from the records
    keys = set()
    for record in records:
        keys.update(record.keys())
    keys = list(keys)
    # könnte helfen abzuschätzen was alles benötigt wird und was nicht wichtig ist. (wird dennoch abgespeichert ?! why not)
    
    # available keys for Zenodo API : 
    # ['submitted', 'metadata', 'owners', 'updated', 'doi_url', 'state', 'conceptdoi', 'files', 'id', 'links', 'status', 'title', 'modified', 'conceptrecid', 'revision', 'stats', 'created', 'recid', 'doi']
    # submitted key is changed into submission_date ...
    # so right side keys in mapping dict are  changed into left side keys
    
    # Zenodo API
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
    # define filename for saving
    filename = '1_Zenodo_debug_'+ filename
    

    
    if not records:
        print("No records to save.")
        return
    

    # MODIFY
    # Normalize each record
    normalized_records = [normalize_data(record, key_mapping_dict) for record in records]

    # Get fieldnames from the first record (after normalization) for csv file writing
    
    fieldnames = normalized_records[0].keys() if normalized_records else []
    #is this still necessary?
    # Write to CSV
    with open(filename, 'w', newline='', encoding='utf-8', errors='replace') as output_file:
        dict_writer = csv.DictWriter(output_file, fieldnames=fieldnames)
        dict_writer.writeheader()
    if not records:
        print("No records to save.")
        return
    #why second time?
    # Normalize each record
    normalized_records = [normalize_data(record, key_mapping_dict) for record in records]
    #why second time??
    # Get fieldnames from the first record (after normalization)
    fieldnames = normalized_records[0].keys() if normalized_records else []

    
    # write to read_pickle
    with open(filename, 'wb') as output_file:  # Open file in binary write mode
        pickle.dump(normalized_records, output_file)  # Serialize the data and save it
        
 
def main():
    ### initialisation ### 
    collected_records = []
    failed_pages = []  # Track failed pages for a final retry
    final_failed_pages_dict = dict()

    print("Starting search for geospatial files on Zenodo...")
    
    # run iteratively for each query_list entry
    for query_string in query_list:
        print(' ')
        print(f"#### searching for query string :  {query_string} ####")
        print(' ')
        
        total_pages = get_total_pages(API_URL, query_string, size=page_size)
        if total_pages == 0:
            print("Failed to get total pages. Skipping this query.")
            continue
        #debug
        total_pages = 1
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

    # ToDo
    # final_failed_pages_dict save
    
    save_results(selected_records,API_version)
    print(f"Saved {len(selected_records)} records to file.")

if __name__ == "__main__":
    main()
