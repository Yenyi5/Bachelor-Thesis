"""
Dataverse
serial implementation of main_05.py script -> simpler and less prone to server overload
However, slower than parallel execution

aktuell funktioniert datenabfrage via url im 2. step nicht (wie in figshare wird erst metadata abgerufen und dann files im 2. Schritt)
URL Konstruktion in get_article_files muss geändert werden.

"""
import requests
import time
import csv
import random
import pickle
from urllib.parse import quote

### parameters ###
page_size = 200
n_desired_records = 500  # Number of desired records
# URL for Harvard Dataverse API platform
api_key = '2a8faeaf-d10d-401c-8977-3b193d3be3be'
header_name={'X-Dataverse-key': '2a8faeaf-d10d-401c-8977-3b193d3be3be'}
API_URL = f"https://dataverse.harvard.edu/api/search"

##################

# query keys that are serially searched for
query_list = ['geospatial', 'gis', 'remote sensing', 'ISO 19115', 'ISO 19119', 'ISO 19139', 'shapefile', 'geodatabase', 'vector', 'raster']
#query_list = ['geospatial']
# file formats for filtering out relevant search results
geospatial_format_list = ['.shp', '.geojson', '.kml','.gml', '.asc', '.tif', '.tiff', '.img','.rst', '.gdb']

# Define the standard keys you want to include in your CSV
standard_save_keys = ['name', 'description', 'published_at', 'identifier', 'latestVersion', 'fileCount', 'totalSize']

def get_total_pages(dataverse_url, query_string, size, retries=3):
    """Fetch the total number of records and calculate total pages."""
    for attempt in range(retries):
        try:
            response = requests.get(dataverse_url, params={'q': query_string, 'per_page': size}, headers={'X-Dataverse-key': '2a8faeaf-d10d-401c-8977-3b193d3be3be'},   timeout=30)

            response.raise_for_status()
            data = response.json()
            total_records = data['data']['total_count']
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

def get_article_files(metadata):
    """Fetch files from a Dataverse dataset using persistent ID."""
    api_url = 'https://dataverse.harvard.edu/api' # api_url.replace('/search', '')
    header_name={'X-Dataverse-key': '2a8faeaf-d10d-401c-8977-3b193d3be3be'}
    # Get the persistent_id from the metadata
    persistent_id = metadata['global_id']  # Assuming this contains the full DOI, e.g., 'doi:10.7910/DVN/68IYCX'
    
    url_constructed= f"{api_url}/datasets/export?exporter=dataverse_json&persistentId={quote(persistent_id)}"
    # [requests.get(url) for i in range(200)]
    #print(f"Fetching files from: {url12345}")
    response = requests.get(url=url_constructed, headers=header_name)
    #print(response.headers)
    #print(f"Response status:< {response.status_code}")
    
    if response.status_code == 200:
        data = response.json()
        
        # data['datasetVersion']['files'][0]['dataFile']['filename'] # oder filesize
        return data['datasetVersion']['files'] # Liste mit file dicts
        # darin enthalten : dict_keys(['description', 'label', 'restricted', 'version', 'datasetVersionId', 'dataFile'])
        # in ['dataFile'] stecken relevante infos : 'filename' und 'filesize'
    else:
        print(f"Error fetching files: {response.status_code} - {response.text}")
        return []

def fetch_records(dataverse_url, query_string, page_number, size, api_key, retries=3):
    """Fetch records from a specific page number with retry logic."""
    backoff_time = 4
    max_backoff = 64

    for attempt in range(retries):
        response = requests.get(dataverse_url, params={'q': query_string, 'start': page_number * size, 'per_page': size}, headers={'X-Dataverse-key': '2a8faeaf-d10d-401c-8977-3b193d3be3be'}, timeout=30)
        response.raise_for_status()
        metadata_list = response.json()['data']['items'] 
        # Filtern der metadata_list : files müssen aussortiert werden, da bereits in den datensets enthalten
        metadata_list = [metadata_list[index] for index in range(len(metadata_list))  if metadata_list[index]['type'] == 'dataset' ]
        # metadata_list : Einträge müssen einzeln durchgegangen werden und mit 'id' key die zugehörigen Files abgerufen werden
        update_metadata_list = list()
        for index, metadata in enumerate(metadata_list):#[:10]:
            try:
                # hier mit get_article_files() die files zu den metadaten runterladen
                if 'global_id' in metadata.keys():
                    data = get_article_files(metadata) # file format info in "name" key
                else:
                    data = []
                    #print(index, 'no global_id found')
                # data keys: 'id', 'name', 'size', 'is_link_only', 'download_url', 'supplied_md5', 'computed_md5', 'mimetype'
                if len(data)>0: # if metadata contains data, then get file information and add to dict
                   
                    single_data_dict = {}
                    # Iterate over each dictionary in the list
                    for dictionary in data:
                        for key, value in dictionary['dataFile'].items():
                            # Add values to the list corresponding to each key in the result dictionary
                            single_data_dict.setdefault(key, []).append(value)
                                                                                                                                    
                    # Rename 'id' from datafile to 'id_file' to distinguish between metadata id and file ids
                    if 'description' in single_data_dict.keys():
                        single_data_dict['file_description'] = single_data_dict.pop('description')

                    # file keys die hinzugefügt werden : ['id', 'persistentId', 'pidURL', 'filename', 'contentType', 'filesize', 'description', 'storageIdentifier', 'rootDataFileId', 'md5', 'checksum', 'creationDate']
                    metadata.update(single_data_dict)
                    # metadata geupdatet keys: 
                        # 'id', 'title', 'doi', 'handle', 'url', 'published_date', 'thumb', 'defined_type', 'defined_type_name', 'group_id', 'url_private_api', 'url_public_api', 'url_private_html', 'url_public_html', 'timeline', 'resource_title', 'resource_doi', 'name', 'size', 'is_link_only', 'download_url', 'supplied_md5', 'computed_md5', 'mimetype'
                    # geupdatete metadata (einzelner update step) werden wieder in eine list zusammengesammelt
                update_metadata_list.append(metadata) 
                
            except  requests.exceptions.RequestException as e:
                print(f"Request error on page {page_number}: {e}")
                continue
    
        return update_metadata_list
               
    
def filter_geospatial_files(records, geospatial_format_list, query_key):
    """Filter for geospatial records based on file formats."""
    filtered_records = []
    for record in records:
        # if 'filename' in file_info and any(record['filename'].lower().endswith(fmt) for fmt in geospatial_format_list):
        if 'filename' in record and any(fmt in record['name'].lower() for fmt in geospatial_format_list):
            record.update({'query_key': query_key})
            filtered_records.append(record)
    return filtered_records

def normalize_data(original_dict, key_mapping_dict):
    """Normalize the keys of a dictionary using a key mapping."""
    normalized_dict = {}
    for original_key in original_dict.keys():
        if original_key in key_mapping_dict:
            new_key = key_mapping_dict[original_key]
            normalized_dict[new_key] = original_dict.get(original_key, None)
        else:
            normalized_dict[original_key] = original_dict.get(original_key, None)
    return normalized_dict

def save_results(records, filename='geospatial_files.pkl'):
    """Save the collected records to a pickle file."""
    key_mapping_dict = {
        'name': 'title',
        'description': 'metadata',
        'published_at': 'created',
        'identifier': 'doi',
        'latestVersion': 'version',
    }

    filename = '1_Harvard_Dataverse_' + filename

    if not records:
        print("No records to save.")
        return
    
    normalized_records = [normalize_data(record, key_mapping_dict) for record in records]

    fieldnames = normalized_records[0].keys() if normalized_records else []

    with open(filename, 'w', newline='', encoding='utf-8', errors='replace') as output_file:
        dict_writer = csv.DictWriter(output_file, fieldnames=fieldnames)
        dict_writer.writeheader()
        dict_writer.writerows(normalized_records)

    with open(filename.replace('.csv', '.pkl'), 'wb') as output_file:
        pickle.dump(normalized_records, output_file)

def main():
    collected_records = []
    failed_pages = []
    final_failed_pages_dict = {}

    print("Starting search for geospatial files on Harvard Dataverse...")
    
    for query_string in query_list:
        print(f"#### Searching for query string: {query_string} ####")
        total_pages = get_total_pages(API_URL, query_string, page_size)
        if total_pages == 0:
            print("Failed to get total pages. Skipping this query.")
            continue
        #total_pages = 1
        for page in range(1, total_pages + 1):
            try:
                records = fetch_records(API_URL, query_string, page, page_size, header_name)
                if records:
                    filtered_records = filter_geospatial_files(records, geospatial_format_list, query_string)
                    collected_records.extend(filtered_records)
                    print(f"Page {page} is valid with {len(filtered_records)} records.")
                else:
                    print(f"Page {page} returned no records.")
                    failed_pages.append(page)
            except Exception as e:
                print(f"Error on page {page}: {e}")
                failed_pages.append(page)

        if failed_pages:
            print(f"Retrying {len(failed_pages)} failed pages...")
            for page in failed_pages:
                try:
                    records = fetch_records(API_URL, query_string, page, page_size, api_key)
                    if records:
                        filtered_records = filter_geospatial_files(records, geospatial_format_list, query_string)
                        collected_records.extend(filtered_records)
                        print(f"Retry successful for page {page} with {len(filtered_records)} records.")
                    else:
                        print(f"Retry failed for page {page}.")
                except Exception as e:
                    print(f"Retry error on page {page}: {e}")
            failed_pages.clear()

    if len(collected_records) >= n_desired_records:
        selected_records = random.sample(collected_records, n_desired_records)
    else:
        selected_records = collected_records

    if not selected_records:
        print("No geospatial records found. Exiting.")
        return

    save_results(selected_records)
    print(f"Saved {len(selected_records)} records to file.")

if __name__ == "__main__":
    main()
