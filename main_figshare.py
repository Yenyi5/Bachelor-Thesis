'''
Created on 08.10.2024

@author: yenyi
'''

"""
Figshare
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
API_URL = "https://api.figshare.com/v2/articles"
API_version = 1 

##################

# query keys that are serially searched for 
query_list = ['geospatial','geo', 'gis', 'remote sensing', 'ISO 19115', 'ISO 19119', 'ISO 19139', 'shapefile', 'geodatabase', 'vector', 'raster']
#query_list = ['geospatial']
# file formats for filtering out relevant search results
geospatial_format_list = ['.shp', '.geojson', '.kml','.gml', '.asc', '.tif', '.tiff', '.img','.rst', '.gdb']  # You can add more formats if needed
                         
def get_article_files(api_url,article_id):
    """Fetches file details of an article by its ID from the Figshare API."""
    response = requests.get(f"{api_url}/{article_id}")
    response.raise_for_status()
    article_details = response.json()

    # Extract the files list from the article details
    if 'files' in article_details:
        return article_details['files'] # multiple files for each "record"
    else:
        print(f"No files found for article ID: {article_id}")
        return []

def fetch_records(api_url, query_string, page_number, size, retries=1):
    """Fetch records from a specific page number with retry logic and enhanced error handling."""
    
    try:

    # neu
        response = requests.get(api_url, params={'search_for': query_string, 'page': page_number, 'page_size': size}, timeout=30)
    # alt
    #response = requests.get(zenodo_url, params={'size': size, 'page': page_number, 'q': query_string}, timeout=30)
        response.raise_for_status()  # Raise an error for bad responses
    except requests.exceptions.HTTPError as e:
        if response.status_code == 400: 
            print(f"Bad Request (400) for '{query_string}'on page {page_number}. Skipping this query.")
            return None, False
    
    metadata_list = response.json()
    
    update_metadata_list = list()
    for metadata in metadata_list:#[:10]:
        try:
            # hier mit get_article_files() die files zu den metadaten runterladen
            data = get_article_files(API_URL,metadata['id']) # file format info in "name" key
            # data keys: 'id', 'name', 'size', 'is_link_only', 'download_url', 'supplied_md5', 'computed_md5', 'mimetype'
            
            if len(data)>0: # if metadata contains data, then get file information and add to dict
                # data beinhaltet den key "name" der den filenamen mit der zugehörigen datapoint endung beinhaltet
                # falls tatsächliches file notwendig ist, dann müsste hier im nächsten schritt noch die datei runtergeladen werden
                # requests.get() with stream=True lädt daten herunter
                # data und metadata dict werden in metadata dict zusammengefügt 
        
                # reformat list of data dicts into a single data dict with multiple values (f.e. file ids)
                # Initialize an empty dictionary to hold the result
                single_data_dict = {}
                # Iterate over each dictionary in the list
                for dictionary in data:
                    for key, value in dictionary.items():
                        # Add values to the list corresponding to each key in the result dictionary
                        single_data_dict.setdefault(key, []).append(value)
                                                                                                                                
                # Rename 'id' from datafile to 'id_file' to distinguish between metadata id and file ids
                single_data_dict['file_id'] = single_data_dict.pop('id')
                single_data_dict['file_format'] = single_data_dict.pop('name')

                metadata.update(single_data_dict)
                # metadata geupdatet keys: 
                    # 'id', 'title', 'doi', 'handle', 'url', 'published_date', 'thumb', 'defined_type', 'defined_type_name', 'group_id', 'url_private_api', 'url_public_api', 'url_private_html', 'url_public_html', 'timeline', 'resource_title', 'resource_doi', 'name', 'size', 'is_link_only', 'download_url', 'supplied_md5', 'computed_md5', 'mimetype'
                # geupdatete metadata (einzelner update step) werden wieder in eine list zusammengesammelt
            update_metadata_list.append(metadata)
            
        except  requests.exceptions.RequestException as e:
            print(f"Request error on page {page_number}: {e}")
            continue
        
    # check if available page number deviates from defined number of page entries, then probably no more pages left
    if len(metadata_list)<size:
        has_more_pages = False
    else:
        has_more_pages = True
        
    return update_metadata_list, has_more_pages

def filter_geospatial_files(records,geospatial_format_list,query_key):
    """Filter for geospatial records based on file formats."""

    filtered_records = []
    # Durchlaufe alle records
    for record in records:
        # Prüfe, ob der key 'files' im record existiert
        if 'file_format' in record:
            # Überprüfe, ob eines der files (hier unter name key) das Dateiformat in geospatial_format_list hat
            for file in record['file_format']:
                if file.lower().endswith(tuple(geospatial_format_list)):

                    # Füge den neuen Key 'query_key' zum record hinzu
                    record.update({'query_key': query_key})
                    
                    # Füge den neuen Key 'sum_size' hinzu, falls mehrere files im record enthalten sind, werden die einzelnen sizes addiert
                    record.update({'sum_size': sum( record['size']) })

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
    """Save the collected records to pickle file."""
    ### Hier muss noch angepasst werden!

    # Figshare API
    key_mapping_dict = {
    'submission_date': 'submitted',
    'metadata_info': 'metadata',
    'file_owners': 'owners',
    'last_updated': 'updated',
    'doi_link': 'doi_url',
    # Add more mappings as needed
    }
    # define filename for saving
    filename = '2_Figshare_'+filename
        
    
    if not records:
        print("No records to save.")
        return
    
    # Normalize each record
    normalized_records = [normalize_data(record, key_mapping_dict) for record in records]

    # Get fieldnames from the first record (after normalization) for csv file writing
    
    fieldnames = normalized_records[0].keys() if normalized_records else []

    # Write to CSV
    with open(filename, 'w', newline='', encoding='utf-8', errors='replace') as output_file:
        dict_writer = csv.DictWriter(output_file, fieldnames=fieldnames)
        dict_writer.writeheader()
    if not records:
        print("No records to save.")
        return
    
    # Normalize each record
    normalized_records = [normalize_data(record, key_mapping_dict) for record in records]

    # Get fieldnames from the first record (after normalization)
    fieldnames = normalized_records[0].keys() if normalized_records else []

    # write to pickle
    with open(filename, 'wb') as output_file:  # Open file in binary write mode
        pickle.dump(normalized_records, output_file)  # Serialize the data and save it
        


def main():
    ### initialisation ###
    collected_records = []
    failed_pages = []  # Track failed pages for a final retry
    final_failed_pages_dict = dict()

    print("Starting search for geospatial files on Figshare...")
    
    # run iteratively for each query_list entry
    for query_string in query_list:
        print(' ')
        print(f"#### searching for query string :  {query_string} ####")
        print(' ')
        
        
        more_pages = True
        page = 1
        while more_pages == True: # while condition instead of loop over total_pages (they are unknown and hard to determine)
            try:
                records,more_pages = fetch_records(API_URL, query_string, page, page_size)
                if records is None: #check if we should skip this query (when 400 Error occurs)
                    break
                
                if records:
                    filtered_records = filter_geospatial_files(records,geospatial_format_list,query_string)
                    collected_records.extend(filtered_records)
                    print(f"Page {page} is valid with {len(filtered_records)} records.")
                else:
                    print(f"Page {page} is invalid or returned no records. Retry at the end.")
                    failed_pages.append(page)
                page +=1
                #debug
                more_pages = True

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
