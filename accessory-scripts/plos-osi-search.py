import requests
import pandas as pd
import json
import math
import numpy as np
import os
import zipfile
from datetime import datetime
from pathlib import Path

#setting timestamp at start of script to calculate run time
startTime = datetime.now() 
#creating variable with current date for appending to filenames
todayDate = datetime.now().strftime("%Y%m%d") 

#read in config file
parent = os.path.abspath(os.path.join(os.getcwd(), '..'))
with open(f'{parent}/config.json', 'r') as file:
    config = json.load(file)

#create directory for PLOS data
if os.path.isdir("inputs"):
        print("inputs directory found - no need to recreate")
else:
    os.mkdir("inputs")
    print("inputs directory has been created")

if os.path.isdir("accessory-outputs"):
        print("accessory outputs directory found - no need to recreate")
else:
    os.mkdir("accessory-outputs")
    print("accessory outputs directory has been created")

#read in PLOS OSI data
##code from Figshare: https://help.figshare.com/article/how-to-use-the-figshare-api 
##current version: https://doi.org/10.6084/m9.figshare.21687686.v9 
print("Retrieving version 9 of PLOS OSI dataset\n")
item_id = 21687686
figshare_url = 'https://api.figshare.com/v2'

file_info = [] #a blank list to hold all the file metadata

r = requests.get(figshare_url + '/articles/' + str(item_id) + '/files')
file_metadata = json.loads(r.text)
for j in file_metadata: #add the item id to each file record- this is used later to name a folder to save the file to
    j['item_id'] = item_id
    file_info.append(j) #Add the file metadata to the list

#Download each file to a subfolder named for the article id and save with the file name
for k in file_info:
    response = requests.get(figshare_url + '/file/download/' + str(k['id']), headers=None)
    Path('inputs/' + str(k['item_id'])).mkdir(parents=True, exist_ok=True)
    with open('inputs/' + str(k['item_id']) + '/' + k['name'], 'wb') as f:
        f.write(response.content)

zip_file_directory = f'inputs/{item_id}'
target_directory = 'inputs'

#looking for primary directory
zip_file_path = None
for file_name in os.listdir(zip_file_directory):
    if 'PLOS-OSI-Dataset' in file_name:
        zip_file_path = os.path.join(zip_file_directory, file_name)
        break

#retrieving specific CSV file
if zip_file_path:
    with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
        for file in zip_ref.namelist():
            if 'Main Data Files/PLOS-Dataset' in file and file.endswith('.csv'):
                extracted_file_path = os.path.join(target_directory, os.path.basename(file))
                # Check if the file already exists before extracting
                if not os.path.exists(extracted_file_path):
                    # Extract the specific file directly to the target directory
                    with zip_ref.open(file) as source, open(extracted_file_path, 'wb') as target:
                        target.write(source.read())
                    print(f"Extracted {extracted_file_path}")
                else:
                    print(f"File {extracted_file_path} already exists. Skipping extraction.")
else:
    print("No matching zip archive found.")

csv_file_directory = "inputs"
csv_files = sorted([f for f in os.listdir(csv_file_directory) if f.endswith('.csv')], reverse=True)

if csv_files:
    csv_file_path = os.path.join(csv_file_directory, csv_files[0])
    df_plos = pd.read_csv(csv_file_path)
    print(f'Reading in {csv_file_path}\n')
else:
    print("No CSV file found in the directory.")

#filtering for SI
##could be expanded to capture 'Code_Location'
df_plos_SI = df_plos[df_plos['Data_Location'].str.contains('Supplementary Information', na=False)]
df_NCBI = df_plos[df_plos['Repositories_data'].str.contains('NCBI|Gene Expression Omnibus', na=False)]

#OpenAlex params
url_openalex = 'https://api.openalex.org/works'
j = 0
page_limit_openalex = config['VARIABLES']['PAGE_LIMITS']['openalex_prod']

params_openalex = {
    'filter': 'authorships.institutions.ror:https://ror.org/00hj54h04,locations.source.host_organization:https://openalex.org/P4310315706', #PLOS ID in OpenAlex
    'per-page': config['VARIABLES']['PAGE_SIZES']['openalex'],
    'select': 'id,doi,title,authorships,primary_location,type',
    'mailto': config['EMAIL']['user_email']
}

##retrieves a single page of results
def retrieve_page_openalex(url, params=None):
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()  
        return response.json()
    except requests.RequestException as e:
        print(f"Error fetching page: {e}")
        return {'results': [], 'meta':{}}
##recursively retrieves pages    
def retrieve_all_data_openalex(url, params):
    global j
    all_data_openalex = []
    data = retrieve_page_openalex(url, params)
    params = params_openalex.copy()
    params['cursor'] = '*'
    next_cursor = '*'
    previous_cursor = None
    
    if not data['results']:
        print("No data found.")
        return all_data_openalex

    all_data_openalex.extend(data['results'])
    
    total_count = data.get('meta', {}).get('count', None)
    per = data.get('meta', {}).get('per_page', None)
    total_pages = math.ceil(total_count/per) + 1

    print(f"Total: {total_count} entries over {total_pages} pages\n")

    while j < page_limit_openalex:
        j += 1
        print(f"Retrieving page {j} of {total_pages} from OpenAlex...\n")
        data = retrieve_page_openalex(url, params)
        next_cursor = data.get('meta', {}).get('next_cursor', None)

        if next_cursor == previous_cursor:
            print("Cursor did not change. Ending loop to avoid infinite loop.")
            break
        
        if not data['results']:
            print("End of OpenAlex response.\n")
            break
        
        all_data_openalex.extend(data['results'])
        
        previous_cursor = next_cursor
        params['cursor'] = next_cursor
    
    return all_data_openalex

print("Retrieving affiliated articles from OpenAlex\n")
openalex = retrieve_all_data_openalex(url_openalex, params_openalex)
df_openalex = pd.json_normalize(openalex)
df_openalex['DOI'] = df_openalex['doi'].str.replace('https://doi.org/', '')
df_openalex_pruned = df_openalex[['DOI', 'title', 'primary_location.source.display_name']]

#merge PLOS articles with SI into university-affiliated OpenAlex
openalex_plos_SI = pd.merge(df_openalex_pruned, df_plos_SI, on='DOI', how="left")
#create 'Matched' column based on whether OpenAlex field is empty
openalex_plos_SI['Matched'] = np.where(openalex_plos_SI['Publication_Day'].isnull(), 'Not matched', 'Matched')
##constructs hypothetical S1 file DOI link that could be queried further if desired (code not included here)
openalex_plos_SI_matched = openalex_plos_SI[openalex_plos_SI['Matched'] == "Matched"]
openalex_plos_SI_matched['hypothetical_deposit'] = openalex_plos_SI_matched['DOI'] + '.s001'
openalex_plos_SI_matched.to_csv(f'accessory-outputs/{todayDate}_PLOS-articles-with-data-in-SI.csv', index=False, sep=",")
print(f'Number of relevant PLOS articles with data location listed as Supp Info: {len(openalex_plos_SI_matched)}\n')

#merge PLOS articles with NCBI data into university-affiliated OpenAlex
openalex_plos_NCBI = pd.merge(df_openalex_pruned, df_NCBI, on='DOI', how="left")
#create 'Matched' column based on whether OpenAlex field is empty
openalex_plos_NCBI['Matched'] = np.where(openalex_plos_NCBI['Publication_Day'].isnull(), 'Not matched', 'Matched')
##constructs hypothetical S1 file DOI link that could be queried further if desired (code not included here)
openalex_plos_NCBI_matched = openalex_plos_NCBI[openalex_plos_NCBI['Matched'] == "Matched"]
openalex_plos_NCBI_matched.to_csv(f'accessory-outputs/{todayDate}_PLOS-articles-with-data-in-NCBI.csv', index=False, sep=",")
print(f'Number of relevant PLOS articles with data location listed as NCBI: {len(openalex_plos_NCBI_matched)}\n')

print(f"Time to run: {datetime.now() - startTime}")
print("Done.")