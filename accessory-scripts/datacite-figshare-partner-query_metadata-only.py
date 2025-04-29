from datetime import datetime
import pandas as pd
import json
import os
import requests

#setting timestamp to calculate run time
startTime = datetime.now() 
#creating variable with current date for appending to filenames
todayDate = datetime.now().strftime("%Y%m%d") 

#toggle for deposits affiliated with specific university (True) or all deposits associated with publisher (False)
##reminder that any affiliation-based query will not be comprehensive for Figshare due to poor metadata
affiliated = False

#read in config file
with open('config.json', 'r') as file:
    config = json.load(file)

#all DataCite Figshare partners
figshare_partners_keys = list(config['FIGSHARE_PARTNERS'].keys())

if affiliated:
    #create permutation string with OR for API parameters
    ut_variations = config['PERMUTATIONS']
    institution_query = ' OR '.join([f'"{variation}"' for variation in ut_variations])
    #pulls in string to append in filenames
    institution_filename = config['INSTITUTION']['filename']
    #pulls in string for print statements during retrieval
    institution = config['INSTITUTION']['name']

#creating directories
if os.path.isdir('outputs'):
    print('outputs directory found - no need to recreate')
else:
    os.mkdir('outputs')
    print('outputs directory has been created')

#API endpoint
url_datacite = 'https://api.datacite.org/dois'

#page limit only needs to be 1 to get 'meta' field
page_limit_datacite = 1

#define variables to be called recursively in function
page_start_datacite = config['VARIABLES']['PAGE_STARTS']['datacite']

#define global functions
##retrieves single page of results
def retrieve_page_datacite(url, params=None):
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()  
        return response.json()
    except requests.RequestException as e:
        print(f'Error retrieving page: {e}')
        return {'data': [], 'meta': {}, 'links': {}}
##recursively retrieves pages
def retrieve_all_resource_types_datacite(url, params, publisher):
    global page_start_datacite
    all_resource_types = []
    all_licenses = []

    data = retrieve_page_datacite(url, params)
    if affiliated:
        print(f'Retrieving data for {publisher} for all deposits ({institution} only)')
    else:
        print(f'Retrieving data for {publisher} for all deposits')

    if not data['meta']:
        print('No metadata found.')
        return all_resource_types, all_licenses

    #other summaries can be added as necessary
    resource_types = data['meta'].get('resourceTypes', [])
    licenses = data['meta'].get('licenses', [])
    
    for resource in resource_types:
        resource['publisher'] = publisher
    for license in licenses:
        license['publisher'] = publisher

    all_resource_types.extend(resource_types)
    all_licenses.extend(licenses)

    return all_resource_types, all_licenses

print(f'Starting DataCite retrieval.\n')

#looped retrieval
all_resource_types_data = []
all_licenses_data = []

for publisher in figshare_partners_keys:
    if affiliated:
        params_datacite = {
            'affiliation': 'true',
            'query': f'publisher:"{publisher}" AND (creators.affiliation.name:({institution_query}) OR creators.name:({institution_query}) OR contributors.affiliation.name:({institution_query}) OR contributors.name:({institution_query}))',
            'page[size]': config['VARIABLES']['PAGE_SIZES']['datacite'], 
            'page[cursor]': 1,
        }
    else:
        params_datacite = {
            'affiliation': 'true',
            'query': f'publisher:"{publisher}"',
            'page[size]': config['VARIABLES']['PAGE_SIZES']['datacite'], 
            'page[cursor]': 1,
        }
    
    #retrieve resource types and licenses for the current publisher
    resource_types_data, licenses_data = retrieve_all_resource_types_datacite(url_datacite, params_datacite, publisher)
    
    all_resource_types_data.extend(resource_types_data)
    all_licenses_data.extend(licenses_data)

#summary of resource types by 'publisher'
df_all_resource_types = pd.DataFrame(all_resource_types_data)
if affiliated:
    df_all_resource_types.to_csv(f'outputs/{todayDate}_{institution_filename}_figshare-partners_datacite-summary_resourceType_per-publisher.csv', index=False)
else:
    df_all_resource_types.to_csv(f'outputs/{todayDate}_all-deposits_figshare-partners_datacite-summary_resourceType_per-publisher.csv', index=False)

#summary of licenses by 'publisher'
df_licenses_data = pd.DataFrame(all_licenses_data)
if affiliated:
    df_licenses_data.to_csv(f'outputs/{todayDate}_{institution_filename}_figshare-partners_datacite-summary_license_per-publisher.csv', index=False)
else:
    df_licenses_data.to_csv(f'outputs/{todayDate}_all-deposits_figshare-partners_datacite-summary_license_per-publisher.csv', index=False)

#summarize counts across publishers
##summarize 'count' by 'title' for each publisher
summary_resource_types = df_all_resource_types.groupby('title')['count'].sum().reset_index()
summary_licenses = df_licenses_data.groupby('title')['count'].sum().reset_index()

##display summaries
print('Summary of Resource Types across all publisher partners:')
print(summary_resource_types)
if affiliated:
    summary_resource_types.to_csv(f'outputs/{todayDate}_{institution_filename}_figshare-partners_datacite-summary_resourceType_all-publishers.csv', index=False)
else:
    summary_resource_types.to_csv(f'outputs/{todayDate}_all-deposits_figshare-partners_datacite-summary_resourceType_all-publishers.csv', index=False)
print('\nSummary of Licenses across all publisher partners:')
print(summary_licenses)
if affiliated:
    summary_licenses.to_csv(f'outputs/{todayDate}_{institution_filename}_figshare-partners_datacite-summary_license_all-publishers.csv', index=False)
else:
    summary_licenses.to_csv(f'outputs/{todayDate}_all-deposits_figshare-partners_datacite-summary_license_all-publishers.csv', index=False)

print('Done.\n')
print(f'Time to run: {datetime.now() - startTime}')