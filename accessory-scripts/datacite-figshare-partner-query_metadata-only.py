from datetime import datetime
import pandas as pd
import json
import os
import sys

#call functions from parent utils.py file
utils_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, utils_dir) 
from utils import retrieve_datacite_summary 

#read in config file
parent = os.path.abspath(os.path.join(os.getcwd(), '..'))
with open(f'{parent}/config.json', 'r') as file:
    config = json.load(file)

#setting timestamp to calculate run time
start_time = datetime.now() 
#creating variable with current date for appending to filenames
today_date = datetime.now().strftime("%Y%m%d") 

#toggle for deposits affiliated with specific university (True) or all deposits associated with publisher (False)
##reminder that any affiliation-based query will not be comprehensive for Figshare due to poor metadata
affiliated = config['INSTITUTION']['affiliated']

#read in config file
parent = os.path.abspath(os.path.join(os.getcwd(), '..'))
with open(f'{parent}/config.json', 'r') as file:
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
if os.path.isdir('accessory-outputs'):
    print('accessory-outputs directory found - no need to recreate')
else:
    os.mkdir('accessory-outputs')
    print('accessory-outputs directory has been created')

#API endpoint
url_datacite = 'https://api.datacite.org/dois'

#page limit only needs to be 1 to get 'meta' field
page_limit_datacite = 1

#define variables to be called recursively in function
page_start_datacite = config['VARIABLES']['PAGE_STARTS']['datacite']

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
    
    resource_types_data, licenses_data = retrieve_datacite_summary(url_datacite, params_datacite, publisher, affiliated, institution=None)
    
    all_resource_types_data.extend(resource_types_data)
    all_licenses_data.extend(licenses_data)

#summary of resource types by 'publisher'
df_all_resource_types = pd.DataFrame(all_resource_types_data)
if affiliated:
    df_all_resource_types.to_csv(f'accessory-outputs/{today_date}_{institution_filename}_figshare-partners_datacite-summary_resourceType_per-publisher.csv', index=False)
else:
    df_all_resource_types.to_csv(f'accessory-outputs/{today_date}_all-deposits_figshare-partners_datacite-summary_resourceType_per-publisher.csv', index=False)

#summary of licenses by 'publisher'
df_licenses_data = pd.DataFrame(all_licenses_data)
if affiliated:
    df_licenses_data.to_csv(f'accessory-outputs/{today_date}_{institution_filename}_figshare-partners_datacite-summary_license_per-publisher.csv', index=False)
else:
    df_licenses_data.to_csv(f'accessory-outputs/{today_date}_all-deposits_figshare-partners_datacite-summary_license_per-publisher.csv', index=False)

#summarize counts across publishers
##summarize 'count' by 'title' for each publisher
summary_resource_types = df_all_resource_types.groupby('title')['count'].sum().reset_index()
summary_licenses = df_licenses_data.groupby('title')['count'].sum().reset_index()

##display summaries
print('Summary of Resource Types across all publisher partners:')
print(summary_resource_types)
if affiliated:
    summary_resource_types.to_csv(f'accessory-outputs/{today_date}_{institution_filename}_figshare-partners_datacite-summary_resourceType_all-publishers.csv', index=False)
else:
    summary_resource_types.to_csv(f'accessory-outputs/{today_date}_all-deposits_figshare-partners_datacite-summary_resourceType_all-publishers.csv', index=False)
print('\nSummary of Licenses across all publisher partners:')
print(summary_licenses)
if affiliated:
    summary_licenses.to_csv(f'accessory-outputs/{today_date}_{institution_filename}_figshare-partners_datacite-summary_license_all-publishers.csv', index=False)
else:
    summary_licenses.to_csv(f'accessory-outputs/{today_date}_all-deposits_figshare-partners_datacite-summary_license_all-publishers.csv', index=False)

print('Done.\n')
print(f'Time to run: {datetime.now() - start_time}')