from datetime import datetime
from urllib.parse import urlparse, parse_qs, quote
import pandas as pd
import json
import math
import numpy as np
import os
import requests

#operator for quick test runs
test = False
#operator for resource type(s) to query for (use '|' for Boolean OR)
resourceType = 'Dataset'
#toggle for cross-validation steps
crossValidate = True
##if you have done a previous DataCite retrieval and don't want to re-run the entire main process
loadPreviousData = False
##toggle for university specifically vs. all Dryad
austin = False

#setting timestamp to calculate run time
startTime = datetime.now() 
#creating variable with current date for appending to filenames
todayDate = datetime.now().strftime('%Y%m%d') 

#read in config file
with open('config.json', 'r') as file:
    config = json.load(file)

#creating directories
if test:
    if os.path.isdir('test'):
        print('test directory found - no need to recreate')
    else:
        os.mkdir('test')
        print('test directory has been created')
    os.chdir('test')
    if os.path.isdir('accessory-outputs'):
        print('test accessory-outputs directory found - no need to recreate')
    else:
        os.mkdir('accessory-outputs')
        print('test accessory-outputs directory has been created')
else:
    if os.path.isdir('accessory-outputs'):
        print('accessory-outputs directory found - no need to recreate')
    else:
        os.mkdir('accessory-outputs')
        print('accessory-outputs directory has been created')

#API endpoints
if austin:
    url_dryad = 'https://datadryad.org/api/v2/search?affiliation=https://ror.org/00hj54h04' #Dryad requires ROR for affiliation search
else:   
    url_dryad = 'https://datadryad.org/api/v2/search' #Dryad requires ROR for affiliation search

url_datacite = 'https://api.datacite.org/dois'

#create permutation string with OR for API parameters
ut_variations = config['PERMUTATIONS']
institution_query = ' OR '.join([f'"{variation}"' for variation in ut_variations])

#pulling in 'uniqueIdentifer' term used as quick, reliable filter ('Austin' for filtering an affiliation field for UT Austin)
uni_identifier = config['INSTITUTION']['uniqueIdentifier']

params_dryad= {
    'per_page': config['VARIABLES']['PAGE_SIZES']['dryad'],
}
if austin:
    params_datacite = {
        'affiliation': 'true',
        'query': f'(creators.affiliation.name:({institution_query}) OR creators.name:({institution_query}) OR contributors.affiliation.name:({institution_query}) OR contributors.name:({institution_query})) AND types.resourceTypeGeneral:"{resourceType}" AND publisher:"Dryad"',
        'page[size]': config['VARIABLES']['PAGE_SIZES']['datacite'],
        'page[cursor]': 1,
    }
else:
    params_datacite = {
        'affiliation': 'true',
        'query': f'types.resourceTypeGeneral:"{resourceType}" AND publisher:"Dryad"',
        'page[size]': config['VARIABLES']['PAGE_SIZES']['datacite'],
        'page[cursor]': 1,
    }

#define different number of pages to retrieve from DataCite API based on 'test' vs. 'prod' env
page_limit_datacite = config['VARIABLES']['PAGE_LIMITS']['datacite_test'] if test else config['VARIABLES']['PAGE_LIMITS']['datacite_prod']

#define variables to be called recursively in function
page_start_datacite = config['VARIABLES']['PAGE_STARTS']['datacite']

#define global functions
##retrieves single page of results
def retrieve_page_dryad(url, params):
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()  
        return response.json()
    except requests.RequestException as e:
        print(f"Error retrieving page: {e}")
        return {'_embedded': {'stash:datasets': []}, 'total': {}}
##recursively retrieves pages
def retrieve_all_data_dryad(url, params):
    page_start_dryad = config['VARIABLES']['PAGE_STARTS']['dryad']
    all_data_dryad = []
    data = retrieve_page_dryad(url, params)
    total_count = data.get('total', None)
    total_pages = math.ceil(total_count/params_dryad['per_page'])

    print(f"Total: {total_count} entries over {total_pages} pages\n")

    while True:
        params.update({"page": page_start_dryad})  
        print(f"Retrieving page {page_start_dryad} of {total_pages} from Dryad...\n")  

        data = retrieve_page_dryad(url, params)
        
        if not data['_embedded']:
            print("No data found.")
            return all_data_dryad
        
        all_data_dryad.extend(data['_embedded']['stash:datasets'])
        
        page_start_dryad += 1

        if not data['_embedded']['stash:datasets']:
            print("End of Dryad response.\n")          
            break
    
    return all_data_dryad

##retrieves single page of results
def retrieve_page_datacite(url, params=None):
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()  
        return response.json()
    except requests.RequestException as e:
        print(f"Error retrieving page: {e}")
        return {'data': [], 'links': {}}
##recursively retrieves pages
def retrieve_all_data_datacite(url, params):
    global page_start_datacite
    all_data_datacite = []
    data = retrieve_page_datacite(url, params)
    
    if not data['data']:
        print("No data found.")
        return all_data_datacite

    all_data_datacite.extend(data['data'])

    total_count = data.get('meta', {}).get('total', None)
    total_pages = math.ceil(total_count/config['VARIABLES']['PAGE_SIZES']['datacite'])

    current_url = data.get('links', {}).get('next', None)
    
    while current_url and page_start_datacite < page_limit_datacite:
        page_start_datacite+=1
        print(f"Retrieving page {page_start_datacite} of {total_pages} from DataCite...\n")
        data = retrieve_page_datacite(current_url)
        
        if not data['data']:
            print("End of response.")
            break
        
        all_data_datacite.extend(data['data'])
        
        current_url = data.get('links', {}).get('next', None)
    
    return all_data_datacite

if not loadPreviousData:
    print('Starting DataCite retrieval of all Dryad datasets.\n')
    data_datacite = retrieve_all_data_datacite(url_datacite, params_datacite)
    print(f'Number of datasets found by DataCite API: {len(data_datacite)}\n')

    if crossValidate:
        print('Starting Dryad retrieval of all Dryad datasets.\n')
        data_dryad = retrieve_all_data_dryad(url_dryad, params_dryad)
        print(f'Number of Dryad datasets found by Dryad API: {len(data_dryad)}\n')

    print('Beginning dataframe generation.\n')

    data_select_datacite = [] 
    for item in data_datacite:
        attributes = item.get('attributes', {})
        doi = attributes.get('doi', None)
        state = attributes.get('state', None)
        publisher = attributes.get('publisher', '')
        publicationYear = attributes.get('publicationYear', '') #temporarily disabling due to Dryad metadata issue
        created = attributes.get('created', '')
        published = attributes.get('published', '')
        updated = attributes.get('updated', '')
        registered = attributes.get('registered', '')
        if registered:
            registeredYear = datetime.fromisoformat(registered[:-1]).year
            registeredDate = datetime.fromisoformat(registered[:-1]).date()
        else:
            registeredYear = None
            registeredDate = None
        if created:
            createdYear = datetime.fromisoformat(created[:-1]).year
            createdDate = datetime.fromisoformat(created[:-1]).date()
        else:
            createdYear = None
            createdDate = None
        if updated:
            updatedYear = datetime.fromisoformat(updated[:-1]).year
            updatedDate = datetime.fromisoformat(updated[:-1]).date()
        else:
            updatedYear = None
            updatedDate = None
        if published:
            publishedYear = datetime.fromisoformat(published[:-1]).year
            publishedDate = datetime.fromisoformat(published[:-1]).date()
        else:
            publishedYear = None
            publishedDate = None
        dates = attributes.get('dates', [])
        date_dict = {item['dateType']: item['date'] for item in dates if 'dateType' in item and 'date' in item}
        issued = date_dict.get('Issued', None)
        available = date_dict.get('Available', None)
        if issued:
            issuedYear = issued[:4] #deals with inconsistent timestamp
            issuedDate = issued[:10] #deals with inconsistent timestamp
        else:
            issuedYear = None
            issuedDate = None
        if available:
            availableYear = available[:4] #deals with inconsistent timestamp
            availableDate = available[:10] #deals with inconsistent timestamp
        else:
            availableYear = None
            availableDate = None
        title=attributes.get('titles', [{}])[0].get('title','')
        creators = attributes.get('creators', [{}])
        creatorsNames = [creator.get('name', '') for creator in creators]
        creatorsAffiliations = ['; '.join([aff['name'] for aff in creator.get('affiliation', [])]) for creator in creators]        
        first_creator = creators[0].get('name', None)
        last_creator = creators[-1].get('name', None)
        affiliations = [affiliation.get('name' '') for creator in creators for affiliation in creator.get('affiliation', [{}])]
        first_affiliation = affiliations[0] if affiliations else None
        last_affiliation = affiliations[-1] if affiliations else None
        views = attributes.get('viewCount', 0)
        downloads = attributes.get('downloadCount', 0)
        citations = attributes.get('citationCount', 0)
        data_select_datacite.append({
            'doi': doi,
            'state': state,
            'publisher': publisher,
            'publicationYear': publicationYear,
            'registeredDate': registeredDate,
            'createdDate': createdDate,
            'updatedDate': updatedDate,
            'publishedDate': publishedDate,
            'issuedDate': issuedDate,
            'availableDate': availableDate,
            'registeredYear': registeredYear,
            'createdYear': createdYear,
            'updatedYear': updatedYear,
            'publishedYear': publishedYear,
            'issuedYear': issuedYear,
            'availableYear': availableYear,
            'title': title,
            'first_author': first_creator,
            'last_author': last_creator,
            'first_affiliation': first_affiliation,
            'last_affiliation': last_affiliation,
            'creatorsNames': creatorsNames,
            'creatorsAffiliations': creatorsAffiliations,
            'views': views,
            'downloads': downloads,
            'citations': citations,
            'source': 'DataCite'
        })

    df_datacite_dryad = pd.json_normalize(data_select_datacite)
    print(f'Number of Dryad datasets found by DataCite API: {len(df_datacite_dryad)}\n')
    if austin:
            df_datacite_dryad.to_csv(f'accessory-outputs/{todayDate}_DataCite-Dryad-output_ut-austin.csv', index=False)
    else:
        df_datacite_dryad.to_csv(f'accessory-outputs/{todayDate}_DataCite-Dryad-output.csv', index=False)

    if crossValidate:
        print('Dryad step\n')
        data_select_dryad = [] 
        for item in data_dryad:
            links = item.get('_links', {})
            doi = item.get('identifier', None)
            publicationDate = item.get('publicationDate', '')
            lastModificationDate = item.get('lastModificationDate', '')
            title=item.get('title', [{}])
            data_select_dryad.append({
                'doi': doi,
                'publicationDate': publicationDate,
                'lastModificationDate': lastModificationDate,
                'title': title,
                'source': 'Dryad API'
            })
        df_dryad = pd.json_normalize(data_select_dryad)
        if austin:
            df_dryad.to_csv(f'accessory-outputs/{todayDate}_DryadAPI-Dryad-output_ut-austin.csv', index=False)
        else:
            df_dryad.to_csv(f'accessory-outputs/{todayDate}_DryadAPI-Dryad-output.csv', index=False)

    print('Beginning dataframe editing.\n')

    if crossValidate:
        print('Repository-specific processing\n')
        df_dryad['publicationYear'] = pd.to_datetime(df_dryad['publicationDate'], format='mixed', errors='coerce').dt.year
        #editing DOI columns to ensure exact matches
        df_dryad['doi'] = df_dryad['doi'].str.replace('doi:', '')
        df_dryad['doi'] = df_dryad['doi'].str.lower()

        print('Beginning cross-validation process.\n')

        #Dryad into DataCite
        df_datacite_dryad_joint = pd.merge(df_datacite_dryad, df_dryad, on='doi', how='left')
        df_datacite_dryad_joint['Match_entry'] = np.where(df_datacite_dryad_joint['source_y'].isnull(), 'Not matched', 'Matched')
        if austin:
            df_datacite_dryad_joint.to_csv(f'accessory-outputs/{todayDate}_Dryad-into-DataCite_joint-all-dataframe_ut-austin.csv', index=False)
        else:
            df_datacite_dryad_joint.to_csv(f'accessory-outputs/{todayDate}_Dryad-into-DataCite_joint-all-dataframe.csv', index=False)
        # print('Counts of matches for Dryad into DataCite')
        # counts_datacite_dryad = df_datacite_dryad_joint['Match_entry'].value_counts()
        # print(counts_datacite_dryad, '\n')
        # df_datacite_dryad_joint_unmatched = df_datacite_dryad_joint[df_datacite_dryad_joint['Match_entry'] == 'Not matched']
        # if austin:
        #     df_datacite_dryad_joint_unmatched.to_csv(f'accessory-outputs/{todayDate}_Dryad-into-DataCite_joint-unmatched-dataframe_ut-austin.csv', index=False)
        # else:
        #     df_datacite_dryad_joint_unmatched.to_csv(f'accessory-outputs/{todayDate}_Dryad-into-DataCite_joint-unmatched-dataframe.csv', index=False)

print('Done.\n')
print(f'Time to run: {datetime.now() - startTime}')