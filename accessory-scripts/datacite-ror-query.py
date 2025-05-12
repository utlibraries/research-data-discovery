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
#setting timestamp to calculate run time
startTime = datetime.now() 
#creating variable with current date for appending to filenames
todayDate = datetime.now().strftime("%Y%m%d") 

#read in config file
with open('config.json', 'r') as file:
    config = json.load(file)

#creating directories
if test:
    if os.path.isdir("test"):
        print("test directory found - no need to recreate")
    else:
        os.mkdir("test")
        print("test directory has been created")
    os.chdir('test')
    if os.path.isdir("accessory-outputs"):
        print("test accessory outputs directory found - no need to recreate")
    else:
        os.mkdir("accessory-outputs")
        print("test accessory outputs directory has been created")
else:
    if os.path.isdir("accessory-outputs"):
        print("accessory outputs directory found - no need to recreate")
    else:
        os.mkdir("accessory-outputs")
        print("accessory outputs directory has been created")

#API endpoints
url_datacite = "https://api.datacite.org/dois"

#load in ROR link
ror = config['INSTITUTION']['ror']

params_datacite = {
    'affiliation': 'true',
    # 'query': f'(creators.affiliation.affiliationIdentifier:"{ror}" OR creators.name:"{ror}" OR contributors.affiliation.affiliationIdentifier:"{ror}" OR contributors.name:"{ror}") AND types.resourceTypeGeneral:"Dataset"',
    'query': f'(creators.affiliation.affiliationIdentifier:"{ror}") AND types.resourceTypeGeneral:"Dataset"',
    'page[size]': config['VARIABLES']['PAGE_SIZES']['datacite'],
    'page[cursor]': 1,
}

#define different number of pages to retrieve from DataCite API based on 'test' vs. 'prod' env
page_limit_datacite = config['VARIABLES']['PAGE_LIMITS']['datacite_test'] if test else config['VARIABLES']['PAGE_LIMITS']['datacite_prod']
#define variables to be called recursively in function
page_start_datacite = config['VARIABLES']['PAGE_STARTS']['datacite']

#define global functions
##retrieves single page of results
def retrieve_page_datacite(url, params=None):
    """Fetch a single page of results with cursor-based pagination."""
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

    current_url = data.get('links', {}).get('next', None)
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

print("Starting DataCite retrieval based on affiliation.\n")
data_datacite = retrieve_all_data_datacite(url_datacite, params_datacite)
print(f"Number of ROR-affiliated datasets found by DataCite API: {len(data_datacite)}\n")
data_select_datacite = [] 
for item in data_datacite:
    attributes = item.get('attributes', {})
    doi = attributes.get('doi', None)
    publisher = attributes.get('publisher', "")
    # publisher_year = attributes.get('publicationYear', '') #temporarily disabling due to Dryad metadata issue
    registered = attributes.get('registered', '')
    if registered:
        publisher_year = datetime.fromisoformat(registered[:-1]).year
        publisher_date = datetime.fromisoformat(registered[:-1]).date()
    else:
        publisher_year = None
        publisher_date = None
    title=attributes.get('titles', [{}])[0].get('title',"")
    creators = attributes.get('creators', [{}])
    creatorsNames = [creator.get('name', '') for creator in creators]
    creatorsAffiliations = [', '.join([aff['name'] for aff in creator.get('affiliation', [])]) for creator in creators]        
    first_creator = creators[0].get('name', None)
    last_creator = creators[-1].get('name', None)
    affiliations = [affiliation.get('name', "") for creator in creators for affiliation in creator.get('affiliation', [{}])]
    first_affiliation = affiliations[0] if affiliations else None
    last_affiliation = affiliations[-1] if affiliations else None
    contributors = attributes.get('contributors', [{}])
    contributorsNames = [contributor.get('name', '') for contributor in contributors]
    contributorsAffiliations = [', '.join([aff['name'] for aff in contributor.get('affiliation', [])]) for contributor in contributors]        
    container = attributes.get('container', {})
    container_identifier = container.get('identifier', None)
    related_identifiers = attributes.get('relatedIdentifiers', [])
    for identifier in related_identifiers:
        relationType = identifier.get('relationType', '')
        relatedIdentifier = identifier.get('relatedIdentifier', '')
    types = attributes.get('types', {})
    resourceType = types.get('resourceTypeGeneral', '')
    data_select_datacite.append({
        "doi": doi,
        "publisher": publisher,
        "publicationYear": publisher_year,
        "title": title,
        'first_author': first_creator,
        'last_author': last_creator,
        'first_affiliation': first_affiliation,
        'last_affiliation': last_affiliation,
        'creatorsNames': creatorsNames,
        'creatorsAffiliations': creatorsAffiliations,
        'contributorsNames': contributorsNames,
        'contributorsAffiliations': contributorsAffiliations,
        "relationType": relationType,
        "relatedIdentifier": relatedIdentifier,
        "containerIdentifier": container_identifier,
        "type": resourceType
    })

df_datacite_initial = pd.json_normalize(data_select_datacite)
df_datacite_initial.to_csv(f"accessory-outputs/{todayDate}_datacite-ror-retrieval.csv")


### The below code is mostly duplicated from the main codebase but may not be used b/c it is unlikely that all of these repositories will be retrieved via a ROR-based query ###

##handling duplication of Figshare deposits (parent vs. child with '.v*')
###creating separate figshare dataframe for downstream processing, not necessary for other repositories with this DOI mechanism in current workflow
figshare = df_datacite_initial[df_datacite_initial['doi'].str.contains('figshare')]
df_datacite_no_figshare = df_datacite_initial[~df_datacite_initial['doi'].str.contains('figshare')]
figshare_no_versions = figshare[~figshare['doi'].str.contains(r'\.v\d+$')]
#mediated workflow sometimes creates individual deposit for each file, want to treat as single dataset here
figshare_deduplicated = figshare_no_versions.drop_duplicates(subset='relatedIdentifier', keep="first")
df_datacite_v1 = pd.concat([df_datacite_no_figshare, figshare_deduplicated], ignore_index=True)

##handling duplication of other repositories with multiple DOIs for same object (parent vs. child)
lineageRepos = df_datacite_v1[df_datacite_v1['publisher'].str.contains('ICPSR|Mendeley|SAGE|Zenodo')]
df_datacite_lineageRepos = df_datacite_v1[~df_datacite_v1['publisher'].str.contains('ICPSR|Mendeley|SAGE|Zenodo')]
lineageRepos_deduplicated = lineageRepos[~lineageRepos['relationType'].str.contains('IsVersionOf|IsNewVersionOf', case=False, na=False)]
###the use of .v* and v* as filters works for these repositories but could accidentally remove non-duplicate DOIs if applied to other repositories
lineageRepos_deduplicated = lineageRepos_deduplicated[~lineageRepos_deduplicated['doi'].str.contains(r'\.v\d+$')]
dois_to_remove = lineageRepos_deduplicated[(lineageRepos_deduplicated['doi'].str.contains(r'v\d$') | lineageRepos_deduplicated['doi'].str.contains(r'v\d-')) & (lineageRepos_deduplicated['publisher'].str.contains('ICPSR', case=False, na=False))]['doi']
###remove the identified DOIs
lineageRepos_deduplicated = lineageRepos_deduplicated[~lineageRepos_deduplicated['doi'].isin(dois_to_remove)]
df_datacite_v2 = pd.concat([df_datacite_lineageRepos, lineageRepos_deduplicated], ignore_index=True)

#standardizing specific repository name that has three permutations; may not be relevant for other institutions
df_datacite_v2.loc[df_datacite_v2['publisher'].str.contains('Digital Rocks', case=False), 'publisher'] = 'Digital Rocks Portal'
df_datacite_v2.to_csv(f"accessory-outputs/{todayDate}_datacite-ror-retrieval-filtered.csv")
print(f"Number of ROR-affiliated datasets left after cleaning: {len(df_datacite_v2)}\n")

print("Done.\n")
print(f"Time to run: {datetime.now() - startTime}")