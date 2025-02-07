from datetime import datetime
from urllib.parse import urlparse, parse_qs
import pandas as pd
import json
import math
import numpy as np
import os
import requests

#operator for quick test runs
test = False
#toggle for cross-validation steps
crossValidate = False

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
    if os.path.isdir("outputs"):
        print("test outputs directory found - no need to recreate")
    else:
        os.mkdir("outputs")
        print("test outputs directory has been created")
else:
    if os.path.isdir("outputs"):
        print("outputs directory found - no need to recreate")
    else:
        os.mkdir("outputs")
        print("outputs directory has been created")

#API endpoints
url_dryad = "https://datadryad.org/api/v2/search?affiliation=https://ror.org/00hj54h04"
url_datacite = "https://api.datacite.org/dois"
url_dataverse = "https://dataverse.tdl.org/api/search/"
url_zenodo = "https://zenodo.org/api/records"

#create permutation string with OR for API parameters
ut_variations = config['PERMUTATIONS']
institution_query = ' OR '.join([f'"{variation}"' for variation in ut_variations])

params_dryad= {
    'per_page': config['VARIABLES']['PAGE_SIZES']['dryad'],
}

params_datacite = {
    'affiliation': 'true',
    'query': f'(creators.affiliation.name:({institution_query}) OR creators.name:({institution_query}) OR contributors.affiliation.name:({institution_query}) OR contributors.name:({institution_query})) AND types.resourceTypeGeneral:"Dataset"',
    'page[size]': config['VARIABLES']['PAGE_SIZES']['datacite'],
    'page[cursor]': 1,
}

headers_dataverse = {
    'X-Dataverse-key': config['KEYS']['dataverseToken']
}
params_dataverse = {
    'q': '10.18738/T8/',
    'subtree': 'utexas',
    'type': 'dataset',
    'start': config['VARIABLES']['PAGE_STARTS']['dataverse'],
    'page': config['VARIABLES']['PAGE_INCREMENTS']['dataverse'],
    'per_page': config['VARIABLES']['PAGE_SIZES']['dataverse']
}

params_zenodo_data = {
    'q': f'creators.affiliation:({institution_query})',
    'size': config['VARIABLES']['PAGE_SIZES']['zenodo'],
    'type': 'dataset',
    'access_token': config['KEYS']['zenodoToken']
}

#define different number of pages to retrieve from DataCite API based on 'test' vs. 'prod' env
page_limit_datacite = config['VARIABLES']['PAGE_LIMITS']['datacite_test'] if test else config['VARIABLES']['PAGE_LIMITS']['datacite_prod']
page_limit_zenodo = config['VARIABLES']['PAGE_LIMITS']['zenodo_test'] if test else config['VARIABLES']['PAGE_LIMITS']['zenodo_prod']

#define variables to be called recursively in function
page_start_datacite = config['VARIABLES']['PAGE_STARTS']['datacite']
page_start_zenodo = config['VARIABLES']['PAGE_STARTS']['zenodo']
page_size_zenodo = config['VARIABLES']['PAGE_SIZES']['zenodo']

# define global functions
## retrieves single page of results
def retrieve_page_dryad(url, params):
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()  
        return response.json()
    except requests.RequestException as e:
        print(f"Error fetching page: {e}")
        return {'_embedded': {'stash:datasets': []}, 'total': {}}
## recursively retrieves pages
def retrieve_all_data_dryad(url, params):
    page_start_dryad = config['VARIABLES']['PAGE_STARTS']['dryad']
    all_data_dryad = []
    data = retrieve_page_dryad(url, params)
    total_count = data.get('total', None)
    total_pages = math.ceil(total_count/params_dryad['per_page'])

    print(f"Total: {total_count} entries over {total_pages} pages")
    print()

    while True:
        params.update({"page": page_start_dryad})  
        print(f"Retrieving page {page_start_dryad} of {total_pages} from Dryad...")  
        print()

        data = retrieve_page_dryad(url, params)
        
        if not data['_embedded']:
            print("No data found.")
            return all_data_dryad
        
        all_data_dryad.extend(data['_embedded']['stash:datasets'])
        
        page_start_dryad += 1

        if not data['_embedded']['stash:datasets']:
            print("End of Dryad response.")
            print()            
            break
    
    return all_data_dryad

## retrieves single page of results
def retrieve_page_datacite(url, params=None):
    """Fetch a single page of results with cursor-based pagination."""
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()  
        return response.json()
    except requests.RequestException as e:
        print(f"Error fetching page: {e}")
        return {'data': [], 'links': {}}
## recursively retrieves pages
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
        print(f"Retrieving page {page_start_datacite} of {total_pages} from DataCite...")
        print()
        data = retrieve_page_datacite(current_url)
        
        if not data['data']:
            print("End of response.")
            break
        
        all_data_datacite.extend(data['data'])
        
        current_url = data.get('links', {}).get('next', None)
    
    return all_data_datacite

## retrieves single page of results
def retrieve_page_dataverse(url, params=None, headers=None):
    """Fetch a single page of results."""
    try:
        response = requests.get(url, params=params, headers=headers)
        response.raise_for_status() 
        return response.json()
    except requests.RequestException as e:
        print(f"Error fetching page: {e}")
        return {'data': {'items': [], 'total_count': 0}}
## recursively retrieves pages
def retrieve_all_data_dataverse(url, params, headers):
    """Fetch all pages of data using pagination."""
    all_data_dataverse = []

    while True: 
        data = retrieve_page_dataverse(url, params, headers)  
        total_count = data['data']['total_count']
        total_pages = math.ceil(total_count/params_dataverse['per_page'])
        print(f"Fetching Page {params_dataverse['page']} of {total_pages} pages...")
        print()

        if not data['data']:
            print("No data found.")
            break
    
        all_data_dataverse.extend(data['data']['items'])
        
        params_dataverse['start'] += params_dataverse['per_page']
        params_dataverse['page'] += 1
        
        if params_dataverse['start'] >= total_count:
            print("End of response.")
            break

    return all_data_dataverse

## retrieves single page of results
def retrieve_page_zenodo(url, params=None):
    """Fetch a single page of results with cursor-based pagination."""
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()  
        return response.json()
    except requests.RequestException as e:
        print(f"Error fetching page: {e}")
        return {'hits': {'hits': [], 'total':{}}, 'links': {}}
# extracting the 'page' parameter value
def extract_page_number(url):
    parsed_url = urlparse(url)
    query_params = parse_qs(parsed_url.query)
    return query_params.get('page', [None])[0]  
## recursively retrieves pages
def retrieve_all_data_zenodo(url, params):
    page_start_zenodo = config['VARIABLES']['PAGE_STARTS']['zenodo']
    all_data_zenodo = []
    data = retrieve_page_zenodo(url, params)
    
    if not data['hits']['hits']:
        print("No data found.")
        return all_data_zenodo

    all_data_zenodo.extend(data['hits']['hits'])
    
    current_url = data.get('links', {}).get('self', None)
    total_count = data.get('hits', {}).get('total',None)
    total_pages = math.ceil(total_count/config['VARIABLES']['PAGE_SIZES']['zenodo'])

    print(f"Total: {total_count} entries over {total_pages} pages")
    print()
    
    while current_url and page_start_zenodo < page_limit_zenodo:
        page_start_zenodo+=1
        page_number = extract_page_number(current_url)
        print(f"Retrieving page {page_start_zenodo} of {total_pages} from Zenodo...")
        print()
        data = retrieve_page_zenodo(current_url)
        
        if not data['hits']['hits']:
            print("End of Zenodo response.")
            print()
            break
        
        all_data_zenodo.extend(data['hits']['hits'])
        
        current_url = data.get('links', {}).get('next', None)
    
    return all_data_zenodo

print("Starting DataCite retrieval based on affiliation.")
print()
data_datacite = retrieve_all_data_datacite(url_datacite, params_datacite)
print(f"Number of datasets found by DataCite API: {len(data_datacite)}")
print()

if crossValidate:
    print("Starting Dryad retrieval.")
    data_dryad = retrieve_all_data_dryad(url_dryad, params_dryad)
    print(f"Number of Dryad datasets found by Dryad API: {len(data_dryad)}")
    print()
    print("Starting Dataverse retrieval.")
    data_dataverse = retrieve_all_data_dataverse(url_dataverse, params_dataverse, headers_dataverse)
    print(f"Number of Dataverse datasets found by Dataverse API: {len(data_dataverse)}")
    print()
    print("Starting Zenodo retrieval.")
    data_zenodo = retrieve_all_data_zenodo(url_zenodo, params_zenodo_data)
    print(f"Number of Zenodo datasets found by Zenodo API: {len(data_zenodo)}")
    print()

print("Beginning dataframe generation.")
print()

data_select_datacite = [] 
for item in data_datacite:
    attributes = item.get('attributes', {})
    doi_dc = attributes.get('doi', None)
    publisher_dc = attributes.get('publisher', "")
    publisher_year_dc = attributes.get('publicationYear', "")
    title_dc=attributes.get('titles', [{}])[0].get('title',"")
    creators_dc = attributes.get('creators', [{}])
    first_creator_dc = creators_dc[0].get('name', None)
    last_creator_dc = creators_dc[-1].get('name', None)
    affiliations_dc = [affiliation.get('name', "") for creator in creators_dc for affiliation in creator.get('affiliation', [{}])]
    first_affiliation_dc = affiliations_dc[0] if affiliations_dc else None
    last_affiliation_dc = affiliations_dc[-1] if affiliations_dc else None
    related_identifier_list_dc = [rel.get('relatedIdentifier', []) for rel in attributes.get('relatedIdentifiers', [])]
    related_identifier_list_dc = related_identifier_list_dc if related_identifier_list_dc else None
    container_dc = attributes.get('container', {})
    container_identifier_dc = container_dc.get('identifier', None)
    data_select_datacite.append({
        'doi': doi_dc,
        'publisher': publisher_dc,
        'publicationYear': publisher_year_dc,
        'title': title_dc,
        'first_author': first_creator_dc,
        'last_author': last_creator_dc,
        'first_affiliation': first_affiliation_dc,
        'last_affiliation': last_affiliation_dc,
        'relatedIdentifier': related_identifier_list_dc,
        'containerIdentifier': container_identifier_dc
    })

df_datacite_initial = pd.json_normalize(data_select_datacite)
df_datacite_dedup = df_datacite_initial[df_datacite_initial['containerIdentifier'].isnull()]
df_datacite = df_datacite_dedup.drop_duplicates(subset='doi', keep="first")

if crossValidate:
    print("Dryad step")
    print()
    data_select_dryad = [] 
    for item in data_dryad:
        links = item.get('_links', {})
        doi_dr = item.get('identifier', None)
        pubDate_dr = item.get('publicationDate', "")
        title_dr=item.get('title', [{}])
        authors_dr = item.get('authors', [{}])
        first_author_first = authors_dr[0].get('firstName', None)
        first_author_last = authors_dr[0].get('lastName', None)
        last_author_first = authors_dr[-1].get('firstName', None)
        last_author_last = authors_dr[-1].get('lastName', None)
        first_affiliation_dr = authors_dr[0].get('affiliation', None)
        last_affiliation_dr = authors_dr[-1].get('affiliation', None)
        related_works_list_dr = [rel.get('identifier', None) for rel in item.get('relatedWorks', [{}])]
        related_works_list_dr = related_works_list_dr if related_works_list_dr else None
        author_last_order_dr = authors_dr[-1].get('order', None)
        data_select_dryad.append({
            'doi': doi_dr,
            'publicationDate': pubDate_dr,
            'title': title_dr,
            'first_author_first': first_author_first,
            'last_author_first': last_author_first,
            'first_author_last': first_author_last,
            'last_author_last': last_author_last,
            'first_affiliation': first_affiliation_dr,
            'last_affiliation': last_affiliation_dr,
            'relatedWorks': related_works_list_dr
        })
    df_dryad = pd.json_normalize(data_select_dryad)

    print("Dataverse step")
    print()
    data_select_dataverse = [] 
    for item in data_dataverse:
        globalID = item.get('global_id', "")
        versionState = item.get('versionState', None),
        pubDate_dataverse = item.get('published_at', "")
        title_dataverse = item.get('name', None)
        authors_dataverse = item.get('authors', [{}])
        contacts_dataverse = item.get('contacts', [{}])
        first_contact_dataverse = contacts_dataverse[0].get('name', None)
        first_affiliation_dataverse = contacts_dataverse[0].get('affiliation', None)
        type = item.get('type', None)
        dataverse = item.get('name_of_dataverse', None)
        data_select_dataverse.append({
            'doi': globalID,
            'status': versionState,
            'publicationDate': pubDate_dataverse,
            'title': title_dataverse,
            'authors': authors_dataverse,
            'contacts': contacts_dataverse,
            'first_contact': first_contact_dataverse,
            'first_contact_affiliation': first_affiliation_dataverse,
            'type': type,
            'dataverse': dataverse
        })
    df_dataverse = pd.json_normalize(data_select_dataverse)

    print("Zenodo step")
    print()
    data_select_zenodo = [] 
    for item in data_zenodo:
        metadata = item.get('metadata', {})
        doi_zen = item.get('doi', None)
        conceptid = item.get('conceptrecid', None)
        pubDate_zen = metadata.get('publication_date', "")
        title_zen=metadata.get('title', "")
        description_zen = metadata.get('description', None)
        creators_zen = metadata.get('creators', [{}])
        first_creator_zen = creators_zen[0].get('name', None)
        last_creator_zen = creators_zen[-1].get('name', None)
        first_affiliation_zen = creators_zen[0].get('affiliation', None)
        last_affiliation_zen = creators_zen[-1].get('affiliation', None)
        related_works_list_zen = [name.get('identifier', None) for name in metadata.get('relatedWorks', [{}])]
        related_works_list_zen = related_works_list_zen if related_works_list_zen else None
        related_works_type_list_zen = [name.get('relation', None) for name in metadata.get('relatedWorks', [{}])]
        related_works_type_list_zen = related_works_type_list_zen if related_works_type_list_zen else None
        data_select_zenodo.append({
            'doi': doi_zen,
            'conceptID': conceptid,
            'publicationDate': pubDate_zen,
            'title': title_zen,
            'description': description_zen,
            'first_author': first_creator_zen,
            'last_author': last_creator_zen,
            'first_affiliation': first_affiliation_zen,
            'last_affiliation': last_affiliation_zen,
            'relatedWorks': related_works_list_zen,
            'relatedWorks_type': related_works_type_list_zen
        })
    df_data_zenodo = pd.json_normalize(data_select_zenodo)

print("Beginning dataframe editing.")
print()

#using str.contains to account for any potential name inconsistency for one repository
df_datacite_dryad = df_datacite[df_datacite["publisher"].str.contains("Dryad")]
df_datacite_dataverse = df_datacite[df_datacite["publisher"].str.contains("Texas Data Repository")]
df_datacite_zenodo = df_datacite[df_datacite["publisher"].str.contains("Zenodo")]
df_remainder = df_datacite[df_datacite["publisher"].str.contains("Dryad|Texas Data Repository|Zenodo") == False]

print(f"Number of Dryad datasets found by DataCite API: {len(df_datacite_dryad)}")
print()
print(f"Number of Dataverse datasets found by DataCite API: {len(df_datacite_dataverse)}")
print()
print(f"Number of Zenodo datasets found by DataCite API: {len(df_datacite_zenodo)}")
print()

if crossValidate:
    print("Repository-specific processing")
    print()
    #subsetting for published datasets
    df_dataverse_pub = df_dataverse[df_dataverse["status"] == "RELEASED"]
    print(f"Number of published Dataverse datasets found by Dataverse API: {len(df_dataverse_pub)}")
    print()

    #removing non-Zenodo deposits indexed by Zenodo (mostly Dryad) from Zenodo output
    ##Zenodo has indexed many Dryad deposits <50 GB in size (does not issue a new DOI but does return a Zenodo 'record' in the API)
    df_data_zenodo_no_dryad = df_data_zenodo[~df_data_zenodo["doi"].str.contains('dryad') == True] 
    #for some reason, Zenodo returns identical entries of most datasets...
    df_data_zenodo_real = df_data_zenodo_no_dryad.drop_duplicates(subset=['publicationDate', 'doi'], keep='first') 
    print(f"Number of non-Dryad Zenodo datasets found by Zenodo API: {len(df_data_zenodo_real)}")
    print()

    #formatting Dataverse author names to be consistent with others
    df_dryad['first_author'] = df_dryad['first_author_last'] + ', ' + df_dryad['first_author_first']
    df_dryad['last_author'] = df_dryad['last_author_last'] + ', ' + df_dryad['last_author_first']
    df_dryad = df_dryad.drop(columns=['first_author_first', 'first_author_last', 
                        'last_author_first', 'last_author_last'])

    df_dryad['publicationYear'] = pd.to_datetime(df_dryad['publicationDate']).dt.year
    df_dataverse_pub['publicationYear'] = pd.to_datetime(df_dataverse_pub['publicationDate'], format='ISO8601').dt.year
    df_data_zenodo_real['publicationYear'] = pd.to_datetime(df_data_zenodo_real['publicationDate'], format='mixed').dt.year

    df_dryad_pruned = df_dryad[['doi', 'publicationYear', 'title', 'first_author', 'first_affiliation', 'last_author', 'last_affiliation']]
    df_dataverse_pruned = df_dataverse_pub[['doi', 'publicationYear', 'title', 'first_contact', 'first_contact_affiliation']]
    df_zenodo_pruned = df_data_zenodo_real[["doi","publicationYear", "title","first_author", "first_affiliation", 'publicationDate', 'description']]

df_datacite_dryad_pruned = df_datacite_dryad[['publisher', 'doi', 'publicationYear', 'title', 'first_author', 'first_affiliation']]
df_datacite_dataverse_pruned = df_datacite_dataverse[['publisher', 'doi', 'publicationYear', 'title', 'first_author', 'first_affiliation']] 
df_datacite_zenodo_pruned = df_datacite_zenodo[['publisher', 'doi', 'publicationYear', 'title', 'first_author', 'first_affiliation']] 
df_datacite_remainder_pruned = df_remainder[['publisher', 'doi', 'publicationYear', 'title', 'first_author', 'first_affiliation']] 

#create new lists for recursive modification
datacite_dataframes_pruned = [df_datacite_dryad_pruned, df_datacite_dataverse_pruned, df_datacite_zenodo_pruned, df_datacite_remainder_pruned]
datacite_dataframes_specific_pruned = [df_datacite_dryad_pruned, df_datacite_dataverse_pruned, df_datacite_zenodo_pruned]

#standardizing how Texas Data Repository is displayed
df_datacite_dataverse_pruned['publisher'] = df_datacite_dataverse_pruned['publisher'].str.replace('Texas Data Repository Dataverse','Texas Data Repository')

for df in datacite_dataframes_specific_pruned:
    df['doi'] = df['doi'].str.lower()

columns_to_rename = {
    "publisher": "repository"
}
for i in range(len(datacite_dataframes_pruned)):
    datacite_dataframes_pruned[i] = datacite_dataframes_pruned[i].rename(columns=columns_to_rename)
    datacite_dataframes_pruned[i]['source'] = 'DataCite'
#assign modified dfs back to original
df_datacite_dryad_pruned, df_datacite_dataverse_pruned, df_datacite_zenodo_pruned, df_datacite_remainder_pruned = datacite_dataframes_pruned

#reload list
datacite_dataframes_specific_pruned = [df_datacite_dryad_pruned, df_datacite_dataverse_pruned, df_datacite_zenodo_pruned]
for i in range(len(datacite_dataframes_specific_pruned)):
    datacite_dataframes_specific_pruned[i] = datacite_dataframes_specific_pruned[i].rename(columns={c: c+'_dc' for c in datacite_dataframes_specific_pruned[i].columns if c not in ['doi']})
#assign modified dfs back to original
df_datacite_dryad_pruned, df_datacite_dataverse_pruned, df_datacite_zenodo_pruned = datacite_dataframes_specific_pruned

if crossValidate:
    #create list of repository-specific dfs
    repositories_dataframes_pruned = [df_dryad_pruned, df_dataverse_pruned, df_zenodo_pruned]

    #editing DOI columns to ensure exact matches
    df_dryad_pruned['doi'] = df_dryad_pruned['doi'].str.replace('doi:', '')
    df_dataverse_pruned['doi'] = df_dataverse_pruned['doi'].str.replace('doi:', '')
    for df in repositories_dataframes_pruned:
        df['doi'] = df['doi'].str.lower()

    #adding suffix to column headers to differentiate identically named columns when merged (vs. automatic .x and .y)
    df_dryad_pruned = df_dryad_pruned.rename(columns={c: c+'_dryad' for c in df_dryad_pruned.columns if c not in ['doi']})
    df_dataverse_pruned = df_dataverse_pruned.rename(columns={c: c+'_dataverse' for c in df_dataverse_pruned.columns if c not in ['doi']})
    df_zenodo_pruned = df_zenodo_pruned.rename(columns={c: c+'_zen' for c in df_data_zenodo_real.columns if c not in ['doi']})

    df_dryad_pruned['source_dryad'] = 'Dryad'
    df_dataverse_pruned['source_dataverse'] = 'Texas Data Repository'
    df_zenodo_pruned['source_zenodo'] = 'Zenodo'

    print("Beginning cross-validation process.")
    print()

    #DataCite into Dryad
    df_dryad_datacite_joint = pd.merge(df_dryad_pruned, df_datacite_dryad_pruned, on='doi', how="left")
    df_dryad_datacite_joint['Match_entry'] = np.where(df_dryad_datacite_joint['source_dc'].isnull(), 'Not matched', 'Matched')
    print("Counts of matches for DataCite into Dryad")
    counts_dryad_datacite = df_dryad_datacite_joint['Match_entry'].value_counts()
    print(counts_dryad_datacite)
    print()
    df_dryad_datacite_joint_unmatched = df_dryad_datacite_joint[df_dryad_datacite_joint['Match_entry'] == "Not matched"]
    df_dryad_datacite_joint_unmatched.to_csv(f'outputs/{todayDate}_DataCite-into-Dryad_joint-unmatched-dataframe.csv', index=False)

    #Dryad into DataCite
    df_datacite_dryad_joint = pd.merge(df_datacite_dryad_pruned, df_dryad_pruned, on='doi', how="left")
    df_datacite_dryad_joint['Match_entry'] = np.where(df_datacite_dryad_joint['source_dryad'].isnull(), 'Not matched', 'Matched')
    print("Counts of matches for Dryad into DataCite")
    counts_datacite_dryad = df_datacite_dryad_joint['Match_entry'].value_counts()
    print(counts_datacite_dryad)
    print()
    df_datacite_dryad_joint_unmatched = df_datacite_dryad_joint[df_datacite_dryad_joint['Match_entry'] == "Not matched"]
    df_datacite_dryad_joint_unmatched.to_csv(f'outputs/{todayDate}_Dryad-into-DataCite_joint-unmatched-dataframe.csv', index=False)

    #DataCite into Dataverse (using non-de-duplicated DataCite data)
    df_dataverse_datacite_joint = pd.merge(df_dataverse_pruned, df_datacite_dataverse_pruned, on='doi', how="left")
    df_dataverse_datacite_joint['Match_entry'] = np.where(df_dataverse_datacite_joint['source_dc'].isnull(), 'Not matched', 'Matched')
    print("Counts of matches for DataCite into Dataverse")
    counts_dataverse_datacite = df_dataverse_datacite_joint['Match_entry'].value_counts()
    print(counts_dataverse_datacite)
    print()
    df_dataverse_datacite_joint_unmatched = df_dataverse_datacite_joint[df_dataverse_datacite_joint['Match_entry'] == "Not matched"]
    df_dataverse_datacite_joint_unmatched.to_csv(f'outputs/{todayDate}_DataCite-into-Dataverse_joint-unmatched-dataframe.csv', index=False)

    #Dataverse into DataCite (using de-duplicated DataCite data)
    df_datacite_dataverse_joint = pd.merge(df_datacite_dataverse_pruned, df_dataverse_pruned, on='doi', how="left")
    df_datacite_dataverse_joint['Match_entry'] = np.where(df_datacite_dataverse_joint['source_dataverse'].isnull(), 'Not matched', 'Matched')
    print("Counts of matches for Dataverse into DataCite")
    counts_datacite_dataverse = df_datacite_dataverse_joint['Match_entry'].value_counts()
    print(counts_datacite_dataverse)
    print()
    df_datacite_dataverse_joint_unmatched = df_datacite_dataverse_joint[df_datacite_dataverse_joint['Match_entry'] == "Not matched"]
    df_datacite_dataverse_joint_unmatched.to_csv(f'outputs/{todayDate}_Dataverse-into-DataCite_joint-unmatched-dataframe.csv', index=False)

    #DataCite into Zenodo
    df_zenodo_datacite_joint = pd.merge(df_zenodo_pruned, df_datacite_zenodo_pruned, on='doi', how="left")
    df_zenodo_datacite_joint['Match_entry'] = np.where(df_zenodo_datacite_joint['source_dc'].isnull(), 'Not matched', 'Matched')
    ##removing multiple DOIs in same 'lineage'
    df_zenodo_datacite_joint = df_zenodo_datacite_joint.sort_values(by=['doi'])
    df_zenodo_datacite_joint_deduplicated = df_zenodo_datacite_joint.drop_duplicates(subset=["publicationDate_zen", "description_zen"], keep='first') 
    ##one problematic dataset splits incorrectly when exported to CSV (conceptrecID = 616927)
    # df_zenodo_datacite_joint_deduplicated.to_excel(folder_to_export_path+'Datacite-into-Zenodo_joint_dataframe.xlsx', index=False)
    print("Counts of matches for DataCite into Zenodo")
    print()
    counts_zenodo_datacite = df_zenodo_datacite_joint_deduplicated['Match_entry'].value_counts()
    print(counts_zenodo_datacite)
    print()

    #Zenodo into DataCite
    df_datacite_zenodo_joint = pd.merge(df_datacite_zenodo_pruned, df_zenodo_pruned, on='doi', how="left")
    df_datacite_zenodo_joint['Match_entry'] = np.where(df_datacite_zenodo_joint['source_zenodo'].isnull(), 'Not matched', 'Matched')
    ##removing multiple DOIs in same 'lineage'
    df_datacite_zenodo_joint = df_datacite_zenodo_joint.sort_values(by=['doi']) 
    df_datacite_zenodo_joint_deduplicated = df_datacite_zenodo_joint.drop_duplicates(subset=["publicationDate_zen", "description_zen"], keep='first') 
    print("Counts of matches for Zenodo into DataCite")
    print()
    counts_datacite_zenodo = df_datacite_zenodo_joint_deduplicated['Match_entry'].value_counts()
    print(counts_datacite_zenodo)
    print()
    df_datacite_zenodo_joint_unmatched = df_datacite_zenodo_joint_deduplicated[df_datacite_zenodo_joint_deduplicated['Match_entry'] == "Not matched"]

    print("Beginning process for vertical concatenation of dataframes")
    print()
    #Dryad
    df_dryad_pruned_select = df_dryad_pruned[["doi","publicationYear_dryad", "title_dryad", "first_author_dryad", "first_affiliation_dryad"]]
    df_dryad_pruned_select = df_dryad_pruned_select.rename(columns={"publicationYear_dryad": "publicationYear", "title_dryad": "title", 'first_author_dryad': 'first_author', "first_affiliation_dryad": "first_affiliation"})
    df_dryad_pruned_select['source'] = 'Dryad'
    df_dryad_pruned_select['repository'] = 'Dryad'

    df_datacite_dryad_joint_unmatched_pruned = df_datacite_dryad_joint_unmatched[["doi","publicationYear_dc","title_dc", "first_author_dc", "first_affiliation_dc"]]
    df_datacite_dryad_joint_unmatched_pruned = df_datacite_dryad_joint_unmatched_pruned.rename(columns={"publicationYear_dc": "publicationYear", "title_dc": "title", 'first_author_dc': 'first_author', "first_affiliation_dc": "first_affiliation"})
    df_datacite_dryad_joint_unmatched_pruned['source'] = 'DataCite'
    df_datacite_dryad_joint_unmatched_pruned['repository'] = 'Dryad'

    df_datacite_dryad_combined = pd.concat([df_dryad_pruned_select, df_datacite_dryad_joint_unmatched_pruned], ignore_index=True)
    df_datacite_dryad_combined_dedup = df_datacite_dryad_combined.drop_duplicates(subset=['doi'],keep='first')
    print("Number of unique entries in Dryad: " + repr(len(df_datacite_dryad_combined_dedup)))
    print()
    df_datacite_dryad_combined_dedup['repository2'] = 'Dryad'
    df_datacite_dryad_combined_dedup['UT_lead'] = df_datacite_dryad_combined_dedup['first_affiliation'].str.contains('Austin', case=False, na=False).map({True: 'affiliated', False: 'not affiliated'})

    #Dataverse
    df_dataverse_pruned_select = df_dataverse_pruned[["doi","publicationYear_dataverse", "title_dataverse", "first_contact_dataverse", "first_contact_affiliation_dataverse"]]
    df_dataverse_pruned_select = df_dataverse_pruned_select.rename(columns={"publicationYear_dataverse": "publicationYear", "title_dataverse": "title", 'first_contact_dataverse': 'first_author', "first_contact_affiliation_dataverse": "first_affiliation"})
    df_dataverse_pruned_select['source'] = 'Texas Data Repository'
    df_dataverse_pruned_select['repository'] = 'Texas Data Repository'

    df_datacite_dataverse_joint_unmatched_pruned = df_datacite_dataverse_joint_unmatched[["doi","publicationYear_dc","title_dc", "first_author_dc", "first_affiliation_dc"]]
    df_datacite_dataverse_joint_unmatched_pruned = df_datacite_dataverse_joint_unmatched_pruned.rename(columns={"publicationYear_dc": "publicationYear", "title_dc": "title", 'first_author_dc': 'first_author', "first_affiliation_dc": "first_affiliation"})
    df_datacite_dataverse_joint_unmatched_pruned['source'] = 'DataCite'
    df_datacite_dataverse_joint_unmatched_pruned['repository'] = 'Texas Data Repository'

    df_datacite_dataverse_combined = pd.concat([df_dataverse_pruned_select, df_datacite_dataverse_joint_unmatched_pruned], ignore_index=True)
    df_datacite_dataverse_combined_dedup = df_datacite_dataverse_combined.drop_duplicates(subset=['doi'],keep='first')
    print("Number of unique entries in Dataverse: " + repr(len(df_datacite_dataverse_combined_dedup)))
    print()
    df_datacite_dataverse_combined_dedup['repository2'] = 'Texas Data Repository'
    df_datacite_dataverse_combined_dedup['UT_lead'] = df_datacite_dataverse_combined_dedup['first_author'].str.contains('Austin', case=False, na=False).map({True: 'affiliated', False: 'not affiliated'})

    #Zenodo
    df_zenodo_pruned_select = df_zenodo_pruned[["doi","publicationYear_zen", "title_zen","first_author_zen", "first_affiliation_zen"]]
    df_zenodo_pruned_select = df_zenodo_pruned_select.rename(columns={"publicationYear_zen": "publicationYear", "title_zen": "title", 'first_author_zen': 'first_author', "first_affiliation_zen": "first_affiliation"})
    df_zenodo_pruned_select['source'] = 'Zenodo'
    df_zenodo_pruned_select['repository'] = 'Zenodo'

    df_datacite_zenodo_joint_unmatched_pruned = df_datacite_zenodo_joint_unmatched[["doi","publicationYear_dc","title_dc", "first_author_dc", "first_affiliation_dc"]]
    df_datacite_zenodo_joint_unmatched_pruned = df_datacite_zenodo_joint_unmatched_pruned.rename(columns={"publicationYear_dc": "publicationYear", "title_dc": "title", 'first_author_dc': 'first_author', "first_affiliation_dc": "first_affiliation"})
    df_datacite_zenodo_joint_unmatched_pruned['source'] = 'DataCite'
    df_datacite_zenodo_joint_unmatched_pruned['repository'] = 'Zenodo'

    df_datacite_zenodo_combined = pd.concat([df_zenodo_pruned_select, df_datacite_zenodo_joint_unmatched_pruned], ignore_index=True)
    ##puts Zenodo-sourced at top, 'lowest' DOI at top; should ensure the parent is retained
    df_datacite_zenodo_combined = df_datacite_zenodo_combined.sort_values(['doi', 'source'], ascending = [True, False]) 
    df_datacite_zenodo_combined_dedup = df_datacite_zenodo_combined.drop_duplicates(subset=['doi'],keep='first')
    print("Number of unique entries in Zenodo: " + repr(len(df_datacite_zenodo_combined_dedup)))
    print()
    df_datacite_zenodo_combined_dedup['repository2'] = 'Zenodo'
    df_datacite_zenodo_combined_dedup['UT_lead'] = df_datacite_zenodo_combined_dedup['first_affiliation'].str.contains('Austin', case=False, na=False).map({True: 'affiliated', False: 'not affiliated'})

#remaining DataCite
df_datacite_remainder_pruned = df_datacite_remainder_pruned.rename(columns={"title_dc": "title", 'first_author_dc': 'first_author', "first_affiliation_dc": "first_affiliation", "source_dc":"source"})
df_datacite_remainder_pruned_select = df_datacite_remainder_pruned[['repository', 'doi', 'publicationYear', 'title', 'first_author', 'first_affiliation', 'source']] 
df_datacite_remainder_pruned_select['repository2'] = 'Other'
df_datacite_remainder_pruned_select['first_affiliation'] = df_datacite_remainder_pruned_select['first_affiliation'].fillna('None')
df_datacite_remainder_pruned_select['UT_lead'] = df_datacite_remainder_pruned_select['first_affiliation'].str.contains('Austin', case=False, na=False).map({True: 'affiliated', False: 'not affiliated'})

##handling partial duplication of EMSL (many distinct deposits related to single project)
emsl = df_datacite_remainder_pruned_select[df_datacite_remainder_pruned_select['repository'] == "Environmental Molecular Sciences Laboratory"]
df_datacite_remainder_pruned_no_emsl = df_datacite_remainder_pruned_select[df_datacite_remainder_pruned_select['repository'] != "Environmental Molecular Sciences Laboratory"]
emsl_deduplicated = emsl.drop_duplicates(subset=['title'],keep='first')
df_remainder_reconstructed = pd.concat([df_datacite_remainder_pruned_no_emsl, emsl_deduplicated], ignore_index=True)

##handling DOI assignment of datasets in Harvard Dataverse (retrieved by DataCite API)
df_remainder_reconstructed_deduplicated = df_remainder_reconstructed[
    ~((df_remainder_reconstructed['repository'].str.contains('Harvard Dataverse')) & 
      (df_remainder_reconstructed['doi'].str.count('/') > 2))
]

#final collation
if crossValidate:
    df_all_repos = pd.concat([df_datacite_dryad_combined_dedup, df_datacite_dataverse_combined_dedup, df_datacite_zenodo_combined_dedup, df_remainder_reconstructed_deduplicated], ignore_index=True)
else:
    columns_to_rename = {
    "repository_dc": "repository",
    "publicationYear_dc": "publicationYear",
    "title_dc": "title",
    "first_author_dc": "first_author",
    "first_affiliation_dc": "first_affiliation",
    "source_dc":'source'
    }
    for i in range(len(datacite_dataframes_specific_pruned)):
        datacite_dataframes_specific_pruned[i] = datacite_dataframes_specific_pruned[i].rename(columns=columns_to_rename)
    
    #assign modified dfs back to original
    df_datacite_dryad_pruned, df_datacite_dataverse_pruned, df_datacite_zenodo_pruned = datacite_dataframes_specific_pruned
    df_datacite_dryad_pruned_select = df_datacite_dryad_pruned[['repository', 'doi', 'publicationYear', 'title', 'first_author', 'first_affiliation', 'source']] 
    df_datacite_dataverse_pruned_select = df_datacite_dataverse_pruned[['repository', 'doi', 'publicationYear', 'title', 'first_author', 'first_affiliation', 'source']] 
    df_datacite_zenodo_pruned_select = df_datacite_zenodo_pruned[['repository', 'doi', 'publicationYear', 'title', 'first_author', 'first_affiliation', 'source']] 

    #removing multiple DOIs in same 'lineage'
    df_datacite_zenodo_pruned_dedup = df_datacite_zenodo_pruned_select.drop_duplicates(subset=['title', 'first_author', 'first_affiliation'], keep='first')

    df_datacite_dryad_pruned_select['repository2'] = 'Dryad'
    df_datacite_dataverse_pruned_select['repository2'] = 'Texas Data Repository'
    df_datacite_zenodo_pruned_dedup['repository2'] = 'Zenodo'

    datacite_dataframes_select_specific_pruned = [df_datacite_dryad_pruned_select, df_datacite_dataverse_pruned_select, df_datacite_zenodo_pruned_dedup]
    for df in datacite_dataframes_select_specific_pruned:
        df['UT_lead'] = df['first_affiliation'].str.contains('Austin', case=False, na=False).map({True: 'affiliated', False: 'not affiliated'})
    df_all_repos = pd.concat([df_datacite_dryad_pruned_select, df_datacite_dataverse_pruned_select, df_datacite_zenodo_pruned_dedup, df_remainder_reconstructed_deduplicated], ignore_index=True)

#standardizing repositories with multiple versions of name in dataframe
df_all_repos['repository'] = df_all_repos['repository'].fillna('None')
df_all_repos.loc[df_all_repos['repository'].str.contains('Digital Rocks', case=False), 'repository'] = 'Digital Rocks Portal'
df_all_repos.loc[df_all_repos['repository'].str.contains('Environmental System Science Data Infrastructure for a Virtual Ecosystem', case=False), 'repository'] = 'ESS-DIVE'
Dataverse_words = r'Texas.*Data.*Repository'
df_all_repos.loc[df_all_repos['repository'].str.contains(Dataverse_words, case=False), 'repository'] = 'Texas Data Repository'

#edge cases
##confusing metadata with UT Austin (but not Dataverse) listed as publisher; have to be manually adjusted over time
df_all_repos.loc[(df_all_repos['doi'].str.contains('zenodo')) & (df_all_repos['repository'].str.contains('University of Texas')), 'repository'] = 'Zenodo'
df_all_repos.loc[(df_all_repos['doi'].str.contains('10.11578/dc')) & (df_all_repos['repository'].str.contains('University of Texas')), 'repository'] = 'Department of Energy (DOE) CODE'

##other edge cases
df_all_repos.loc[df_all_repos['doi'].str.contains('10.23729/547d8c47-3723-4396-8f84-322c02ccadd0'), 'repository'] = 'Finnish Fairdata' #labeled publisher as author's name

#adding categorization
df_all_repos['non-TDR IR'] = np.where(df_all_repos['repository'].str.contains('University|UCLA|UNC|Harvard|ASU|Dataverse', case=True), 'non-TDR institutional', 'not university or TDR')
df_all_repos['US federal'] = np.where(df_all_repos['repository'].str.contains('NOAA|NIH|NSF|U.S.|DOE|DOD|DOI|National|Designsafe', case=True), 'Federal US repo', 'not federal US repo')
df_all_repos['GREI'] = np.where(df_all_repos['repository'].str.contains('Dryad|figshare|Zenodo|Vivli|Mendeley|Open Science Framework', case=False), 'GREI member', 'not GREI member')

df_all_repos.to_csv(f'outputs/{todayDate}_full-concatenated-dataframe.csv', index=False)

print("Done.")
print()
print(f"Time to run: {datetime.now() - startTime}")