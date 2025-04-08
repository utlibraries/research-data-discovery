from datetime import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.firefox.options import Options
from urllib.parse import urlparse, parse_qs, quote
import pandas as pd
import json
import math
import numpy as np
import os
import requests
import time
import xml.etree.ElementTree as ET

#operator for quick test runs
test = False
#toggle for cross-validation steps
crossValidate = False
##toggle for Dataverse cross-validation specifically
dataverse = False
#if you have done a previous DataCite retrieval and don't want to re-run the entire main process (skip to Figshare steps)
loadPreviousData = False

#toggles for executing Figshare processes (see README for details)
##identifying which publishers/articles are linked to figshare deposits that do have affiliation metadata
figshareWorkflow1 = False
##looking for datasets with a journal publisher listed as publisher, X-ref'ing with university articles from that publisher
figshareWorkflow2 = True
##finding university articles from publisher that uses certain formula for Figshare DOIs, construct hypothetical DOI, test if it exists
figshareWorkflow3 = False
##retrieving file-level information for Figshare deposits
figshareValidator = False

#toggle to load main dataset
loadPreviousData = False
#toggle to load main dataset + Figshare
loadPreviousDataPlus = False
#toggle for executing NCBI process
ncbiWorkflow = True
#toggle for skipping web retrieval of NCBI data (just XML to dataframe conversion)
loadNCBIdata = False

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
url_crossref = "https://api.crossref.org/works/"
url_crossref_issn = "https://api.crossref.org/journals/{issn}/works"
url_dryad = "https://datadryad.org/api/v2/search?affiliation=https://ror.org/00hj54h04" #Dryad requires ROR for affiliation search
url_datacite = "https://api.datacite.org/dois"
url_dataverse = "https://dataverse.tdl.org/api/search/"
url_figshare = "https://api.figshare.com/v2/articles/{id}/files?page_size=10"
url_openalex = 'https://api.openalex.org/works'
url_zenodo = "https://zenodo.org/api/records"

#create permutation string with OR for API parameters
ut_variations = config['PERMUTATIONS']
institution_query = ' OR '.join([f'"{variation}"' for variation in ut_variations])

#pulling in 'uniqueIdentifer' term used as quick, reliable filter ('Austin' for filtering an affiliation field for UT Austin)
uni_identifier = config['INSTITUTION']['uniqueIdentifier']

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
    #UT Austin dataverse, may contain non-UT affiliated objects, and UT-affiliated objects may be in other TDR installations
    #'subtree': 'utexas', 
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
page_limit_openalex = config['VARIABLES']['PAGE_LIMITS']['openalex_test'] if test else config['VARIABLES']['PAGE_LIMITS']['openalex_prod']

#define variables to be called recursively in function
page_start_datacite = config['VARIABLES']['PAGE_STARTS']['datacite']
page_start_zenodo = config['VARIABLES']['PAGE_STARTS']['zenodo']
page_size_zenodo = config['VARIABLES']['PAGE_SIZES']['zenodo']

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

##retrieves single page of results
def retrieve_page_dataverse(url, params=None, headers=None):
    """Fetch a single page of results."""
    try:
        response = requests.get(url, params=params, headers=headers)
        response.raise_for_status() 
        return response.json()
    except requests.RequestException as e:
        print(f"Error retrieving page: {e}")
        return {'data': {'items': [], 'total_count': 0}}
##recursively retrieves pages
def retrieve_all_data_dataverse(url, params, headers):
    """Fetch all pages of data using pagination."""
    all_data_dataverse = []

    while True: 
        data = retrieve_page_dataverse(url, params, headers)  
        total_count = data['data']['total_count']
        total_pages = math.ceil(total_count/params_dataverse['per_page'])
        print(f"Fetching Page {params_dataverse['page']} of {total_pages} pages...\n")

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

##retrieves single page of results
def retrieve_page_zenodo(url, params=None):
    """Fetch a single page of results with cursor-based pagination."""
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()  
        return response.json()
    except requests.RequestException as e:
        print(f"Error retrieving page: {e}")
        return {'hits': {'hits': [], 'total':{}}, 'links': {}}
##extracting the 'page' parameter value
def extract_page_number(url):
    parsed_url = urlparse(url)
    query_params = parse_qs(parsed_url.query)
    return query_params.get('page', [None])[0]  
##recursively retrieves pages
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

    print(f"Total: {total_count} entries over {total_pages} pages\n")
    
    while current_url and page_start_zenodo < page_limit_zenodo:
        page_start_zenodo+=1
        page_number = extract_page_number(current_url)
        print(f"Retrieving page {page_start_zenodo} of {total_pages} from Zenodo...\n")
        data = retrieve_page_zenodo(current_url)
        
        if not data['hits']['hits']:
            print("End of Zenodo response.\n")
            break
        
        all_data_zenodo.extend(data['hits']['hits'])
        
        current_url = data.get('links', {}).get('next', None)
    
    return all_data_zenodo

##retrieves single page of results
def retrieve_page_openalex(url, params=None):
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()  
        return response.json()
    except requests.RequestException as e:
        print(f"Error retrieveing page: {e}")
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

    print(f"Total: {total_count} entries over {total_pages} pages")
    print()

    while j < page_limit_openalex:
        j += 1
        print(f"Retrieving page {j} of {total_pages} from OpenAlex...")
        print()
        data = retrieve_page_openalex(url, params)
        next_cursor = data.get('meta', {}).get('next_cursor', None)

        if next_cursor == previous_cursor:
            print("Cursor did not change. Ending loop to avoid infinite loop.")
            break
        
        if not data['results']:
            print("End of OpenAlex response.")
            print()
            break
        
        all_data_openalex.extend(data['results'])
        
        previous_cursor = next_cursor
        params['cursor'] = next_cursor
    
    return all_data_openalex

##retrieves single page of results
def retrieve_page_crossref(url, params=None):
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()  
        return response.json()
    except requests.RequestException as e:
        print(f"Error retrieving page: {e}")
        return {'message': {'items': [], 'total-results':{}}}
##recursively retrieves pages
def retrieve_all_data_crossref(url, params):
    global k

    all_data_crossref = []
    data = retrieve_page_crossref(url, params)
    params = params_crossref_journal.copy()
    params['cursor'] = '*'
    next_cursor = '*'
    previous_cursor = None
    
    if not data['message']['items']:
        print("No data found.")
        return all_data_crossref

    all_data_crossref.extend(data['message']['items'])
    
    while k < page_limit_crossref:
        k += 1
        print(f"Retrieving page {k} from CrossRef...")
        print()

        data = retrieve_page_crossref(url, params)
        next_cursor = data.get('message', {}).get('next-cursor', None)
        
        if not data['message']['items']:
            print("Finished this journal.")
            print()
            break
        
        all_data_crossref.extend(data['message']['items'])
        
        previous_cursor = next_cursor
        params['cursor'] = next_cursor
    
    return all_data_crossref
#recursively retrieves specified journals in Crossref API
def retrieve_all_journals(url_template, journal_list):
    all_data = []  

    for journal_name, issn in journal_list.items():
        print(f"Retrieving data from {journal_name} (ISSN: {issn})")
        custom_url = url_template.format(issn=issn)
        params = params_crossref_journal.copy()
        params['filter'] += f",issn:{issn}"
        
        journal_data = retrieve_all_data_crossref(custom_url, params)
        all_data.extend(journal_data)

    return all_data

#checks if hypothetical DOI exists
def check_link(doi):
    url = f"https://doi.org/{doi}"
    response = requests.head(url, allow_redirects=True)
    return response.status_code == 200

if not loadPreviousData and not loadPreviousDataPlus:
    print("Starting DataCite retrieval based on affiliation.\n")
    data_datacite = retrieve_all_data_datacite(url_datacite, params_datacite)
    print(f"Number of datasets found by DataCite API: {len(data_datacite)}\n")

    if crossValidate:
        print("Starting Dryad retrieval.\n")
        data_dryad = retrieve_all_data_dryad(url_dryad, params_dryad)
        print(f"Number of Dryad datasets found by Dryad API: {len(data_dryad)}\n")
        if dataverse:
            print("Starting Dataverse retrieval.\n")
            data_dataverse = retrieve_all_data_dataverse(url_dataverse, params_dataverse, headers_dataverse)
            print(f"Number of Dataverse datasets found by Dataverse API: {len(data_dataverse)}\n")
        print("Starting Zenodo retrieval.\n")
        data_zenodo = retrieve_all_data_zenodo(url_zenodo, params_zenodo_data)
        print(f"Number of Zenodo datasets found by Zenodo API: {len(data_zenodo)}\n")

    print("Beginning dataframe generation.\n")

    data_select_datacite = [] 
    for item in data_datacite:
        attributes = item.get('attributes', {})
        doi = attributes.get('doi', None)
        publisher = attributes.get('publisher', "")
        # publisher_year = attributes.get('publicationYear', "") #temporarily disabling due to Dryad metadata issue
        registered = attributes.get('registered', "")
        if registered:
            publisher_year = datetime.fromisoformat(registered[:-1]).year
        else:
            publisher_year = None
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
        rights_list = attributes.get('rightsList', [])
        rights = [right.get('rights', 'Rights unclear') for right in rights_list]
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
            "type": resourceType,
            "rights": rights
        })

    df_datacite_initial = pd.json_normalize(data_select_datacite)

    #creating column for source of detected affiliation
    pattern = '|'.join([f'({perm})' for perm in ut_variations])
    #search for permutations in the 'affiliations' column
    df_datacite_initial['affiliation_source'] = df_datacite_initial.apply(
    lambda row: 'creator.affiliationName' if pd.Series(row['creatorsAffiliations']).str.contains(pattern, case=False, na=False).any()
    else ('creator.name' if pd.Series(row['creatorsNames']).str.contains(pattern, case=False, na=False).any()
    else ('contributor.affiliationName' if pd.Series(row['contributorsAffiliations']).str.contains(pattern, case=False, na=False).any()
    else ('contributor.name' if pd.Series(row['contributorsNames']).str.contains(pattern, case=False, na=False).any() else None))),
    axis=1)
    #pull out the identified permutation and put it into a new column
    df_datacite_initial['affiliation_permutation'] = df_datacite_initial.apply(
    lambda row: next((perm for perm in ut_variations if pd.Series(row['creatorsAffiliations'] + row['creatorsNames'] + row['contributorsAffiliations'] + row['contributorsNames']).str.contains(perm, case=False, na=False).any()), None),
    axis=1)
    #ensure the new column only counts unique entries
    df_datacite_initial['affiliation_permutation'] = df_datacite_initial['affiliation_permutation'].apply(
        lambda x: list(set(x)) if isinstance(x, list) else x)

    #handling version duplication (Figshare, ICPSR, etc.)
    ##handling duplication of Figshare deposits (parent vs. child with '.v*')
    ###creating separate figshare dataframe for downstream processing, not necessary for other repositories with this DOI mechanism in current workflow
    figshare = df_datacite_initial[df_datacite_initial['doi'].str.contains('figshare')]
    df_datacite_no_figshare = df_datacite_initial[~df_datacite_initial['doi'].str.contains('figshare')]
    figshare_no_versions = figshare[~figshare['doi'].str.contains(r'\.v\d+$')]
    #mediated workflow sometimes creates individual deposit for each file, want to treat as single dataset here
    figshare_deduplicated = figshare_no_versions.drop_duplicates(subset='relatedIdentifier', keep="first")

    df_datacite_v1 = pd.concat([df_datacite_no_figshare, figshare_deduplicated], ignore_index=True)

    ##handling duplication of ICPSR, SAGE, Mendeley Data, Zenodo deposits (parent vs. child)
    lineageRepos = df_datacite_v1[df_datacite_v1['publisher'].str.contains('ICPSR|Mendeley|SAGE|Zenodo')]
    df_datacite_lineageRepos = df_datacite_v1[~df_datacite_v1['publisher'].str.contains('ICPSR|Mendeley|SAGE|Zenodo')]
    lineageRepos_deduplicated = lineageRepos[~lineageRepos['relationType'].str.contains('IsVersionOf|IsNewVersionOf', case=False, na=False)]
    ###the use of .v* and v* as filters works for these repositories but could accidentally remove non-duplicate DOIs if applied to other repositories
    lineageRepos_deduplicated = lineageRepos_deduplicated[~lineageRepos_deduplicated['doi'].str.contains(r'\.v\d+$')]
    dois_to_remove = lineageRepos_deduplicated[(lineageRepos_deduplicated['doi'].str.contains(r'v\d$') | lineageRepos_deduplicated['doi'].str.contains(r'v\d-')) & (lineageRepos_deduplicated['publisher'].str.contains('ICPSR', case=False, na=False))]['doi']
    # Remove the identified DOIs
    lineageRepos_deduplicated = lineageRepos_deduplicated[~lineageRepos_deduplicated['doi'].isin(dois_to_remove)]
    df_datacite_v2 = pd.concat([df_datacite_lineageRepos, lineageRepos_deduplicated], ignore_index=True)

    #handling file-level DOI granularity (Dataverse)
    df_datacite_dedup = df_datacite_v2[~(df_datacite_v2['publisher'].str.contains('Dataverse|Texas Data Repository', case=False, na=False) & df_datacite_v2['containerIdentifier'].notnull())]
    df_datacite_dedup = df_datacite_dedup[~(df_datacite_dedup['doi'].str.count('/') >= 3)]

    #final sweeping dedpulication step, will catch a few odd edge cases that have been manually discovered
    ##mainly addresses hundreds of EMSL datasets that seem overly granularized (many deposits share all metadata other than DOI including detailed titles)
    df_sorted = df_datacite_dedup.sort_values(by='doi')
    # df_datacite = df_sorted.drop_duplicates(subset=['title', 'first_author', 'publisher'], keep="first")
    df_datacite = df_sorted.drop_duplicates(subset=['title', 'first_author', 'relationType', 'relatedIdentifier', 'containerIdentifier'], keep='first')

    #the file exported here is intended only to be used to compare affiliation source fields; the fields will be dropped in later steps in the workflow
    df_datacite.to_csv(f"outputs/{todayDate}_datacite-output-for-affiliation-source.csv", index=False) 

    if crossValidate:
        print("Dryad step\n")
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

        if dataverse:
            print("Dataverse step\n")
            data_select_dataverse = [] 
            for item in data_dataverse:
                globalID = item.get('global_id', "")
                versionState = item.get('versionState', None)
                pubDate_dataverse = item.get('published_at', "")
                title_dataverse = item.get('name', None)
                authors_dataverse = item.get('authors', [{}])
                contacts_dataverse = item.get('contacts', [{}])
                first_contact_dataverse = contacts_dataverse[0].get('name', None)
                first_affiliation_contact = contacts_dataverse[0].get('affiliation', None)
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
                    'first_contact_affiliation': first_affiliation_contact,
                    'type': type,
                    'dataverse': dataverse
                })
            df_dataverse = pd.json_normalize(data_select_dataverse)

        print("Zenodo step\n")
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

    print("Beginning dataframe editing.\n")

    #using str.contains to account for any potential name inconsistency for one repository
    df_datacite_dryad = df_datacite[df_datacite["publisher"].str.contains("Dryad")]
    df_datacite_dataverse = df_datacite[df_datacite["publisher"].str.contains("Texas Data Repository")]
    df_datacite_zenodo = df_datacite_v1[df_datacite_v1["publisher"].str.contains("Zenodo")]
    df_remainder = df_datacite[df_datacite["publisher"].str.contains("Dryad|Texas Data Repository|Zenodo") == False]

    print(f"Number of Dryad datasets found by DataCite API: {len(df_datacite_dryad)}\n")
    print(f"Number of Dataverse datasets found by DataCite API: {len(df_datacite_dataverse)}\n")
    print(f"Number of Zenodo datasets found by DataCite API: {len(df_datacite_zenodo)}\n")

    if crossValidate:
        print("Repository-specific processing\n")
        if dataverse:
            #subsetting for published datasets
            df_dataverse_pub = df_dataverse[df_dataverse["status"].str.contains('RELEASED') == True]
            #looking for UT Austin in any of four fields
            pattern = '|'.join([f'({perm})' for perm in ut_variations])
            df_dataverse_pub['authors'] = df_dataverse_pub['authors'].apply(lambda x: str(x))
            df_dataverse_pub['contacts'] = df_dataverse_pub['contacts'].apply(lambda x: str(x))
            df_dataverse_pub_filtered = df_dataverse_pub[df_dataverse_pub['authors'].str.contains(pattern, case=False, na=False) | df_dataverse_pub['contacts'].str.contains(pattern, case=False, na=False)]
            print(f"Number of published Dataverse datasets found by Dataverse API: {len(df_dataverse_pub)}\n")

        #removing non-Zenodo deposits indexed by Zenodo (mostly Dryad) from Zenodo output
        ##Zenodo has indexed many Dryad deposits <50 GB in size (does not issue a new DOI but does return a Zenodo 'record' in the API)
        df_data_zenodo_true = df_data_zenodo[df_data_zenodo["doi"].str.contains('zenodo') == True] 
        #for some reason, Zenodo's API sometimes returns identical entries of most datasets...
        df_data_zenodo_real = df_data_zenodo_true.drop_duplicates(subset=['publicationDate', 'doi'], keep='first') 
        print(f"Number of non-Dryad Zenodo datasets found by Zenodo API: {len(df_data_zenodo_real)}\n")

        #formatting author names to be consistent with others
        df_dryad['first_author'] = df_dryad['first_author_last'] + ', ' + df_dryad['first_author_first']
        df_dryad['last_author'] = df_dryad['last_author_last'] + ', ' + df_dryad['last_author_first']
        df_dryad = df_dryad.drop(columns=['first_author_first', 'first_author_last', 
                            'last_author_first', 'last_author_last'])

        df_dryad['publicationYear'] = pd.to_datetime(df_dryad['publicationDate']).dt.year
        if dataverse:
            df_dataverse_pub_filtered['publicationYear'] = pd.to_datetime(df_dataverse_pub_filtered['publicationDate'], format='ISO8601').dt.year
        df_data_zenodo_real['publicationYear'] = pd.to_datetime(df_data_zenodo_real['publicationDate'], format='mixed').dt.year

        df_dryad_pruned = df_dryad[['doi', 'publicationYear', 'title', 'first_author', 'first_affiliation', 'last_author', 'last_affiliation']]
        if dataverse:
           df_dataverse_pruned = df_dataverse_pub_filtered[['doi', 'publicationYear', 'title', 'first_contact', 'first_contact_affiliation']]
        df_zenodo_pruned = df_data_zenodo_real[["doi","publicationYear", "title","first_author", "first_affiliation", 'publicationDate', 'description']]

    df_datacite_dryad_pruned = df_datacite_dryad[['publisher', 'doi', 'publicationYear', 'title', 'first_author', 'first_affiliation', 'type']]
    df_datacite_dataverse_pruned = df_datacite_dataverse[['publisher', 'doi', 'publicationYear', 'title', 'first_author', 'first_affiliation', 'type']] 
    df_datacite_zenodo_pruned = df_datacite_zenodo[['publisher', 'doi', 'publicationYear', 'title', 'first_author', 'first_affiliation', 'type']] 
    df_datacite_remainder_pruned = df_remainder[['publisher', 'doi', 'publicationYear', 'title', 'first_author', 'first_affiliation', 'type']] 

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
        if dataverse:
            repositories_dataframes_pruned = [df_dryad_pruned, df_dataverse_pruned, df_zenodo_pruned]
        else:
            repositories_dataframes_pruned = [df_dryad_pruned, df_zenodo_pruned]

        #editing DOI columns to ensure exact matches
        df_dryad_pruned['doi'] = df_dryad_pruned['doi'].str.replace('doi:', '')
        if dataverse:
            df_dataverse_pruned['doi'] = df_dataverse_pruned['doi'].str.replace('doi:', '')
        for df in repositories_dataframes_pruned:
            df['doi'] = df['doi'].str.lower()

        #adding suffix to column headers to differentiate identically named columns when merged (vs. automatic .x and .y)
        df_dryad_pruned = df_dryad_pruned.rename(columns={c: c+'_dryad' for c in df_dryad_pruned.columns if c not in ['doi']})
        if dataverse:
            df_dataverse_pruned = df_dataverse_pruned.rename(columns={c: c+'_dataverse' for c in df_dataverse_pruned.columns if c not in ['doi']})
        df_zenodo_pruned = df_zenodo_pruned.rename(columns={c: c+'_zen' for c in df_data_zenodo_real.columns if c not in ['doi']})

        df_dryad_pruned['source_dryad'] = 'Dryad'
        if dataverse:
            df_dataverse_pruned['source_dataverse'] = 'Texas Data Repository'
        df_zenodo_pruned['source_zenodo'] = 'Zenodo'

        print("Beginning cross-validation process.\n")

        #DataCite into Dryad
        df_dryad_datacite_joint = pd.merge(df_dryad_pruned, df_datacite_dryad_pruned, on='doi', how="left")
        df_dryad_datacite_joint['Match_entry'] = np.where(df_dryad_datacite_joint['source_dc'].isnull(), 'Not matched', 'Matched')
        print("Counts of matches for DataCite into Dryad")
        counts_dryad_datacite = df_dryad_datacite_joint['Match_entry'].value_counts()
        print(counts_dryad_datacite, "\n")
        df_dryad_datacite_joint_unmatched = df_dryad_datacite_joint[df_dryad_datacite_joint['Match_entry'] == "Not matched"]
        df_dryad_datacite_joint_unmatched.to_csv(f'outputs/{todayDate}_DataCite-into-Dryad_joint-unmatched-dataframe.csv', index=False)

        #Dryad into DataCite
        df_datacite_dryad_joint = pd.merge(df_datacite_dryad_pruned, df_dryad_pruned, on='doi', how="left")
        df_datacite_dryad_joint['Match_entry'] = np.where(df_datacite_dryad_joint['source_dryad'].isnull(), 'Not matched', 'Matched')
        print("Counts of matches for Dryad into DataCite")
        counts_datacite_dryad = df_datacite_dryad_joint['Match_entry'].value_counts()
        print(counts_datacite_dryad, "\n")
        df_datacite_dryad_joint_unmatched = df_datacite_dryad_joint[df_datacite_dryad_joint['Match_entry'] == "Not matched"]
        df_datacite_dryad_joint_unmatched.to_csv(f'outputs/{todayDate}_Dryad-into-DataCite_joint-unmatched-dataframe.csv', index=False)

        if dataverse:
        #DataCite into Dataverse (using non-de-duplicated DataCite data)
            df_dataverse_datacite_joint = pd.merge(df_dataverse_pruned, df_datacite_dataverse_pruned, on='doi', how="left")
            df_dataverse_datacite_joint['Match_entry'] = np.where(df_dataverse_datacite_joint['source_dc'].isnull(), 'Not matched', 'Matched')
            print("Counts of matches for DataCite into Dataverse")
            counts_dataverse_datacite = df_dataverse_datacite_joint['Match_entry'].value_counts()
            print(counts_dataverse_datacite, "\n")
            df_dataverse_datacite_joint_unmatched = df_dataverse_datacite_joint[df_dataverse_datacite_joint['Match_entry'] == "Not matched"]
            df_dataverse_datacite_joint_unmatched.to_csv(f'outputs/{todayDate}_DataCite-into-Dataverse_joint-unmatched-dataframe.csv', index=False)

            #Dataverse into DataCite (using de-duplicated DataCite data)
            df_datacite_dataverse_joint = pd.merge(df_datacite_dataverse_pruned, df_dataverse_pruned, on='doi', how="left")
            df_datacite_dataverse_joint['Match_entry'] = np.where(df_datacite_dataverse_joint['source_dataverse'].isnull(), 'Not matched', 'Matched')
            print("Counts of matches for Dataverse into DataCite")
            counts_datacite_dataverse = df_datacite_dataverse_joint['Match_entry'].value_counts()
            print(counts_datacite_dataverse, "\n")
            df_datacite_dataverse_joint_unmatched = df_datacite_dataverse_joint[df_datacite_dataverse_joint['Match_entry'] == "Not matched"]
            df_datacite_dataverse_joint_unmatched.to_csv(f'outputs/{todayDate}_Dataverse-into-DataCite_joint-unmatched-dataframe.csv', index=False)

        #DataCite into Zenodo
        df_zenodo_datacite_joint = pd.merge(df_zenodo_pruned, df_datacite_zenodo_pruned, on='doi', how="left")
        df_zenodo_datacite_joint['Match_entry'] = np.where(df_zenodo_datacite_joint['source_dc'].isnull(), 'Not matched', 'Matched')
        ##removing multiple DOIs in same 'lineage'
        df_zenodo_datacite_joint = df_zenodo_datacite_joint.sort_values(by=['doi'])
        df_zenodo_datacite_joint_deduplicated = df_zenodo_datacite_joint.drop_duplicates(subset=["publicationDate_zen", "description_zen"], keep='first') 
        ##one problematic dataset splits incorrectly when exported to CSV (conceptrecID = 616927)
        df_zenodo_datacite_joint_deduplicated.to_excel('outputs/Datacite-into-Zenodo_joint_dataframe.xlsx', index=False)
        print("Counts of matches for DataCite into Zenodo\n")
        counts_zenodo_datacite = df_zenodo_datacite_joint_deduplicated['Match_entry'].value_counts()
        print(counts_zenodo_datacite, "\n")

        #Zenodo into DataCite
        df_datacite_zenodo_joint = pd.merge(df_datacite_zenodo_pruned, df_zenodo_pruned, on='doi', how="left")
        df_datacite_zenodo_joint['Match_entry'] = np.where(df_datacite_zenodo_joint['source_zenodo'].isnull(), 'Not matched', 'Matched')
        ##removing multiple DOIs in same 'lineage'
        df_datacite_zenodo_joint = df_datacite_zenodo_joint.sort_values(by=['doi']) 
        df_datacite_zenodo_joint_deduplicated = df_datacite_zenodo_joint.drop_duplicates(subset=["publicationDate_zen", "description_zen"], keep='first') 
        print("Counts of matches for Zenodo into DataCite\n")
        counts_datacite_zenodo = df_datacite_zenodo_joint_deduplicated['Match_entry'].value_counts()
        print(counts_datacite_zenodo, "\n")
        df_datacite_zenodo_joint_unmatched = df_datacite_zenodo_joint_deduplicated[df_datacite_zenodo_joint_deduplicated['Match_entry'] == "Not matched"]
        df_datacite_zenodo_joint_unmatched.to_excel('outputs/Zenodo-into-DataCite_joint_dataframe.xlsx', index=False)

        print("Beginning process for vertical concatenation of dataframes\n")
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
        print("Number of unique entries in Dryad: " + repr(len(df_datacite_dryad_combined_dedup))+ "\n")
        df_datacite_dryad_combined_dedup['repository2'] = 'Dryad'
        df_datacite_dryad_combined_dedup['uni_lead'] = df_datacite_dryad_combined_dedup['first_affiliation'].str.contains('Austin', case=False, na=False).map({True: 'affiliated', False: 'not affiliated'})

        if dataverse:
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
            print("Number of unique entries in Dataverse: " + repr(len(df_datacite_dataverse_combined_dedup))+ "\n")
            df_datacite_dataverse_combined_dedup['repository2'] = 'Texas Data Repository'
            df_datacite_dataverse_combined_dedup['uni_lead'] = df_datacite_dataverse_combined_dedup['first_author'].str.contains('Austin', case=False, na=False).map({True: 'affiliated', False: 'not affiliated'})
        
        df_datacite_dataverse_combined = pd.concat([df_dataverse_pruned_select, df_datacite_dataverse_joint_unmatched_pruned], ignore_index=True)
        df_datacite_dataverse_combined_dedup = df_datacite_dataverse_combined.drop_duplicates(subset=['doi'],keep='first')
        print("Number of unique entries in Dataverse: " + repr(len(df_datacite_dataverse_combined_dedup))+ "\n")
        df_datacite_dataverse_combined_dedup['repository2'] = 'Texas Data Repository'
        df_datacite_dataverse_combined_dedup['uni_lead'] = df_datacite_dataverse_combined_dedup['first_author'].str.contains('Austin', case=False, na=False).map({True: 'affiliated', False: 'not affiliated'})

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
        print("Number of unique entries in Zenodo: " + repr(len(df_datacite_zenodo_combined_dedup))+ "\n")
        df_datacite_zenodo_combined_dedup['repository2'] = 'Zenodo'
        df_datacite_zenodo_combined_dedup['uni_lead'] = df_datacite_zenodo_combined_dedup['first_affiliation'].str.contains('Austin', case=False, na=False).map({True: 'affiliated', False: 'not affiliated'})

    #remaining DataCite
    df_datacite_remainder_pruned = df_datacite_remainder_pruned.rename(columns={"title_dc": "title", 'first_author_dc': 'first_author', "first_affiliation_dc": "first_affiliation", "source_dc":"source", "type_dc":"type"})
    df_datacite_remainder_pruned_select = df_datacite_remainder_pruned[['repository', 'doi', 'publicationYear', 'title', 'first_author', 'first_affiliation', 'source', 'type']] 
    df_datacite_remainder_pruned_select['repository2'] = 'Other'
    df_datacite_remainder_pruned_select['first_affiliation'] = df_datacite_remainder_pruned_select['first_affiliation'].fillna('None')
    df_datacite_remainder_pruned_select['uni_lead'] = df_datacite_remainder_pruned_select['first_affiliation'].str.contains('Austin', case=False, na=False).map({True: 'affiliated', False: 'not affiliated'})
    df_datacite_remainder_pruned_select['uni_lead'] = df_datacite_remainder_pruned_select['first_affiliation'].str.contains('Austin', case=False, na=False).map({True: 'affiliated', False: 'not affiliated'})

    #final collation
    if crossValidate and dataverse:
        df_all_repos = pd.concat([df_datacite_dryad_combined_dedup, df_datacite_dataverse_combined_dedup, df_datacite_zenodo_combined_dedup, df_datacite_remainder_pruned_select], ignore_index=True)
    elif crossValidate and not dataverse:
        df_all_repos = pd.concat([df_datacite_dryad_combined_dedup, df_datacite_zenodo_combined_dedup, df_datacite_remainder_pruned_select], ignore_index=True)
    else:
        columns_to_rename = {
        "repository_dc": "repository",
        "publicationYear_dc": "publicationYear",
        "title_dc": "title",
        "first_author_dc": "first_author",
        "first_affiliation_dc": "first_affiliation",
        "source_dc":'source',
        "type_dc":'type'
        }
        for i in range(len(datacite_dataframes_specific_pruned)):
            datacite_dataframes_specific_pruned[i] = datacite_dataframes_specific_pruned[i].rename(columns=columns_to_rename)
        
        #assign modified dfs back to original
        df_datacite_dryad_pruned, df_datacite_dataverse_pruned, df_datacite_zenodo_pruned = datacite_dataframes_specific_pruned
        df_datacite_dryad_pruned_select = df_datacite_dryad_pruned[['repository', 'doi', 'publicationYear', 'title', 'first_author', 'first_affiliation', 'source', 'type']] 
        df_datacite_dataverse_pruned_select = df_datacite_dataverse_pruned[['repository', 'doi', 'publicationYear', 'title', 'first_author', 'first_affiliation', 'source', 'type']] 
        df_datacite_zenodo_pruned_select = df_datacite_zenodo_pruned[['repository', 'doi', 'publicationYear', 'title', 'first_author', 'first_affiliation', 'source', 'type']] 

        #removing multiple DOIs in same 'lineage'
        df_datacite_zenodo_pruned_dedup = df_datacite_zenodo_pruned_select.drop_duplicates(subset=['title', 'first_author', 'first_affiliation'], keep='first')

        df_datacite_dryad_pruned_select['repository2'] = 'Dryad'
        df_datacite_dataverse_pruned_select['repository2'] = 'Texas Data Repository'
        df_datacite_zenodo_pruned_dedup['repository2'] = 'Zenodo'

        datacite_dataframes_select_specific_pruned = [df_datacite_dryad_pruned_select, df_datacite_dataverse_pruned_select, df_datacite_zenodo_pruned_dedup]
        for df in datacite_dataframes_select_specific_pruned:
            df['uni_lead'] = df['first_affiliation'].str.contains('Austin', case=False, na=False).map({True: 'affiliated', False: 'not affiliated'})
        df_all_repos = pd.concat([df_datacite_dryad_pruned_select, df_datacite_dataverse_pruned_select, df_datacite_zenodo_pruned_dedup, df_datacite_remainder_pruned_select], ignore_index=True)

    #standardizing repositories with multiple versions of name in dataframe
    df_all_repos['repository'] = df_all_repos['repository'].fillna('None')
    df_all_repos.loc[df_all_repos['repository'].str.contains('Digital Rocks', case=False), 'repository'] = 'Digital Rocks Portal'
    df_all_repos.loc[df_all_repos['repository'].str.contains('Environmental System Science Data Infrastructure for a Virtual Ecosystem', case=False), 'repository'] = 'ESS-DIVE'
    df_all_repos.loc[df_all_repos['repository'].str.contains('Texas Data Repository', case=False), 'repository'] = 'Texas Data Repository'
    df_all_repos.loc[df_all_repos['repository'].str.contains('ICPSR', case=True), 'repository'] = 'ICPSR'
    df_all_repos.loc[df_all_repos['repository'].str.contains('Environmental Molecular Sciences Laboratory', case=True), 'repository'] = 'Environ Mol Sci Lab'

    #EDGE CASES, likely unnecessary for other universities, but you will need to find your own edge cases
    ##confusing metadata with UT Austin (but not Dataverse) listed as publisher; have to be manually adjusted over time
    df_all_repos.loc[(df_all_repos['doi'].str.contains('zenodo')) & (df_all_repos['repository'].str.contains('University of Texas')), 'repository'] = 'Zenodo' #10.5281/zenodo.10198511
    df_all_repos.loc[(df_all_repos['doi'].str.contains('zenodo')) & (df_all_repos['repository'].str.contains('University of Texas')), 'repository2'] = 'Zenodo' #10.5281/zenodo.10198511
    df_all_repos.loc[(df_all_repos['doi'].str.contains('10.11578/dc')) & (df_all_repos['repository'].str.contains('University of Texas')), 'repository'] = 'Department of Energy (DOE) CODE'
    ##other edge cases
    df_all_repos.loc[df_all_repos['doi'].str.contains('10.23729/547d8c47-3723-4396-8f84-322c02ccadd0'), 'repository'] = 'Finnish Fairdata' #labeled publisher as author's name

    #adding categorization
    df_all_repos['non_TDR_IR'] = np.where(df_all_repos['repository'].str.contains('University|UCLA|UNC|Harvard|ASU|Dataverse', case=True), 'non-TDR institutional', 'not university or TDR')
    df_all_repos['US_federal'] = np.where(df_all_repos['repository'].str.contains('NOAA|NIH|NSF|U.S.|DOE|DOD|DOI|National|Designsafe', case=True), 'Federal US repo', 'not federal US repo')
    df_all_repos['GREI'] = np.where(df_all_repos['repository'].str.contains('Dryad|figshare|Zenodo|Vivli|Mendeley|Open Science Framework', case=False), 'GREI member', 'not GREI member')

    # #standardizing licenses
    # df_all_repos['rights'] = df_all_repos['rights'].apply(lambda x: ' '.join(x) if isinstance(x, list) else x).astype(str).str.strip('[]')
    # df_all_repos['rights_standardized'] = 'Rights unclear'  #default value
    # df_all_repos.loc[df_all_repos['rights'].str.contains('Creative Commons Zero|CC0'), 'rights_standardized'] = 'CC0'
    # df_all_repos.loc[df_all_repos['rights'].str.contains('Creative Commons Attribution Non Commercial Share Alike'), 'rights_standardized'] = 'CC BY-NC-SA'
    # df_all_repos.loc[df_all_repos['rights'].str.contains('Creative Commons Attribution Non Commercial'), 'rights_standardized'] = 'CC BY-NC'
    # df_all_repos.loc[df_all_repos['rights'].str.contains('Creative Commons Attribution 3.0|Creative Commons Attribution 4.0|Creative Commons Attribution-NonCommercial'), 'rights_standardized'] = 'CC BY'
    # df_all_repos.loc[df_all_repos['rights'].str.contains('GNU General Public License'), 'rights_standardized'] = 'GNU GPL'
    # df_all_repos.loc[df_all_repos['rights'].str.contains('Apache License'), 'rights_standardized'] = 'Apache'
    # df_all_repos.loc[df_all_repos['rights'].str.contains('MIT License'), 'rights_standardized'] = 'MIT'
    # df_all_repos.loc[df_all_repos['rights'].str.contains('BSD'), 'rights_standardized'] = 'BSD'
    # df_all_repos.loc[df_all_repos['rights'].str.contains('ODC-BY'), 'rights_standardized'] = 'ODC-BY'
    # df_all_repos.loc[df_all_repos['rights'].str.contains('Open Access'), 'rights_standardized'] = 'Rights unclear'
    # df_all_repos.loc[df_all_repos['rights'].str.contains('Closed Access'), 'rights_standardized'] = 'Restricted access'
    # df_all_repos.loc[df_all_repos['rights'].str.contains('Restricted Access'), 'rights_standardized'] = 'Restricted access'
    # df_all_repos.loc[df_all_repos['rights'].str.contains('Databrary'), 'rights_standardized'] = 'Custom terms'
    # df_all_repos.loc[df_all_repos['rights'].str.contains('UCAR'), 'rights_standardized'] = 'Custom terms'
    # df_all_repos.loc[df_all_repos['rights'] == '', 'rights_standardized'] = 'Rights unclear'

    df_all_repos.to_csv(f'outputs/{todayDate}_full-concatenated-dataframe.csv', index=False)

###### FIGSHARE WORKFLOW ######
#These sections are for cleaning up identified figshare deposits or identifying associated ones that lack affiliation metadata

if loadPreviousData:
    #for reading in previously generated file of all associated datasets
    print("Reading in previous DataCite output file\n")
    directory = './outputs' 
    pattern = '_full-concatenated-dataframe.csv'

    files = os.listdir(directory)
    files.sort(reverse=True)
    latest_file = None
    for file in files:
        if pattern in file:
            latest_file = file
            break

    if latest_file:
        file_path = os.path.join(directory, latest_file)
        df_all_repos = pd.read_csv(file_path)
        print(f"The most recent file '{latest_file}' has been loaded successfully.")
    else:
        print(f"No file with '{pattern}' was found in the directory '{directory}'.")

### This codeblock will retrieve information on the articles associated with already retrieved figshare deposits that do record affiliation metadata ###
#this step may become redundant if the large-scale retrieval is modified to include the identifiers to begin with
if figshareWorkflow1:

    figshare = df_all_repos[df_all_repos['doi'].str.contains('figshare')]

    print("Retrieving additional figshare metadata\n")
    results = []
    for doi in figshare['doi']:
        try:
            response = requests.get(f'{url_datacite}/{doi}')
            if response.status_code == 200:
                print(f"Retrieving {doi}\n")
                results.append(response.json())
            else:
                print(f"Error retrieving {doi}: {response.status_code}, {response.text}")
        except requests.exceptions.RequestException as e:
            print(f"Timeout error on DOI {doi}: {e}")

    data_figshare = {
        'datasets': results
    }

    data_figshare_select = [] 
    datasets = data_figshare.get('datasets', [])
    for item in datasets:
        data = item.get('data', {})
        attributes = data.get('attributes', {})
        doi_dc = attributes.get('doi', None)
        publisher_dc = attributes.get('publisher', "")
        # publisher_year_dc = attributes.get('publicationYear', "")
        registered = attributes.get('registered', "")
        if registered:
            publisher_year_dc = datetime.fromisoformat(registered[:-1]).year
        else:
            publisher_year_dc = None
        title_dc = attributes.get('titles', [{}])[0].get('title', "")
        related_identifiers = attributes.get('relatedIdentifiers', [])
        types = attributes.get('types', {})
        resourceType = types.get('resourceTypeGeneral', '')
        rights_list = attributes.get('rightsList', [])
        rights = [right.get('rights', 'Rights unclear') for right in rights_list]
        for rel in related_identifiers: #'explodes' deposits with multiple relatedIdentifiers
            data_figshare_select.append({
                "doi": doi_dc,
                "publisher": publisher_dc,
                "publicationYear": publisher_year_dc,
                "title": title_dc,
                "relationType": rel.get('relationType'),
                "relatedIdentifier": rel.get('relatedIdentifier'),
                "relatedIdentifierType": rel.get('relatedIdentifierType'),
                "resourceType": resourceType,
                "rights": rights
            })

    df_figshare_initial = pd.json_normalize(data_figshare_select)
    #only want ones with supplemental relationship
    df_figshare_supplemental = df_figshare_initial[df_figshare_initial['relationType'] == "IsSupplementTo"]
    #mediated workflow sometimes creates individual deposit for each file, want to treat as single dataset here
    df_figshare_supplemental = df_figshare_supplemental.drop_duplicates(subset='relatedIdentifier', keep="first")

    #retrieving metadata about related identifiers that were identified
    print("Retrieving articles from Crossref\n")
    results = []
    for doi in df_figshare_supplemental['relatedIdentifier']:
        try:
            response = requests.get(f'{url_crossref}/{doi}')
            if response.status_code == 200:
                print(f"Retrieving {doi}\n")
                results.append(response.json())
            else:
                print(f"Error retrieving {doi}: {response.status_code}, {response.text}")
        except requests.exceptions.RequestException as e:
            print(f"Timeout error on DOI {doi}: {e}")

    data_figshare_crossref = {
        'articles': results
    }

    data_figshare_crossref_select = []
    articles = data_figshare_crossref.get('articles', [])
    for item in articles:
        message = item.get('message', {})
        publisher = message.get('publisher', None)
        journal = message.get('container-title', None)[0]
        doi = message.get('DOI', "")
        title_list = message.get('title', [])
        title = title_list[0] if title_list else None
        author = message.get('author', None)
        created = message.get('created', {})
        createdDate = created.get('date-time', None)
        
        data_figshare_crossref_select.append({
            'publisher_article': publisher,
            'journal': journal, 
            'doi_article': doi,
            'author_article': author,
            'title_article': title,
            'published_article': createdDate,
    })
        
    df_crossref = pd.json_normalize(data_figshare_crossref_select)
    df_crossref['relatedIdentifier'] = df_crossref['doi_article']
    df_datacite_crossref = pd.merge(df_figshare_supplemental, df_crossref, on="relatedIdentifier", how="left")
    df_datacite_crossref.to_csv(f"outputs/{todayDate}_figshare-datasets-with-article-info.csv", index=False)

### This codeblock will retrieve all figshare deposits with a listed journal/publisher as 'publisher,' extract related identifiers, retrieve all articles published by a certain publisher, cross-reference article DOIs against dataset related identifiers, and produce a match list. ###

if figshareWorkflow2:

    #figshare DOIs sometimes have a .v* for version number; this toggles whether to include them (True) or only include the parent (False)
    countVersions = False

    #pull in map of publisher names and OpenAlex codes
    publisher_mapping = config['FIGSHARE_PARTNERS']
    #create empty object to store results
    data_select_datacite = [] 
    data_select_openalex = []

    for publisher_name, openalex_code in publisher_mapping.items():
        try:
            #update both params for each publisher in map
            params_openalex = {
            'filter': f'authorships.institutions.ror:https://ror.org/00hj54h04,type:article,from_publication_date:2000-01-01,locations.source.host_organization:{openalex_code}',
            'per-page': config['VARIABLES']['PAGE_SIZES']['openalex'],
            'select': 'id,doi,title,authorships,primary_location,type',
            'mailto': config['EMAIL']['user_email']
            }
            j = 0
            #define different number of pages to retrieve from OpenAlex API based on 'test' vs. 'prod' env
            page_limit_openalex = config['VARIABLES']['PAGE_LIMITS']['openalex_test'] if test else config['VARIABLES']['PAGE_LIMITS']['openalex_prod']
            #DataCite params (different from general affiliation-based retrieval params)
            ## !! Warning: if you do not set a resourceType in the query (recommended if you want to get broad coverage), this will be a very large retrieval. In the test env, there may not be enough records to find a match with a university-affiliated article !!
            params_datacite_figshare = {
                'affiliation': 'true',
                'query': f'(publisher:"{publisher_name}") AND types.resourceTypeGeneral:"Dataset"',
                'page[size]': config['VARIABLES']['PAGE_SIZES']['datacite'],
                'page[cursor]': 1,
            }
            page_start_datacite = config['VARIABLES']['PAGE_STARTS']['datacite'] #reset to 0 (default) after large-scale general retrieval through DataCite

            print(f"Starting DataCite retrieval for {publisher_name}.\n")
            data_datacite = retrieve_all_data_datacite(url_datacite, params_datacite_figshare)
            print(f"Number of datasets found by DataCite API: {len(data_datacite)}\n")
            for item in data_datacite:
                attributes = item.get('attributes', {})
                doi_dc = attributes.get('doi', None)
                publisher_dc = attributes.get('publisher', "")
                # publisher_year_dc = attributes.get('publicationYear', "")
                registered = attributes.get('registered', "")
                if registered:
                    publisher_year_dc = datetime.fromisoformat(registered[:-1]).year
                else:
                    publisher_year_dc = None
                title_dc = attributes.get('titles', [{}])[0].get('title', "")
                creators_dc = attributes.get('creators', [{}])
                affiliations_dc = [affiliation.get('name', "") for creator in creators_dc for affiliation in creator.get('affiliation', [{}])]
                related_identifiers = attributes.get('relatedIdentifiers', [])
                container_dc = attributes.get('container', {})
                container_identifier_dc = container_dc.get('identifier', None)
                types = attributes.get('types', {})
                resourceType = types.get('resourceTypeGeneral', '')

                for rel in related_identifiers: #'explodes' deposits with multiple relatedIdentifiers
                    data_select_datacite.append({
                        "doi": doi_dc,
                        "publisher": publisher_dc,
                        "publicationYear": publisher_year_dc,
                        "title": title_dc,
                        "creators": creators_dc,
                        "affiliations": affiliations_dc,
                        "relationType": rel.get('relationType'),
                        "relatedIdentifier": rel.get('relatedIdentifier'),
                        "relatedIdentifierType": rel.get('relatedIdentifierType'),
                        "containerIdentifier": container_identifier_dc,
                        "type": resourceType
                    })
            print(f"Starting OpenAlex retrieval for {publisher_name}.\n")
            openalex = retrieve_all_data_openalex(url_openalex, params_openalex)
            for item in openalex:
                doi = item.get('doi')
                title = item.get('title')
                publication_year = item.get('publication_year')
                source_display_name = item.get('primary_location', {}).get('source', {}).get('display_name')
                
                for authorship in item.get('authorships', []):
                    if authorship.get('author_position') == 'first':
                        author_name = authorship.get('author', {}).get('display_name')
                        institutions = [inst.get('display_name') for inst in authorship.get('institutions', [])]
                        
                        data_select_openalex.append({
                            'doi_article': doi,
                            'title_article': title,
                            'publication_year': publication_year,
                            'journal': source_display_name,
                            'first_author': author_name,
                            'first_affiliation': institutions
                        })
        except Exception as e:
            print(f"An error occurred for publisher {publisher_name}: {e}")
            continue  # Skip to the next iteration

    df_datacite_initial = pd.json_normalize(data_select_datacite)
    
    if countVersions:
        ##These steps will count different versions as distinct datasets and remove the 'parent' (redundant with most recent version)
        df_datacite_initial['base'] = df_datacite_initial['doi'].apply(lambda x: x.split('.v')[0])
        df_datacite_initial['version'] = df_datacite_initial['doi'].apply(lambda x: int(x.split('.v')[1]) if '.v' in x else 0)
        max_versions = df_datacite_initial.groupby('base')['version'].max().reset_index()
        df_datacite_initial = df_datacite_initial.merge(max_versions, on='base', suffixes=('', '_max'))
        df_deduplicated = df_datacite_initial(subset='base')
    else:
        ##This step will remove all child deposits with a .v*  to retain only the 'parent'
        df_deduplicated = df_datacite_initial[~df_datacite_initial['doi'].str.contains(r'\.v\d+$')]

    df_datacite_supplement = df_deduplicated[df_deduplicated['relationType'] == "IsSupplementTo"]
    #mediated workflow sometimes creates individual deposit for each file, want to treat as single dataset here
    df_datacite_supplement_dedup = df_datacite_supplement.drop_duplicates(subset='relatedIdentifier', keep="first")
    
    df_openalex = pd.json_normalize(data_select_openalex)
    df_openalex['relatedIdentifier'] = df_openalex['doi_article'].str.replace('https://doi.org/', '')

    #output all UT linked deposits, no deduplication (for Figshare validator workflow)
    df_openalex_datacite = pd.merge(df_openalex, df_datacite_supplement, on='relatedIdentifier', how="left")
    df_openalex_datacite = df_openalex_datacite[df_openalex_datacite['doi'].notnull()]
    df_openalex_datacite.to_csv(f"outputs/{todayDate}_figshare-discovery-all.csv", index=False)

    #working with deduplicated dataset for rest of process
    df_openalex_datacite_dedup = pd.merge(df_openalex, df_datacite_supplement_dedup, on='relatedIdentifier', how="left")
    new_figshare = df_openalex_datacite_dedup[df_openalex_datacite_dedup['doi'].notnull()]
    new_figshare.to_csv(f"outputs/{todayDate}_figshare-discovery-deduplicated.csv", index=False)
    new_figshare = new_figshare[["doi","publicationYear","title", "first_author", "first_affiliation", "type"]]

    #standardizing licenses
    # new_figshare['rights'] = new_figshare['rights'].apply(lambda x: ' '.join(x) if isinstance(x, list) else x).astype(str).str.strip('[]')
    # new_figshare['rights_standardized'] = 'Rights unclear'  #default value
    # new_figshare.loc[new_figshare['rights'].str.contains('Creative Commons Zero|CC0'), 'rights_standardized'] = 'CC0'
    # new_figshare.loc[new_figshare['rights'].str.contains('Creative Commons Attribution Non Commercial Share Alike'), 'rights_standardized'] = 'CC BY-NC-SA'
    # new_figshare.loc[new_figshare['rights'].str.contains('Creative Commons Attribution Non Commercial'), 'rights_standardized'] = 'CC BY-NC'
    # new_figshare.loc[new_figshare['rights'].str.contains('Creative Commons Attribution 3.0|Creative Commons Attribution 4.0|Creative Commons Attribution-NonCommercial'), 'rights_standardized'] = 'CC BY'
    # new_figshare.loc[new_figshare['rights'] == '', 'rights_standardized'] = 'Rights unclear'

    #adding in columns to reconcatenate with full dataset
    new_figshare['first_affiliation'] = new_figshare['first_affiliation'].apply(lambda x: ' '.join(x) if isinstance(x, list) else x)    
    new_figshare['uni_lead'] = new_figshare['first_affiliation'].str.contains(uni_identifier, case=False, na=False).map({True: 'affiliated', False: 'not affiliated'})
    new_figshare['repository'] = "figshare"
    new_figshare['source'] = "DataCite+" #slight differentiation from records only retrieved from DataCite
    new_figshare['repository2'] = "figshare"
    new_figshare["non_TDR_IR"] = "not university or TDR"
    new_figshare['US_federal'] = "not federal US repo"
    new_figshare['GREI'] = "GREI member"

    df_all_repos_plus = pd.concat([df_all_repos, new_figshare], ignore_index=True)
    #de-duplicating in case some DOIs were caught twice (for the few publishers that do cross-walk affiliation metadata), you could use a sorting method to determine which one to 'keep'; the default will retain the ones returned from the main workflow
    df_all_repos_plus_dedup = df_all_repos_plus.drop_duplicates(subset='doi', keep="first")
    df_all_repos_plus_dedup.to_csv(f"outputs/{todayDate}_full-concatenated-dataframe-plus.csv")
    df_all_repos_plus.to_csv(f"outputs/{todayDate}_full-concatenated-dataframe-plus-figshare.csv", index=False)

### This codeblock identifies publishers known to create figshare deposits (can be any object resource type) with a ".s00*" system, finds affiliated articles, constructs a hypothetical figshare DOI for them, and tests its existence ###
# !! Warning: Depending on the number of articles, this can be an extremely time-intensive process !! #

if figshareWorkflow3:
    #toggle to select which indexer to use: 'OpenAlex' or 'Crossref'
    indexer = "OpenAlex"

    #OpenAlex params
    j = 0
    page_limit_openalex = config['VARIABLES']['PAGE_LIMITS']['openalex']

    #Crossref params
    k = 0
    page_limit_crossref = config['VARIABLES']['PAGE_LIMITS']['crossref']
    params_crossref_journal = {
        'select': 'DOI,prefix,title,author,container-title,publisher,created',
        'filter': 'type:journal-article',
        'rows': config['VARIABLES']['PAGE_SIZES']['crossref'],
        'query': 'affiliation:University+Texas+Austin',
        'mailto': config['EMAIL']['user_email'],
        'cursor': '*',
    }

    params_openalex = {
        'filter': 'authorships.institutions.ror:https://ror.org/00hj54h04,locations.source.host_organization:https://openalex.org/P4310315706', #PLOS ID in OpenAlex
        'per-page': config['VARIABLES']['PAGE_SIZES']['openalex'],
        'select': 'id,doi,title,authorships,primary_location,type',
        'mailto': config['EMAIL']['user_email']
    }

    #JSON dictionary of journals for Crossref API query (PLOS in this example)
    with open('journal_list.json', 'r') as file:
        journal_list = json.load(file)

    if indexer == "OpenAlex":
        openalex = retrieve_all_data_openalex(url_openalex, params_openalex)
        df_openalex = pd.json_normalize(openalex)
        df_openalex['hypothetical_dataset'] = df_openalex['doi'] + '.s001'
        
        #Check if each DOI with suffix redirects to a real page and create a new column
        df_openalex['Valid'] = df_openalex['hypothetical_dataset'].apply(check_link)
        df_openalex.to_csv(f"outputs/{todayDate}_openalex-articles-with-hypothetical-deposits.csv", index=False)
        print(f'Number of valid datasets: {len(df_openalex)}.')
    else:
        crossref_data = retrieve_all_journals(url_crossref_issn, journal_list)

        data_journals_select = []
        for item in crossref_data:
            publisher = item.get('publisher', None)
            journal = item.get('container-title', None)[0]
            doi = item.get('DOI', "")
            title_list = item.get('title', [])
            title = title_list[0] if title_list else None
            author = item.get('author', None)
            created = item.get('created', {})
            createdDate = created.get('date-time', None)
            
            data_journals_select.append({
                'publisher': publisher,
                'journal': journal, 
                'doi': doi,
                'author': author,
                'title': title,
                'published': createdDate,
        })

        df_crossref = pd.DataFrame(data_journals_select)
        df_crossref['doi_html'] = "https://doi.org/" + df_crossref['doi']
        df_crossref['hypothetical_dataset'] = df_crossref['doi_html'] + '.s001'

        # Check if each DOI with suffix redirects to a real page and create a new column
        df_crossref['Valid'] = df_crossref['hypothetical_dataset'].apply(check_link)
        df_crossref.to_csv(f"outputs/{todayDate}_crossref-articles-with-hypothetical-deposits.csv", index=False)
        print(f'Number of valid datasets: {len(df_crossref)}.')

#### This codeblock takes discovered Figshare deposits and retrieves additional metadata through the Figshare API to assess the object classification ####
if figshareValidator:
    #for reading in previously generated file of all associated datasets
    print("Reading in previous Figshare output file\n")
    directory = './outputs'
    pattern = '_figshare-discovery-all.csv'

    files = os.listdir(directory)
    files.sort(reverse=True)
    latest_file = None
    for file in files:
        if pattern in file:
            latest_file = file
            break

    if latest_file:
        file_path = os.path.join(directory, latest_file)
        figshare = pd.read_csv(file_path)
        print(f"The most recent file '{latest_file}' has been loaded successfully.")
    else:
        print(f"No file with '{pattern}' was found in the directory '{directory}'.")

    print(f"Number of Figshare datasets to query: {len(figshare)}\n")
    #extracting deposit ID from DOI
    figshare['id'] = figshare['doi'].str.extract(r'figshare\.(\d+)')

    print("Retrieving additional Figshare metadata from Figshare API\n")
    results = []
    for id in figshare['id']:
        try:
            response = requests.get(url_figshare.format(id=id))
            if response.status_code == 200:
                print(f"Retrieving {id}\n")
                results.append({'id': id, 'data': response.json()})
            else:
                print(f"Error retrieving {id}: {response.status_code}, {response.text}")
        except requests.exceptions.RequestException as e:
            print(f"Timeout error on ID {id}: {e}")

    # Extract 'name', 'size', and unique 'mimetype' from the API response
    data_figshare_select = []
    for item in results:
        id = item.get('id')
        dataset = item.get('data', [])
        mimetypes_set = set()
        for file_info in dataset:
            mimetypes_set.add(file_info.get('mimetype'))
            data_figshare_select.append({
                "id": id,
                "name": file_info.get('name'),
                "size": file_info.get('size'),
                "mimeTypeSet": mimetypes_set,
                "mimeType": file_info.get('mimetype'),
            })

    df_figshare_metadata = pd.DataFrame(data_figshare_select)
    format_map = config['FORMAT_MAP']
    df_figshare_metadata['fileFormat'] = df_figshare_metadata['mimeType'].apply(lambda x: format_map.get(x, x))
    df_figshare_metadata['fileFormatsSet'] = df_figshare_metadata['mimeTypeSet'].apply(lambda x: '; '.join([format_map.get(fmt, fmt) for fmt in x]) if x != "no files" else "no files")

    extension_criteria = {
    '.csv': 'CSV',
    '.doc': 'MS Word',
    '.m': 'MATLAB Script',
    '.ppt': 'MS PowerPoint',
    '.R': 'R Script',
    '.rds': 'R Data File',
    '.xls': 'MS Excel'
    }

    # Apply criteria to replace fileFormat entry based on file extension and mimetype
    def apply_extension_criteria(row):
        for ext, fmt in extension_criteria.items():
            if row['name'].endswith(ext) and row['mimeType'] == 'text/plain':
                return fmt
            if row['name'].endswith(ext) and row['mimeType'] == 'application/CDFV2':
                return fmt
            if row['name'].endswith(ext) and row['mimeType'] == 'application/x-xz':
                return fmt
        return row['fileFormat']

    df_figshare_metadata['editedFileFormat'] = df_figshare_metadata.apply(apply_extension_criteria, axis=1)
    # df_figshare_metadata.to_csv(f"outputs/{todayDate}_figshare-discovery-all-metadata.csv", index=False)

    #combines all file types for one deposit ('id') into semi-colon-delimited string
    df_figshare_metadata_unified = df_figshare_metadata.groupby('id')['editedFileFormat'].apply(lambda x: '; '.join(set(x))).reset_index()
    #alphabetically orders file formats
    df_figshare_metadata_unified['ordered_formats'] = df_figshare_metadata_unified['formats'].apply(lambda x: '; '.join(sorted(x.split('; '))))

    #basic assessment of 'dataset' classification
    ##list of strings for software formats to check for
    software = ["MATLAB Script", "R Script", "Python", "Shell Script"]
    ##create two new columns for software detection
    df_figshare_metadata_unified['onlySoftware'] = df_figshare_metadata_unified['ordered_formats'].apply(lambda x: x if x in software else '')
    df_figshare_metadata_unified['containsSoftware'] = df_figshare_metadata_unified['ordered_formats'].apply(lambda x: any(s in x for s in software))
    ##list of formats that are less likely to be data to check for
    notData = ["MS Word", "PDF", "MS Word; PDF"]
    # Create a new column with 'Suspect' values
    df_figshare_metadata_unified['possiblyNotData'] = df_figshare_metadata_unified['ordered_formats'].apply(lambda x: 'Suspect' if x in notData else '')
    df_figshare_metadata_combined = pd.merge(figshare, df_figshare_metadata_unified, on='id', how="left")
    df_figshare_metadata_combined.to_csv(f"outputs/{todayDate}_figshare-discovery-all-metadata_combined.csv", index=False)

##### NCBI Bioproject #####
if ncbiWorkflow:
    print("Starting NCBI process.\n")

    #set path for browser
    script_dir = os.path.dirname(os.path.abspath(__file__))
    outputs_dir = os.path.join(script_dir, 'outputs')

    #check if previous output file exists
    directory = './outputs'
    pattern = 'bioproject_result'

    files = os.listdir(directory)
    for file in files:
        if pattern in file:
            existingOutput = True
            print(f"A previous '{pattern}' download was found in the directory '{directory}'.")
            break
    else:
        existingOutput = False
        print(f"No file with '{pattern}' was found in the directory '{directory}'.")

    #read in config file
    if not loadNCBIdata:
        institution_name = config['INSTITUTION']['name']
        #URL encode name
        encoded_institution_name = quote(institution_name)

        #set up temporary Firefox 'profile' to direct downloads (not saved outside of script)
        options = Options()
        options.set_preference("browser.download.folderList", 2)
        options.set_preference("browser.download.dir", outputs_dir)
        options.set_preference("browser.helperApps.neverAsk.saveToDisk", "application/octet-stream")
        #blocking pop-up window to cancel download
        options.set_preference("browser.download.manager.showWhenStarting", False)
        options.set_preference("browser.download.manager.focusWhenStarting", False)
        options.set_preference("browser.download.useDownloadDir", True)
        options.set_preference("browser.download.manager.alertOnEXEOpen", False)
        options.set_preference("browser.download.manager.closeWhenDone", True)
        options.set_preference("browser.download.manager.showAlertOnComplete", False)
        options.set_preference("browser.download.manager.useWindow", False)
        options.set_preference("services.sync.prefs.sync.browser.download.manager.showWhenStarting", False)
        options.set_preference("browser.download.alwaysOpenPanel", False)  # Disable the download panel
        options.set_preference("browser.download.panel.shown", False)  # Ensure the download panel is not shown

        #initialize Selenium WebDriver
        driver = webdriver.Firefox(options=options)
        ##searches all fields; searching Submitter Organization does not recover all results
        ncbi_url = f"https://www.ncbi.nlm.nih.gov/bioproject?term={encoded_institution_name}"
        driver.get(ncbi_url)

        try:
            #Load page and find the "Send to" dropdown
            send_to_link = WebDriverWait(driver, 20).until(EC.element_to_be_clickable((By.ID, "sendto")))
            send_to_link.click()

            #Load dropdown and select "File" radio button
            file_option = WebDriverWait(driver, 20).until(EC.element_to_be_clickable((By.ID, "dest_File")))
            file_option.click()

            #Load "Format" dropdown and select "XML"
            format_dropdown = WebDriverWait(driver, 20).until(EC.element_to_be_clickable((By.ID, "file_format")))
            format_dropdown.click()
            xml_option = WebDriverWait(driver, 20).until(EC.element_to_be_clickable((By.XPATH, "//option[@value='xml']")))
            xml_option.click()

            #click the "Create File" button
            create_file_button = WebDriverWait(driver, 20).until(EC.element_to_be_clickable((By.XPATH, "//button[@cmd='File']")))
            create_file_button.click()

            print("Download complete, about to close window.\n")
            time.sleep(10)

            #overwrite any existing file with 'bioproject_result.xml' rather than continually creating new version with (*) appended in filename (e.g., bioproject_result(1).xml)
            ##will delete previous one and then rename the one with (*) appended
            if existingOutput:
                downloaded_file = max([os.path.join(outputs_dir, f) for f in os.listdir(outputs_dir)], key=os.path.getctime)
                target_file = os.path.join(outputs_dir, "bioproject_result.xml")
                if os.path.exists(target_file):
                    os.remove(target_file)
                    print(f"Deleted existing file: {target_file}")
                os.rename(downloaded_file, target_file)
                print(f"Renamed {downloaded_file} to {target_file}")

        except TimeoutException:
            print("Element not found or not clickable within the specified time.")

        finally:
            #close WebDriver
            driver.quit()

    #read in XML file (required regardless of whether you downloaded version in this run or not)
    print("Loading previously generated XML file.\n")
    with open(f'{outputs_dir}/bioproject_result.xml', 'r', encoding='utf-8') as file:
        data = file.read()

    #wrapping in a root element for parsing
    wrapped_data = f"<root>{data}</root>"
    #parse the wrapped content
    root = ET.fromstring(wrapped_data)

    #select certain fields from XML
    def filter_ncbi(doc):
        data_select = {}
        project = doc.find('Project')
        if project is not None:
            project_id = project.find('ProjectID')
            if project_id is not None:
                archive_id = project_id.find('ArchiveID')
                if archive_id is not None:
                    data_select['doi'] = archive_id.get('accession') #this is not a DOI but will be aligned with DOI column in main dataframe
                    data_select['repository'] = archive_id.get('archive')
                    data_select['ID'] = archive_id.get('id')
                center_id = project_id.find('CenterID')
                if center_id is not None:
                    data_select['Center'] = center_id.get('center')
                    data_select['CenterName'] = center_id.text
            
            project_descr = project.find('ProjectDescr')
            if project_descr is not None:
                name = project_descr.find('Name')
                if name is not None:
                    data_select['Name'] = name.text
                title = project_descr.find('Title')
                if title is not None:
                    data_select['title'] = title.text
                description = project_descr.find('Description')
                if description is not None:
                    data_select['Description'] = description.text

        submission = doc.find('Submission')
        if submission is not None:
            data_select['LastUpdate'] = submission.get('last_update')
            data_select['SubmissionID'] = submission.get('submission_id')
            data_select['Submitted'] = submission.get('submitted')

            organization = submission.find('.//Organization/Name')
            if organization is not None:
                data_select['Affiliation'] = organization.text

        return data_select

    #cxtract data from each element and store in a list
    data_list = []
    for doc in root.findall('DocumentSummary'):
        data_list.append(filter_ncbi(doc))

    #dataframe conversion and standardization for alignment with main dataframe
    ncbi = pd.DataFrame(data_list)
    ncbi['publicationYear'] = pd.to_datetime(ncbi['Submitted']).dt.year
    ##look for one of the permutation strings listed in config.json
    ncbi['first_affiliation'] = ncbi.apply(lambda row: next((perm for perm in ut_variations if perm in row['Affiliation']), None), axis=1)

    ##removing hits that have one of the keywords in a different field like the title
    ncbi_df_select = ncbi[ncbi['Affiliation'].str.contains(uni_identifier)]
    ncbi_df_select = ncbi_df_select[["repository","doi", "publicationYear", "title","first_affiliation"]]   
    ##adding columns for alignment with main dataframe
    ncbi_df_select['first_author'] = "Not specified"
    ncbi_df_select['uni_lead'] = ncbi_df_select['first_affiliation'].str.contains(uni_identifier, case=False, na=False).map({True: 'affiliated', False: 'not affiliated'})
    ncbi_df_select['source'] = "NCBI"
    ncbi_df_select['type'] = "Dataset"
    ncbi_df_select['repository2'] = "NCBI"
    ncbi_df_select["non_TDR_IR"] = "not university or TDR"
    ncbi_df_select['US_federal'] = "Federal US repo"
    ncbi_df_select['GREI'] = "not GREI member"
    # ncbi_df_select['rights'] = "Rights unclear"
    # ncbi_df_select['rights_standardized'] = "Rights unclear"

    if loadPreviousDataPlus:
        #for reading in previously generated file of all associated datasets
        print("Reading in previous DataCite+ output file\n")
        directory = './outputs' 
        pattern = '_full-concatenated-dataframe-plus-figshare.csv'

        files = os.listdir(directory)
        files.sort(reverse=True)
        latest_file = None
        for file in files:
            if pattern in file:
                latest_file = file
                break

        if latest_file:
            file_path = os.path.join(directory, latest_file)
            df_all_repos_plus = pd.read_csv(file_path)
            print(f"The most recent file '{latest_file}' has been loaded successfully.")
        else:
            print(f"No file with '{pattern}' was found in the directory '{directory}'.")

    if loadPreviousData:
        df_all_repos_plus_ncbi = pd.concat([df_all_repos, ncbi_df_select], ignore_index=True)
        df_all_repos_plus_ncbi.to_csv(f"outputs/{todayDate}_full-concatenated-dataframe-plus-ncbi.csv", index=False)
    elif loadPreviousDataPlus:
        df_all_repos_plus_ncbi = pd.concat([df_all_repos_plus, ncbi_df_select], ignore_index=True)
        df_all_repos_plus_ncbi.to_csv(f"outputs/{todayDate}_full-concatenated-dataframe-plus-figshare-ncbi.csv", index=False)
    elif not loadPreviousData and not loadPreviousData and not figshareWorkflow2:
        df_all_repos_plus_ncbi = pd.concat([df_all_repos, ncbi_df_select], ignore_index=True)
        df_all_repos_plus_ncbi.to_csv(f"outputs/{todayDate}_full-concatenated-dataframe-plus-ncbi.csv", index=False)
    elif not loadPreviousData and not loadPreviousData and figshareWorkflow2:
        df_all_repos_plus_ncbi = pd.concat([df_all_repos_plus, ncbi_df_select], ignore_index=True)
        df_all_repos_plus_ncbi.to_csv(f"outputs/{todayDate}_full-concatenated-dataframe-plus-figshare-ncbi.csv", index=False)

    ncbi_df_select.to_csv(f"outputs/{todayDate}_NCBI-select-output-aligned.csv", index=False)

print("Done.\n")
print(f"Time to run: {datetime.now() - startTime}")