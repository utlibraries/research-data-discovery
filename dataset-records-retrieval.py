from datetime import datetime
from pathlib import Path
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
#if you don't want to run the entire retrieval process (skip to Figshare steps)
loadPreviousData = False

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
url_openalex = 'https://api.openalex.org/works'
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

if not loadPreviousData:
    print("Starting DataCite retrieval based on affiliation.\n")
    data_datacite = retrieve_all_data_datacite(url_datacite, params_datacite)
    print(f"Number of datasets found by DataCite API: {len(data_datacite)}\n")

    if crossValidate:
        print("Starting Dryad retrieval.\n")
        data_dryad = retrieve_all_data_dryad(url_dryad, params_dryad)
        print(f"Number of Dryad datasets found by Dryad API: {len(data_dryad)}\n")
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
        publisher_year = attributes.get('publicationYear', "")
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

    #creating column for source of detected affiliation
    pattern = '|'.join([f'({perm})' for perm in ut_variations])
    #search for permutations in the 'affiliations' column
    df_datacite_initial['affiliation_source'] = df_datacite_initial.apply(
    lambda row: 'creator.affiliation' if pd.Series(row['creatorsAffiliations']).str.contains(pattern, case=False, na=False).any()
    else ('creator.name' if pd.Series(row['creatorsNames']).str.contains(pattern, case=False, na=False).any()
    else ('contributor.affiliation' if pd.Series(row['contributorsAffiliations']).str.contains(pattern, case=False, na=False).any()
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
    df_datacite.to_csv(f"outputs/{todayDate}_datacite-output-for-affiliation-source.csv") 

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

        #formatting Dataverse author names to be consistent with others
        df_dryad['first_author'] = df_dryad['first_author_last'] + ', ' + df_dryad['first_author_first']
        df_dryad['last_author'] = df_dryad['last_author_last'] + ', ' + df_dryad['last_author_first']
        df_dryad = df_dryad.drop(columns=['first_author_first', 'first_author_last', 
                            'last_author_first', 'last_author_last'])

        df_dryad['publicationYear'] = pd.to_datetime(df_dryad['publicationDate']).dt.year
        df_dataverse_pub_filtered['publicationYear'] = pd.to_datetime(df_dataverse_pub_filtered['publicationDate'], format='ISO8601').dt.year
        df_data_zenodo_real['publicationYear'] = pd.to_datetime(df_data_zenodo_real['publicationDate'], format='mixed').dt.year

        df_dryad_pruned = df_dryad[['doi', 'publicationYear', 'title', 'first_author', 'first_affiliation', 'last_author', 'last_affiliation']]
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
        print("Number of unique entries in Dataverse: " + repr(len(df_datacite_dataverse_combined_dedup))+ "\n")
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
        print("Number of unique entries in Zenodo: " + repr(len(df_datacite_zenodo_combined_dedup))+ "\n")
        df_datacite_zenodo_combined_dedup['repository2'] = 'Zenodo'
        df_datacite_zenodo_combined_dedup['UT_lead'] = df_datacite_zenodo_combined_dedup['first_affiliation'].str.contains('Austin', case=False, na=False).map({True: 'affiliated', False: 'not affiliated'})

    #remaining DataCite
    df_datacite_remainder_pruned = df_datacite_remainder_pruned.rename(columns={"title_dc": "title", 'first_author_dc': 'first_author', "first_affiliation_dc": "first_affiliation", "source_dc":"source", "type_dc":"type"})
    df_datacite_remainder_pruned_select = df_datacite_remainder_pruned[['repository', 'doi', 'publicationYear', 'title', 'first_author', 'first_affiliation', 'source', 'type']] 
    df_datacite_remainder_pruned_select['repository2'] = 'Other'
    df_datacite_remainder_pruned_select['first_affiliation'] = df_datacite_remainder_pruned_select['first_affiliation'].fillna('None')
    df_datacite_remainder_pruned_select['UT_lead'] = df_datacite_remainder_pruned_select['first_affiliation'].str.contains('Austin', case=False, na=False).map({True: 'affiliated', False: 'not affiliated'})

    # ##handling pseudo-duplication of EMSL (many distinct deposits related to single project)
    # emsl = df_datacite_remainder_pruned_select[df_datacite_remainder_pruned_select['repository'] == "Environmental Molecular Sciences Laboratory"]
    # df_datacite_remainder_pruned_no_emsl = df_datacite_remainder_pruned_select[df_datacite_remainder_pruned_select['repository'] != "Environmental Molecular Sciences Laboratory"]
    # emsl_deduplicated = emsl.drop_duplicates(subset=['title'],keep='first')
    # df_remainder_reconstructed = pd.concat([df_datacite_remainder_pruned_no_emsl, emsl_deduplicated], ignore_index=True)

    #final collation
    if crossValidate:
        df_all_repos = pd.concat([df_datacite_dryad_combined_dedup, df_datacite_dataverse_combined_dedup, df_datacite_zenodo_combined_dedup, df_datacite_remainder_pruned_select], ignore_index=True)
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
            df['UT_lead'] = df['first_affiliation'].str.contains('Austin', case=False, na=False).map({True: 'affiliated', False: 'not affiliated'})
        df_all_repos = pd.concat([df_datacite_dryad_pruned_select, df_datacite_dataverse_pruned_select, df_datacite_zenodo_pruned_dedup, df_datacite_remainder_pruned_select], ignore_index=True)

    #standardizing repositories with multiple versions of name in dataframe
    df_all_repos['repository'] = df_all_repos['repository'].fillna('None')
    df_all_repos.loc[df_all_repos['repository'].str.contains('Digital Rocks', case=False), 'repository'] = 'Digital Rocks Portal'
    df_all_repos.loc[df_all_repos['repository'].str.contains('Environmental System Science Data Infrastructure for a Virtual Ecosystem', case=False), 'repository'] = 'ESS-DIVE'
    df_all_repos.loc[df_all_repos['repository'].str.contains('Texas Data Repository', case=False), 'repository'] = 'Texas Data Repository'
    df_all_repos.loc[df_all_repos['repository'].str.contains('ICPSR', case=True), 'repository'] = 'ICPSR'
    df_all_repos.loc[df_all_repos['repository'].str.contains('Environmental Molecular Sciences Laboratory', case=True), 'repository'] = 'Environ Mol Sci Lab'

    #edge cases
    ##confusing metadata with UT Austin (but not Dataverse) listed as publisher; have to be manually adjusted over time
    df_all_repos.loc[(df_all_repos['doi'].str.contains('zenodo')) & (df_all_repos['repository'].str.contains('University of Texas')), 'repository'] = 'Zenodo' #10.5281/zenodo.10198511
    df_all_repos.loc[(df_all_repos['doi'].str.contains('zenodo')) & (df_all_repos['repository'].str.contains('University of Texas')), 'repository2'] = 'Zenodo' #10.5281/zenodo.10198511
    df_all_repos.loc[(df_all_repos['doi'].str.contains('10.11578/dc')) & (df_all_repos['repository'].str.contains('University of Texas')), 'repository'] = 'Department of Energy (DOE) CODE'

    ##other edge cases
    df_all_repos.loc[df_all_repos['doi'].str.contains('10.23729/547d8c47-3723-4396-8f84-322c02ccadd0'), 'repository'] = 'Finnish Fairdata' #labeled publisher as author's name

    #adding categorization
    df_all_repos['non-TDR IR'] = np.where(df_all_repos['repository'].str.contains('University|UCLA|UNC|Harvard|ASU|Dataverse', case=True), 'non-TDR institutional', 'not university or TDR')
    df_all_repos['US federal'] = np.where(df_all_repos['repository'].str.contains('NOAA|NIH|NSF|U.S.|DOE|DOD|DOI|National|Designsafe', case=True), 'Federal US repo', 'not federal US repo')
    df_all_repos['GREI'] = np.where(df_all_repos['repository'].str.contains('Dryad|figshare|Zenodo|Vivli|Mendeley|Open Science Framework', case=False), 'GREI member', 'not GREI member')

    df_all_repos.to_csv(f'outputs/{todayDate}_full-concatenated-dataframe.csv', index=False)

###### FIGSHARE WORKFLOW ######
#These sections are for cleaning up identified figshare deposits or identifying associated ones that lack affiliation metadata
figshareArticleLink = False
figshareWorkflow1 = True
figshareWorkflow2 = False

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
if figshareArticleLink:

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
        publisher_year_dc = attributes.get('publicationYear', "")
        title_dc = attributes.get('titles', [{}])[0].get('title', "")
        related_identifiers = attributes.get('relatedIdentifiers', [])
        types = attributes.get('types', {})
        resourceType = types.get('resourceTypeGeneral', '')
        for rel in related_identifiers: #'explodes' deposits with multiple relatedIdentifiers
            data_figshare_select.append({
                "doi": doi_dc,
                "publisher": publisher_dc,
                "publicationYear": publisher_year_dc,
                "title": title_dc,
                "relationType": rel.get('relationType'),
                "relatedIdentifier": rel.get('relatedIdentifier'),
                "relatedIdentifierType": rel.get('relatedIdentifierType'),
                "resourceType": resourceType
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
    df_datacite_crossref.to_csv(f"outputs/{todayDate}_figshare-datasets-with-article-info.csv")

### This codeblock will retrieve all figshare deposits with a listed journal/publisher as 'publisher,' extract related identifiers, retrieve all articles published by a certain publisher, cross-reference article DOIs against dataset related identifiers, and produce a match list. ###

if figshareWorkflow1:

    #figshare DOIs sometimes have a .v* for version number; this toggles whether to include them (True) or only include the parent (False)
    countVersions = False

    #OpenAlex params
    j = 0
    page_limit_openalex = config['VARIABLES']['PAGE_LIMITS']['openalex']

    params_openalex = {
        'filter': 'authorships.institutions.ror:https://ror.org/00hj54h04,locations.source.host_organization:https://openalex.org/P4310319787|https://openalex.org/P4310320547|https://openalex.org/P4310317820', #UT Austin; The Royal Society | Taylor & Francis | Karger Publishers
        'per-page': config['VARIABLES']['PAGE_SIZES']['openalex'],
        'select': 'id,doi,title,authorships,primary_location,type',
        'mailto': config['EMAIL']['user_email'] 
    }

    #DataCite params (different from general affiliation-based retrieval params)
    ## !! Warning: if you do not set a resourceType in the query (recommended if you want to get broad coverage), this will be a very large retrieval. In the test env, there may not be enough records to find a match with a university-affiliated article !!
    params_datacite_figshare = {
        'affiliation': 'true',
        'query': '(publisher:"The Royal Society" OR publisher:"Taylor & Francis" OR publisher:"Karger Publishers") AND types.resourceTypeGeneral:"Dataset"', #matches publishers in OpenAlex params    
        'page[size]': config['VARIABLES']['PAGE_SIZES']['datacite'],
        'page[cursor]': 1,
    }
    page_start_datacite = config['VARIABLES']['PAGE_STARTS']['datacite'] #reset to 0 (default) after large-scale general retrieval through DataCite

    print("Starting DataCite retrieval.\n")
    data_datacite = retrieve_all_data_datacite(url_datacite, params_datacite_figshare)
    print(f"Number of datasets found by DataCite API: {len(data_datacite)}\n")

    data_select_datacite = [] 
    for item in data_datacite:
        attributes = item.get('attributes', {})
        doi_dc = attributes.get('doi', None)
        publisher_dc = attributes.get('publisher', "")
        publisher_year_dc = attributes.get('publicationYear', "")
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
    df_datacite_supplement = df_datacite_supplement.drop_duplicates(subset='relatedIdentifier', keep="first")

    openalex = retrieve_all_data_openalex(url_openalex, params_openalex)
    data_select_openalex = []
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
    df_openalex = pd.json_normalize(data_select_openalex)
    df_openalex['relatedIdentifier'] = df_openalex['doi_article'].str.replace('https://doi.org/', '')

    df_openalex_datacite = pd.merge(df_openalex, df_datacite_supplement, on='relatedIdentifier', how="left")
    new_figshare = df_openalex_datacite[df_openalex_datacite['doi'].notnull()]
    new_figshare.to_csv(f"outputs/{todayDate}_figshare-discovery.csv")
    new_figshare = new_figshare[["doi","publicationYear","title", "first_author", "first_affiliation", "type"]]

    #adding in columns to reconcatenate with full dataset
    new_figshare['first_affiliation'] = new_figshare['first_affiliation'].apply(lambda x: ' '.join(x) if isinstance(x, list) else x)    
    new_figshare['UT_lead'] = new_figshare['first_affiliation'].str.contains('Austin', case=False, na=False).map({True: 'affiliated', False: 'not affiliated'})
    new_figshare['repository'] = "figshare"
    new_figshare['source'] = "DataCite+" #slight differentiation from main dataset
    new_figshare['repository2'] = "figshare"
    new_figshare["non-TDR IR"] = "not university or TDR"
    new_figshare['US federal'] = "not federal US repo"
    new_figshare['GREI'] = "GREI member"

    df_all_repos_plus = pd.concat([df_all_repos, new_figshare], ignore_index=True)
    df_all_repos_plus.to_csv(f"outputs/{todayDate}_full-concatenated-dataframe-plus.csv")

### This codeblock identifies publishers known to create figshare deposits (can be any object resource type) with a ".s00*" system, finds affiliated articles, constructs a hypothetical figshare DOI for them, and tests its existence ###
# !! Warning: Depending on the number of articles, this can be an extremely time-intensive process !! #

if figshareWorkflow2:
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
        df_openalex.to_csv(f"outputs/{todayDate}_openalex-articles-with-hypothetical-deposits.csv")
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
        df_crossref.to_csv(f"outputs/{todayDate}_crossref-articles-with-hypothetical-deposits.csv")
        print(f'Number of valid datasets: {len(df_crossref)}.')

print("Done.\n")
print(f"Time to run: {datetime.now() - startTime}")