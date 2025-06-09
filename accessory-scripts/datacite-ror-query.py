from datetime import datetime
from urllib.parse import urlparse, parse_qs, quote
import pandas as pd
import json
import math
import numpy as np
import os
import requests
import re

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
    state = attributes.get('state', None)
    publisher = attributes.get('publisher', '')
    # publisher_year = attributes.get('publicationYear', '') #temporarily disabling due to Dryad metadata issue
    registered = attributes.get('registered', '')
    if registered:
        publisher_year = datetime.fromisoformat(registered[:-1]).year
        publisher_date = datetime.fromisoformat(registered[:-1]).date()
    else:
        publisher_year = None
        publisher_date = None
    title=attributes.get('titles', [{}])[0].get('title','')
    creators = attributes.get('creators', [{}])
    creatorsNames = [creator.get('name', '') for creator in creators]
    creatorsAffiliations = ['; '.join([aff['name'] for aff in creator.get('affiliation', [])]) for creator in creators]        
    first_creator = creators[0].get('name', None)
    last_creator = creators[-1].get('name', None)
    affiliations = [affiliation.get('name' '') for creator in creators for affiliation in creator.get('affiliation', [{}])]
    first_affiliation = affiliations[0] if affiliations else None
    last_affiliation = affiliations[-1] if affiliations else None
    contributors = attributes.get('contributors', [{}])
    contributorsNames = [contributor.get('name', '') for contributor in contributors]
    contributorsAffiliations = ['; '.join([aff['name'] for aff in contributor.get('affiliation', [])]) for contributor in contributors]        
    container = attributes.get('container', {})
    container_identifier = container.get('identifier', None)
    related_identifiers = attributes.get('relatedIdentifiers', [])
    for identifier in related_identifiers:
        relationType = identifier.get('relationType', '')
        relatedIdentifier = identifier.get('relatedIdentifier', '')
    types = attributes.get('types', {})
    resourceType = types.get('resourceTypeGeneral', '')
    sizes = attributes.get('sizes', [])
    cleaned_sizes = [int(re.sub(r'\D', '', size)) for size in sizes if re.sub(r'\D', '', size).isdigit()]
    total_size = sum(cleaned_sizes) if cleaned_sizes else 'No file size information'   
    formats_list = attributes.get('formats', [])
    formats = set(formats_list) if formats_list else 'No file information'    
    rights_list = attributes.get('rightsList', [])
    rights = [right['rights'] for right in rights_list if 'rights' in right] or ['Rights unspecified']
    rightsCode = [right['rightsIdentifier'] for right in rights_list if 'rightsIdentifier' in right] or ['Unknown']
    views = attributes.get('viewCount', 0)
    downloads = attributes.get('downloadCount', 0)
    citations = attributes.get('citationCount', 0)
    data_select_datacite.append({
        'doi': doi,
        'state': state,
        'publisher': publisher,
        'publicationYear': publisher_year,
        'publicationDate': publisher_date,
        'title': title,
        'first_author': first_creator,
        'last_author': last_creator,
        'first_affiliation': first_affiliation,
        'last_affiliation': last_affiliation,
        'creatorsNames': creatorsNames,
        'creatorsAffiliations': creatorsAffiliations,
        'contributorsNames': contributorsNames,
        'contributorsAffiliations': contributorsAffiliations,
        'relationType': relationType,
        'relatedIdentifier': relatedIdentifier,
        'containerIdentifier': container_identifier,
        'type': resourceType,
        'depositSize': total_size,
        'formats': formats,
        'rights': rights,
        'rightsCode': rightsCode,
        'views': views,
        'downloads': downloads,
        'citations': citations,
        'source': 'DataCite'
    })

df_datacite_initial = pd.json_normalize(data_select_datacite)
#handling malformatted publisher field for Zenodo
df_datacite_initial.loc[df_datacite_initial['doi'].str.contains('zenodo', case=False, na=False), 'publisher'] = 'Zenodo'
#handling Figshare partners
df_datacite_initial.loc[df_datacite_initial['publisher'].str.contains('Taylor & Francis|SAGE|The Royal Society|SciELO journals', case=True), 'publisher'] = 'figshare'
df_datacite_initial.to_csv(f"accessory-outputs/{todayDate}_datacite-ror-retrieval.csv")

### The below code is mostly duplicated from the main codebase but may not be used b/c it is unlikely that all of these repositories will be retrieved via a ROR-based query ###

figshare = df_datacite_initial[df_datacite_initial['doi'].str.contains('figshare')]
df_datacite_no_figshare = df_datacite_initial[~df_datacite_initial['doi'].str.contains('figshare')]
figshare_no_versions = figshare[~figshare['doi'].str.contains(r'\.v\d+$')]
#mediated workflow sometimes creates individual deposit for each file, want to treat as single dataset here
for col in figshare_no_versions.columns:
    if figshare_no_versions[col].apply(lambda x: isinstance(x, list)).any():
        figshare_no_versions[col] = figshare_no_versions[col].apply(lambda x: tuple(x) if isinstance(x, list) else x)
figshare_no_versions['hadPartialDuplicate'] = figshare_no_versions.duplicated(subset=['publisher', 'publicationDate', 'creatorsNames', 'creatorsAffiliations', 'type', 'relatedIdentifier'], keep=False)

#aggregating related entries together
# figshare_no_versions_combined = figshare_no_versions.groupby('relatedIdentifier').agg(lambda x: '; '.join(sorted(map(str, set(x))))).reset_index()
sum_columns = ['depositSize', 'views', 'citations', 'downloads']

def agg_func(column_name, column):
    if column_name in sum_columns:
        return 'sum'
    else:
        return lambda x: sorted(set(x))

agg_funcs = {col: agg_func(col, figshare_no_versions[col]) for col in figshare_no_versions.columns if col != 'relatedIdentifier'}

figshare_no_versions_combined = figshare_no_versions.groupby('relatedIdentifier').agg(agg_funcs).reset_index()
# Convert all list-type columns to comma-separated strings
for col in figshare_no_versions_combined.columns:
    if figshare_no_versions_combined[col].apply(lambda x: isinstance(x, list)).any():
        figshare_no_versions_combined[col] = figshare_no_versions_combined[col].apply(lambda x: '; '.join(map(str, x)))
figshare_deduplicated = figshare_no_versions_combined.drop_duplicates(subset='relatedIdentifier', keep='first')
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

#handling file-level DOI granularity (all Dataverse installations)
##may need to expand search terms if you find a Dataverse installation without 'Dataverse' in name
df_datacite_dedup = df_datacite_v2[~(df_datacite_v2['publisher'].str.contains('Dataverse|Texas Data Repository', case=False, na=False) & df_datacite_v2['containerIdentifier'].notnull())]
df_datacite_dedup = df_datacite_dedup[~(df_datacite_dedup['doi'].str.count('/') >= 3)]

#final sweeping dedpulication step, will catch a few odd edge cases that have been manually discovered
##mainly addresses hundreds of EMSL datasets that seem overly granularized (many deposits share all metadata other than DOI including detailed titles) - will not be relevant for all institutions
df_sorted = df_datacite_dedup.sort_values(by='doi')
df_datacite = df_sorted.drop_duplicates(subset=['title', 'first_author', 'relationType', 'relatedIdentifier', 'containerIdentifier'], keep='first')

#standardizing specific repository name that has three permutations; may not be relevant for other institutions
df_datacite.loc[df_datacite['publisher'].str.contains('Digital Rocks', case=False), 'publisher'] = 'Digital Porous Media Portal'
df_datacite.to_csv(f"accessory-outputs/{todayDate}_datacite-ror-retrieval-filtered.csv")
print(f"Number of ROR-affiliated datasets left after cleaning: {len(df_datacite)}\n")

print("Starting single-affiliation-string-based query")
params_datacite = {
    'affiliation': 'true',
    'query': f'(creators.affiliation.name:"The University of Texas at Austin") AND types.resourceTypeGeneral:"Dataset"',
    'page[size]': config['VARIABLES']['PAGE_SIZES']['datacite'],
    'page[cursor]': 1,
}

#define different number of pages to retrieve from DataCite API based on 'test' vs. 'prod' env
page_limit_datacite = config['VARIABLES']['PAGE_LIMITS']['datacite_test'] if test else config['VARIABLES']['PAGE_LIMITS']['datacite_prod']
#define variables to be called recursively in function
page_start_datacite = config['VARIABLES']['PAGE_STARTS']['datacite']

print("Starting DataCite retrieval based on affiliation.\n")
data_datacite = retrieve_all_data_datacite(url_datacite, params_datacite)
print(f"Number of official-UT-affiliated datasets found by DataCite API: {len(data_datacite)}\n")
data_select_datacite = [] 
for item in data_datacite:
    attributes = item.get('attributes', {})
    doi = attributes.get('doi', None)
    state = attributes.get('state', None)
    publisher = attributes.get('publisher', '')
    # publisher_year = attributes.get('publicationYear', '') #temporarily disabling due to Dryad metadata issue
    registered = attributes.get('registered', '')
    if registered:
        publisher_year = datetime.fromisoformat(registered[:-1]).year
        publisher_date = datetime.fromisoformat(registered[:-1]).date()
    else:
        publisher_year = None
        publisher_date = None
    title=attributes.get('titles', [{}])[0].get('title','')
    creators = attributes.get('creators', [{}])
    creatorsNames = [creator.get('name', '') for creator in creators]
    creatorsAffiliations = ['; '.join([aff['name'] for aff in creator.get('affiliation', [])]) for creator in creators]        
    first_creator = creators[0].get('name', None)
    last_creator = creators[-1].get('name', None)
    affiliations = [affiliation.get('name' '') for creator in creators for affiliation in creator.get('affiliation', [{}])]
    first_affiliation = affiliations[0] if affiliations else None
    last_affiliation = affiliations[-1] if affiliations else None
    contributors = attributes.get('contributors', [{}])
    contributorsNames = [contributor.get('name', '') for contributor in contributors]
    contributorsAffiliations = ['; '.join([aff['name'] for aff in contributor.get('affiliation', [])]) for contributor in contributors]        
    container = attributes.get('container', {})
    container_identifier = container.get('identifier', None)
    related_identifiers = attributes.get('relatedIdentifiers', [])
    for identifier in related_identifiers:
        relationType = identifier.get('relationType', '')
        relatedIdentifier = identifier.get('relatedIdentifier', '')
    types = attributes.get('types', {})
    resourceType = types.get('resourceTypeGeneral', '')
    sizes = attributes.get('sizes', [])
    cleaned_sizes = [int(re.sub(r'\D', '', size)) for size in sizes if re.sub(r'\D', '', size).isdigit()]
    total_size = sum(cleaned_sizes) if cleaned_sizes else 'No file size information'   
    formats_list = attributes.get('formats', [])
    formats = set(formats_list) if formats_list else 'No file information'    
    rights_list = attributes.get('rightsList', [])
    rights = [right['rights'] for right in rights_list if 'rights' in right] or ['Rights unspecified']
    rightsCode = [right['rightsIdentifier'] for right in rights_list if 'rightsIdentifier' in right] or ['Unknown']
    views = attributes.get('viewCount', 0)
    downloads = attributes.get('downloadCount', 0)
    citations = attributes.get('citationCount', 0)
    data_select_datacite.append({
        'doi': doi,
        'state': state,
        'publisher': publisher,
        'publicationYear': publisher_year,
        'publicationDate': publisher_date,
        'title': title,
        'first_author': first_creator,
        'last_author': last_creator,
        'first_affiliation': first_affiliation,
        'last_affiliation': last_affiliation,
        'creatorsNames': creatorsNames,
        'creatorsAffiliations': creatorsAffiliations,
        'contributorsNames': contributorsNames,
        'contributorsAffiliations': contributorsAffiliations,
        'relationType': relationType,
        'relatedIdentifier': relatedIdentifier,
        'containerIdentifier': container_identifier,
        'type': resourceType,
        'depositSize': total_size,
        'formats': formats,
        'rights': rights,
        'rightsCode': rightsCode,
        'views': views,
        'downloads': downloads,
        'citations': citations,
        'source': 'DataCite'
    })

df_datacite_initial = pd.json_normalize(data_select_datacite)
#handling malformatted publisher field for Zenodo
df_datacite_initial.loc[df_datacite_initial['doi'].str.contains('zenodo', case=False, na=False), 'publisher'] = 'Zenodo'
#standardizing specific repository name that has three permutations; may not be relevant for other institutions
df_datacite_initial.loc[df_datacite_initial['publisher'].str.contains('Digital Rocks', case=False), 'publisher'] = 'Digital Porous Media Portal'
df_datacite_initial.loc[df_datacite_initial['publisher'].str.contains('Environmental System Science Data Infrastructure for a Virtual Ecosystem', case=False), 'publisher'] = 'ESS-DIVE'
df_datacite_initial.loc[df_datacite_initial['publisher'].str.contains('Texas Data Repository|Texas Research Data Repository', case=False), 'publisher'] = 'Texas Data Repository'
df_datacite_initial.loc[df_datacite_initial['publisher'].str.contains('ICPSR', case=True), 'publisher'] = 'ICPSR'
df_datacite_initial.loc[df_datacite_initial['publisher'].str.contains('Environmental Molecular Sciences Laboratory', case=True), 'publisher'] = 'Environ Mol Sci Lab'
df_datacite_initial.loc[df_datacite_initial['publisher'].str.contains('BCO-DMO', case=True), 'publisher'] = 'Biol Chem Ocean Data Mgmt Office'
df_datacite_initial.loc[df_datacite_initial['publisher'].str.contains('Taylor & Francis|SAGE|The Royal Society|SciELO journals', case=True), 'publisher'] = 'figshare'
df_datacite_initial.loc[df_datacite_initial['publisher'].str.contains('Oak Ridge', case=True), 'publisher'] = 'Oak Ridge National Laboratory'
df_datacite_initial.loc[df_datacite_initial['publisher'].str.contains('PARADIM', case=True), 'publisher'] = 'PARADIM'
df_datacite_initial.loc[df_datacite_initial['publisher'].str.contains('4TU', case=True), 'publisher'] = '4TU.ResearchData'
#handling Figshare partners
df_datacite_initial.loc[df_datacite_initial['publisher'].str.contains('Taylor & Francis|SAGE|The Royal Society|SciELO journals', case=True), 'publisher'] = 'figshare'
df_datacite_initial.to_csv(f"accessory-outputs/{todayDate}_datacite-single-affiliation-retrieval.csv")

### The below code is mostly duplicated from the main codebase but may not be used b/c it is unlikely that all of these repositories will be retrieved via a ROR-based query ###

figshare = df_datacite_initial[df_datacite_initial['doi'].str.contains('figshare')]
df_datacite_no_figshare = df_datacite_initial[~df_datacite_initial['doi'].str.contains('figshare')]
figshare_no_versions = figshare[~figshare['doi'].str.contains(r'\.v\d+$')]
#mediated workflow sometimes creates individual deposit for each file, want to treat as single dataset here
for col in figshare_no_versions.columns:
    if figshare_no_versions[col].apply(lambda x: isinstance(x, list)).any():
        figshare_no_versions[col] = figshare_no_versions[col].apply(lambda x: tuple(x) if isinstance(x, list) else x)
figshare_no_versions['hadPartialDuplicate'] = figshare_no_versions.duplicated(subset=['publisher', 'publicationDate', 'creatorsNames', 'creatorsAffiliations', 'type', 'relatedIdentifier'], keep=False)

#aggregating related entries together
# figshare_no_versions_combined = figshare_no_versions.groupby('relatedIdentifier').agg(lambda x: '; '.join(sorted(map(str, set(x))))).reset_index()
sum_columns = ['depositSize', 'views', 'citations', 'downloads']

def agg_func(column_name, column):
    if column_name in sum_columns:
        return 'sum'
    else:
        return lambda x: sorted(set(x))

agg_funcs = {col: agg_func(col, figshare_no_versions[col]) for col in figshare_no_versions.columns if col != 'relatedIdentifier'}

figshare_no_versions_combined = figshare_no_versions.groupby('relatedIdentifier').agg(agg_funcs).reset_index()
# Convert all list-type columns to comma-separated strings
for col in figshare_no_versions_combined.columns:
    if figshare_no_versions_combined[col].apply(lambda x: isinstance(x, list)).any():
        figshare_no_versions_combined[col] = figshare_no_versions_combined[col].apply(lambda x: '; '.join(map(str, x)))
figshare_deduplicated = figshare_no_versions_combined.drop_duplicates(subset='relatedIdentifier', keep='first')
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

#handling file-level DOI granularity (all Dataverse installations)
##may need to expand search terms if you find a Dataverse installation without 'Dataverse' in name
df_datacite_dedup = df_datacite_v2[~(df_datacite_v2['publisher'].str.contains('Dataverse|Texas Data Repository', case=False, na=False) & df_datacite_v2['containerIdentifier'].notnull())]
df_datacite_dedup = df_datacite_dedup[~(df_datacite_dedup['doi'].str.count('/') >= 3)]

#final sweeping dedpulication step, will catch a few odd edge cases that have been manually discovered
##mainly addresses hundreds of EMSL datasets that seem overly granularized (many deposits share all metadata other than DOI including detailed titles) - will not be relevant for all institutions
df_sorted = df_datacite_dedup.sort_values(by='doi')
df_datacite = df_sorted.drop_duplicates(subset=['title', 'first_author', 'relationType', 'relatedIdentifier', 'containerIdentifier'], keep='first')

df_datacite.to_csv(f"accessory-outputs/{todayDate}_datacite-single-affiliation-retrieval-filtered.csv")
print(f"Number of official-UT-affiliated datasets left after cleaning: {len(df_datacite)}\n")

print("Done.\n")
print(f"Time to run: {datetime.now() - startTime}")