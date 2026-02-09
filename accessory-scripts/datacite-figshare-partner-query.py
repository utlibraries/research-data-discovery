from datetime import datetime
import pandas as pd
import json
import os
import requests
import sys

#call functions from parent utils.py file
utils_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, utils_dir) 
from utils import retrieve_datacite, retrieve_openalex 

#read in config file
parent = os.path.abspath(os.path.join(os.getcwd(), '..'))
with open(f'{parent}/config.json', 'r') as file:
    config = json.load(file)

#operator for quick test runs
test = config['TOGGLES']['test']
#toggles for executing Figshare validator (see README for details)
figshare_validator = True
#setting timestamp to calculate run time
start_time = datetime.now() 
#creating variable with current date for appending to filenames
today_date = datetime.now().strftime('%Y%m%d') 

#define publisher in variables that can be dynamically updated (e.g., filenames, params)
publisher = 'Taylor & Francis'
publisher_filename = 'taylor_francis'

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
url_datacite = 'https://api.datacite.org/dois'
url_figshare = 'https://api.figshare.com/v2/articles/{id}/files?page_size=10'
url_openalex = 'https://api.openalex.org/works'

##per page
per_page_datacite = config['VARIABLES']['PAGE_SIZES']['datacite']

#define different number of pages to retrieve from DataCite API based on 'test' vs. 'prod' env
page_limit_datacite = config['VARIABLES']['PAGE_LIMITS']['datacite_test'] if test else config['VARIABLES']['PAGE_LIMITS']['datacite_prod']
page_limit_openalex = config['VARIABLES']['PAGE_LIMITS']['openalex_test'] if test else config['VARIABLES']['PAGE_LIMITS']['openalex_prod']

#define variables to be called recursively in function
page_start_datacite = config['VARIABLES']['PAGE_STARTS']['datacite']

#figshare DOIs sometimes have a .v* for version number; this toggles whether to include them (True) or only include the parent (False)
countVersions = False

#pull in map of publisher names and OpenAlex codes
publisher_mapping = config['FIGSHARE_PARTNERS']
#create empty object to store results
data_select_datacite = [] 
data_select_openalex = []

#update both params for each publisher in map
params_openalex = {
    'filter': 'authorships.institutions.ror:https://ror.org/00hj54h04,type:article,from_publication_date:2000-01-01,locations.source.host_organization:https://openalex.org/P4310320547',
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
    'query': f'publisher:"{publisher}" AND publicationYear:[2021 TO 2025]',
    'page[size]': config['VARIABLES']['PAGE_SIZES']['datacite'],
    'page[cursor]': 1,
}
page_start_datacite = config['VARIABLES']['PAGE_STARTS']['datacite'] #reset to 0 (default) after large-scale general retrieval through DataCite
per_page_datacite = config['VARIABLES']['PAGE_SIZES']['datacite']

print(f'Starting DataCite retrieval.\n')
data_datacite = retrieve_datacite(url_datacite, params_datacite_figshare, page_start_datacite, page_limit_datacite, per_page_datacite)
print(f'Number of datasets found by DataCite API: {len(data_datacite)}\n')
for item in data_datacite:
    attributes = item.get('attributes', {})
    doi_dc = attributes.get('doi', None)
    publisher_dc = attributes.get('publisher', '')
    publisher_year_dc = attributes.get('publicationYear', '')
    title_dc = attributes.get('titles', [{}])[0].get('title', '')
    creators_dc = attributes.get('creators', [{}])
    affiliations_dc = [affiliation.get('name', '') for creator in creators_dc for affiliation in creator.get('affiliation', [{}])]
    related_identifiers = attributes.get('relatedIdentifiers', [])
    container_dc = attributes.get('container', {})
    container_identifier_dc = container_dc.get('identifier', None)
    types = attributes.get('types', {})
    resourceType = types.get('resourceTypeGeneral', '')

    for rel in related_identifiers: #'explodes' deposits with multiple relatedIdentifiers
        data_select_datacite.append({
            'doi': doi_dc,
            'publisher': publisher_dc,
            'publication_year': publisher_year_dc,
            'title': title_dc,
            'creators': creators_dc,
            'affiliations': affiliations_dc,
            'relation_type': rel.get('relationType'),
            'related_identifier': rel.get('relatedIdentifier'),
            'related_identifier_type': rel.get('relatedIdentifierType'),
            'container_identifier': container_identifier_dc,
            'type': resourceType
        })
print(f'Starting OpenAlex retrieval.\n')
openalex = retrieve_openalex(url_openalex, params_openalex, page_limit_openalex)
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

df_datacite_supplement = df_deduplicated[df_deduplicated['relation_type'] == 'IsSupplementTo']
#mediated workflow sometimes creates individual deposit for each file, want to treat as single dataset here
df_datacite_supplement_dedup = df_datacite_supplement.drop_duplicates(subset='related_identifier', keep='first')

df_openalex = pd.json_normalize(data_select_openalex)
df_openalex['related_identifier'] = df_openalex['doi_article'].str.replace('https://doi.org/', '')

#output all UT linked deposits, no deduplication (for Figshare validator workflow)
##this will keep all deposits, even if there are many separate ones that are all related to the same article
df_openalex_datacite = pd.merge(df_openalex, df_datacite_supplement, on='related_identifier', how='left')
df_openalex_datacite = df_openalex_datacite[df_openalex_datacite['doi'].notnull()]
df_openalex_datacite.to_csv(f'accessory-outputs/{today_date}_{publisher_filename}_figshare-discovery-all.csv', index=False)

#if you want the number of unique articles with at least one Figshare deposit
df_openalex_datacite_dedup = pd.merge(df_openalex, df_datacite_supplement_dedup, on='related_identifier', how='left')
new_figshare = df_openalex_datacite_dedup[df_openalex_datacite_dedup['doi'].notnull()]
new_figshare = new_figshare[['doi','publication_year_y','title', 'first_author', 'first_affiliation', 'type']]
new_figshare['first_affiliation'] = new_figshare['first_affiliation'].apply(lambda x: ' '.join(x) if isinstance(x, list) else x)    
new_figshare.to_csv(f'accessory-outputs/{today_date}_{publisher_filename}_figshare-discovery-deduplicated.csv', index=False)

if figshare_validator:
    #for reading in previously generated file of all associated datasets
    print('Reading in previous Figshare output file\n')
    directory = './accessory-outputs'
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
        print(f'The most recent file "{latest_file}" has been loaded successfully.')
    else:
        print(f'No file with "{pattern}" was found in the directory "{directory}".')
    
    print(f'Number of Figshare datasets to query: {len(figshare)}\n')

    #extracting deposit ID from DOI
    figshare['id'] = figshare['doi'].str.extract(r'figshare\.(\d+)')

    print('Retrieving additional Figshare metadata from Figshare API\n')
    results = []
    for id in figshare['id']:
        try:
            response = requests.get(url_figshare.format(id=id))
            if response.status_code == 200:
                print(f'Retrieving {id}\n')
                results.append({'id': id, 'data': response.json()})
            else:
                print(f'Error retrieving {id}: {response.status_code}, {response.text}')
        except requests.exceptions.RequestException as e:
            print(f'Timeout error on ID {id}: {e}')

    data_figshare_select = []
    for item in results:
        id = item.get('id')
        dataset = item.get('data', [])
        mime_types_set = set()
        for file_info in dataset:
            mime_types_set.add(file_info.get('mimetype'))
            data_figshare_select.append({
                'id': id,
                'name': file_info.get('name'),
                'size': file_info.get('size'),
                'mime_type_set': mime_types_set,
                'mime_type': file_info.get('mimetype'),
            })

    df_figshare_metadata = pd.DataFrame(data_figshare_select)
    format_map = config['FORMAT_MAP']
    df_figshare_metadata['file_format'] = df_figshare_metadata['mime_type'].apply(lambda x: format_map.get(x, x))
    df_figshare_metadata['file_formats_set'] = df_figshare_metadata['mime_type_set'].apply(lambda x: ('no files' if x == 'no files' else '; '.join( [str(format_map.get(fmt,fmt)) for fmt in x if format_map.get(fmt, fmt) is not None or fmt is not None])))

    #some mime_types are not correct and need to be adjusted
    extension_criteria = {
        '.csv': 'CSV',
        '.doc': 'MS Word',
        '.m': 'MATLAB Script',
        '.ppt': 'MS PowerPoint',
        '.R': 'R Script',
        '.rds': 'R Data File',
        '.xls': 'MS Excel'
    }

    # Apply criteria to replace file_format entry based on file extension and mime_type
    def apply_extension_criteria(row):
        for ext, fmt in extension_criteria.items():
            if row['name'].endswith(ext) and row['mime_type'] == 'text/plain':
                return fmt
            if row['name'].endswith(ext) and row['mime_type'] == 'application/CDFV2':
                return fmt
            if row['name'].endswith(ext) and row['mime_type'] == 'application/x-xz':
                return fmt
        return row['file_format']

    df_figshare_metadata['edited_file_format'] = df_figshare_metadata.apply(apply_extension_criteria, axis=1)
    df_figshare_metadata.to_csv(f'accessory-outputs/{today_date}_{publisher_filename}_figshare-discovery-all_metadata.csv', index=False)

    #combines all file types for one deposit ('id') into semi-colon-delimited string
    df_figshare_metadata_unified = df_figshare_metadata.groupby('id')['edited_file_format'].apply(lambda x: '; '.join(sorted(set([fmt for fmt in x if fmt is not None]))) if any(fmt is not None for fmt in x) else 'no files').reset_index()
    df_figshare_metadata_unified['ordered_formats'] = df_figshare_metadata_unified['edited_file_format'].apply(lambda x: '; '.join(sorted(x.split('; '))) if x != 'no files' else 'no files')

    #basic assessment of 'dataset' classification
    ##list of strings for software formats to check for
    software_formats = set(config['SOFTWARE_FORMATS'].values())
    ##create two new columns for software detection
    df_figshare_metadata_unified['only_software'] = df_figshare_metadata_unified['ordered_formats'].apply(lambda x: x if x in software_formats else '')
    df_figshare_metadata_unified['contains_software'] = df_figshare_metadata_unified['ordered_formats'].apply(lambda x: any(s in x for s in software_formats))
    df_figshare_metadata_combined = pd.merge(figshare, df_figshare_metadata_unified, on='id', how='left')
    ##list of formats that are less likely to be data to check for
    not_data = ['', 'undefined', 'MS PowerPoint', 'MS Word', 'PDF', 'MS Word; PDF']
    ##create new column that flags objects labeled as 'dataset' that might not be datasets
    df_figshare_metadata_combined['possibly_not_data'] = df_figshare_metadata_combined.apply(lambda row: 'Suspect' if row['ordered_formats'] in not_data and row['type'] == 'Dataset' else '',axis=1)
    ##create new column of objects not labeled as 'dataset' that might be datasets
    df_figshare_metadata_combined['possibly_data'] = df_figshare_metadata_combined.apply(lambda row: 'Candidate' if row['ordered_formats'] not in not_data and row['type'] !='Dataset' else '',axis=1)
    df_figshare_metadata_combined.to_csv(f'accessory-outputs/{today_date}_{publisher_filename}_figshare-discovery-all_metadata_combined.csv', index=False)

print('Done.\n')
print(f'Time to run: {datetime.now() - start_time}')