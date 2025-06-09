import requests
import pandas as pd
import json
import math
import os
from datetime import datetime

#operator for quick test runs
test = False
#operator for resource type(s) to query for (use '|' for Boolean OR)
resourceType = 'dataset'
#setting timestamp to calculate run time
startTime = datetime.now() 
#creating variable with current date for appending to filenames
todayDate = datetime.now().strftime('%Y%m%d') 

#read in config file
with open('config.json', 'r') as file:
    config = json.load(file)
#load institutional name permutations
ut_variations = config['PERMUTATIONS']
#load institution string for filenames
institution = config['INSTITUTION']['filename']

#creating directories
if test:
    if os.path.isdir('test'):
        print('test directory found - no need to recreate')
    else:
        os.mkdir('test')
        print('test directory has been created')
    os.chdir('test')
    if os.path.isdir('accessory-outputs'):
        print('test accessory outputs directory found - no need to recreate')
    else:
        os.mkdir('accessory-outputs')
        print('test accessory outputs directory has been created')
else:
    if os.path.isdir('accessory-outputs'):
        print('accessory outputs directory found - no need to recreate')
    else:
        os.mkdir('accessory-outputs')
        print('accessory outputs directory has been created')

url_crossref = "https://api.crossref.org/works?"
page_limit_crossref = config['VARIABLES']['PAGE_LIMITS']['crossref_test'] if test else config['VARIABLES']['PAGE_LIMITS']['crossref_prod']
params_crossref = {
    'filter': f'type:{resourceType}',
    'rows': config['VARIABLES']['PAGE_SIZES']['crossref'], 
    'query.affiliation': "university+of+texas+austin",
    'cursor': '*',
    'mailto': config['EMAIL']['user_email'] #to access polite pool
}

#retrieves single page of results
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
    k = 0

    all_data_crossref = []
    data = retrieve_page_crossref(url, params)
    next_cursor = params['cursor']
    previous_cursor = None
    
    if not data['message']['items']:
        print("No data found.")
        return all_data_crossref

    all_data_crossref.extend(data['message']['items'])
    
    while k < page_limit_crossref:

        # data = retrieve_page_crossref(url, params=params)
        next_cursor = data.get('message', {}).get('next-cursor', None)
        if next_cursor is None:
            print("Retrieval complete, ending Crossref API query.")
            break
        params['cursor'] = next_cursor
        data = retrieve_page_crossref(url, params=params)
        total_count = data.get('message', {}).get('total-results', 0)
        total_pages = math.ceil(total_count / params['rows'])
        print(f'Retrieving page {k+1} of {total_pages}')
        
        all_data_crossref.extend(data['message']['items'])
        k += 1
    
    return all_data_crossref
#determines which author (first vs. last or both) is affiliated
def determine_affiliation(row):
    if row['first_author'] == row['last_author']:
        return 'single author'

    first_affiliated = any(variation in (row['first_affiliation'] or '') for variation in ut_variations)
    last_affiliated = any(variation in (row['last_affiliation'] or '') for variation in ut_variations)

    if first_affiliated and last_affiliated:
        return 'both lead and senior'
    elif first_affiliated and not last_affiliated:
        return 'only lead'
    elif last_affiliated and not first_affiliated:
        return 'only senior'
    else:
        return 'neither lead nor senior'

print("Starting Crossref retrieval")
data_crossref = retrieve_all_data_crossref(url_crossref, params_crossref)

data_select_crossref = [] 
for item in data_crossref:
    publisher = item.get('publisher', None)
    doi = item.get('DOI', None)
    title_list = item.get('title', [])
    title = title_list[0] if title_list else None
    indexed = item.get('indexed', {})
    indexed_date_str = indexed.get('date-time', None)
    indexed_year = (
        datetime.fromisoformat(indexed.get('date-time').replace("Z", "")).year
        if indexed.get('date-time') else None
    )
    type = item.get('type', None)
    source = item.get('source', None)
    score = item.get('score', 0)
    authors = item.get('author', [])
    authorNames = [f"{author.get('family', '')}, {author.get('given', '')}"  for author in authors]
    authorNamesStr = '; '.join(authorNames)
    authorAffiliations = ['; '.join([affiliation.get('name', '') for affiliation in author.get('affiliation', [])]) if author.get('affiliation', []) else '' for author in authors]    
    first_author = authorNames[0] if authorNames else None
    last_author = authorNames[-1] if authorNames else None
    first_affiliation = authorAffiliations[0] if authorAffiliations else None
    last_affiliation = authorAffiliations[-1] if authorAffiliations else None
    data_select_crossref.append({
        'repository': publisher,
        'doi': doi,
        'title': title,
        'publicationYear': indexed_year,
        'type': type,
        'source': source,
        'authors': authorNames,
        'affiliations': authorAffiliations,
        'first_author': first_author,
        'first_affiliation': first_affiliation,
        'last_author': last_author,
        'last_affiliation': last_affiliation,
        'score': score
    })

df_data_select_crossref = pd.DataFrame(data_select_crossref)
df_data_select_crossref_deduplicated = df_data_select_crossref.drop_duplicates(subset='doi', keep='first')

#creating column for source of detected affiliation
pattern = '|'.join([f'({perm})' for perm in ut_variations])
df_data_select_crossref_deduplicated['affiliation_source'] = df_data_select_crossref_deduplicated.apply(
    lambda row: 'affiliation' if pd.Series(row['affiliations']).str.contains(pattern, case=False, na=False).any()
    else ('author' if pd.Series(row['authors']).str.contains(pattern, case=False, na=False).any()
    else None), axis=1)
df_data_select_crossref_deduplicated['affiliation_permutation'] = df_data_select_crossref_deduplicated['affiliations'].apply(
    lambda affs: next((p for p in ut_variations if any(p in aff for aff in affs)), None)
)

df_data_select_crossref_deduplicated.to_csv(f"accessory-outputs/{todayDate}_crossref-all-objects.csv", index=False)

#removing anything that doesn't actually have some form of 'UT Austin'
df_data_select_crossref_true = df_data_select_crossref_deduplicated[df_data_select_crossref_deduplicated['affiliation_permutation'].notna()].copy()
#standardizing platform names
df_data_select_crossref_true.loc[df_data_select_crossref_true['repository'].str.contains('H1 Connect', case=False), 'repository'] = 'H1 Connect (Faculty Opinions)'
df_data_select_crossref_true.loc[df_data_select_crossref_true['repository'].str.contains('Faculty Opinions', case=False), 'repository'] = 'H1 Connect (Faculty Opinions)'
df_data_select_crossref_true.to_csv(f"accessory-outputs/{todayDate}_crossref-{institution}-objects.csv", index=False)

# get summary counts of repositories (key: primary_location.source.display_name)
repo_count = df_data_select_crossref_true['repository'].value_counts()
print("Counts for full data")
print(repo_count)

#restricting to only columns found in DataCite output and only repositories that are actual data
df_data_select_crossref_pruned = df_data_select_crossref_true[['repository', 'doi', 'publicationYear', 'title', 'first_author', 'first_affiliation', 'last_author', 'last_affiliation', 'source', 'type']] 
#adding columns for harmonizing with DataCite output
df_data_select_crossref_pruned['uni_lead'] = df_data_select_crossref_pruned.apply(determine_affiliation, axis=1)
df_data_select_crossref_pruned['repository2'] = 'Other'
df_data_select_crossref_pruned['non_TDR_IR'] = 'not university or TDR'
df_data_select_crossref_pruned['US_federal'] = 'not federal US repo'
df_data_select_crossref_pruned['GREI'] = 'not GREI member'

#will need to be customized for a different institution, although some are likely to recur (e.g., H1, Authorea)
df_data_select_crossref_pruned_repos = df_data_select_crossref_pruned[~df_data_select_crossref_pruned['repository'].str.contains('H1 Connect|Authorea|NumFOCUS|Exploration Geophysicists|College of Radiology')]

df_data_select_crossref_pruned_repos.to_csv(f"accessory-outputs/{todayDate}_crossref-{institution}-true-datasets.csv", index=False)

print('\nDone.\n')
print(f'Time to run: {datetime.now() - startTime}')