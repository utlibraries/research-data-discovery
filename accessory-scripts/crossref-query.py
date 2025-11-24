import requests
import pandas as pd
import json
import math
import numpy as np
import os
from datetime import datetime

#operator for quick test runs
test = False
#operator for resource type(s) to query for (use '|' for Boolean OR)
resourceType = 'dataset'
#toggle to disable resource type filter
resourceFilter = True
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
if resourceFilter:
    params_crossref = {
        'filter': f'type:{resourceType}',
        'rows': config['VARIABLES']['PAGE_SIZES']['crossref'], 
        'query.affiliation': "university+of+texas+austin",
        'cursor': '*',
        'mailto': config['EMAIL']['user_email'] #to access polite pool
    }
else:
    params_crossref = {
        'rows': config['VARIABLES']['PAGE_SIZES']['crossref'], 
        'query.affiliation': "university+of+texas+austin",
        'cursor': '*',
        'mailto': config['EMAIL']['user_email'] #to access polite pool
    }

#defining some metadata assessment objects
##assess 'descriptiveness of dataset title'
words = config['WORDS']
###add integers
numbers = list(map(str, range(1, 1000000)))
###combine all into a single set
nondescriptive_words = set(
    words['articles'] +
    words['conjunctions'] +
    words['prepositions'] +
    words['auxiliary_verbs'] +
    words['possessives'] +
    words['descriptors'] +
    words['order'] +
    words['version'] +
    numbers
)

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
    previous_cursor = None

    while k < page_limit_crossref:
        data = retrieve_page_crossref(url, params)
        items = data.get('message', {}).get('items', [])
        next_cursor = data.get('message', {}).get('next-cursor')
        total_count = data.get('message', {}).get('total-results', 0)
        total_pages = math.ceil(total_count / params['rows'])
        print(f'Retrieving page {k+1} of {total_pages}')
        if not items:
            print("No more items found.")
            break

        all_data_crossref.extend(items)
        previous_cursor = next_cursor
        params['cursor'] = next_cursor
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
    
#function to count descriptive words
def count_words(text):
    # If text is None, NaN, or not a string, treat as empty
    if not isinstance(text, str):
        return 0, 0
    words = text.split()
    total_words = len(words)
    descriptive_count = sum(1 for word in words if word not in nondescriptive_words)
    return total_words, descriptive_count

## account for when a single word may or may not be descriptive but is certainly uninformative if in a certain combination
def adjust_descriptive_count(row):
    title = row.get('title_reformatted', '')
    if not isinstance(title, str):
        title = ''
    title_lower = title.lower()
    if ('supplemental material' in title_lower or
        'supplementary material' in title_lower or
        'supplementary materials' in title_lower or
        'supplemental materials' in title_lower):
        count = row.get('descriptive_word_count_title', 0)
        return max(0, count - 1)
    return row.get('descriptive_word_count_title', 0)

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
    authors_formatted = []
    for author in authors:
        last = author.get('family', '').strip()
        first = author.get('given', '').strip()
        name = f"{last}, {first}" if last and first else last or first
        affiliations = author.get('affiliation', [])
        updated_affiliations = []
        for affil in affiliations:  # <-- Use the current author's affiliations
            affil_name = affil.get('name', '') if isinstance(affil, dict) else affil
            if 'Austin' in affil_name:
                affil_name = "University of Texas at Austin"
            updated_affiliations.append(affil_name)
        # Remove duplicates, if any
        unique_affiliations = list(dict.fromkeys(updated_affiliations))
        affil_str = ', '.join(unique_affiliations) if unique_affiliations else "No affiliation listed"
        authors_formatted.append(f"{name} ({affil_str})")
    authors_formatted.append(f"{name} ({affil_str})")
    first_author = authorNames[0] if authorNames else None
    last_author = authorNames[-1] if authorNames else None
    first_affiliation = authorAffiliations[0] if authorAffiliations else None
    last_affiliation = authorAffiliations[-1] if authorAffiliations else None
    relation = item.get('relation', {})
    relation_type = next(iter(relation.keys()), None)
    relation_id = None
    if relation_type:
        relation_list = relation.get(relation_type, [])
        if relation_list and isinstance(relation_list, list):
            first_relation = relation_list[0]
            related_identifiers = first_relation.get('id', None)
            id_type = first_relation.get('id-type', None)
    license = item.get('license', 'Rights unclear')
    licenseURL = [lic.get('URL', 'Rights unclear') for lic in license if isinstance(lic, dict)]
    citations = item.get('is-referenced-by-count', 0)
    data_select_crossref.append({
        'doi': doi,
        'state': 'findable', #mirroring DataCite, no equivalent field
        'repository': publisher,
        'publicationYear': indexed_year,
        'publicationDate': indexed_date_str,
        'title': title,
        'first_author': first_author,
        'first_affiliation': first_affiliation,
        'last_author': last_author,
        'last_affiliation': last_affiliation,
        'creatorsNames': authorNames,
        'creatorsAffiliations': authorAffiliations,
        'creatorsFormatted': authors_formatted,
        'contributorsNames': 'No equivalent field', #filler to match DataCite, no equivalent field
        'contributorsAffiliations': 'No equivalent field', #filler to match DataCite, no equivalent field
        'contributorsFormatted': 'Not applicable', #filler to match DataCite, no equivalent field
        'relationType': relation_type,
        'relatedIdentifier':related_identifiers, #filler to match DataCite, no equivalent field
        'containerIdentifier': 'No equivalent field', #filler to match DataCite, no equivalent field
        'type': type,
        'subjects': 'No keyword information', #filler to match DataCite, no equivalent field
        'depositSize': 'No file size information', #filler to match DataCite, no equivalent field
        'formats': 'No file information', #filler to match DataCite, no equivalent field
        'fileCount': 'No file information', #filler to match DataCite, no equivalent field
        'rights': license, #filler to match DataCite, no equivalent field
        'rightsCode': licenseURL,
        'views': 'No metrics information', #filler to match DataCite, no equivalent field
        'downloads': 'No metrics information', #filler to match DataCite, no equivalent field
        'citations': citations, 
        'source': source,
        'affiliation_source': 'creator.affiliationName', #mirroring DataCite
        'affiliation_permutation': '', #setting to blank, populates later
        'hadPartialDuplicate': 'Not applicable', #filler to match DataCite, no equivalent field
        'fileFormat': 'No file information', #filler to match DataCite, no equivalent field
        'containsCode': 'No file information', #filler to match DataCite, no equivalent field
        'onlyCode': 'No file information', #filler to match DataCite, no equivalent field 
        'doi_article': 'Not applicable', #filler to match Figshare workflow, no equivalent process or field
        'title_article': 'Not applicable', #filler to match Figshare workflow, no equivalent process or field
        'publication_year': 'Not applicable', #filler to match Figshare workflow, no equivalent process or field
        'journal': 'Not applicable', #filler to match Figshare workflow, no equivalent process or field
        'relatedIdentifierType': 'Not applicable' #filler to match Figshare workflow, no equivalent process or field
    })

df_data_select_crossref = pd.DataFrame(data_select_crossref)
df_data_select_crossref_deduplicated = df_data_select_crossref.drop_duplicates(subset='doi', keep='first')

#creating column for source of detected affiliation
pattern = '|'.join([f'({perm})' for perm in ut_variations])
df_data_select_crossref_deduplicated['affiliation_source'] = df_data_select_crossref_deduplicated.apply(
    lambda row: 'affiliation' if pd.Series(row['creatorsAffiliations']).str.contains(pattern, case=False, na=False).any()
    else ('author' if pd.Series(row['creatorsNames']).str.contains(pattern, case=False, na=False).any()
    else None), axis=1)
df_data_select_crossref_deduplicated['affiliation_permutation'] = df_data_select_crossref_deduplicated['creatorsAffiliations'].apply(
    lambda affs: next((p for p in ut_variations if any(p in aff for aff in affs)), None)
)

#select metadata assessment
##titles
df_data_select_crossref_deduplicated['title_reformatted'] = df_data_select_crossref_deduplicated['title'].str.replace('_', ' ') #gets around text linked by underscores counting as 1 word
df_data_select_crossref_deduplicated['title_reformatted'] = df_data_select_crossref_deduplicated['title_reformatted'].str.lower()
df_data_select_crossref_deduplicated[['total_word_count_title', 'descriptive_word_count_title']] = df_data_select_crossref_deduplicated['title_reformatted'].apply(lambda x: pd.Series(count_words(x)))

df_data_select_crossref_deduplicated['descriptive_word_count_title'] = df_data_select_crossref_deduplicated.apply(adjust_descriptive_count, axis=1)
df_data_select_crossref_deduplicated['nondescriptive_word_count_title'] = df_data_select_crossref_deduplicated['total_word_count_title'] - df_data_select_crossref_deduplicated['descriptive_word_count_title']

##licenses
###Note: most Crossref datasets don't have any licensing information
df_data_select_crossref_deduplicated['rights'] = (
    df_data_select_crossref_deduplicated['rights'].apply(lambda x: ' '.join([str(item) for item in x]) if isinstance(x, list)
                     else '' if isinstance(x, dict)
                     else str(x) if pd.notnull(x)
                     else '').str.strip('[]')
)
df_data_select_crossref_deduplicated['rights_standardized'] = 'Rights unclear'  #default value
df_data_select_crossref_deduplicated.loc[df_data_select_crossref_deduplicated['rights'].str.contains('Creative Commons Zero|CC0'), 'rights_standardized'] = 'CC0'
df_data_select_crossref_deduplicated.loc[df_data_select_crossref_deduplicated['rights'].str.contains('Creative Commons Attribution Non Commercial Share Alike'), 'rights_standardized'] = 'CC BY-NC-SA'
df_data_select_crossref_deduplicated.loc[df_data_select_crossref_deduplicated['rights'].str.contains('Creative Commons Attribution Non Commercial'), 'rights_standardized'] = 'CC BY-NC'
df_data_select_crossref_deduplicated.loc[df_data_select_crossref_deduplicated['rights'].str.contains('Creative Commons Attribution 3.0|Creative Commons Attribution 4.0|Creative Commons Attribution-NonCommercial'), 'rights_standardized'] = 'CC BY'
df_data_select_crossref_deduplicated.loc[df_data_select_crossref_deduplicated['rights'].str.contains('GNU General Public License'), 'rights_standardized'] = 'GNU GPL'
df_data_select_crossref_deduplicated.loc[df_data_select_crossref_deduplicated['rights'].str.contains('Apache License'), 'rights_standardized'] = 'Apache'
df_data_select_crossref_deduplicated.loc[df_data_select_crossref_deduplicated['rights'].str.contains('MIT License'), 'rights_standardized'] = 'MIT'
df_data_select_crossref_deduplicated.loc[df_data_select_crossref_deduplicated['rights'].str.contains('BSD'), 'rights_standardized'] = 'BSD'
df_data_select_crossref_deduplicated.loc[df_data_select_crossref_deduplicated['rights'].str.contains('ODC-BY'), 'rights_standardized'] = 'ODC-BY'
df_data_select_crossref_deduplicated.loc[df_data_select_crossref_deduplicated['rights'].str.contains('Open Access'), 'rights_standardized'] = 'Rights unclear'
df_data_select_crossref_deduplicated.loc[df_data_select_crossref_deduplicated['rights'].str.contains('Closed Access'), 'rights_standardized'] = 'Restricted access'
df_data_select_crossref_deduplicated.loc[df_data_select_crossref_deduplicated['rights'].str.contains('Restricted Access'), 'rights_standardized'] = 'Restricted access'
df_data_select_crossref_deduplicated.loc[df_data_select_crossref_deduplicated['rights'].str.contains('Databrary'), 'rights_standardized'] = 'Custom terms'
df_data_select_crossref_deduplicated.loc[df_data_select_crossref_deduplicated['rights'].str.contains('UCAR'), 'rights_standardized'] = 'Custom terms'
df_data_select_crossref_deduplicated.loc[df_data_select_crossref_deduplicated['rights'] == '', 'rights_standardized'] = 'Rights unclear'

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
# df_data_select_crossref_pruned = df_data_select_crossref_true[['repository', 'doi', 'publicationYear', 'publicationDate', 'title', 'creatorsNames', 'creatorsAffiliations', 'creatorsFormatted', 'contributorsNames', 'contributorsAffiliations', 'contributorsFormatted', 'first_author', 'first_affiliation', 'last_author', 'last_affiliation', 'source', 'type']] 
df_data_select_crossref_pruned = df_data_select_crossref_true
#adding columns for harmonizing with DataCite output
df_data_select_crossref_pruned['uni_lead'] = df_data_select_crossref_pruned.apply(determine_affiliation, axis=1)
df_data_select_crossref_pruned['repository2'] = 'Other'
df_data_select_crossref_pruned['non_TDR_IR'] = 'not university or TDR'
df_data_select_crossref_pruned['US_federal'] = 'not federal US repo'
df_data_select_crossref_pruned['GREI'] = 'not GREI member'
df_data_select_crossref_pruned['scope'] = np.where(df_data_select_crossref_pruned['repository'].str.contains('Dryad|figshare|Zenodo|Mendeley|Open Science Framework|4TU|ASU Library|Boise State|Borealis|Dataverse|Oregon|Princeton|University|Wyoming|DaRUS', case=False), 'Generalist', 'Specialist')
df_data_select_crossref_pruned['type_reclassified'] = 'Dataset'

#will need to be customized for a different institution, although some are likely to recur (e.g., H1, Authorea)
df_data_select_crossref_pruned_repos = df_data_select_crossref_pruned[~df_data_select_crossref_pruned['repository'].str.contains('H1 Connect|Wiley|NumFOCUS|Exploration Geophysicists|College of Radiology')]

df_data_select_crossref_pruned_repos.to_csv(f"accessory-outputs/{todayDate}_crossref-{institution}-true-datasets.csv", index=False)

print('\nDone.\n')
print(f'Time to run: {datetime.now() - startTime}')