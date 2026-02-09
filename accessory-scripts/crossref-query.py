import pandas as pd
import json
import numpy as np
import os
import sys
from datetime import datetime

#call functions from parent utils.py file
utils_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, utils_dir) 
from utils import adjust_descriptive_count, count_words, determine_affiliation, retrieve_crossref 

#read in config file
parent = os.path.abspath(os.path.join(os.getcwd(), '..'))
with open(f'{parent}/config.json', 'r') as file:
    config = json.load(file)

#operator for quick test runs
test = config['TOGGLES']['test']
#operator for resource type(s) to query for (use '|' for Boolean OR)
resource_type = 'dataset'
#toggle to disable resource type filter
resource_filter = True
#setting timestamp to calculate run time
start_time = datetime.now() 
#creating variable with current date for appending to filenames
today_date = datetime.now().strftime('%Y%m%d') 

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
if resource_filter:
    params_crossref = {
        'filter': f'type:{resource_type}',
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

print("Starting Crossref retrieval.\n")
data_crossref = retrieve_crossref(url_crossref, params_crossref, page_limit_crossref)

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
    author_names = [f"{author.get('family', '')}, {author.get('given', '')}"  for author in authors]
    author_names_str = '; '.join(author_names)
    author_affiliations = ['; '.join([affiliation.get('name', '') for affiliation in author.get('affiliation', [])]) if author.get('affiliation', []) else '' for author in authors]    
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
    first_author = author_names[0] if author_names else None
    last_author = author_names[-1] if author_names else None
    first_affiliation = author_affiliations[0] if author_affiliations else None
    last_affiliation = author_affiliations[-1] if author_affiliations else None
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
        'publication_year': indexed_year,
        'publication_date': indexed_date_str,
        'title': title,
        'first_author': first_author,
        'first_affiliation': first_affiliation,
        'last_author': last_author,
        'last_affiliation': last_affiliation,
        'creators_names': author_names,
        'creators_affiliations': author_affiliations,
        'creators_formatted': authors_formatted,
        'contributors_names': 'No equivalent field', #filler to match DataCite, no equivalent field
        'contributors_affiliations': 'No equivalent field', #filler to match DataCite, no equivalent field
        'contributors_formatted': 'Not applicable', #filler to match DataCite, no equivalent field
        'relation_type': relation_type,
        'related_identifier':'No equivalent field', #filler to match DataCite, no equivalent field
        'container_identifier': 'No equivalent field', #filler to match DataCite, no equivalent field
        'type': type,
        'subjects': 'No keyword information', #filler to match DataCite, no equivalent field
        'deposit_size': 'No file size information', #filler to match DataCite, no equivalent field
        'formats': 'No file information', #filler to match DataCite, no equivalent field
        'file_count': 'No file information', #filler to match DataCite, no equivalent field
        'rights': license, #filler to match DataCite, no equivalent field
        'rights_code': licenseURL,
        'views': 'No metrics information', #filler to match DataCite, no equivalent field
        'downloads': 'No metrics information', #filler to match DataCite, no equivalent field
        'citations': citations, 
        'source': source,
        'affiliation_source': 'creator.affiliationName', #mirroring DataCite
        'affiliation_permutation': '', #setting to blank, populates later
        'had_partial_duplicate': 'Not applicable', #filler to match DataCite, no equivalent field
        'file_format': 'No file information', #filler to match DataCite, no equivalent field
        'contains_code': 'No file information', #filler to match DataCite, no equivalent field
        'only_code': 'No file information', #filler to match DataCite, no equivalent field 
        'doi_article': 'Not applicable', #filler to match Figshare workflow, no equivalent process or field
        'title_article': 'Not applicable', #filler to match Figshare workflow, no equivalent process or field
        'journal': 'Not applicable', #filler to match Figshare workflow, no equivalent process or field
        'related_identifier_type': 'Not applicable' #filler to match Figshare workflow, no equivalent process or field
    })

df_data_select_crossref = pd.DataFrame(data_select_crossref)
df_data_select_crossref_deduplicated = df_data_select_crossref.drop_duplicates(subset='doi', keep='first')

#creating column for source of detected affiliation
pattern = '|'.join([f'({perm})' for perm in ut_variations])
df_data_select_crossref_deduplicated['affiliation_source'] = df_data_select_crossref_deduplicated.apply(
    lambda row: 'affiliation' if pd.Series(row['creators_affiliations']).str.contains(pattern, case=False, na=False).any()
    else ('author' if pd.Series(row['creators_names']).str.contains(pattern, case=False, na=False).any()
    else None), axis=1)
df_data_select_crossref_deduplicated['affiliation_permutation'] = df_data_select_crossref_deduplicated['creators_affiliations'].apply(
    lambda affs: next((p for p in ut_variations if any(p in aff for aff in affs)), None)
)

#select metadata assessment
##titles
df_data_select_crossref_deduplicated['title_reformatted'] = df_data_select_crossref_deduplicated['title'].str.replace('_', ' ') #gets around text linked by underscores counting as 1 word
df_data_select_crossref_deduplicated['title_reformatted'] = df_data_select_crossref_deduplicated['title_reformatted'].str.lower()
df_data_select_crossref_deduplicated[['total_word_count_title', 'descriptive_word_count_title']] = df_data_select_crossref_deduplicated['title_reformatted'].apply(lambda x: pd.Series(count_words(x, nondescriptive_words)))

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

df_data_select_crossref_deduplicated.to_csv(f"accessory-outputs/{today_date}_crossref-all-objects.csv", index=False)

#removing anything that doesn't actually have some form of 'UT Austin'
df_data_select_crossref_true = df_data_select_crossref_deduplicated[df_data_select_crossref_deduplicated['affiliation_permutation'].notna()].copy()
#standardizing platform names
df_data_select_crossref_true.loc[df_data_select_crossref_true['repository'].str.contains('H1 Connect', case=False), 'repository'] = 'H1 Connect (Faculty Opinions)'
df_data_select_crossref_true.loc[df_data_select_crossref_true['repository'].str.contains('Faculty Opinions', case=False), 'repository'] = 'H1 Connect (Faculty Opinions)'
df_data_select_crossref_true.to_csv(f"accessory-outputs/{today_date}_crossref-{institution}-objects.csv", index=False)

# get summary counts of repositories (key: primary_location.source.display_name)
repo_count = df_data_select_crossref_true['repository'].value_counts()
print("Counts for full data")
print(repo_count)

#restricting to only columns found in DataCite output and only repositories that are actual data
# df_data_select_crossref_pruned = df_data_select_crossref_true[['repository', 'doi', 'publicationYear', 'publicationDate', 'title', 'creators_names', 'creators_affiliations', 'creators_formatted', 'contributors_names', 'contributors_affiliations', 'contributors_formatted', 'first_author', 'first_affiliation', 'last_author', 'last_affiliation', 'source', 'type']] 
df_data_select_crossref_pruned = df_data_select_crossref_true
#adding columns for harmonizing with DataCite output
df_data_select_crossref_pruned['uni_lead'] = df_data_select_crossref_pruned.apply(lambda row: determine_affiliation(row, ut_variations), axis=1)
df_data_select_crossref_pruned['repository2'] = 'Other'
df_data_select_crossref_pruned['non_TDR_IR'] = 'not university or TDR'
df_data_select_crossref_pruned['US_federal'] = 'not federal US repo'
df_data_select_crossref_pruned['GREI'] = 'not GREI member'
df_data_select_crossref_pruned['scope'] = np.where(df_data_select_crossref_pruned['repository'].str.contains('Dryad|figshare|Zenodo|Mendeley|Open Science Framework|4TU|ASU Library|Boise State|Borealis|Dataverse|Oregon|Princeton|University|Wyoming|DaRUS', case=False), 'Generalist', 'Specialist')
df_data_select_crossref_pruned['type_reclassified'] = 'Dataset'

#will need to be customized for a different institution, although some are likely to recur (e.g., H1, Authorea)
df_data_select_crossref_pruned_repos = df_data_select_crossref_pruned[~df_data_select_crossref_pruned['repository'].str.contains('H1 Connect|Wiley|NumFOCUS|Exploration Geophysicists|College of Radiology')]

df_data_select_crossref_pruned_repos.to_csv(f"accessory-outputs/{today_date}_crossref-{institution}-true-datasets.csv", index=False)

print('\nDone.\n')
print(f'Time to run: {datetime.now() - start_time}')