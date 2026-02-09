import os
import json
import pandas as pd
import requests
from datetime import datetime

#setting timestamp to calculate run time
start_time = datetime.now() 
#creating variable with current date for appending to filenames
today_date = datetime.now().strftime('%Y%m%d') 

#API urls
url_crossref = "https://api.crossref.org/works/"
url_datacite = 'https://api.datacite.org/dois'

#read in config file
parent = os.path.abspath(os.path.join(os.getcwd(), '..'))
with open(f'{parent}/config.json', 'r') as file:
    config = json.load(file)
institution = config['INSTITUTION']['filename']

#for reading in previously generated file of all discovered datasets
##this includes datasets published through mediated workflows in which 'figshare' is not listed as the 'publisher'
print('Reading in previous Figshare output file\n')
script_dir = os.getcwd()
parent_dir = os.path.abspath(os.path.join(os.getcwd(), '..'))
outputs_dir = os.path.join(parent_dir, 'outputs')
pattern = '_full-concatenated-dataframe'

files = os.listdir(outputs_dir)
files = [f for f in files if pattern in f]
files.sort(reverse=True)

#handles possibility that user has not run any processes beyond core process
##if a file with ''_full-concatenated-dataframe-plus...' is found, that one is preferred
contains_but_not_ends = [f for f in files if not f.endswith(pattern)]
ends_with_pattern = [f for f in files if f.endswith(pattern)]

##sorting by modification time
contains_but_not_ends.sort(key=lambda x: os.path.getmtime(os.path.join(outputs_dir, x)), reverse=True)
ends_with_pattern.sort(key=lambda x: os.path.getmtime(os.path.join(outputs_dir, x)), reverse=True)

# Choose the most appropriate file
latest_file = None
if contains_but_not_ends:
    latest_file = contains_but_not_ends[0]
elif ends_with_pattern:
    latest_file = ends_with_pattern[0]

if latest_file:
    file_path = os.path.join(outputs_dir, latest_file)
    df = pd.read_csv(file_path)
    print(f'The most recent file "{latest_file}" has been loaded successfully.')
else:
    print(f'No file with "{pattern}" was found in the directory "{outputs_dir}".')

#filtering for Figshare datasets
figshare = df[df['repository'].str.contains('figshare')]
print(f'Number of Figshare datasets to query: {len(figshare)}\n')
#handles output from main workflow where related Figshare DOIs are combined in semi-colon-delimited string (just gets first DOI)
figshare['doi_check'] = figshare['doi'].str.split(';').str[0]

print('Retrieving additional DataCite metadata for affiliated Figshare deposits\n')
results = []
for doi in figshare['doi_check']:
    try:
        response = requests.get(f'{url_datacite}/{doi}')
        if response.status_code == 200:
            print(f'Retrieving {doi}\n')
            results.append(response.json())
        else:
            print(f'Error retrieving {doi}: {response.status_code}, {response.text}')
    except requests.exceptions.RequestException as e:
        print(f'Timeout error on DOI {doi}: {e}')

data_datacite_new = {
    'datasets': results
}
data_select_datacite_new = []
datasets = data_datacite_new.get('datasets', []) 

for item in datasets:
    data = item.get('data', {})
    attributes = data.get('attributes', {})
    doi = attributes.get('doi', None)
    state = attributes.get('state', None)
    publisher = attributes.get('publisher', '')
    registered = attributes.get('registered', '')
    if registered:
        publisher_year = datetime.fromisoformat(registered.rstrip('Z')).year
        publisher_date = datetime.fromisoformat(registered.rstrip('Z')).date()
    else:
        publisher_year = None
        publisher_date = None
    updated = attributes.get('updated', '')
    title = attributes.get('titles', [{}])[0].get('title', '')
    creators = attributes.get('creators', [{}])
    creators_names = [creator.get('name', '') for creator in creators]
    creators_affiliations = ['; '.join(creator.get('affiliation', [])) for creator in creators]
    first_creator = creators[0].get('name', None) if creators else None
    last_creator = creators[-1].get('name', None) if creators else None
    affiliations = [
        aff.get('name', '')
        for creator in creators
        for aff in (creator.get('affiliation') if isinstance(creator.get('affiliation'), list) else [])
        if isinstance(aff, dict)
    ]
    first_affiliation = affiliations[0] if affiliations else None
    last_affiliation = affiliations[-1] if affiliations else None
    contributors = attributes.get('contributors', [{}])
    contributors_names = [contributor.get('name', '') for contributor in contributors]
    contributors_affiliations = ['; '.join(contributor.get('affiliation', [])) for contributor in contributors]
    container = attributes.get('container', {})
    container_identifier = container.get('identifier', None)
    types = attributes.get('types', {})

    #only retrieving if relation_type is 'IsSupplementTo'
    related_identifiers = attributes.get('relatedIdentifiers', [])
    for identifier in related_identifiers:
        relation_type = identifier.get('relationType', '')
        related_identifier = identifier.get('relatedIdentifier', '')

        if relation_type == 'IsSupplementTo' and related_identifier:
            data_select_datacite_new.append({
                'doi': doi,
                'publisher': publisher,
                'publication_year': publisher_year,
                'publication_date': publisher_date,
                'updated_date': updated,
                'title': title,
                'creators_names': creators_names,
                'creators_affiliations': creators_affiliations,
                'contributors_names': contributors_names,
                'contributors_affiliations': contributors_affiliations,
                'relation_type': relation_type,
                'related_identifier': related_identifier
            })

df_datacite_new = pd.json_normalize(data_select_datacite_new)
df_datacite_new.to_csv(f"accessory-outputs/{today_date}_{institution}-affiliated-figshare-datasets-expanded-metadata.csv")

# df_figshare_supplemental = df_datacite_new.drop_duplicates(subset='related_identifier', keep="first")
# print(f'Number of DOIs to retrieve: {len(df_figshare_supplemental)}\n')

#retrieving metadata about related identifiers (linked articles) that were identified
print("Retrieving metadata about related articles from Crossref\n")
results = []
for doi in df_datacite_new['related_identifier']:
    try:
        response = requests.get(f'{url_crossref}/{doi}')
        if response.status_code == 200:
            print(f"Retrieving {doi}")
            print()
            results.append(response.json())
        else:
            print(f"Error fetching {doi}: {response.status_code}, {response.text}")
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
        'publisher': publisher,
        'journal': journal, 
        'doi': doi,
        'author': author,
        'title': title,
        'published': createdDate,
})
    
df_crossref = pd.json_normalize(data_figshare_crossref_select)
df_crossref.to_csv(f"accessory-outputs/{today_date}_{institution}-affiliated-figshare-associated-articles.csv")

#merge back with original dataset dataframe
df_crossref['related_identifier'] = df_crossref['doi']
df_joint = pd.merge(df_datacite_new, df_crossref, on="related_identifier", how="left")
df_joint.to_csv(f"accessory-outputs/{today_date}_{institution}-affiliated-figshare-associated-articles-merged.csv")

print(f"Time to run: {datetime.now() - start_time}")
print("Done.")