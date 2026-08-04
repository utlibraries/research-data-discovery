import json
import math
import os
import pandas as pd
import requests
from pathlib import Path
from urllib.parse import urlparse, parse_qs

# Getting root directory
ROOT_DIR = Path(__file__).resolve().parent

# Loads secrets/dynamic values from .env and static values from config.json, merged into a
# single dict shaped like the original env.json (env['KEYS'], env['VARIABLES'], etc.)
def load_env_config():
    env = {}
    with open(ROOT_DIR / '.env', 'r', encoding='utf-8') as file:
        for line in file:
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            key, value = line.split('=', 1)
            value = value.strip()
            if len(value) >= 2 and value[0] == value[-1] and value[0] in ('"', "'"):
                value = value[1:-1]
            env[key] = json.loads(value)

    with open(ROOT_DIR / 'config.json', 'r', encoding='utf-8') as file:
        config = json.load(file)
    env.update(config)

    return env

### API retrieval functions ###

# Retrieves single page of Dryad results
def retrieve_page_dryad(url, params):
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        print(f'Error retrieving page: {e}')
        return {'_embedded': {'stash:datasets': []}, 'total': {}}
# Retrieves all pages of Dryad results
def retrieve_dryad(url, params, page_start, per_page):
    all_data_dryad = []
    params = params.copy()
    params['page'] = page_start
    params['per_page'] = per_page

    data = retrieve_page_dryad(url, params)
    total_count = data.get('total', 0)
    total_pages = math.ceil(total_count / per_page) if per_page else 1

    print(f'Total: {total_count} entries over {total_pages} pages\n')

    while True:
        print(f'Retrieving page {params["page"]} of {total_pages} from Dryad...\n')
        data = retrieve_page_dryad(url, params)

        if not data.get('_embedded'):
            print('No data found.')
            return all_data_dryad

        datasets = data['_embedded'].get('stash:datasets', [])
        all_data_dryad.extend(datasets)

        params['page'] += 1

        if not datasets or params['page'] > total_pages:
            print('End of Dryad response.\n')
            break

    return all_data_dryad

# Retrieves single page of DataCite results
def retrieve_page_datacite(url, params=None):
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        print(f'Error retrieving page: {e}')
        return {'data': [], 'links': {}}
# Retrieves all pages of DataCite results
def retrieve_datacite(url, params, page_start, page_limit, per_page):
    all_data_datacite = []
    current_page = page_start

    data = retrieve_page_datacite(url, params)
    if not data['data']:
        print('No data found.')
        return all_data_datacite

    all_data_datacite.extend(data['data'])

    total_count = data.get('meta', {}).get('total', 0)
    total_pages = math.ceil(total_count / per_page) if per_page else 1

    current_url = data.get('links', {}).get('next', None)

    while current_url and current_page < page_limit:
        current_page += 1
        print(f'Retrieving page {current_page} of {total_pages} from DataCite...\n')
        data = retrieve_page_datacite(current_url)
        if not data['data']:
            print('End of response.')
            break
        all_data_datacite.extend(data['data'])
        current_url = data.get('links', {}).get('next', None)

    return all_data_datacite
# Retrieves all pages of DataCite aggregate metadata
def retrieve_datacite_summary(url, params, publisher, affiliated, institution):
    all_resource_types = []
    all_licenses = []

    data = retrieve_page_datacite(url, params)
    if affiliated:
        print(f'Retrieving data for {publisher} for all deposits ({institution} only).\n')
    else:
        print(f'Retrieving data for {publisher} for all deposits.\n')

    if not data['meta']:
        print('No metadata found.')
        return all_resource_types, all_licenses

    resource_types = data['meta'].get('resourceTypes', [])
    licenses = data['meta'].get('licenses', [])
    
    for resource in resource_types:
        resource['publisher'] = publisher
    for license in licenses:
        license['publisher'] = publisher

    all_resource_types.extend(resource_types)
    all_licenses.extend(licenses)

    return all_resource_types, all_licenses

# Retrieves single page of Dataverse results
def retrieve_page_dataverse(url, params=None, headers=None):
    try:
        response = requests.get(url, params=params, headers=headers)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        print(f'Error retrieving page: {e}')
        return {'data': {'items': [], 'total_count': 0}}
# Retrieves all pages of DataCite results
def retrieve_dataverse(url, params, headers, page_start, per_page):
    all_data_dataverse = []
    params = params.copy()
    params['start'] = page_start
    params['page'] = 1

    while True:
        data = retrieve_page_dataverse(url, params, headers)
        total_count = data['data']['total_count']
        total_pages = math.ceil(total_count / per_page) if per_page else 1
        print(f'Retrieving page {params["page"]} of {total_pages} pages...\n')

        if not data['data']:
            print('No data found.')
            break

        all_data_dataverse.extend(data['data']['items'])

        params['start'] += per_page
        params['page'] += 1

        if params['start'] >= total_count:
            print('End of response.')
            break

    return all_data_dataverse

# Retrieves single page of Zenodo results
def retrieve_page_zenodo(url, params=None):
    try:
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        print(f'Error retrieving page: {e}')
        return {'hits': {'hits': [], 'total': {}}, 'links': {}}
# Retrieves page number in Zenodo query
def extract_page_number(url):
    parsed_url = urlparse(url)
    query_params = parse_qs(parsed_url.query)
    return query_params.get('page', [None])[0]
# Retrieves all pages of Zenodo results
def retrieve_zenodo(url, params, page_start, page_limit, per_page):
    all_data_zenodo = []
    current_page = page_start
    params = params.copy()
    params['page'] = current_page
    params['size'] = per_page

    data = retrieve_page_zenodo(url, params)
    if not data['hits']['hits']:
        print('No data found.')
        return all_data_zenodo

    all_data_zenodo.extend(data['hits']['hits'])

    current_url = data.get('links', {}).get('self', None)
    total_count = data.get('hits', {}).get('total', 0)
    total_pages = math.ceil(total_count / per_page) if per_page else 1
    print(f'Total: {total_count} entries over {total_pages} pages\n')

    while current_url and current_page < page_limit:
        print(f'Retrieving page {current_page} of {total_pages} from Zenodo...\n')
        current_page += 1
        data = retrieve_page_zenodo(current_url, {'access_token': params['access_token']})
        if not data['hits']['hits']:
            print('End of Zenodo response.\n')
            break

        all_data_zenodo.extend(data['hits']['hits'])
        current_url = data.get('links', {}).get('next', None)

    return all_data_zenodo

# Retrieves single page of OpenAlex results
def retrieve_page_openalex(url, params=None):
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        print(f'Error retrieving page: {e}')
        return {'results': [], 'meta': {}}
# Retrieves all pages of OpenAlex results
def retrieve_openalex(url, params, page_limit):
    all_data_openalex = []
    params = params.copy()
    params['cursor'] = '*'
    next_cursor = '*'
    previous_cursor = None
    current_page = 0

    data = retrieve_page_openalex(url, params)
    if not data['results']:
        print('No data found.')
        return all_data_openalex

    all_data_openalex.extend(data['results'])

    total_count = data.get('meta', {}).get('count', 0)
    per_page = data.get('meta', {}).get('per_page', 1)
    total_pages = math.ceil(total_count / per_page) + 1

    print(f'Total: {total_count} entries over {total_pages} pages\n')

    while current_page < page_limit:
        current_page += 1
        print(f'Retrieving page {current_page} of {total_pages} from OpenAlex...\n')
        data = retrieve_page_openalex(url, params)
        next_cursor = data.get('meta', {}).get('next_cursor', None)

        if next_cursor == previous_cursor:
            print('Cursor did not change. Ending loop to avoid infinite loop.')
            break

        if not data['results']:
            print('End of OpenAlex response.\n')
            break

        all_data_openalex.extend(data['results'])

        previous_cursor = next_cursor
        params['cursor'] = next_cursor

    return all_data_openalex

# Retrieves single page of Crossref results
def retrieve_page_crossref(url, params=None):
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        print(f'Error retrieving page: {e}')
        return {'message': {'items': [], 'total-results': {}}}
# Retrieves all pages of Crossref results
def retrieve_crossref(url, params, page_limit):
    all_data_crossref = []
    params = params.copy()
    params['cursor'] = '*'
    next_cursor = '*'
    previous_cursor = None
    current_page = 1

    data = retrieve_page_crossref(url, params)
    if not data['message']['items']:
        print('No data found.')
        return all_data_crossref

    all_data_crossref.extend(data['message']['items'])

    while current_page < page_limit:
        current_page += 1
        print(f'Retrieving page {current_page} from CrossRef...\n')
        data = retrieve_page_crossref(url, params)
        next_cursor = data.get('message', {}).get('next-cursor', None)

        if not data['message']['items']:
            print('Finished retrieval.\n')
            break

        all_data_crossref.extend(data['message']['items'])

        previous_cursor = next_cursor
        params['cursor'] = next_cursor

    return all_data_crossref
# Retrieves results for specified journals in Crossref API
def retrieve_all_journals(url_template, journal_list, params_crossref_journal, page_limit_crossref, retrieve_crossref_func):
    all_data = []
    for journal_name, issn in journal_list.items():
        print(f'Retrieving data from {journal_name} (ISSN: {issn})')
        custom_url = url_template.format(issn=issn)
        params = params_crossref_journal.copy()
        params['filter'] += f',issn:{issn}'
        journal_data = retrieve_crossref_func(custom_url, params, page_limit_crossref)
        all_data.extend(journal_data)
    return all_data

### Metadata cleaning / assessment functions ###

# Formats a list of person dicts (creators/contributors/authors) as 'Name (Affiliation)' strings,
# normalizing any 'Austin' affiliation to the full institution name
def format_people_with_affiliations(people, get_name=lambda p: p.get('name', '').strip(), affiliation_key='affiliation', dedupe_affiliations=False):
    formatted = []
    for person in people:
        name = get_name(person)
        affiliations = person.get(affiliation_key, [])
        updated_affiliations = []
        for affil in affiliations:
            affil_name = affil.get('name', '') if isinstance(affil, dict) else affil
            if 'Austin' in affil_name:
                affil_name = 'University of Texas at Austin'
            updated_affiliations.append(affil_name)
        if dedupe_affiliations:
            updated_affiliations = list(dict.fromkeys(updated_affiliations))
        affil_str = ', '.join(updated_affiliations) if updated_affiliations else 'No affiliation listed'
        formatted.append(f'{name} ({affil_str})')
    return formatted

# Maps free-text rights/license strings in a 'rights' column to a standardized license label
def standardize_rights(df, rights_column='rights'):
    rights_standardized = pd.Series('Rights unclear', index=df.index)
    rights = df[rights_column]
    rights_standardized[rights.str.contains('Creative Commons Zero|CC0')] = 'CC0'
    rights_standardized[rights.str.contains('Creative Commons Attribution Non Commercial Share Alike')] = 'CC BY-NC-SA'
    rights_standardized[rights.str.contains('Creative Commons Attribution Non Commercial')] = 'CC BY-NC'
    rights_standardized[rights.str.contains('Creative Commons Attribution 3.0|Creative Commons Attribution 4.0|Creative Commons Attribution-NonCommercial')] = 'CC BY'
    rights_standardized[rights.str.contains('GNU General Public License')] = 'GNU GPL'
    rights_standardized[rights.str.contains('Apache License')] = 'Apache'
    rights_standardized[rights.str.contains('MIT License')] = 'MIT'
    rights_standardized[rights.str.contains('BSD')] = 'BSD'
    rights_standardized[rights.str.contains('ODC-BY')] = 'ODC-BY'
    rights_standardized[rights.str.contains('Open Access')] = 'Rights unclear'
    rights_standardized[rights.str.contains('Closed Access')] = 'Restricted access'
    rights_standardized[rights.str.contains('Restricted Access')] = 'Restricted access'
    rights_standardized[rights.str.contains('Databrary')] = 'Custom terms'
    rights_standardized[rights.str.contains('UCAR')] = 'Custom terms'
    rights_standardized[rights == ''] = 'Rights unclear'
    return rights_standardized

# Conditionally folds an optional pipeline stage's dataframe into the running combined dataframe;
# no-op (and stage not recorded) if the stage produced nothing, so a skipped/failed stage is harmless
def merge_stage(df_current, stage_df, stage_name, completed_stages):
    if stage_df is not None and not stage_df.empty:
        df_current = pd.concat([df_current, stage_df], ignore_index=True)
        completed_stages.append(stage_name)
    return df_current, completed_stages

# Saves a pipeline stage's raw output under a fixed filename so a later run can skip re-fetching it
def save_stage_checkpoint(df, data_dir, stage_name):
    df.to_csv(Path(data_dir) / f'checkpoint_{stage_name}.csv', index=False, encoding='utf-8-sig')

# Loads a pipeline stage's previously checkpointed raw output
def load_stage_checkpoint(data_dir, stage_name):
    return pd.read_csv(Path(data_dir) / f'checkpoint_{stage_name}.csv')

# Determines which author (first vs. last or both) is affiliated
def determine_affiliation(row, ut_variations):
    if row['first_author'] == row['last_author']:
        return 'single author'

    first_affiliated = any(variation in (row['first_affiliation'] if isinstance(row['first_affiliation'], str) else '') for variation in ut_variations)
    last_affiliated = any(variation in (row['last_affiliation'] if isinstance(row['last_affiliation'], str) else '') for variation in ut_variations)

    if first_affiliated and last_affiliated:
        return 'both lead and senior'
    elif first_affiliated and not last_affiliated:
        return 'only lead'
    elif last_affiliated and not first_affiliated:
        return 'only senior'
    else:
        return 'neither lead nor senior'

# Standard function to look for file with specified pattern in name in specified directory
def load_most_recent_file(outputs_dir, pattern):
    files = os.listdir(outputs_dir)
    files.sort(reverse=True)

    latest_file = None
    for file in files:
        if pattern in file:
            latest_file = file
            break

    if not latest_file:
        print(f"No file with '{pattern}' was found in the directory '{outputs_dir}'.")
        return None
    else:
        file_path = os.path.join(outputs_dir, latest_file)
        df = pd.read_csv(file_path)
        print(f"The most recent file '{latest_file}' has been loaded successfully.")
        return df

# Checks if hypothetical DOI exists (for PLOS SI workflow)
def check_link(doi):
    url = f'https://doi.org/{doi}'
    response = requests.head(url, allow_redirects=True)
    return response.status_code == 200

# Counts descriptive words in text field
def count_words(text, nondescriptive_words):
    if not isinstance(text, str) or text.strip() == '':
        return 0, 0
    words = text.split()
    total_words = len(words)
    descriptive_count = sum(1 for word in words if word.lower() not in nondescriptive_words)
    return total_words, descriptive_count

## Adjust for specific phrases in descriptive word counting
def adjust_descriptive_count(row):
    title = row.get('title_reformatted')
    desc_count = row.get('descriptive_word_count_title', 0)
    
    if not isinstance(title, str):
        return desc_count
    if not isinstance(desc_count, int):
        try:
            desc_count = int(desc_count)
        except (ValueError, TypeError):
            desc_count = 0

    keywords = [
        'supplemental material',
        'supplementary material',
        'supplementary materials',
        'supplemental materials',
        'supporting materials'
    ]

    if any(keyword in title.lower() for keyword in keywords):
        return max(0, desc_count - 1)
    return desc_count

# Adds title-descriptiveness metadata columns (title_reformatted, word counts) to a dataframe in place
def add_title_metadata(df, nondescriptive_words, title_column='title'):
    df['title_reformatted'] = df[title_column].str.replace('_', ' ') #gets around text linked by underscores counting as 1 word
    df['title_reformatted'] = df['title_reformatted'].str.lower()
    df[['total_word_count_title', 'descriptive_word_count_title']] = df['title_reformatted'].apply(lambda x: pd.Series(count_words(x, nondescriptive_words)))
    df['descriptive_word_count_title'] = df.apply(adjust_descriptive_count, axis=1)
    df['nondescriptive_word_count_title'] = df['total_word_count_title'] - df['descriptive_word_count_title']
    return df