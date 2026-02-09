import math
import os
import pandas as pd
import requests
from urllib.parse import urlparse, parse_qs

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

        if not datasets:
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
        current_page += 1
        print(f'Retrieving page {current_page} of {total_pages} from Zenodo...\n')
        data = retrieve_page_zenodo(current_url)
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
            print('Finished this journal.\n')
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

# Determines which author (first vs. last or both) is affiliated
def determine_affiliation(row, ut_variations):
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
    words = text.split()
    total_words = len(words)
    descriptive_count = sum(1 for word in words if word not in nondescriptive_words)
    return total_words, descriptive_count

## Adjust for specific phrases in descriptive word counting
def adjust_descriptive_count(row):
    if ('supplemental material' in row['title_reformatted'].lower() or
            'supplementary material' in row['title_reformatted'].lower() or
            'supplementary materials' in row['title_reformatted'].lower() or
            'supplemental materials' in row['title_reformatted'].lower()):
        return max(0, row['descriptive_word_count_title'] - 1)
    return row['descriptive_word_count_title']