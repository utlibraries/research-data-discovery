import os
import pandas as pd
import requests
import zipfile
from datetime import datetime

#setting timestamp to calculate run time
startTime = datetime.now() 
#creating variable with current date for appending to filenames
todayDate = datetime.now().strftime('%Y%m%d') 

#toggle to load previously generated enriched DataCite records
loadPreviousDataCite = False
#toggle to retrieve Crossref metadata for articles linked to affiliated Figshare records
crossref = True
#toggle to run deduplication process of primary workflow on RADS data
deduplicationTest = True

#API urls
url_crossref = "https://api.crossref.org/works/"
url_datacite = 'https://api.datacite.org/dois'
url_zenodo = 'https://zenodo.org/api/records'

#download file from Zenodo API
##does not require API key for this endpoint
response = requests.get(f"{url_zenodo}/11073357")
data = response.json()
file_info = data["files"][0]
zip_url = file_info["links"]["self"]
file_name = os.path.basename(file_info["key"]) #avoid issue of filename having directory path in it

script_dir = os.getcwd()
parent_dir = os.path.abspath(os.path.join(os.getcwd(), '..'))
outputs_dir = os.path.join(script_dir, 'outputs')

if os.path.isdir('inputs'):
        print('inputs directory found - no need to recreate')
else:
    os.mkdir('inputs')
    print('inputs directory has been created')

inputs_dir = os.path.join(script_dir, 'inputs')
nested_dir = os.path.join(
        inputs_dir,
        "rads-metadata-PLOSone_publication",
        "DataCurationNetwork-rads-metadata-413c521",
        "data_all_dois"
    )

#retrieve most recent output file
def load_most_recent_file(dir, pattern):
    files = os.listdir(dir)
    files.sort(reverse=True)

    latest_file = None
    for file in files:
        if pattern in file:
            latest_file = file
            break

    if not latest_file:
        print(f"No file with '{pattern}' was found in the directory '{dir}'.")
        return None
    else:
        file_path = os.path.join(dir, latest_file)
        df = pd.read_csv(file_path)
        print(f"The most recent file '{latest_file}' has been loaded successfully.")
        return df

#look for existing file in inputs
pattern = 'All_dois_20221119.csv'
df = load_most_recent_file(nested_dir, pattern)

#if it doesn't exist, download from Zenodo
if df is None:
    print("Downloading files")
    zip_path = os.path.join("inputs", file_name)
    if not os.path.exists(zip_path):
        r = requests.get(zip_url)
        with open(zip_path, "wb") as f:
            f.write(r.content)
        print(f"Downloaded ZIP to {zip_path}")

    #extract ZIP to 'inputs/unzipped'
    unzipped_dir = os.path.join("inputs", "rads-metadata-PLOSone_publication")
    os.makedirs(unzipped_dir, exist_ok=True)
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(unzipped_dir)
    print(f"Extracted ZIP to {unzipped_dir}")

    #load the specific CSV file
    csv_path = os.path.join(
        unzipped_dir,
        "DataCurationNetwork-rads-metadata-413c521",
        "data_all_dois",
        "All_dois_20221119.csv"
    )

    df = pd.read_csv(csv_path)

print(f"Loaded CSV with {len(df)} rows.")
df_select = df[df['publisher'].str.contains('figshare')]
df_granular = df_select[df_select['resourceTypeGeneral'].str.contains('Dataset', case=True, na=False)]
figshare_no_versions = df_granular[~df_granular['DOI'].str.contains(r'\.v\d+$')]
print(f'Number of DOIs to retrieve: {len(figshare_no_versions)}')

if not loadPreviousDataCite:
    print('Retrieving additional DataCite metadata for affiliated Figshare deposits\n')
    results = []
    for doi in figshare_no_versions['DOI']:
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
        title = attributes.get('titles', [{}])[0].get('title', '')
        creators = attributes.get('creators', [{}])
        creatorsNames = [creator.get('name', '') for creator in creators]
        creatorsAffiliations = ['; '.join(creator.get('affiliation', [])) for creator in creators]
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
        contributorsNames = [contributor.get('name', '') for contributor in contributors]
        contributorsAffiliations = ['; '.join(contributor.get('affiliation', [])) for contributor in contributors]
        container = attributes.get('container', {})
        container_identifier = container.get('identifier', None)
        types = attributes.get('types', {})

        related_identifiers = attributes.get('relatedIdentifiers', [])
        for identifier in related_identifiers:
            relationType = identifier.get('relationType', '')
            relatedIdentifier = identifier.get('relatedIdentifier', '')

            if relationType == 'IsSupplementTo' and relatedIdentifier:
                data_select_datacite_new.append({
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
                    'containerIdentifier': container_identifier
                })

    df_datacite_new = pd.json_normalize(data_select_datacite_new)
    df_datacite_new.to_csv(f"outputs/{todayDate}_RADS-figshare-datasets.csv")
else:
    pattern = '_RADS-figshare-datasets.csv'
    df_datacite_new = load_most_recent_file(outputs_dir, pattern)

df_figshare_supplemental = df_datacite_new.drop_duplicates(subset='relatedIdentifier', keep="first")
print(f'Number of DOIs to retrieve: {len(df_figshare_supplemental)}\n')

#retrieving metadata about related identifiers (linked articles) that were identified
print("Retrieving metadata about related articles from Crossref\n")
if crossref:
    results = []
    for doi in df_figshare_supplemental['relatedIdentifier']:
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
    df_crossref.to_csv(f"outputs/{todayDate}_RADS-figshare-associated-articles.csv")

    #merge back with original dataset dataframe
    df_crossref['relatedIdentifier'] = df_crossref['doi']
    df_joint = pd.merge(df_figshare_supplemental, df_crossref, on="relatedIdentifier", how="left")
    df_joint.to_csv(f"outputs/{todayDate}_RADS-figshare-associated-articles-merged.csv")

#for testing effect of applying the same deduplication steps of the primary workflow of this codebase to the RADS dataset
if deduplicationTest:
    df_select = df[df['publisher'].str.contains('ICPSR|figshare|Zenodo')]
    df_granular = df_select[df_select['resourceTypeGeneral'].str.contains('Dataset', case=True, na=False)]
    print(f'Number of DOIs to retrieve: {len(df_granular)}\n')

    print('Retrieving additional DataCite metadata for unmatched deposits\n')
    results = []
    for doi in df_granular['DOI']:
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
        title = attributes.get('titles', [{}])[0].get('title', '')
        creators = attributes.get('creators', [{}])
        creatorsNames = [creator.get('name', '') for creator in creators]
        creatorsAffiliations = ['; '.join(creator.get('affiliation', [])) for creator in creators]
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
        contributorsNames = [contributor.get('name', '') for contributor in contributors]
        contributorsAffiliations = ['; '.join(contributor.get('affiliation', [])) for contributor in contributors]
        container = attributes.get('container', {})
        container_identifier = container.get('identifier', None)
        types = attributes.get('types', {})

        related_identifiers = attributes.get('relatedIdentifiers', [])
        for identifier in related_identifiers:
            relationType = identifier.get('relationType', '')
            relatedIdentifier = identifier.get('relatedIdentifier', '')

            data_select_datacite_new.append({
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
                'containerIdentifier': container_identifier
        })

    df_datacite_new = pd.json_normalize(data_select_datacite_new)
    df_datacite_new.to_csv(f'outputs/{todayDate}_RADS-dataset-versioning-check.csv')

    #create dataframe to start counting summaries
    ##initialize summary DataFrame
    summary_df = pd.DataFrame(columns=['publisher', 'count', 'status'])

    #function to append group counts to summary DataFrame
    def append_group_counts(df, status):
        group_counts = df['publisher'].value_counts().reset_index()
        group_counts.columns = ['publisher', 'count']
        group_counts['status'] = status
        return group_counts

    summary_df = pd.concat([summary_df, append_group_counts(df_granular, 'original')])

    #cleaning steps
    ##remove entries with multiple relatedIdentifiers
    df_datacite_clean = df_datacite_new.drop_duplicates(subset='doi', keep="first")
    ##handling duplication of ICPSR, Mendeley Data, Zenodo deposits (parent vs. child)
    df_datacite_new_dedup = df_datacite_clean[~df_datacite_clean['relationType'].str.contains('IsVersionOf|IsNewVersionOf', case=False, na=False)]
    ###the use of .v* and v* as filters works for these repositories but could accidentally remove non-duplicate DOIs if applied to other repositories
    df_datacite_new_dedup = df_datacite_new_dedup[~df_datacite_new_dedup['doi'].str.contains(r'\.v\d+$')]
    dois_to_remove = df_datacite_new_dedup[(df_datacite_new_dedup['doi'].str.contains(r'v\d$') | df_datacite_new_dedup['doi'].str.contains(r'v\d-')) & (df_datacite_new_dedup['publisher'].str.contains('ICPSR', case=False, na=False))]['doi']
    df_datacite_new_dedup = df_datacite_new_dedup[~df_datacite_new_dedup['doi'].isin(dois_to_remove)]

    # Step 1: Filter rows with value >= 20
    summary_df = pd.concat([summary_df, append_group_counts(df_datacite_new_dedup, 'version removal')])

    figshare = df_datacite_new_dedup[df_datacite_new_dedup['doi'].str.contains('figshare')]
    df_datacite_no_figshare = df_datacite_new_dedup[~df_datacite_new_dedup['doi'].str.contains('figshare')]
    #mediated workflow sometimes creates individual deposit for each file, want to treat as single dataset here
    for col in figshare.columns:
        if figshare[col].apply(lambda x: isinstance(x, list)).any():
            figshare[col] = figshare[col].apply(lambda x: tuple(x) if isinstance(x, list) else x)
    figshare['hadPartialDuplicate'] = figshare.duplicated(subset=['publicationDate', 'creatorsAffiliations', 'relatedIdentifier'], keep=False)

    #aggregating related entries together
    # figshare_no_versions_combined = figshare_no_versions.groupby('relatedIdentifier').agg(lambda x: '; '.join(sorted(map(str, set(x))))).reset_index()
    sum_columns = ['depositSize', 'views', 'citations', 'downloads']

    def agg_func(column_name, column):
        if column_name in sum_columns:
            return 'sum'
        else:
            return lambda x: sorted(set(x))

    agg_funcs = {col: agg_func(col, figshare[col]) for col in figshare.columns if col != 'relatedIdentifier'}

    figshare_no_versions_combined = figshare.groupby('relatedIdentifier').agg(agg_funcs).reset_index()
    #convert all list-type columns to comma-separated strings
    for col in figshare_no_versions_combined.columns:
        if figshare_no_versions_combined[col].apply(lambda x: isinstance(x, list)).any():
            figshare_no_versions_combined[col] = figshare_no_versions_combined[col].apply(lambda x: '; '.join(map(str, x)))
    figshare_deduplicated = figshare_no_versions_combined.drop_duplicates(subset='relatedIdentifier', keep='first')
    df_datacite_v1 = pd.concat([df_datacite_no_figshare, figshare_deduplicated], ignore_index=True)
    df_datacite_v1.to_csv(f'outputs/{todayDate}_RADS-dataset-versioning-check_deduplicated.csv')
    summary_df = pd.concat([summary_df, append_group_counts(df_datacite_v1, 'figshare consolidation')])
    print(summary_df)
    summary_df.to_csv(f"outputs/{todayDate}_RADS-deduplication-reanalysis-summary.csv")

print(f"Time to run: {datetime.now() - startTime}")
print("Done.")