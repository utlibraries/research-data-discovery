import os
import gzip
import json
import csv
import pandas as pd
import requests
import tarfile
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
#toggle to randomly pull more detailed metadata for RADS deposits
RADSrandomcheck = False
#for random sampling of RADS dataset
randomCount = 4000
#toggle for DataCite public data file
DataCitePDF = False

#API urls
url_crossref = "https://api.crossref.org/works/"
url_datacite = 'https://api.datacite.org/dois'
url_zenodo = 'https://zenodo.org/api/records'

#download file from Zenodo API
##does not require API key for this endpoint
print("Retrieving ")
response = requests.get(f"{url_zenodo}/11073357")
data = response.json()
file_info = data["files"][0]
zip_url = file_info["links"]["self"]
file_name = os.path.basename(file_info["key"]) #avoid issue of filename having directory path in it

script_dir = os.getcwd()
parent_dir = os.path.abspath(os.path.join(os.getcwd(), '..'))
outputs_dir = os.path.join(script_dir, 'accessory-outputs')

if os.path.isdir('inputs'):
        print('inputs directory found - no need to recreate')
else:
    os.mkdir('inputs')
    print('inputs directory has been created')
if os.path.isdir('accessory-outputs'):
        print('accessory-outputs directory found - no need to recreate')
else:
    os.mkdir('accessory-outputs')
    print('accessory-outputs directory has been created')

inputs_dir = os.path.join(script_dir, 'inputs')
nested_dir = os.path.join(
        inputs_dir,
        "rads-metadata-PLOSone_publication",
        "DataCurationNetwork-rads-metadata-413c521",
        "data_all_dois"
    )

def load_most_recent_file(dir, pattern):
    if not os.path.exists(dir):
        print(f"The directory '{dir}' does not exist, proceeding to download.\n")
        return None

    files = os.listdir(dir)
    files.sort(reverse=True)

    latest_file = None
    for file in files:
        if pattern in file:
            latest_file = file
            break

    if not latest_file:
        print(f"No file with '{pattern}' was found in the directory '{dir}', but the correct directory does exist. Check that the download is complete\n")
        return None
    else:
        file_path = os.path.join(dir, latest_file)
        df = pd.read_csv(file_path)
        print(f"The most recent file '{latest_file}' has been loaded successfully.\n")
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
        updated = attributes.get('updated', '')
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
                    'publisher': publisher,
                    'publicationYear': publisher_year,
                    'publicationDate': publisher_date,
                    'updatedDate': updated,
                    'title': title,
                    'creatorsNames': creatorsNames,
                    'creatorsAffiliations': creatorsAffiliations,
                    'contributorsNames': contributorsNames,
                    'contributorsAffiliations': contributorsAffiliations,
                    'relationType': relationType,
                    'relatedIdentifier': relatedIdentifier
                })

    df_datacite_new = pd.json_normalize(data_select_datacite_new)
    df_datacite_new.to_csv(f"accessory-outputs/{todayDate}_RADS-figshare-datasets.csv")

else:
    pattern = '_RADS-figshare-datasets.csv'
    df_datacite_new = load_most_recent_file(outputs_dir, pattern)
figshare_no_versions = figshare_no_versions.rename(columns={'doi':'doi_alt', 'DOI': 'doi'})
df_datacite_merged = pd.merge(df_datacite_new, figshare_no_versions, how="left", on="doi")
df_datacite_merged.to_csv(f"accessory-outputs/{todayDate}_RADS-figshare-datasets-merged.csv")

#doing a broader check of all RADS deposits
df_datacite = df[df['group'] == 'Affiliation - Datacite']
df_datacite = df_datacite[df_datacite['resourceTypeGeneral'].str.contains('Dataset', case=True, na=False)]
df_random = df_datacite.sample(n=randomCount, random_state=47)

if RADSrandomcheck:
    print('Retrieving additional DataCite metadata for random RADS deposits\n')
    results = []
    for doi in df_random['DOI']:
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
        creatorsNames = [creator.get('name', '') for creator in creators]
        creatorsAffiliations = ['; '.join(creator.get('affiliation', [])) for creator in creators]
        contributors = attributes.get('contributors', [{}])
        contributorsNames = [contributor.get('name', '') for contributor in contributors]
        contributorsAffiliations = ['; '.join(contributor.get('affiliation', [])) for contributor in contributors]
        container = attributes.get('container', {})
        container_identifier = container.get('identifier', None)
        types = attributes.get('types', {})
        data_select_datacite_new.append({
            'doi': doi,
            'state': state,
            'publisher': publisher,
            'publicationYear': publisher_year,
            'publicationDate': publisher_date,
            'lastUpdated': updated,
            'title': title,
            'creatorsNames': creatorsNames,
            'creatorsAffiliations': creatorsAffiliations,
            'contributorsNames': contributorsNames,
            'contributorsAffiliations': contributorsAffiliations,
                })

    df_datacite_new = pd.json_normalize(data_select_datacite_new)
    df_datacite_new.to_csv(f"accessory-outputs/{todayDate}_RADS-random-{randomCount}-datasets.csv")

    df_random = df_random.rename(columns={'doi':'doi_alt', 'DOI': 'doi'})
    df_datacite_merged = pd.merge(df_datacite_new, df_random, how="left", on="doi")
    df_datacite_merged.loc[df_datacite_merged['publisher_x'].str.contains('Taylor & Francis|SAGE|The Royal Society|SciELO journals|Optica', case=True), 'publisher_x'] = 'figshare'
    df_datacite_merged.to_csv(f"accessory-outputs/{todayDate}_RADS-random-{randomCount}-datasets-merged.csv")

# List of institutions to check for
institutions = [
    "University of Minnesota",
    "Cornell University",
    "Duke University",
    "University of Michigan",
    "Washington University in St. Louis",
    "Virginia Tech"
]

def find_institutions(affiliation):
    if isinstance(affiliation, list):
        affiliation = ' '.join(str(item) for item in affiliation)
    elif not isinstance(affiliation, str):
        return 'No match found'

    affiliation_lower = affiliation.lower()
    found_institutions = [
        institution for institution in institutions
        if institution.lower() in affiliation_lower
    ]
    return '; '.join(found_institutions) if found_institutions else 'No match found'

# Apply the function to the 'affiliations' column and create a new column 'institution_found'
df_datacite_merged['institution_found'] = df_datacite_merged['creatorsAffiliations'].apply(find_institutions)
if RADSrandomcheck:
    df_datacite_merged.to_csv(f"accessory-outputs/{todayDate}_RADS-random-{randomCount}-datasets-merged-validation-check.csv")
else:
    df_datacite_merged.to_csv(f"accessory-outputs/{todayDate}_RADS-figshare-datasets-merged-validation-check.csv")

###indexing through 2023 DataCite public data file
if DataCitePDF:

    import os
    import tarfile
    import gzip
    import json
    import csv

    # Define the DOI to search for
    # search_doi = "10.6084/m9.figshare.15253711"
    search_doi =  "10.6084/m9.figshare.c.5984410"
    search_dois = [
        "10.6084/m9.figshare.c.5984410",
        "10.6084/m9.figshare.c.15253711"
    ]

    # Define the tarball path
    tarball_path = r"C:\Users\bmg3525\Downloads\DataCite_Public_Data_File_2023(1).tar.gz"

    # Define the results file path
    results_file = r"C:\Users\bmg3525\Documents\scientometrics\scripts\research-data-discovery\script-sandbox\outputs\search_results.txt"
    
    # Function to search for DOIs in JSONL files
    def search_jsonl_gz(file_obj, search_dois):
        results = []
        with gzip.open(file_obj, 'rt', encoding='utf-8') as f:
            for line in f:
                data = json.loads(line)
                if any(doi in json.dumps(data) for doi in search_dois):
                    results.append(data)
        return results

    # Function to search for DOIs in CSV files
    def search_csv_gz(file_obj, search_dois):
        results = []
        with gzip.open(file_obj, 'rt', encoding='utf-8') as f:
            reader = csv.reader(f)
            for row in reader:
                if any(doi in ','.join(row) for doi in search_dois):
                    results.append(row)
        return results

    # Initialize results list
    all_results = []

    print("Indexing archive...")

    # Open the tarball and iterate through its contents
    with tarfile.open(tarball_path, 'r:gz') as tar:
        members = tar.getmembers()
        print(f"Found {len(members)} files to process.")
        
        for idx, member in enumerate(members):
            if member.isfile() and '10.6084' in member.name:
                print(f"Processing file {idx + 1}/{len(members)}: {member.name}")
                if member.name.endswith(".jsonl.gz") or member.name.endswith(".csv.gz"):
                    file_obj = tar.extractfile(member)
                    if member.name.endswith(".jsonl.gz"):
                        results = search_jsonl_gz(file_obj, search_dois)
                        if results:
                            all_results.extend(results)
                    elif member.name.endswith(".csv.gz"):
                        results = search_csv_gz(file_obj, search_dois)
                        if results:
                            all_results.extend(results)

    # Save results to the results file
    with open(results_file, 'w', encoding='utf-8') as f:
        for result in all_results:
            f.write(json.dumps(result) + '\n')

    print(f"Search completed. Results saved to {results_file}.")




# df_figshare_supplemental = df_datacite_new.drop_duplicates(subset='relatedIdentifier', keep="first")
# print(f'Number of DOIs to retrieve: {len(df_figshare_supplemental)}\n')

# #retrieving metadata about related identifiers (linked articles) that were identified
# print("Retrieving metadata about related articles from Crossref\n")
# if crossref:
#     results = []
#     for doi in df_figshare_supplemental['relatedIdentifier']:
#         try:
#             response = requests.get(f'{url_crossref}/{doi}')
#             if response.status_code == 200:
#                 print(f"Retrieving {doi}")
#                 print()
#                 results.append(response.json())
#             else:
#                 print(f"Error fetching {doi}: {response.status_code}, {response.text}")
#         except requests.exceptions.RequestException as e:
#             print(f"Timeout error on DOI {doi}: {e}")

#     data_figshare_crossref = {
#         'articles': results
#     }

#     data_figshare_crossref_select = []
#     articles = data_figshare_crossref.get('articles', [])
#     for item in articles:
#         message = item.get('message', {})
#         publisher = message.get('publisher', None)
#         journal = message.get('container-title', None)[0]
#         doi = message.get('DOI', "")
#         title_list = message.get('title', [])
#         title = title_list[0] if title_list else None
#         author = message.get('author', None)
#         created = message.get('created', {})
#         createdDate = created.get('date-time', None)
        
#         data_figshare_crossref_select.append({
#             'publisher': publisher,
#             'journal': journal, 
#             'doi': doi,
#             'author': author,
#             'title': title,
#             'published': createdDate,
#     })
        
#     df_crossref = pd.json_normalize(data_figshare_crossref_select)
#     df_crossref.to_csv(f"accessory-outputs/{todayDate}_RADS-figshare-associated-articles.csv")

#     #merge back with original dataset dataframe
#     df_crossref['relatedIdentifier'] = df_crossref['doi']
#     df_joint = pd.merge(df_figshare_supplemental, df_crossref, on="relatedIdentifier", how="left")
#     df_joint.to_csv(f"accessory-outputs/{todayDate}_RADS-figshare-associated-articles-merged.csv")

# #for testing effect of applying the same deduplication steps of the primary workflow of this codebase to the RADS dataset
# if deduplicationTest:
#     df_select = df[df['publisher'].str.contains('ICPSR|figshare|Zenodo')]
#     df_granular = df_select[df_select['resourceTypeGeneral'].str.contains('Dataset', case=True, na=False)]
#     print(f'Number of DOIs to retrieve: {len(df_granular)}\n')

#     print('Retrieving additional DataCite metadata for unmatched deposits\n')
#     results = []
#     for doi in df_granular['DOI']:
#         try:
#             response = requests.get(f'{url_datacite}/{doi}')
#             if response.status_code == 200:
#                 print(f'Retrieving {doi}\n')
#                 results.append(response.json())
#             else:
#                 print(f'Error retrieving {doi}: {response.status_code}, {response.text}')
#         except requests.exceptions.RequestException as e:
#             print(f'Timeout error on DOI {doi}: {e}')

#     data_datacite_new = {
#         'datasets': results
#     }

#     data_select_datacite_new = []
#     datasets = data_datacite_new.get('datasets', []) 
#     for item in datasets:
#         data = item.get('data', {})
#         attributes = data.get('attributes', {})
#         doi = attributes.get('doi', None)
#         state = attributes.get('state', None)
#         publisher = attributes.get('publisher', '')
#         registered = attributes.get('registered', '')
#         if registered:
#             publisher_year = datetime.fromisoformat(registered.rstrip('Z')).year
#             publisher_date = datetime.fromisoformat(registered.rstrip('Z')).date()
#         else:
#             publisher_year = None
#             publisher_date = None
#         title = attributes.get('titles', [{}])[0].get('title', '')
#         creators = attributes.get('creators', [{}])
#         creatorsNames = [creator.get('name', '') for creator in creators]
#         creatorsAffiliations = ['; '.join(creator.get('affiliation', [])) for creator in creators]
#         first_creator = creators[0].get('name', None) if creators else None
#         last_creator = creators[-1].get('name', None) if creators else None
#         affiliations = [
#             aff.get('name', '')
#             for creator in creators
#             for aff in (creator.get('affiliation') if isinstance(creator.get('affiliation'), list) else [])
#             if isinstance(aff, dict)
#         ]
#         first_affiliation = affiliations[0] if affiliations else None
#         last_affiliation = affiliations[-1] if affiliations else None
#         contributors = attributes.get('contributors', [{}])
#         contributorsNames = [contributor.get('name', '') for contributor in contributors]
#         contributorsAffiliations = ['; '.join(contributor.get('affiliation', [])) for contributor in contributors]
#         container = attributes.get('container', {})
#         container_identifier = container.get('identifier', None)
#         types = attributes.get('types', {})

#         related_identifiers = attributes.get('relatedIdentifiers', [])
#         for identifier in related_identifiers:
#             relationType = identifier.get('relationType', '')
#             relatedIdentifier = identifier.get('relatedIdentifier', '')

#             data_select_datacite_new.append({
#                 'doi': doi,
#                 'state': state,
#                 'publisher': publisher,
#                 'publicationYear': publisher_year,
#                 'publicationDate': publisher_date,
#                 'title': title,
#                 'first_author': first_creator,
#                 'last_author': last_creator,
#                 'first_affiliation': first_affiliation,
#                 'last_affiliation': last_affiliation,
#                 'creatorsNames': creatorsNames,
#                 'creatorsAffiliations': creatorsAffiliations,
#                 'contributorsNames': contributorsNames,
#                 'contributorsAffiliations': contributorsAffiliations,
#                 'relationType': relationType,
#                 'relatedIdentifier': relatedIdentifier,
#                 'containerIdentifier': container_identifier
#         })

#     df_datacite_new = pd.json_normalize(data_select_datacite_new)
#     df_datacite_new.to_csv(f'accessory-outputs/{todayDate}_RADS-dataset-versioning-check.csv')

#     #create dataframe to start counting summaries
#     ##initialize summary DataFrame
#     summary_df = pd.DataFrame(columns=['publisher', 'count', 'status'])

#     #function to append group counts to summary DataFrame
#     def append_group_counts(df, status):
#         group_counts = df['publisher'].value_counts().reset_index()
#         group_counts.columns = ['publisher', 'count']
#         group_counts['status'] = status
#         return group_counts

#     summary_df = pd.concat([summary_df, append_group_counts(df_granular, 'original')])

#     #cleaning steps
#     ##remove entries with multiple relatedIdentifiers
#     df_datacite_clean = df_datacite_new.drop_duplicates(subset='doi', keep="first")
#     ##handling duplication of ICPSR, Mendeley Data, Zenodo deposits (parent vs. child)
#     df_datacite_new_dedup = df_datacite_clean[~df_datacite_clean['relationType'].str.contains('IsVersionOf|IsNewVersionOf', case=False, na=False)]
#     ###the use of .v* and v* as filters works for these repositories but could accidentally remove non-duplicate DOIs if applied to other repositories
#     df_datacite_new_dedup = df_datacite_new_dedup[~df_datacite_new_dedup['doi'].str.contains(r'\.v\d+$')]
#     dois_to_remove = df_datacite_new_dedup[(df_datacite_new_dedup['doi'].str.contains(r'v\d$') | df_datacite_new_dedup['doi'].str.contains(r'v\d-')) & (df_datacite_new_dedup['publisher'].str.contains('ICPSR', case=False, na=False))]['doi']
#     df_datacite_new_dedup = df_datacite_new_dedup[~df_datacite_new_dedup['doi'].isin(dois_to_remove)]

#     # Step 1: Filter rows with value >= 20
#     summary_df = pd.concat([summary_df, append_group_counts(df_datacite_new_dedup, 'version removal')])

#     figshare = df_datacite_new_dedup[df_datacite_new_dedup['doi'].str.contains('figshare')]
#     df_datacite_no_figshare = df_datacite_new_dedup[~df_datacite_new_dedup['doi'].str.contains('figshare')]
#     #mediated workflow sometimes creates individual deposit for each file, want to treat as single dataset here
#     for col in figshare.columns:
#         if figshare[col].apply(lambda x: isinstance(x, list)).any():
#             figshare[col] = figshare[col].apply(lambda x: tuple(x) if isinstance(x, list) else x)
#     figshare['hadPartialDuplicate'] = figshare.duplicated(subset=['publicationDate', 'creatorsAffiliations', 'relatedIdentifier'], keep=False)

#     #aggregating related entries together
#     # figshare_no_versions_combined = figshare_no_versions.groupby('relatedIdentifier').agg(lambda x: '; '.join(sorted(map(str, set(x))))).reset_index()
#     sum_columns = ['depositSize', 'views', 'citations', 'downloads']

#     def agg_func(column_name, column):
#         if column_name in sum_columns:
#             return 'sum'
#         else:
#             return lambda x: sorted(set(x))

#     agg_funcs = {col: agg_func(col, figshare[col]) for col in figshare.columns if col != 'relatedIdentifier'}

#     figshare_no_versions_combined = figshare.groupby('relatedIdentifier').agg(agg_funcs).reset_index()
#     #convert all list-type columns to comma-separated strings
#     for col in figshare_no_versions_combined.columns:
#         if figshare_no_versions_combined[col].apply(lambda x: isinstance(x, list)).any():
#             figshare_no_versions_combined[col] = figshare_no_versions_combined[col].apply(lambda x: '; '.join(map(str, x)))
#     figshare_deduplicated = figshare_no_versions_combined.drop_duplicates(subset='relatedIdentifier', keep='first')
#     df_datacite_v1 = pd.concat([df_datacite_no_figshare, figshare_deduplicated], ignore_index=True)
#     df_datacite_v1.to_csv(f'accessory-outputs/{todayDate}_RADS-dataset-versioning-check_deduplicated.csv')
#     summary_df = pd.concat([summary_df, append_group_counts(df_datacite_v1, 'figshare consolidation')])
#     print(summary_df)
#     summary_df.to_csv(f"accessory-outputs/{todayDate}_RADS-deduplication-reanalysis-summary.csv")

print(f"Time to run: {datetime.now() - startTime}")
print("Done.")