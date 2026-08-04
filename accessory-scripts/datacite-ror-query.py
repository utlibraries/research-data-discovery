from datetime import datetime
import pandas as pd
import os
import re
import sys

# Call functions from parent utils.py file
utils_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, utils_dir)
from utils import format_people_with_affiliations, load_env_config, retrieve_datacite

# Parses a single DataCite API item into the flat dict shape used by the ROR- and exact-affiliation queries
def parse_datacite_item(item):
    attributes = item.get('attributes', {})
    doi = attributes.get('doi', None)
    state = attributes.get('state', None)
    publisher = attributes.get('publisher', '')
    registered = attributes.get('registered', '')
    if registered:
        publisher_year = datetime.fromisoformat(registered[:-1]).year
        publisher_date = datetime.fromisoformat(registered[:-1]).date()
    else:
        publisher_year = None
        publisher_date = None
    title = attributes.get('titles', [{}])[0].get('title', '')
    creators = attributes.get('creators', [{}])
    creators_names = [creator.get('name', '') for creator in creators]
    creators_affiliations = ['; '.join([aff['name'] for aff in creator.get('affiliation', [])]) for creator in creators]
    first_creator = creators[0].get('name', None)
    last_creator = creators[-1].get('name', None)
    affiliations = [affiliation.get('name' '') for creator in creators for affiliation in creator.get('affiliation', [{}])]
    first_affiliation = affiliations[0] if affiliations else None
    last_affiliation = affiliations[-1] if affiliations else None
    contributors = attributes.get('contributors', [{}])
    contributors_names = [contributor.get('name', '') for contributor in contributors]
    contributors_affiliations = ['; '.join([aff['name'] for aff in contributor.get('affiliation', [])]) for contributor in contributors]
    container = attributes.get('container', {})
    container_identifier = container.get('identifier', None)
    related_identifiers = attributes.get('relatedIdentifiers', [])
    relation_type = ''
    related_identifier = ''
    for identifier in related_identifiers:
        relation_type = identifier.get('relationType', '')
        related_identifier = identifier.get('relatedIdentifier', '')
    types = attributes.get('types', {})
    resource_type = types.get('resourceTypeGeneral', '')
    sizes = attributes.get('sizes', [])
    cleaned_sizes = [int(re.sub(r'\D', '', size)) for size in sizes if re.sub(r'\D', '', size).isdigit()]
    total_size = sum(cleaned_sizes) if cleaned_sizes else 'No file size information'
    formats_list = attributes.get('formats', [])
    formats = set(formats_list) if formats_list else 'No file information'
    rights_list = attributes.get('rightsList', [])
    rights = [right['rights'] for right in rights_list if 'rights' in right] or ['Rights unspecified']
    rights_code = [right['rightsIdentifier'] for right in rights_list if 'rightsIdentifier' in right] or ['Unknown']
    views = attributes.get('viewCount', 0)
    downloads = attributes.get('downloadCount', 0)
    citations = attributes.get('citationCount', 0)
    return {
        'doi': doi,
        'state': state,
        'publisher': publisher,
        'publication_year': publisher_year,
        'publication_date': publisher_date,
        'title': title,
        'first_author': first_creator,
        'last_author': last_creator,
        'first_affiliation': first_affiliation,
        'last_affiliation': last_affiliation,
        'creators_names': creators_names,
        'creators_affiliations': creators_affiliations,
        'contributors_names': contributors_names,
        'contributors_affiliations': contributors_affiliations,
        'relation_type': relation_type,
        'related_identifier': related_identifier,
        'container_identifier': container_identifier,
        'type': resource_type,
        'deposit_size': total_size,
        'formats': formats,
        'rights': rights,
        'rights_code': rights_code,
        'views': views,
        'downloads': downloads,
        'citations': citations,
        'source': 'DataCite'
    }

# Standardizes repository/publisher names in place using the shared REPOSITORY_MAPPING (see config.json),
# matching the same pattern-based approach used in dataset-records-retrieval.py
def standardize_publishers(df, repo_map):
    df['publisher'] = df['publisher'].fillna('None')
    for repository in repo_map:
        mask = df[repository['column']].str.contains(
            repository['pattern'], case=repository['case'], na=False
        )
        df.loc[mask, 'publisher'] = repository['replacement']
    return df

# Consolidates mediated Figshare deposits, de-duplicates lineage-based repos (parent vs. child version DOIs),
# removes file-level DOI granularity, and does a final sweeping de-duplication pass. Shared across the ROR-,
# exact-, and wildcard-affiliation queries below, which differ only in which repos participate in each step.
def deduplicate_datacite_records(df, lineage_repo_pattern, dataverse_pattern, needs_hashable_fix=False):
    figshare = df[df['publisher'].str.contains('figshare')]
    df_no_figshare = df[~df['publisher'].str.contains('figshare')]
    figshare_no_versions = figshare[~figshare['doi'].str.contains(r'\.v\d+$')]
    # Mediated workflow sometimes creates individual deposit for each file, want to treat as single dataset here
    for col in figshare_no_versions.columns:
        if figshare_no_versions[col].apply(lambda x: isinstance(x, list)).any():
            figshare_no_versions[col] = figshare_no_versions[col].apply(lambda x: tuple(x) if isinstance(x, list) else x)
    figshare_no_versions['had_partial_duplicate'] = figshare_no_versions.duplicated(subset=['publisher', 'publication_date', 'creators_names', 'creators_affiliations', 'type', 'related_identifier'], keep=False)

    # Aggregating related entries together
    sum_columns = ['deposit_size', 'views', 'citations', 'downloads']

    def agg_func(column_name):
        if column_name in sum_columns:
            return 'sum'
        else:
            return lambda x: sorted(set(x))

    # Handling mixed-type columns that are expected to be only numeric
    for col in sum_columns:
        if col in figshare_no_versions.columns:
            figshare_no_versions[col] = pd.to_numeric(figshare_no_versions[col], errors='coerce')

    if needs_hashable_fix:
        # Convert sets/lists to strings in all other columns except 'related_identifier'
        for col in figshare_no_versions.columns:
            if col not in sum_columns and col != 'related_identifier':
                figshare_no_versions[col] = figshare_no_versions[col].apply(
                    lambda val: ', '.join(sorted(map(str, val))) if isinstance(val, (set, list)) else val
                )

        # Standardize object type of related_identifier
        figshare_no_versions['related_identifier'] = figshare_no_versions['related_identifier'].apply(
            lambda val: ', '.join(sorted(map(str, val))) if isinstance(val, (set, list)) else val
        )

    agg_funcs = {col: agg_func(col) for col in figshare_no_versions.columns if col != 'related_identifier'}

    figshare_no_versions_combined = figshare_no_versions.groupby('related_identifier').agg(agg_funcs).reset_index()
    # Convert all list-type columns to comma-separated strings
    for col in figshare_no_versions_combined.columns:
        if figshare_no_versions_combined[col].apply(lambda x: isinstance(x, list)).any():
            figshare_no_versions_combined[col] = figshare_no_versions_combined[col].apply(lambda x: '; '.join(map(str, x)))
    figshare_deduplicated = figshare_no_versions_combined.drop_duplicates(subset='related_identifier', keep='first')
    df_v1 = pd.concat([df_no_figshare, figshare_deduplicated], ignore_index=True)

    # Handling duplication of lineage-based repos (parent vs. child)
    lineage_repos = df_v1[df_v1['publisher'].str.contains(lineage_repo_pattern)]
    df_lineage_repos = df_v1[~df_v1['publisher'].str.contains(lineage_repo_pattern)]
    lineage_repos_deduplicated = lineage_repos[~lineage_repos['relation_type'].str.contains('IsVersionOf|IsNewVersionOf', case=False, na=False)]
    # The use of .v* and v* as filters works for these repositories but could accidentally remove non-duplicate DOIs if applied to other repositories
    lineage_repos_deduplicated = lineage_repos_deduplicated[~lineage_repos_deduplicated['doi'].str.contains(r'\.v\d+$')]
    dois_to_remove = lineage_repos_deduplicated[(lineage_repos_deduplicated['doi'].str.contains(r'v\d$') | lineage_repos_deduplicated['doi'].str.contains(r'v\d-')) & (lineage_repos_deduplicated['publisher'].str.contains('ICPSR', case=False, na=False))]['doi']
    # Remove the identified DOIs
    lineage_repos_deduplicated = lineage_repos_deduplicated[~lineage_repos_deduplicated['doi'].isin(dois_to_remove)]
    df_v2 = pd.concat([df_lineage_repos, lineage_repos_deduplicated], ignore_index=True)

    # Handling file-level DOI granularity (all Dataverse installations)
    df_dedup = df_v2[~(df_v2['publisher'].str.contains(dataverse_pattern, case=False, na=False) & df_v2['container_identifier'].notnull())]
    # Multi-condition to avoid removing multi-DOI consolidated Figshare deposits
    df_dedup = df_dedup[~((df_dedup['doi'].str.count('/') >= 3) & (df_dedup['publisher'] != 'figshare'))]

    # Final sweeping dedpulication step, will catch a few odd edge cases that have been manually discovered
    df_sorted = df_dedup.sort_values(by='doi')
    return df_sorted.drop_duplicates(subset=['title', 'first_author', 'relation_type', 'related_identifier', 'container_identifier'], keep='first')

# Read in env file
env = load_env_config()

# Operator for quick test runs
test = env['TOGGLES']['test']
# Setting timestamp to calculate run time
start_time = datetime.now()
# Creating variable with current date for appending to filenames
today_date = datetime.now().strftime("%Y%m%d")
# Toggles for which test to run
ror_affiliation = env['INSTITUTION']['ror_affiliation']
exact_affiliation = env['INSTITUTION']['exact_affiliation']
wildcard_affiliation = env['INSTITUTION']['wildcard_affiliation']

# Creating directories
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

# API endpoints
url_datacite = "https://api.datacite.org/dois"

# Load in ROR link
ror = env['INSTITUTION']['ror']

# Pull in map of repository names for standarization
repo_map = env['REPOSITORY_MAPPING']

params_datacite = {
    'affiliation': 'true',
    # 'query': f'(creators.affiliation.affiliationIdentifier:"{ror}" OR creators.name:"{ror}" OR contributors.affiliation.affiliationIdentifier:"{ror}" OR contributors.name:"{ror}") AND types.resourceTypeGeneral:"Dataset"',
    'query': f'(creators.affiliation.affiliationIdentifier:"{ror}") AND types.resourceTypeGeneral:"Dataset"',
    'page[size]': env['VARIABLES']['PAGE_SIZES']['datacite'],
    'page[cursor]': 1,
}

# Define different number of pages to retrieve from DataCite API based on 'test' vs. 'prod' env
page_limit_datacite = env['VARIABLES']['PAGE_LIMITS']['datacite_test'] if test else env['VARIABLES']['PAGE_LIMITS']['datacite_prod']
# Page number start
page_start_datacite = env['VARIABLES']['PAGE_STARTS']['datacite']
## Per page
per_page_datacite = env['VARIABLES']['PAGE_SIZES']['datacite']

if ror_affiliation:
    print("Starting DataCite retrieval based on ROR-based affiliation.\n")
    data_datacite = retrieve_datacite(url_datacite, params_datacite, page_start_datacite, page_limit_datacite, per_page_datacite)
    print(f"Number of ROR-affiliated datasets found by DataCite API: {len(data_datacite)}\n")
    data_select_datacite = [parse_datacite_item(item) for item in data_datacite]

    df_datacite_initial = pd.json_normalize(data_select_datacite)
    df_datacite_initial = standardize_publishers(df_datacite_initial, repo_map)
    df_datacite_initial.to_csv(f"accessory-outputs/{today_date}_datacite-ror-retrieval.csv")

    ### The below code is mostly duplicated from the main codebase but may not be used b/c it is unlikely that all of these repositories will be retrieved via a ROR-based query ###

    df_datacite = deduplicate_datacite_records(
        df_datacite_initial,
        lineage_repo_pattern='ICPSR|Mendeley|Zenodo',
        dataverse_pattern='Dataverse|Texas Data Repository',
    )
    df_datacite.to_csv(f"accessory-outputs/{today_date}_datacite-ror-retrieval-filtered.csv")
    print(f"Number of ROR-affiliated datasets left after cleaning: {len(df_datacite)}\n")

if exact_affiliation:
    print("Starting single-affiliation-string-based query")
    params_datacite = {
        'affiliation': 'true',
        'query': f'(creators.affiliation.name:"The University of Texas at Austin") AND types.resourceTypeGeneral:"Dataset"',
        'page[size]': env['VARIABLES']['PAGE_SIZES']['datacite'],
        'page[cursor]': 1,
    }

    print("Starting DataCite retrieval based on affiliation.\n")
    data_datacite = retrieve_datacite(url_datacite, params_datacite, page_start_datacite, page_limit_datacite, per_page_datacite)
    print(f"Number of official-UT-affiliated datasets found by DataCite API: {len(data_datacite)}\n")
    data_select_datacite = [parse_datacite_item(item) for item in data_datacite]

    df_datacite_initial = pd.json_normalize(data_select_datacite)
    df_datacite_initial = standardize_publishers(df_datacite_initial, repo_map)
    df_datacite_initial.to_csv(f"accessory-outputs/{today_date}_datacite-single-affiliation-retrieval.csv")

    ### The below code is mostly duplicated from the main codebase but may not be used b/c it is unlikely that all of these repositories will be retrieved via a ROR-based query ###

    df_datacite = deduplicate_datacite_records(
        df_datacite_initial,
        lineage_repo_pattern='ICPSR|Mendeley|SAGE|Zenodo',
        dataverse_pattern='Dataverse|Texas Data Repository',
    )
    df_datacite.to_csv(f"accessory-outputs/{today_date}_datacite-single-affiliation-retrieval-filtered.csv")
    print(f"Number of official-UT-affiliated datasets left after cleaning: {len(df_datacite)}\n")

if wildcard_affiliation:
    print("Starting single-affiliation-string-based wildcard query")
    query_value = r'university\ of\ texas\ austin'
    query = f'creators.affiliation.name:{query_value}'
    params_datacite = {
        'affiliation': 'true',
        'query': query,
        'page[size]': env['VARIABLES']['PAGE_SIZES']['datacite'],
        'page[cursor]': 1,
    }

    print("Starting DataCite retrieval based on wildcard affiliation.\n")
    data_datacite = retrieve_datacite(url_datacite, params_datacite, page_start_datacite, page_limit_datacite, per_page_datacite)
    print(f"Number of possibly-UT-affiliated datasets found by DataCite API: {len(data_datacite)}\n")
    data_select_datacite = []
    for item in data_datacite:
        attributes = item.get('attributes', {})
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
        creators_names = [creator.get('name', '') for creator in creators]
        contributors_affiliations = [
            '; '.join(aff.get('name', '') for aff in creator.get('affiliation', []))
            for creator in creators
        ]
        creators_formatted = format_people_with_affiliations(creators)
        first_creator = creators[0].get('name', None) if creators else None
        last_creator = creators[-1].get('name', None) if creators else None
        creators_affiliations = [
            aff.get('name', '')
            for creator in creators
            for aff in (creator.get('affiliation') if isinstance(creator.get('affiliation'), list) else [])
            if isinstance(aff, dict)
        ]
        first_affiliation = contributors_affiliations[0] if contributors_affiliations else None
        last_affiliation = contributors_affiliations[-1] if contributors_affiliations else None
        contributors = attributes.get('contributors', [{}])
        contributors_names = [contributor.get('name', '') for contributor in contributors]
        contributors_affiliations = [
            '; '.join(aff.get('name', '') for aff in contributor.get('affiliation', []))
            for contributor in contributors
        ]
        contributors_formatted = format_people_with_affiliations(contributors)
        container = attributes.get('container', {})
        container_identifier = container.get('identifier', None)
        related_identifiers = attributes.get('relatedIdentifiers', [])
        relation_type = ''
        related_identifier = ''
        for identifier in related_identifiers:
            relation_type = identifier.get('relationType', '')
            related_identifier = identifier.get('relatedIdentifier', '')
        types = attributes.get('types', {})
        resource_type = types.get('resourceTypeGeneral', '')
        subjects = attributes.get('subjects', [])
        if subjects:
            subject_list = [subj.get('subject', '').strip() for subj in subjects if subj.get('subject')]
            subjects_combined = '; '.join(subject_list) if subject_list else 'No keywords provided'
        else:
            subjects_combined = 'No keywords provided'
        sizes = attributes.get('sizes', [])
        cleaned_sizes = [int(re.sub(r'\D', '', size)) for size in sizes if re.sub(r'\D', '', size).isdigit()]
        total_size = sum(cleaned_sizes) if cleaned_sizes else 'No file size information'
        formats_list = attributes.get('formats', [])
        formats = set(formats_list) if formats_list else 'No file information'
        file_count = len(formats_list) if formats_list else 'No file information'
        rights_list = attributes.get('rightsList', [])
        rights = [right['rights'] for right in rights_list if 'rights' in right] or ['Rights unspecified']
        rights_code = [right['rightsIdentifier'] for right in rights_list if 'rightsIdentifier' in right] or ['Unknown']
        views = attributes.get('viewCount', 0)
        downloads = attributes.get('downloadCount', 0)
        citations = attributes.get('citationCount', 0)
        data_select_datacite.append({
            'doi': doi,
            'state': state,
            'publisher': publisher,
            'publisher_original': publisher,
            'publication_year': publisher_year,
            'publication_date': publisher_date,
            'title': title,
            'first_author': first_creator,
            'last_author': last_creator,
            'first_affiliation': first_affiliation,
            'last_affiliation': last_affiliation,
            'creators_names': creators_names,
            'creators_affiliations': creators_affiliations,
            'creators_formatted': creators_formatted,
            'contributors_names': contributors_names,
            'contributors_affiliations': contributors_affiliations,
            'contributors_formatted': contributors_formatted,
            'relation_type': relation_type,
            'related_identifier': related_identifier,
            'container_identifier': container_identifier,
            'type': resource_type,
            'subjects': subjects_combined,
            'deposit_size': total_size,
            'formats': formats,
            'file_count': file_count,
            'rights': rights,
            'rights_code': rights_code,
            'views': views,
            'downloads': downloads,
            'citations': citations,
            'source': 'DataCite'
        })

    df_datacite_initial = pd.json_normalize(data_select_datacite)
    df_datacite_initial = standardize_publishers(df_datacite_initial, repo_map)
    df_datacite_initial.to_csv(f"accessory-outputs/{today_date}_datacite-single-affiliation-retrieval-wildcard-search.csv")

    ### The below code is mostly duplicated from the main codebase but may not be used b/c it is unlikely that all of these repositories will be retrieved via a ROR-based query ###

    df_datacite = deduplicate_datacite_records(
        df_datacite_initial,
        lineage_repo_pattern='ICPSR|Mendeley|SAGE|Zenodo',
        dataverse_pattern='Dataverse|Texas Data Repository|CUHK Research Data Repository|Qualitative Data Repository',
        needs_hashable_fix=True,
    )
    df_datacite.to_csv(f"accessory-outputs/{today_date}_datacite-single-affiliation-retrieval-wildcard-search-filtered.csv")
    print(f"Number of probably-UT-affiliated datasets left after cleaning: {len(df_datacite)}\n")

print("Done.\n")
print(f"Time to run: {datetime.now() - start_time}")