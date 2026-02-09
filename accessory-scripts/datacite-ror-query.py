from datetime import datetime
from urllib.parse import urlparse, parse_qs, quote
import pandas as pd
import json
import os
import re
import sys

#call functions from parent utils.py file
utils_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, utils_dir) 
from utils import retrieve_datacite 

#read in config file
parent = os.path.abspath(os.path.join(os.getcwd(), '..'))
with open(f'{parent}/config.json', 'r') as file:
    config = json.load(file)

#operator for quick test runs
test = config['TOGGLES']['test']
#setting timestamp to calculate run time
start_time = datetime.now() 
#creating variable with current date for appending to filenames
today_date = datetime.now().strftime("%Y%m%d") 
#toggles for which test to run
ror_affiliation = config['INSTITUTION']['ror_affiliation']
exact_affiliation = config['INSTITUTION']['exact_affiliation']
wildcard_affiliation = config['INSTITUTION']['wildcard_affiliation']

#creating directories
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

#API endpoints
url_datacite = "https://api.datacite.org/dois"

#load in ROR link
ror = config['INSTITUTION']['ror']

params_datacite = {
    'affiliation': 'true',
    # 'query': f'(creators.affiliation.affiliationIdentifier:"{ror}" OR creators.name:"{ror}" OR contributors.affiliation.affiliationIdentifier:"{ror}" OR contributors.name:"{ror}") AND types.resourceTypeGeneral:"Dataset"',
    'query': f'(creators.affiliation.affiliationIdentifier:"{ror}") AND types.resourceTypeGeneral:"Dataset"',
    'page[size]': config['VARIABLES']['PAGE_SIZES']['datacite'],
    'page[cursor]': 1,
}

#define different number of pages to retrieve from DataCite API based on 'test' vs. 'prod' env
page_limit_datacite = config['VARIABLES']['PAGE_LIMITS']['datacite_test'] if test else config['VARIABLES']['PAGE_LIMITS']['datacite_prod']
#page number start
page_start_datacite = config['VARIABLES']['PAGE_STARTS']['datacite']
##per page
per_page_datacite = config['VARIABLES']['PAGE_SIZES']['datacite']

if ror_affiliation:
    print("Starting DataCite retrieval based on ROR-based affiliation.\n")
    data_datacite = retrieve_datacite(url_datacite, params_datacite, page_start_datacite, page_limit_datacite, per_page_datacite)
    print(f"Number of ROR-affiliated datasets found by DataCite API: {len(data_datacite)}\n")
    data_select_datacite = [] 
    for item in data_datacite:
        attributes = item.get('attributes', {})
        doi = attributes.get('doi', None)
        state = attributes.get('state', None)
        publisher = attributes.get('publisher', '')
        # publisher_year = attributes.get('publicationYear', '') #temporarily disabling due to Dryad metadata issue
        registered = attributes.get('registered', '')
        if registered:
            publisher_year = datetime.fromisoformat(registered[:-1]).year
            publisher_date = datetime.fromisoformat(registered[:-1]).date()
        else:
            publisher_year = None
            publisher_date = None
        title=attributes.get('titles', [{}])[0].get('title','')
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
        data_select_datacite.append({
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
        })

    df_datacite_initial = pd.json_normalize(data_select_datacite)
    #handling malformatted publisher field for Zenodo
    df_datacite_initial.loc[df_datacite_initial['doi'].str.contains('zenodo', case=False, na=False), 'publisher'] = 'Zenodo'
    #handling Figshare partners
    for col in df_datacite_initial.columns:
        if df_datacite_initial[col].apply(lambda x: isinstance(x, list)).any():
            df_datacite_initial[col] = df_datacite_initial[col].apply(lambda x: tuple(x) if isinstance(x, list) else x)
    df_datacite_initial['had_partial_duplicate'] = df_datacite_initial.duplicated(subset=['publisher', 'publication_date', 'creators_names', 'creators_affiliations', 'type', 'related_identifier'], keep=False)
    df_datacite_initial.loc[df_datacite_initial['publisher'].str.contains('Taylor & Francis|SAGE|The Royal Society|SciELO journals', case=True), 'publisher'] = 'figshare'
    df_datacite_initial.to_csv(f"accessory-outputs/{today_date}_datacite-ror-retrieval.csv")

    ### The below code is mostly duplicated from the main codebase but may not be used b/c it is unlikely that all of these repositories will be retrieved via a ROR-based query ###

    figshare = df_datacite_initial[df_datacite_initial['publisher'].str.contains('figshare')]
    df_datacite_no_figshare = df_datacite_initial[~df_datacite_initial['publisher'].str.contains('figshare')]
    figshare_no_versions = figshare[~figshare['doi'].str.contains(r'\.v\d+$')]
    #mediated workflow sometimes creates individual deposit for each file, want to treat as single dataset here
    for col in figshare_no_versions.columns:
        if figshare_no_versions[col].apply(lambda x: isinstance(x, list)).any():
            figshare_no_versions[col] = figshare_no_versions[col].apply(lambda x: tuple(x) if isinstance(x, list) else x)
    figshare_no_versions['had_partial_duplicate'] = figshare_no_versions.duplicated(subset=['publisher', 'publication_date', 'creators_names', 'creators_affiliations', 'type', 'related_identifier'], keep=False)

    #aggregating related entries together
    # figshare_no_versions_combined = figshare_no_versions.groupby('related_identifier').agg(lambda x: '; '.join(sorted(map(str, set(x))))).reset_index()
    sum_columns = ['deposit_size', 'views', 'citations', 'downloads']

    def agg_func(column_name):
        if column_name in sum_columns:
            return 'sum'
        else:
            return lambda x: sorted(set(x))

    #handling mixed-type columns that are expected to be only numeric
    for col in sum_columns:
        if col in figshare_no_versions.columns:
            figshare_no_versions[col] = pd.to_numeric(figshare_no_versions[col], errors='coerce')
    agg_funcs = {col: agg_func(col)for col in figshare_no_versions.columns if col != 'related_identifier'}

    figshare_no_versions_combined = figshare_no_versions.groupby('related_identifier').agg(agg_funcs).reset_index()
    # Convert all list-type columns to comma-separated strings
    for col in figshare_no_versions_combined.columns:
        if figshare_no_versions_combined[col].apply(lambda x: isinstance(x, list)).any():
            figshare_no_versions_combined[col] = figshare_no_versions_combined[col].apply(lambda x: '; '.join(map(str, x)))
    figshare_deduplicated = figshare_no_versions_combined.drop_duplicates(subset='related_identifier', keep='first')
    df_datacite_v1 = pd.concat([df_datacite_no_figshare, figshare_deduplicated], ignore_index=True)

    ##handling duplication of ICPSR, Mendeley Data, Zenodo deposits (parent vs. child)
    lineageRepos = df_datacite_v1[df_datacite_v1['publisher'].str.contains('ICPSR|Mendeley|Zenodo')]
    df_datacite_lineageRepos = df_datacite_v1[~df_datacite_v1['publisher'].str.contains('ICPSR|Mendeley|Zenodo')]
    lineageRepos_deduplicated = lineageRepos[~lineageRepos['relation_type'].str.contains('IsVersionOf|IsNewVersionOf', case=False, na=False)]
    ###the use of .v* and v* as filters works for these repositories but could accidentally remove non-duplicate DOIs if applied to other repositories
    lineageRepos_deduplicated = lineageRepos_deduplicated[~lineageRepos_deduplicated['doi'].str.contains(r'\.v\d+$')]
    dois_to_remove = lineageRepos_deduplicated[(lineageRepos_deduplicated['doi'].str.contains(r'v\d$') | lineageRepos_deduplicated['doi'].str.contains(r'v\d-')) & (lineageRepos_deduplicated['publisher'].str.contains('ICPSR', case=False, na=False))]['doi']
    # Remove the identified DOIs
    lineageRepos_deduplicated = lineageRepos_deduplicated[~lineageRepos_deduplicated['doi'].isin(dois_to_remove)]
    df_datacite_v2 = pd.concat([df_datacite_lineageRepos, lineageRepos_deduplicated], ignore_index=True)

    #handling file-level DOI granularity (all Dataverse installations)
    ##may need to expand search terms if you find a Dataverse installation without 'Dataverse' in name
    df_datacite_dedup = df_datacite_v2[~(df_datacite_v2['publisher'].str.contains('Dataverse|Texas Data Repository', case=False, na=False) & df_datacite_v2['container_identifier'].notnull())]
    ##multi-condition to avoid removing multi-DOI consolidated Figshare deposits
    df_datacite_dedup = df_datacite_dedup[~((df_datacite_dedup['doi'].str.count('/') >= 3) & (df_datacite_dedup['publisher'] != 'figshare'))]

    #final sweeping dedpulication step, will catch a few odd edge cases that have been manually discovered
    ##mainly addresses hundreds of EMSL datasets that seem overly granularized (many deposits share all metadata other than DOI including detailed titles) - will not be relevant for all institutions
    df_sorted = df_datacite_dedup.sort_values(by='doi')
    df_datacite = df_sorted.drop_duplicates(subset=['title', 'first_author', 'relation_type', 'related_identifier', 'container_identifier'], keep='first')

    #standardizing specific repository name; may not be relevant for other institutions
    df_datacite.loc[df_datacite['publisher'].str.contains('Digital Rocks', case=False), 'publisher'] = 'Digital Porous Media Portal'
    df_datacite.to_csv(f"accessory-outputs/{today_date}_datacite-ror-retrieval-filtered.csv")
    print(f"Number of ROR-affiliated datasets left after cleaning: {len(df_datacite)}\n")

if exact_affiliation:
    print("Starting single-affiliation-string-based query")
    params_datacite = {
        'affiliation': 'true',
        'query': f'(creators.affiliation.name:"The University of Texas at Austin") AND types.resourceTypeGeneral:"Dataset"',
        'page[size]': config['VARIABLES']['PAGE_SIZES']['datacite'],
        'page[cursor]': 1,
    }

    #define different number of pages to retrieve from DataCite API based on 'test' vs. 'prod' env
    page_limit_datacite = config['VARIABLES']['PAGE_LIMITS']['datacite_test'] if test else config['VARIABLES']['PAGE_LIMITS']['datacite_prod']
    #define variables to be called recursively in function
    page_start_datacite = config['VARIABLES']['PAGE_STARTS']['datacite']

    print("Starting DataCite retrieval based on affiliation.\n")
    data_datacite = retrieve_datacite(url_datacite, params_datacite, page_start_datacite, page_limit_datacite, per_page_datacite)
    print(f"Number of official-UT-affiliated datasets found by DataCite API: {len(data_datacite)}\n")
    data_select_datacite = [] 
    for item in data_datacite:
        attributes = item.get('attributes', {})
        doi = attributes.get('doi', None)
        state = attributes.get('state', None)
        publisher = attributes.get('publisher', '')
        # publisher_year = attributes.get('publicationYear', '') #temporarily disabling due to Dryad metadata issue
        registered = attributes.get('registered', '')
        if registered:
            publisher_year = datetime.fromisoformat(registered[:-1]).year
            publisher_date = datetime.fromisoformat(registered[:-1]).date()
        else:
            publisher_year = None
            publisher_date = None
        title=attributes.get('titles', [{}])[0].get('title','')
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
        data_select_datacite.append({
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
        })

    df_datacite_initial = pd.json_normalize(data_select_datacite)
    #handling malformatted publisher field for Zenodo
    df_datacite_initial.loc[df_datacite_initial['doi'].str.contains('zenodo', case=False, na=False), 'publisher'] = 'Zenodo'
    #standardizing specific repository name that has three permutations; may not be relevant for other institutions
    df_datacite_initial.loc[df_datacite_initial['publisher'].str.contains('Digital Rocks', case=False), 'publisher'] = 'Digital Porous Media Portal'
    df_datacite_initial.loc[df_datacite_initial['publisher'].str.contains('Environmental System Science Data Infrastructure for a Virtual Ecosystem', case=False), 'publisher'] = 'ESS-DIVE'
    df_datacite_initial.loc[df_datacite_initial['publisher'].str.contains('Texas Data Repository|Texas Research Data Repository', case=False), 'publisher'] = 'Texas Data Repository'
    df_datacite_initial.loc[df_datacite_initial['publisher'].str.contains('ICPSR', case=True), 'publisher'] = 'ICPSR'
    df_datacite_initial.loc[df_datacite_initial['publisher'].str.contains('Environmental Molecular Sciences Laboratory', case=True), 'publisher'] = 'Environ Mol Sci Lab'
    df_datacite_initial.loc[df_datacite_initial['publisher'].str.contains('BCO-DMO', case=True), 'publisher'] = 'Biol Chem Ocean Data Mgmt Office'
    df_datacite_initial.loc[df_datacite_initial['publisher'].str.contains('Taylor & Francis|SAGE|The Royal Society|SciELO journals', case=True), 'publisher'] = 'figshare'
    df_datacite_initial.loc[df_datacite_initial['publisher'].str.contains('Oak Ridge', case=True), 'publisher'] = 'Oak Ridge National Laboratory'
    df_datacite_initial.loc[df_datacite_initial['publisher'].str.contains('PARADIM', case=True), 'publisher'] = 'PARADIM'
    df_datacite_initial.loc[df_datacite_initial['publisher'].str.contains('4TU', case=True), 'publisher'] = '4TU.ResearchData'
    df_datacite_initial.loc[df_datacite_initial['publisher'].str.contains('Scratchpads', case=True), 'publisher'] = 'Global Biodiversity Information Facility (GBIF)'

    #handling Figshare partners
    df_datacite_initial.loc[df_datacite_initial['publisher'].str.contains('Taylor & Francis|SAGE|The Royal Society|SciELO journals', case=True), 'publisher'] = 'figshare'
    df_datacite_initial.to_csv(f"accessory-outputs/{today_date}_datacite-single-affiliation-retrieval.csv")

    ### The below code is mostly duplicated from the main codebase but may not be used b/c it is unlikely that all of these repositories will be retrieved via a ROR-based query ###

    figshare = df_datacite_initial[df_datacite_initial['publisher'].str.contains('figshare')]
    df_datacite_no_figshare = df_datacite_initial[~df_datacite_initial['publisher'].str.contains('figshare')]
    figshare_no_versions = figshare[~figshare['doi'].str.contains(r'\.v\d+$')]
    #mediated workflow sometimes creates individual deposit for each file, want to treat as single dataset here
    for col in figshare_no_versions.columns:
        if figshare_no_versions[col].apply(lambda x: isinstance(x, list)).any():
            figshare_no_versions[col] = figshare_no_versions[col].apply(lambda x: tuple(x) if isinstance(x, list) else x)
    figshare_no_versions['had_partial_duplicate'] = figshare_no_versions.duplicated(subset=['publisher', 'publication_date', 'creators_names', 'creators_affiliations', 'type', 'related_identifier'], keep=False)

    #aggregating related entries together
    # figshare_no_versions_combined = figshare_no_versions.groupby('related_identifier').agg(lambda x: '; '.join(sorted(map(str, set(x))))).reset_index()
    sum_columns = ['deposit_size', 'views', 'citations', 'downloads']

    def agg_func(column_name):
        if column_name in sum_columns:
            return 'sum'
        else:
            return lambda x: sorted(set(x))

    #handling mixed-type columns that are expected to be only numeric
    for col in sum_columns:
        if col in figshare_no_versions.columns:
            figshare_no_versions[col] = pd.to_numeric(figshare_no_versions[col], errors='coerce')
    agg_funcs = {col: agg_func(col)for col in figshare_no_versions.columns if col != 'related_identifier'}

    figshare_no_versions_combined = figshare_no_versions.groupby('related_identifier').agg(agg_funcs).reset_index()
    # Convert all list-type columns to comma-separated strings
    for col in figshare_no_versions_combined.columns:
        if figshare_no_versions_combined[col].apply(lambda x: isinstance(x, list)).any():
            figshare_no_versions_combined[col] = figshare_no_versions_combined[col].apply(lambda x: '; '.join(map(str, x)))
    figshare_deduplicated = figshare_no_versions_combined.drop_duplicates(subset='related_identifier', keep='first')
    df_datacite_v1 = pd.concat([df_datacite_no_figshare, figshare_deduplicated], ignore_index=True)

    ##handling duplication of ICPSR, SAGE, Mendeley Data, Zenodo deposits (parent vs. child)
    lineageRepos = df_datacite_v1[df_datacite_v1['publisher'].str.contains('ICPSR|Mendeley|SAGE|Zenodo')]
    df_datacite_lineageRepos = df_datacite_v1[~df_datacite_v1['publisher'].str.contains('ICPSR|Mendeley|SAGE|Zenodo')]
    lineageRepos_deduplicated = lineageRepos[~lineageRepos['relation_type'].str.contains('IsVersionOf|IsNewVersionOf', case=False, na=False)]
    ###the use of .v* and v* as filters works for these repositories but could accidentally remove non-duplicate DOIs if applied to other repositories
    lineageRepos_deduplicated = lineageRepos_deduplicated[~lineageRepos_deduplicated['doi'].str.contains(r'\.v\d+$')]
    dois_to_remove = lineageRepos_deduplicated[(lineageRepos_deduplicated['doi'].str.contains(r'v\d$') | lineageRepos_deduplicated['doi'].str.contains(r'v\d-')) & (lineageRepos_deduplicated['publisher'].str.contains('ICPSR', case=False, na=False))]['doi']
    # Remove the identified DOIs
    lineageRepos_deduplicated = lineageRepos_deduplicated[~lineageRepos_deduplicated['doi'].isin(dois_to_remove)]
    df_datacite_v2 = pd.concat([df_datacite_lineageRepos, lineageRepos_deduplicated], ignore_index=True)

    #handling file-level DOI granularity (all Dataverse installations)
    ##may need to expand search terms if you find a Dataverse installation without 'Dataverse' in name
    df_datacite_dedup = df_datacite_v2[~(df_datacite_v2['publisher'].str.contains('Dataverse|Texas Data Repository', case=False, na=False) & df_datacite_v2['container_identifier'].notnull())]
    ##multi-condition to avoid removing multi-DOI consolidated Figshare deposits
    df_datacite_dedup = df_datacite_dedup[~((df_datacite_dedup['doi'].str.count('/') >= 3) & (df_datacite_dedup['publisher'] != 'figshare'))]

    #final sweeping dedpulication step, will catch a few odd edge cases that have been manually discovered
    ##mainly addresses hundreds of EMSL datasets that seem overly granularized (many deposits share all metadata other than DOI including detailed titles) - will not be relevant for all institutions
    df_sorted = df_datacite_dedup.sort_values(by='doi')
    df_datacite = df_sorted.drop_duplicates(subset=['title', 'first_author', 'relation_type', 'related_identifier', 'container_identifier'], keep='first')

    df_datacite.to_csv(f"accessory-outputs/{today_date}_datacite-single-affiliation-retrieval-filtered.csv")
    print(f"Number of official-UT-affiliated datasets left after cleaning: {len(df_datacite)}\n")

if wildcard_affiliation:
    print("Starting single-affiliation-string-based wildcard query")
    query_value = r'university\ of\ texas\ austin'
    query = f'creators.affiliation.name:{query_value}'
    params_datacite = {
        'affiliation': 'true',
        'query': query,
        'page[size]': config['VARIABLES']['PAGE_SIZES']['datacite'],
        'page[cursor]': 1,
    }

    #define different number of pages to retrieve from DataCite API based on 'test' vs. 'prod' env
    page_limit_datacite = config['VARIABLES']['PAGE_LIMITS']['datacite_test'] if test else config['VARIABLES']['PAGE_LIMITS']['datacite_prod']
    #define variables to be called recursively in function
    page_start_datacite = config['VARIABLES']['PAGE_STARTS']['datacite']

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
        creators_formatted = []
        for creator in creators:
            name = creator.get('name', '').strip()
            affiliations = creator.get('affiliation', [])
            updated_affiliations = []
            for affil in affiliations:
                affil_name = affil.get('name', '') if isinstance(affil, dict) else affil
                if 'Austin' in affil_name:
                    affil_name = 'University of Texas at Austin'
                updated_affiliations.append(affil_name)
            affil_str = ', '.join(updated_affiliations) if updated_affiliations else 'No affiliation listed'
            creators_formatted.append(f'{name} ({affil_str})')
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
        contributors_formatted = []
        for contributor in contributors:
            name = contributor.get('name', '').strip()
            affiliations = contributor.get('affiliation', [])
            updated_affiliations = []
            for affil in affiliations:
                affil_name = affil.get('name', '') if isinstance(affil, dict) else affil
                if 'Austin' in affil_name:
                    affil_name = 'University of Texas at Austin'
                updated_affiliations.append(affil_name)
            affil_str = ', '.join(updated_affiliations) if updated_affiliations else 'No affiliation listed'
            contributors_formatted.append(f'{name} ({affil_str})')
        container = attributes.get('container', {})
        container_identifier = container.get('identifier', None)
        related_identifiers = attributes.get('relatedIdentifiers', [])
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

    #standardizing specific repository name that has three permutations; may not be relevant for other institutions
    df_datacite_initial['publisher'] = df_datacite_initial['publisher'].fillna('None')
    df_datacite_initial.loc[df_datacite_initial['publisher'].str.contains('Digital Rocks', case=False), 'publisher'] = 'Digital Porous Media Portal'
    df_datacite_initial.loc[df_datacite_initial['publisher'].str.contains('Environmental System Science Data Infrastructure for a Virtual Ecosystem|Southeast Texas Urban Integrated Field Laboratory', case=False), 'publisher'] = 'ESS-DIVE'
    df_datacite_initial.loc[df_datacite_initial['publisher'].str.contains('Texas Data Repository|Texas Research Data Repository', case=False), 'publisher'] = 'Texas Data Repository'
    df_datacite_initial.loc[df_datacite_initial['publisher'].str.contains('ICPSR', case=True), 'publisher'] = 'ICPSR'
    df_datacite_initial.loc[df_datacite_initial['publisher'].str.contains('Environmental Molecular Sciences Laboratory', case=True), 'publisher'] = 'Environ Mol Sci Lab'
    df_datacite_initial.loc[df_datacite_initial['publisher'].str.contains('BCO-DMO|Biological and Chemical Oceanography Data', case=True), 'publisher'] = 'Biol Chem Ocean Data Mgmt Office'
    df_datacite_initial.loc[df_datacite_initial['publisher'].str.contains('BCO-DMO', case=True), 'publisher'] = 'Biol Chem Ocean Data Mgmt Office'
    df_datacite_initial.loc[df_datacite_initial['publisher'].str.contains('Taylor & Francis|SAGE|The Royal Society|SciELO journals', case=True), 'publisher'] = 'figshare'
    df_datacite_initial.loc[df_datacite_initial['publisher'].str.contains('Oak Ridge', case=True), 'publisher'] = 'Oak Ridge National Laboratory'
    df_datacite_initial.loc[df_datacite_initial['publisher'].str.contains('PARADIM', case=True), 'publisher'] = 'PARADIM'
    df_datacite_initial.loc[df_datacite_initial['publisher'].str.contains('4TU', case=True), 'publisher'] = '4TU.ResearchData'
    df_datacite_initial.loc[df_datacite_initial['publisher'].str.contains('Scratchpads|Biodiversity Collection|Algae', case=True), 'publisher'] = 'Global Biodiversity Information Facility (GBIF)'
    df_datacite_initial.loc[df_datacite_initial['publisher'].str.contains('NCAR', case=True), 'publisher'] = 'NSF NCAR Earth Observing Laboratory'
    df_datacite_initial.loc[df_datacite_initial['publisher'].str.contains('Consortium of Universities for the Advancement of Hydrologic Science, Inc', case=False), 'publisher'] = 'CUAHSI'
    df_datacite_initial.loc[df_datacite_initial['publisher'].str.contains('Bureau of Economic Geology (UT-BEG)', case=False), 'publisher'] = 'AmeriFlux'
    df_datacite_initial.loc[df_datacite_initial['doi'].str.contains('zenodo', case=False), 'publisher'] = 'Zenodo'

    #handling Figshare partners
    df_datacite_initial.loc[df_datacite_initial['publisher'].str.contains('Taylor & Francis|SAGE|The Royal Society|SciELO journals', case=True), 'publisher'] = 'figshare'
    df_datacite_initial.to_csv(f"accessory-outputs/{today_date}_datacite-single-affiliation-retrieval-wildcard-search.csv")

    ### The below code is mostly duplicated from the main codebase but may not be used b/c it is unlikely that all of these repositories will be retrieved via a ROR-based query ###

    figshare = df_datacite_initial[df_datacite_initial['publisher'].str.contains('figshare')]
    df_datacite_no_figshare = df_datacite_initial[~df_datacite_initial['publisher'].str.contains('figshare')]
    figshare_no_versions = figshare[~figshare['doi'].str.contains(r'\.v\d+$')]
    #mediated workflow sometimes creates individual deposit for each file, want to treat as single dataset here
    for col in figshare_no_versions.columns:
        if figshare_no_versions[col].apply(lambda x: isinstance(x, list)).any():
            figshare_no_versions[col] = figshare_no_versions[col].apply(lambda x: tuple(x) if isinstance(x, list) else x)
    figshare_no_versions['had_partial_duplicate'] = figshare_no_versions.duplicated(subset=['publisher', 'publication_date', 'creators_names', 'creators_affiliations', 'type', 'related_identifier'], keep=False)

    #aggregating related entries together
    # figshare_no_versions_combined = figshare_no_versions.groupby('relatedIdentifier').agg(lambda x: '; '.join(sorted(map(str, set(x))))).reset_index()
    sum_columns = ['deposit_size', 'views', 'citations', 'downloads']

    def agg_func(column_name):
        if column_name in sum_columns:
            return 'sum'
        else:
            return lambda x: sorted(set(x))

    # Convert numeric columns safely
    for col in sum_columns:
        if col in figshare_no_versions.columns:
            figshare_no_versions[col] = pd.to_numeric(figshare_no_versions[col], errors='coerce')

    # Convert sets/lists to strings in all other columns except 'related_identifier'
    for col in figshare_no_versions.columns:
        if col not in sum_columns and col != 'related_identifier':
            figshare_no_versions[col] = figshare_no_versions[col].apply(
                lambda val: ', '.join(sorted(map(str, val))) if isinstance(val, (set, list)) else val
            )

    # Standardize object type of related_identifier
    def to_hashable(val):
        if isinstance(val, (set, list)):
            return ', '.join(sorted(map(str, val)))
        return val

    figshare_no_versions['related_identifier'] = figshare_no_versions['related_identifier'].apply(to_hashable)

    agg_funcs = {col: agg_func(col) for col in figshare_no_versions.columns if col != 'related_identifier'}

    figshare_no_versions_combined = figshare_no_versions.groupby('related_identifier').agg(agg_funcs).reset_index()
    # Convert all list-type columns to comma-separated strings
    for col in figshare_no_versions_combined.columns:
        if figshare_no_versions_combined[col].apply(lambda x: isinstance(x, list)).any():
            figshare_no_versions_combined[col] = figshare_no_versions_combined[col].apply(lambda x: '; '.join(map(str, x)))
    figshare_deduplicated = figshare_no_versions_combined.drop_duplicates(subset='related_identifier', keep='first')
    df_datacite_v1 = pd.concat([df_datacite_no_figshare, figshare_deduplicated], ignore_index=True)

    ##handling duplication of ICPSR, SAGE, Mendeley Data, Zenodo deposits (parent vs. child)
    lineageRepos = df_datacite_v1[df_datacite_v1['publisher'].str.contains('ICPSR|Mendeley|SAGE|Zenodo')]
    df_datacite_lineageRepos = df_datacite_v1[~df_datacite_v1['publisher'].str.contains('ICPSR|Mendeley|SAGE|Zenodo')]
    lineageRepos_deduplicated = lineageRepos[~lineageRepos['relation_type'].str.contains('IsVersionOf|IsNewVersionOf', case=False, na=False)]
    ###the use of .v* and v* as filters works for these repositories but could accidentally remove non-duplicate DOIs if applied to other repositories
    lineageRepos_deduplicated = lineageRepos_deduplicated[~lineageRepos_deduplicated['doi'].str.contains(r'\.v\d+$')]
    dois_to_remove = lineageRepos_deduplicated[(lineageRepos_deduplicated['doi'].str.contains(r'v\d$') | lineageRepos_deduplicated['doi'].str.contains(r'v\d-')) & (lineageRepos_deduplicated['publisher'].str.contains('ICPSR', case=False, na=False))]['doi']
    # Remove the identified DOIs
    lineageRepos_deduplicated = lineageRepos_deduplicated[~lineageRepos_deduplicated['doi'].isin(dois_to_remove)]
    df_datacite_v2 = pd.concat([df_datacite_lineageRepos, lineageRepos_deduplicated], ignore_index=True)

    #handling file-level DOI granularity (all Dataverse installations)
    ##may need to expand search terms if you find a Dataverse installation without 'Dataverse' in name
    df_datacite_dedup = df_datacite_v2[~(df_datacite_v2['publisher'].str.contains('Dataverse|Texas Data Repository|CUHK Research Data Repository|Qualitative Data Repository', case=False, na=False) & df_datacite_v2['container_identifier'].notnull())]
    ##multi-condition to avoid removing multi-DOI consolidated Figshare deposits
    df_datacite_dedup = df_datacite_dedup[~((df_datacite_dedup['doi'].str.count('/') >= 3) & (df_datacite_dedup['publisher'] != 'figshare'))]

    #final sweeping dedpulication step, will catch a few odd edge cases that have been manually discovered
    ##mainly addresses hundreds of EMSL datasets that seem overly granularized (many deposits share all metadata other than DOI including detailed titles) - will not be relevant for all institutions
    df_sorted = df_datacite_dedup.sort_values(by='doi')
    df_datacite = df_sorted.drop_duplicates(subset=['title', 'first_author', 'relation_type', 'related_identifier', 'container_identifier'], keep='first')

    df_datacite.to_csv(f"accessory-outputs/{today_date}_datacite-single-affiliation-retrieval-wildcard-search-filtered.csv")
    print(f"Number of probably-UT-affiliated datasets left after cleaning: {len(df_datacite)}\n")

print("Done.\n")
print(f"Time to run: {datetime.now() - start_time}")