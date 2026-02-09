from datetime import datetime
from pprint import pformat
from rapidfuzz import process, fuzz
from urllib.parse import quote
from utils import adjust_descriptive_count, check_link, count_words, determine_affiliation, retrieve_all_journals, retrieve_crossref, retrieve_datacite, retrieve_dataverse, retrieve_dryad, retrieve_openalex, retrieve_zenodo #custom functions file
import pandas as pd
import json
import numpy as np
import os
import re
import requests
import time

#read in config file
with open('config.json', 'r') as file:
    config = json.load(file)

#operator for quick test runs
test = config['TOGGLES']['test']

#operator for resource types to query for (use OR and put in parentheses for multiple types)
##GENERAL DataCite query
resource_types = ['Dataset', 'Software']
# Datacite format
datacite_resource_type = '(' + ' OR '.join(resource_types) + ')'
# Zenodo format
zenodo_resource_type = '(' + ' OR '.join([f'type:{rt.lower()}' for rt in resource_types]) + ')'

##create string to include in filenames based on resource type
resource_filename = '-'.join([rt.lower() for rt in resource_types])
##toggle based on whether resource_type is used in the API query
resource_type_filter = config['TOGGLES']['resource_type_filter']
if resource_type_filter:
    resource_filename = resource_filename
else:
    resource_filename = 'all-resource-types'

#operator for resource type(s) to query for (use OR and put in parentheses for multiple types)
##Figshare workflow
figshare_resource_types = ['Dataset', 'Software']
figshare_datacite_resource_type = '(' + ' OR '.join(figshare_resource_types) + ')'
figshare_resource_filename = '-'.join([rt.lower() for rt in figshare_resource_types])
##toggle based on whether resource_type is used in the API query
figshare_resource_type_filter = config['TOGGLES']['figshare_resource_type_filter']
if figshare_resource_type_filter:
    figshare_resource_filename = figshare_resource_filename
else:
    figshare_resource_filename = 'all-resource-types'

#toggle for cross-validation steps
cross_validate = config['TOGGLES']['cross_validate']
##toggle for Dataverse cross-validation specifically
dataverse = config['TOGGLES']['dataverse']
##toggle for de-duplicating partial Dataverse replicates (multiple deposits for one manuscript within one dataverse) - see README for details
dataverse_duplicates = config['TOGGLES']['dataverse_duplicates']
##toggle for UT Austin specific edge cases (set to False if you are not at UT Austin)
austin = config['TOGGLES']['austin']

#toggles for executing Figshare processes (see README for details)
##looking for datasets with a journal publisher listed as publisher, X-ref'ing with university articles from that publisher
figshare_workflow_1 = config['TOGGLES']['figshare_workflow_1']
##finding university articles from publisher that uses certain formula for Figshare DOIs, construct hypothetical DOI, test if it exists
figshare_workflow_2 = config['TOGGLES']['figshare_workflow_2']

##if you have done a previous DataCite retrieval and don't want to re-run the entire main process (skip to Figshare steps)
load_previous_data = config['TOGGLES']['load_previous_data']
#if you have done a previous DataCite retrieval and Figshare workflow 1 and don't want to re-run these
load_previous_data_plus = config['TOGGLES']['load_previous_data_plus']
#toggle for executing NCBI process
ncbi_workflow = config['TOGGLES']['ncbi_workflow']
##loading package in only if running NCBI workflow
if ncbi_workflow:
    import xml.etree.ElementTree as ET
#toggle for whether to use biopython approach to NCBI (TRUE = biopython; FALSE = Selenium)
biopython = config['TOGGLES']['biopython']
##loading packages in only if running NCBI workflow and depending on selection
if biopython and ncbi_workflow:
    from Bio import Entrez
elif not biopython and ncbi_workflow:
    from selenium import webdriver
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.common.exceptions import TimeoutException
    from selenium.webdriver.firefox.options import Options

#toggle for skipping web retrieval of NCBI data (just XML to dataframe conversion)
load_ncbi_data = config['TOGGLES']['load_ncbi_data']
#toggle for loading previous DataCite + Figshare workflow 1 + NCBI
load_previous_data_plus_ncbi = config['TOGGLES']['load_previous_data_plus_ncbi']
#toggle to load in externally generated Crossref data
load_crossref = config['TOGGLES']['load_crossref']

#conditional toggles, if loading in previous data, automatically set certain other toggles to False regardless of how they are set
##should minimize how much you need to edit multiple toggles (W.I.P.)
if load_previous_data_plus:
    figshare_workflow_1 = False
    figshare_workflow_2 = False
if load_previous_data_plus_ncbi:
    figshare_workflow_1 = False
    figshare_workflow_2 = False
    ncbi_workflow = False

#creating directories
##write logs regardless of env
if os.path.isdir('logs'):
        print('logs directory found - no need to recreate')
else:
    os.mkdir('logs')
    print('logs directory has been created')
if test:
    if os.path.isdir('test'):
        print('test directory found - no need to recreate')
    else:
        os.mkdir('test')
        print('test directory has been created')
    os.chdir('test')
    if os.path.isdir('outputs'):
        print('test outputs directory found - no need to recreate')
    else:
        os.mkdir('outputs')
        print('test outputs directory has been created')
    if os.path.isdir('logs'):
        print('logs directory found - no need to recreate')
    else:
        os.mkdir('logs')
        print('test logs directory has been created')
else:
    if os.path.isdir('outputs'):
        print('outputs directory found - no need to recreate')
    else:
        os.mkdir('outputs')
        print('outputs directory has been created')

#setting timestamp to calculate run time
start_time = datetime.now() 
##with timezone for log file
start_timezone = datetime.now().astimezone()
start_timezone_formatted = start_timezone.strftime('%Y-%m-%d %H:%M:%S %Z%z')
#creating variable with current date for appending to filenames
today = datetime.now().strftime('%Y%m%d') 

#read in email address for polite requests (required for biopython NCBI workflow, can be used for other APIs)
email = config['EMAIL']['user_email']

#create permutation string with OR for API parameters
ut_variations = config['PERMUTATIONS']
institution_query = ' OR '.join([f'"{variation}"' for variation in ut_variations])
##if you need a smaller set of previously identified permutations for an easier API call
ut_variations_small = config['PERMUTATIONS_IDENTIFIED']
institution_query_small = ' OR '.join([f'"{variation}"' for variation in ut_variations_small])

#pull in ROR ID (necessary for Dryad API)
ror_id = config['INSTITUTION']['ror']

#pulling in 'uniqueIdentifer' term used as quick, reliable filter ('Austin' for filtering an affiliation field for UT Austin)
uni_identifier = config['INSTITUTION']['uniqueIdentifier']

#API endpoints
url_crossref = 'https://api.crossref.org/works/'
url_crossref_issn = 'https://api.crossref.org/journals/{issn}/works'
url_dryad = f'https://datadryad.org/api/v2/search?affiliation={ror_id}' #Dryad requires ROR for affiliation search
url_datacite = 'https://api.datacite.org/dois'
url_dataverse = 'https://dataverse.tdl.org/api/search/'
url_openalex = 'https://api.openalex.org/works?'
url_zenodo = 'https://zenodo.org/api/records'

##per page
per_page_datacite = config['VARIABLES']['PAGE_SIZES']['datacite']
per_page_dryad = config['VARIABLES']['PAGE_SIZES']['dryad']
per_page_dataverse = config['VARIABLES']['PAGE_SIZES']['dataverse']
per_page_zenodo = config['VARIABLES']['PAGE_SIZES']['zenodo']

##page start
page_start_dryad = config['VARIABLES']['PAGE_STARTS']['dryad']
page_start_dataverse = config['VARIABLES']['PAGE_STARTS']['dataverse']
page_start_datacite = config['VARIABLES']['PAGE_STARTS']['datacite']
page_start_zenodo = config['VARIABLES']['PAGE_STARTS']['zenodo']

#page count, differ based on 'test' vs. 'prod' env
page_limit_datacite = config['VARIABLES']['PAGE_LIMITS']['datacite_test'] if test else config['VARIABLES']['PAGE_LIMITS']['datacite_prod']
page_limit_zenodo = config['VARIABLES']['PAGE_LIMITS']['zenodo_test'] if test else config['VARIABLES']['PAGE_LIMITS']['zenodo_prod']
page_limit_openalex = config['VARIABLES']['PAGE_LIMITS']['openalex_test'] if test else config['VARIABLES']['PAGE_LIMITS']['openalex_prod']

params_dryad= {
    'per_page': per_page_dryad,
}

if resource_type_filter:
    params_datacite = {
        'affiliation': 'true',
        'query': f'(creators.affiliation.name:({institution_query}) OR creators.name:({institution_query}) OR contributors.affiliation.name:({institution_query}) OR contributors.name:({institution_query})) AND types.resourceTypeGeneral:{datacite_resource_type}',
        'page[size]': per_page_datacite,
        'page[cursor]': 1,
    }
else:
    params_datacite = {
        'affiliation': 'true',
        'query': f'(creators.affiliation.name:({institution_query}) OR creators.name:({institution_query}) OR contributors.affiliation.name:({institution_query}) OR contributors.name:({institution_query}))',
        'page[size]': per_page_datacite,
        'page[cursor]': 1,
    }

headers_dataverse = {
    'X-Dataverse-key': config['KEYS']['dataverseToken']
}
params_dataverse = {
    'q': '10.18738/T8/',
    #UT Austin dataverse, may contain non-UT affiliated objects, and UT-affiliated objects may be in other TDR installations
    #'subtree': 'utexas', 
    'type': 'dataset', #dataverse may also mint DOIs for files
    'start': page_start_dataverse,
    'page': config['VARIABLES']['PAGE_INCREMENTS']['dataverse'],
    'per_page': per_page_dataverse
}

params_zenodo = {
    'q': f'(creators.affiliation:({institution_query_small}) OR creators.name:({institution_query_small}) OR contributors.affiliation:({institution_query_small}) OR contributors.name:({institution_query_small})) AND {zenodo_resource_type}',
    'size': per_page_zenodo,
    'access_token': config['KEYS']['zenodoToken']
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
##software formats
software_formats = set(config['SOFTWARE_FORMATS'].values())
##convert mimeType to readable format
format_map = config['FORMAT_MAP']

# Running script
if not load_previous_data and not load_previous_data_plus and not load_previous_data_plus_ncbi:
    print('Starting DataCite retrieval based on affiliation.\n')
    data_datacite = retrieve_datacite(url_datacite, params_datacite, page_start_datacite, page_limit_datacite, per_page_datacite)
    print(f'Number of datasets found by DataCite API: {len(data_datacite)}\n')

    if cross_validate:
        print('Starting Dryad retrieval.\n')
        data_dryad = retrieve_dryad(url_dryad, params_dryad, page_start_dryad, per_page_dryad)
        print(f'Number of Dryad datasets found by Dryad API: {len(data_dryad)}\n')
        if dataverse:
            print('Starting Dataverse retrieval.\n')
            data_dataverse = retrieve_dataverse(url_dataverse, params_dataverse, headers_dataverse, page_start_dataverse, per_page_dataverse)
            print(f'Number of Dataverse datasets found by Dataverse API: {len(data_dataverse)}\n')
        print('Starting Zenodo retrieval.\n')
        data_zenodo = retrieve_zenodo(url_zenodo, params_zenodo, page_start_zenodo, page_limit_zenodo, per_page_zenodo)
        print(f'Number of Zenodo datasets found by Zenodo API: {len(data_zenodo)}\n')

    print('Beginning dataframe generation.\n')

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
        first_affiliation = creators_affiliations[0] if creators_affiliations else None
        last_affiliation = creators_affiliations[-1] if creators_affiliations else None
        contributors = attributes.get('contributors', [{}])
        contributors_names = [contributor.get('name', '') for contributor in contributors]
        contributors_affiliations = [
            aff.get('name', '')
            for contributor in contributors
            for aff in (contributor.get('affiliation') if isinstance(contributor.get('affiliation'), list) else [])
            if isinstance(aff, dict)
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
    df_datacite_initial.to_csv(f'outputs/{today}_{resource_filename}_datacite-initial-output.csv', index=False, encoding='utf-8')

    if cross_validate:
        #first processing DataCite outputs
        #split out DataCite results for repos to be cross-validated against
        ##coercing all DOIs with 'zenodo' to have publisher of 'Zenodo'
        df_datacite_initial.loc[df_datacite_initial['doi'].str.contains('zenodo', case=False, na=False), 'publisher'] = 'Zenodo'
        ##using str.contains to account for any potential name inconsistency for one repository
        df_datacite_dryad = df_datacite_initial[df_datacite_initial['publisher'].str.contains('Dryad')]
        df_datacite_dataverse = df_datacite_initial[df_datacite_initial['publisher'].str.contains('Texas Data Repository')]
        df_datacite_zenodo = df_datacite_initial[df_datacite_initial['publisher'].str.contains('Zenodo')]
        df_remainder = df_datacite_initial[df_datacite_initial['publisher'].str.contains('Dryad|Texas Data Repository|Zenodo') == False]

        print(f'Number of Dryad datasets found by DataCite API: {len(df_datacite_dryad)}\n')
        print(f'Number of Dataverse datasets found by DataCite API: {len(df_datacite_dataverse)}\n')
        print(f'Number of Zenodo datasets found by DataCite API: {len(df_datacite_zenodo)}\n')

        df_datacite_dryad_pruned = df_datacite_dryad[['publisher', 'doi', 'publication_year', 'title', 'first_author', 'first_affiliation', 'last_author', 'last_affiliation', 'type']]
        df_datacite_dataverse_pruned = df_datacite_dataverse[['publisher', 'doi', 'publication_year', 'title', 'first_author', 'first_affiliation', 'last_author', 'last_affiliation', 'type']] 
        df_datacite_zenodo_pruned = df_datacite_zenodo[['publisher', 'doi', 'publication_year', 'title', 'first_author', 'first_affiliation', 'last_author', 'last_affiliation','type']] 
        df_datacite_remainder_pruned = df_remainder[['publisher', 'doi', 'publication_year', 'title', 'first_author', 'first_affiliation', 'last_author', 'last_affiliation','type']] 

        #create new lists for recursive modification
        datacite_dataframes_pruned = [df_datacite_dryad_pruned, df_datacite_dataverse_pruned, df_datacite_zenodo_pruned, df_datacite_remainder_pruned]
        datacite_dataframes_specific_pruned = [df_datacite_dryad_pruned, df_datacite_dataverse_pruned, df_datacite_zenodo_pruned]

        #standardizing how Texas Data Repository is displayed
        df_datacite_dataverse_pruned['publisher'] = df_datacite_dataverse_pruned['publisher'].str.replace('Texas Data Repository Dataverse','Texas Data Repository')

        for df in datacite_dataframes_specific_pruned:
            df['doi'] = df['doi'].str.lower()

        columns_to_rename = {
            'publisher': 'repository'
        }
        for i in range(len(datacite_dataframes_pruned)):
            datacite_dataframes_pruned[i] = datacite_dataframes_pruned[i].rename(columns=columns_to_rename)
            datacite_dataframes_pruned[i]['source'] = 'DataCite'
        #assign modified dfs back to original
        df_datacite_dryad_pruned, df_datacite_dataverse_pruned, df_datacite_zenodo_pruned, df_datacite_remainder_pruned = datacite_dataframes_pruned

        #reload list
        datacite_dataframes_specific_pruned = [df_datacite_dryad_pruned, df_datacite_dataverse_pruned, df_datacite_zenodo_pruned]
        for i in range(len(datacite_dataframes_specific_pruned)):
            datacite_dataframes_specific_pruned[i] = datacite_dataframes_specific_pruned[i].rename(columns={c: c+'_dc' for c in datacite_dataframes_specific_pruned[i].columns if c not in ['doi']})
        #assign modified dfs back to original
        df_datacite_dryad_pruned, df_datacite_dataverse_pruned, df_datacite_zenodo_pruned = datacite_dataframes_specific_pruned

        #initialize dfs for APIs
        df_dryad_undetected = pd.DataFrame()
        df_dataverse_undetected = pd.DataFrame()
        df_zenodo_undetected = pd.DataFrame()

        print('Dryad step\n')
        if data_dryad:
            data_select_dryad = [] 
            for item in data_dryad:
                links = item.get('_links', {})
                doi_dr = item.get('identifier', None)
                pubDate_dr = item.get('publicationDate', '')
                title_dr=item.get('title', [{}])
                authors_dr = item.get('authors', [{}])
                first_author_first = authors_dr[0].get('firstName', None)
                first_author_last = authors_dr[0].get('lastName', None)
                last_author_first = authors_dr[-1].get('firstName', None)
                last_author_last = authors_dr[-1].get('lastName', None)
                first_affiliation_dr = authors_dr[0].get('affiliation', None)
                last_affiliation_dr = authors_dr[-1].get('affiliation', None)
                related_works_list_dr = [rel.get('identifier', None) for rel in item.get('relatedWorks', [{}])]
                related_works_list_dr = related_works_list_dr if related_works_list_dr else None
                author_last_order_dr = authors_dr[-1].get('order', None)
                data_select_dryad.append({
                    'doi': doi_dr,
                    'publication_date': pubDate_dr,
                    'title': title_dr,
                    'first_author_first': first_author_first,
                    'last_author_first': last_author_first,
                    'first_author_last': first_author_last,
                    'last_author_last': last_author_last,
                    'first_affiliation': first_affiliation_dr,
                    'last_affiliation': last_affiliation_dr,
                    'related_works': related_works_list_dr
                })
            df_dryad = pd.json_normalize(data_select_dryad)
            df_dryad.to_csv(f'outputs/{today}_Dryad-API-output.csv', index=False, encoding='utf-8')
            #formatting author names to be consistent with others
            df_dryad['first_author'] = df_dryad['first_author_last'] + ', ' + df_dryad['first_author_first']
            df_dryad['last_author'] = df_dryad['last_author_last'] + ', ' + df_dryad['last_author_first']
            df_dryad = df_dryad.drop(columns=['first_author_first', 'first_author_last', 'last_author_first', 'last_author_last'])
            df_dryad['publication_year'] = pd.to_datetime(df_dryad['publication_date']).dt.year
            df_dryad_pruned = df_dryad[['doi', 'publication_year', 'title', 'first_author', 'first_affiliation', 'last_author', 'last_affiliation']]

            #editing DOI columns to ensure exact matches
            df_dryad_pruned['doi'] = df_dryad_pruned['doi'].str.replace('doi:', '')
            df_dryad_pruned['doi'] = df_dryad_pruned['doi'].str.lower()
            #adding suffix to column headers to differentiate identically named columns when merged (vs. automatic .x and .y)
            df_dryad_pruned = df_dryad_pruned.rename(columns={c: c+'_dryad' for c in df_dryad_pruned.columns if c not in ['doi']})
            df_dryad_pruned['source_dryad'] = 'Dryad'        

            #DataCite into Dryad
            df_dryad_datacite_joint = pd.merge(df_dryad_pruned, df_datacite_dryad_pruned, on='doi', how='left')
            df_dryad_datacite_joint['Match_entry'] = np.where(df_dryad_datacite_joint['source_dc'].isnull(), 'Not matched', 'Matched')
            print('Counts of matches for DataCite into Dryad')
            counts_dryad_datacite = df_dryad_datacite_joint['Match_entry'].value_counts()
            print(counts_dryad_datacite, '\n')
            df_dryad_datacite_joint_unmatched = df_dryad_datacite_joint[df_dryad_datacite_joint['Match_entry'] == 'Not matched']
            df_dryad_datacite_joint_unmatched.to_csv(f'outputs/{today}_DataCite-into-Dryad_joint-unmatched-dataframe.csv', index=False, encoding='utf-8')
            df_dryad_undetected = df_dryad_datacite_joint_unmatched[['doi']]

            #Dryad into DataCite
            df_datacite_dryad_joint = pd.merge(df_datacite_dryad_pruned, df_dryad_pruned, on='doi', how='left')
            df_datacite_dryad_joint['Match_entry'] = np.where(df_datacite_dryad_joint['source_dryad'].isnull(), 'Not matched', 'Matched')
            print('Counts of matches for Dryad into DataCite')
            counts_datacite_dryad = df_datacite_dryad_joint['Match_entry'].value_counts()
            print(counts_datacite_dryad, '\n')
            df_datacite_dryad_joint_unmatched = df_datacite_dryad_joint[df_datacite_dryad_joint['Match_entry'] == 'Not matched']
            df_datacite_dryad_joint_unmatched.to_csv(f'outputs/{today}_Dryad-into-DataCite_joint-unmatched-dataframe.csv', index=False, encoding='utf-8')

        if dataverse:
            if data_dataverse:
                print('Dataverse step\n')
                data_select_dataverse = [] 
                for item in data_dataverse:
                    globalID = item.get('global_id', '')
                    versionState = item.get('versionState', None)
                    pubDate_dataverse = item.get('published_at', '')
                    title_dataverse = item.get('name', None)
                    authors_dataverse = item.get('authors', [{}])
                    contacts_dataverse = item.get('contacts', [{}])
                    first_contact_dataverse = contacts_dataverse[0].get('name', None)
                    first_affiliation_contact = contacts_dataverse[0].get('affiliation', None)
                    last_contact_dataverse = contacts_dataverse[-1].get('name', None)
                    last_affiliation_contact = contacts_dataverse[-1].get('affiliation', None)
                    type = item.get('type', None)
                    dataverse = item.get('name_of_dataverse', None)
                    data_select_dataverse.append({
                        'doi': globalID,
                        'status': versionState,
                        'publication_date': pubDate_dataverse,
                        'title': title_dataverse,
                        'authors': authors_dataverse,
                        'contacts': contacts_dataverse,
                        'first_contact': first_contact_dataverse,
                        'first_contact_affiliation': first_affiliation_contact,
                        'last_contact': last_contact_dataverse,
                        'last_contact_affiliation': last_affiliation_contact,
                        'type': type,
                        'dataverse': dataverse
                    })
                df_dataverse = pd.json_normalize(data_select_dataverse)
                df_dataverse.to_csv(f'outputs/{today}_TDR-API-output.csv', index=False, encoding='utf-8')

                #subsetting for published datasets
                df_dataverse_pub = df_dataverse[df_dataverse['status'].str.contains('RELEASED') == True]
                df_dataverse_pub['doi'] = df_dataverse_pub['doi'].str.lower()
                #looking for UT Austin in any of four fields
                pattern = '|'.join([f'({perm})' for perm in ut_variations])
                df_dataverse_pub['authors'] = df_dataverse_pub['authors'].apply(lambda x: str(x))
                df_dataverse_pub['contacts'] = df_dataverse_pub['contacts'].apply(lambda x: str(x))
                df_dataverse_pub_filtered = df_dataverse_pub[df_dataverse_pub['authors'].str.contains(pattern, case=False, na=False) | df_dataverse_pub['contacts'].str.contains(pattern, case=False, na=False)]
                print(f'Number of published Dataverse datasets found by Dataverse API: {len(df_dataverse_pub)}\n')
                df_dataverse_pub_filtered['publication_year'] = pd.to_datetime(df_dataverse_pub_filtered['publication_date'], format='ISO8601').dt.year
                df_dataverse_pruned = df_dataverse_pub_filtered[['doi', 'publication_year', 'title', 'first_contact', 'first_contact_affiliation', 'last_contact', 'last_contact_affiliation']]

                df_dataverse_pruned['doi'] = df_dataverse_pruned['doi'].str.replace('doi:', '')
                df_dataverse_pruned = df_dataverse_pruned.rename(columns={c: c+'_dataverse' for c in df_dataverse_pruned.columns if c not in ['doi']})
                df_dataverse_pruned['source_dataverse'] = 'Texas Data Repository'
                
                #DataCite into Dataverse (using non-de-duplicated DataCite data)
                df_dataverse_datacite_joint = pd.merge(df_dataverse_pruned, df_datacite_dataverse_pruned, on='doi', how='left')
                df_dataverse_datacite_joint['Match_entry'] = np.where(df_dataverse_datacite_joint['source_dc'].isnull(), 'Not matched', 'Matched')
                print('Counts of matches for DataCite into Dataverse')
                counts_dataverse_datacite = df_dataverse_datacite_joint['Match_entry'].value_counts()
                print(counts_dataverse_datacite, '\n')
                df_dataverse_datacite_joint_unmatched = df_dataverse_datacite_joint[df_dataverse_datacite_joint['Match_entry'] == 'Not matched']
                df_dataverse_datacite_joint_unmatched.to_csv(f'outputs/{today}_DataCite-into-Dataverse_joint-unmatched-dataframe.csv', index=False, encoding='utf-8')
                df_dataverse_undetected = df_dataverse_datacite_joint_unmatched[['doi']]

                #Dataverse into DataCite (using de-duplicated DataCite data)
                df_datacite_dataverse_joint = pd.merge(df_datacite_dataverse_pruned, df_dataverse_pruned, on='doi', how='left')
                df_datacite_dataverse_joint['Match_entry'] = np.where(df_datacite_dataverse_joint['source_dataverse'].isnull(), 'Not matched', 'Matched')
                print('Counts of matches for Dataverse into DataCite')
                counts_datacite_dataverse = df_datacite_dataverse_joint['Match_entry'].value_counts()
                print(counts_datacite_dataverse, '\n')
                df_datacite_dataverse_joint_unmatched = df_datacite_dataverse_joint[df_datacite_dataverse_joint['Match_entry'] == 'Not matched']
                df_datacite_dataverse_joint_unmatched.to_csv(f'outputs/{today}_Dataverse-into-DataCite_joint-unmatched-dataframe.csv', index=False, encoding='utf-8')

        print('Zenodo step\n')
        if data_zenodo:
            data_select_zenodo = [] 
            for item in data_zenodo:
                metadata = item.get('metadata', {})
                doi = item.get('doi', None)
                parentDOI = item.get('conceptdoi', None)
                conceptID = item.get('conceptrecid', None)
                pubDate_zen = metadata.get('publication_date', '')
                title_zen = metadata.get('title', '')
                description_zen = metadata.get('description', None)
                creators_zen = metadata.get('creators', [{}])
                first_creator_zen = creators_zen[0].get('name', None)
                last_creator_zen = creators_zen[-1].get('name', None)
                first_affiliation_zen = creators_zen[0].get('affiliation', None)
                last_affiliation_zen = creators_zen[-1].get('affiliation', None)
                related_works_list_zen = [name.get('identifier', None) for name in metadata.get('relatedWorks', [{}])]
                related_works_list_zen = related_works_list_zen if related_works_list_zen else None
                related_works_type_list_zen = [name.get('relation', None) for name in metadata.get('relatedWorks', [{}])]
                related_works_type_list_zen = related_works_type_list_zen if related_works_type_list_zen else None
                data_select_zenodo.append({
                    'doi': parentDOI, #want parent to avoid de-duplication issues later
                    'publication_date': pubDate_zen,
                    'title': title_zen,
                    'description': description_zen,
                    'first_author': first_creator_zen,
                    'last_author': last_creator_zen,
                    'first_affiliation': first_affiliation_zen,
                    'last_affiliation': last_affiliation_zen,
                    'related_works': related_works_list_zen,
                    'related_works_type': related_works_type_list_zen
                })
            df_data_zenodo = pd.json_normalize(data_select_zenodo)
            df_data_zenodo.to_csv(f'outputs/{today}_Zenodo-API-output.csv', index=False, encoding='utf-8')
            #removing non-Zenodo deposits indexed by Zenodo (mostly Dryad) from Zenodo output
            ##Zenodo has indexed many Dryad deposits <50 GB in size (does not issue a new DOI but does return a Zenodo 'record' in the API)
            df_data_zenodo['doi'] = df_data_zenodo['doi'].str.lower()
            df_data_zenodo_true = df_data_zenodo[df_data_zenodo['doi'].str.contains('zenodo') == True] 
            #for some reason, Zenodo's API sometimes returns identical entries of most datasets...
            df_data_zenodo_real = df_data_zenodo_true.drop_duplicates(subset=['publication_date', 'doi'], keep='first') 
            print(f'Number of non-Dryad Zenodo datasets found by Zenodo API: {len(df_data_zenodo_real)}\n')
            df_data_zenodo_real['publication_year'] = pd.to_datetime(df_data_zenodo_real['publication_date'], format='mixed').dt.year
            df_zenodo_pruned = df_data_zenodo_real[['doi','publication_year', 'title','first_author', 'first_affiliation', 'last_author', 'last_affiliation', 'publication_date', 'description']]
            df_zenodo_pruned = df_zenodo_pruned.rename(columns={c: c+'_zen' for c in df_data_zenodo_real.columns if c not in ['doi']})
            df_zenodo_pruned['source_zenodo'] = 'Zenodo'

            #DataCite into Zenodo
            df_zenodo_datacite_joint = pd.merge(df_zenodo_pruned, df_datacite_zenodo_pruned, on='doi', how='left')
            df_zenodo_datacite_joint['Match_entry'] = np.where(df_zenodo_datacite_joint['source_dc'].isnull(), 'Not matched', 'Matched')
            ##removing multiple DOIs in same 'lineage'
            df_zenodo_datacite_joint = df_zenodo_datacite_joint.sort_values(by=['doi'])
            df_zenodo_datacite_joint_deduplicated = df_zenodo_datacite_joint.drop_duplicates(subset=['publicationDate_zen', 'description_zen'], keep='last') 
            ##one problematic dataset splits incorrectly when exported to CSV (conceptrecID = 616927)
            print('Counts of matches for DataCite into Zenodo\n')
            counts_zenodo_datacite = df_zenodo_datacite_joint_deduplicated['Match_entry'].value_counts()
            print(counts_zenodo_datacite, '\n')
            df_zenodo_datacite_joint_unmatched = df_zenodo_datacite_joint_deduplicated[df_zenodo_datacite_joint_deduplicated['Match_entry'] == 'Not matched']
            df_zenodo_datacite_joint_unmatched.to_excel(f'outputs/{today}_DataCite-into-Zenodo-unmatched-dataframe.xlsx', index=False)
            df_zenodo_undetected = df_zenodo_datacite_joint_unmatched[['doi']]

            #Zenodo into DataCite
            df_datacite_zenodo_joint = pd.merge(df_datacite_zenodo_pruned, df_zenodo_pruned, on='doi', how='left')
            df_datacite_zenodo_joint['Match_entry'] = np.where(df_datacite_zenodo_joint['source_zenodo'].isnull(), 'Not matched', 'Matched')
            ##removing multiple DOIs in same 'lineage'
            df_datacite_zenodo_joint = df_datacite_zenodo_joint.sort_values(by=['doi']) 
            df_datacite_zenodo_joint_deduplicated = df_datacite_zenodo_joint.drop_duplicates(subset=['publicationDate_zen', 'description_zen'], keep='first') 
            print('Counts of matches for Zenodo into DataCite\n')
            counts_datacite_zenodo = df_datacite_zenodo_joint_deduplicated['Match_entry'].value_counts()
            print(counts_datacite_zenodo, '\n')
            df_datacite_zenodo_joint_unmatched = df_datacite_zenodo_joint_deduplicated[df_datacite_zenodo_joint_deduplicated['Match_entry'] == 'Not matched']
            df_datacite_zenodo_joint_unmatched.to_excel(f'outputs/{today}_Zenodo-into-DataCite_joint-unmatched-dataframe.xlsx', index=False)

        #get DataCite metadata for all entries not previously detected by DataCite API query
        dfs_to_concat = []
        if not df_dryad_undetected.empty:
            dfs_to_concat.append(df_dryad_undetected)
            print(f'Adding missing {len(df_dryad_undetected)} Dryad DOIs.\n')
        if dataverse and not df_dataverse_undetected.empty:
            dfs_to_concat.append(df_dataverse_undetected)
            print(f'Adding missing {len(df_dataverse_undetected)} Dataverse DOIs.\n')
        if not df_zenodo_undetected.empty:
            dfs_to_concat.append(df_zenodo_undetected)
            print(f'Adding missing {len(df_zenodo_undetected)} Zenodo DOIs.\n')
        if dfs_to_concat:
            datacite_new = pd.concat(dfs_to_concat, ignore_index=True)
        else:
            print('No repository DataFrames available to concatenate.\n')

        print('Retrieving additional DataCite metadata for unmatched deposits\n')
        results = []
        if test:
            datacite_new = datacite_new.head(10)
        for doi in datacite_new['doi']:
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
                'source': 'repository cross-validation'
            })

        df_datacite_new = pd.json_normalize(data_select_datacite_new)
        df_datacite_new.to_csv(f'outputs/{today}_datacite-additional-cross-validation.csv', index=False, encoding='utf-8')
    if cross_validate:
        df_datacite_all = pd.concat([df_datacite_initial, df_datacite_new], ignore_index=True)
    else:
        df_datacite_all = df_datacite_initial

    #creating column for source of detected affiliation
    pattern = '|'.join([f'({perm})' for perm in ut_variations])
    #search for permutations in the 'affiliations' column
    df_datacite_all['affiliation_source'] = df_datacite_all.apply(
        lambda row: 'creator.affiliationName' if pd.Series(row['creators_affiliations']).str.contains(pattern, case=False, na=False).any()
            else ('creator.name' if pd.Series(row['creators_names']).str.contains(pattern, case=False, na=False).any()
            else ('contributor.affiliationName' if pd.Series(row['contributors_affiliations']).str.contains(pattern, case=False, na=False).any()
            else ('contributor.name' if pd.Series(row['contributors_names']).str.contains(pattern, case=False, na=False).any() else None))),
        axis=1)
    # pull out the identified permutation and put it into a new column
    df_datacite_all['affiliation_permutation'] = df_datacite_all.apply(
    lambda row:
        next(
            #exact match (case-sensitive)
            (perm for perm in ut_variations
             if any(perm == entry for entry in row['creators_affiliations'] + row['creators_names'] + row['contributors_affiliations'] + row['contributors_names'])),
            #full-phrase match (case-insensitive)
            next(
                (perm for perm in ut_variations
                 if any(
                     pd.Series(row['creators_affiliations'] + row['creators_names'] + row['contributors_affiliations'] + row['contributors_names'])
                     .str.contains(fr'\b{re.escape(perm)}\b', case=True, na=False)
                 )),
                None
            )
        ),
    axis=1
    )

    #handling version duplication (Figshare, ICPSR, etc.)
    ##handling duplication of Figshare deposits (parent vs. child with '.v*')
    ###creating separate figshare dataframe for downstream processing, not necessary for other repositories with this DOI mechanism in current workflow
    figshare = df_datacite_all[df_datacite_all['doi'].str.contains('figshare', na=False)]
    df_datacite_no_figshare = df_datacite_all[~df_datacite_all['doi'].str.contains('figshare', na=False)]
    figshare_no_versions = figshare[~figshare['doi'].str.contains(r'\.v\d+$', na=False)]
    #mediated workflow sometimes creates individual deposit for each file, want to treat as single dataset here
    for col in figshare_no_versions.columns:
        if figshare_no_versions[col].apply(lambda x: isinstance(x, list)).any():
            figshare_no_versions[col] = figshare_no_versions[col].apply(lambda x: tuple(x) if isinstance(x, list) else x)
    figshare_no_versions['had_partial_duplicate'] = figshare_no_versions.duplicated(subset=['publisher', 'publication_date', 'first_author', 'last_author', 'first_affiliation', 'type', 'related_identifier'], keep=False)

    #aggregating related entries together
    sum_columns = ['deposit_size', 'views', 'citations', 'downloads']

    def agg_func(column_name):
        sum_columns = ['deposit_size', 'views', 'citations', 'downloads']
        if column_name in sum_columns:
            return 'sum'
        else:
            def flatten_and_join(x):
                #flatten if elements are lists
                flattened = []
                for item in x:
                    if isinstance(item, list):
                        flattened.extend(map(str, item))
                    elif pd.notnull(item):
                        flattened.append(str(item))
                return '; '.join(sorted(set(flattened)))
            return flatten_and_join

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

    ##handling SESAR (physical geological sample repository)
    sesar = df_datacite_v1[df_datacite_v1['publisher'].str.contains('SESAR', na=False, case=True)]
    df_datacite_no_sesar = df_datacite_v1[~df_datacite_v1['publisher'].str.contains('SESAR', na=False, case=True)]
    ###set multiple column grouping
    group_cols = ['publication_date', 'first_author', 'last_author', 'contributors_names']
    ####convert list
    agg_funcs = {col: agg_func(col) for col in sesar.columns if col not in group_cols}
    for col in group_cols:
        if col == 'contributors_names':
            sesar[col] = sesar[col].apply(
                lambda x: ';'.join(map(str, x)) if isinstance(x, list) else (str(x) if pd.notnull(x) else '')
            )
        else:
            sesar[col] = sesar[col].apply(lambda x: str(x) if pd.notnull(x) else '')

    sesar_grouped = sesar.groupby(group_cols).agg(agg_funcs).reset_index()
    df_datacite_v2 = pd.concat([df_datacite_no_sesar, sesar_grouped], ignore_index=True)

    ##handling duplication of ICPSR, SAGE, Mendeley Data, Zenodo deposits (parent vs. child)
    lineageRepos = df_datacite_v2[(df_datacite_v2['publisher'].str.contains('ICPSR|Mendeley|SAGE|Zenodo|4TU|Materials Cloud'))|(df_datacite_v2['doi'].str.contains('zenodo'))]
    df_datacite_lineageRepos = df_datacite_v2[~(df_datacite_v2['publisher'].str.contains('ICPSR|Mendeley|SAGE|Zenodo|4TU|Materials Cloud')|df_datacite_v2['doi'].str.contains('zenodo'))]
    lineageRepos_deduplicated = lineageRepos[~lineageRepos['relation_type'].str.contains('IsVersionOf|IsNewVersionOf', case=False, na=False)]
    ###the use of .v* and v* as filters works for these repositories but could accidentally remove non-duplicate DOIs if applied to other repositories
    lineageRepos_deduplicated = lineageRepos_deduplicated[~lineageRepos_deduplicated['doi'].str.contains(r'\.v\d+$')]
    dois_to_remove = lineageRepos_deduplicated[(lineageRepos_deduplicated['doi'].str.contains(r'v\d$') | lineageRepos_deduplicated['doi'].str.contains(r'v\d-')) & (lineageRepos_deduplicated['publisher'].str.contains('ICPSR', case=False, na=False))]['doi']
    # Remove the identified DOIs
    lineageRepos_deduplicated = lineageRepos_deduplicated[~lineageRepos_deduplicated['doi'].isin(dois_to_remove)]
    df_datacite_v3 = pd.concat([df_datacite_lineageRepos, lineageRepos_deduplicated], ignore_index=True)
    
    #handling historic Dryad DOI assignment to some files (may not occur for all institutions, does not occur for UT Austin)
    df_datacite_dedup = df_datacite_v3[~((df_datacite_v3['publisher'] == 'Dryad') & (df_datacite_v3['doi'].str.count('/') >= 2))]

    #handling Code Ocean (software repo, always ends in v*, only retain v1)
    df_datacite_v3 = df_datacite_v3[~((df_datacite_v3['publisher'] == 'Code Ocean') & ~df_datacite_v3['doi'].str.endswith('v1'))]

    #handling file-level DOI granularity (all Dataverse installations)
    ##may need to expand search terms if you find a Dataverse installation without 'Dataverse' in name
    df_datacite_dedup = df_datacite_v3[~(df_datacite_v3['publisher'].str.contains('Dataverse|Texas Data Repository', case=False, na=False) & df_datacite_v3['container_identifier'].notnull())]
    ###should catch other Dataverse installations' files but exclude aggregated entries
    df_datacite_dedup = df_datacite_dedup[~((df_datacite_dedup['doi'].str.count('/') >= 3) & (df_datacite_dedup['publisher'] != 'figshare') & (~df_datacite_dedup['doi'].str.contains(';')))]

    #handling same granularity in other repositories where files have more than one (1) '/'
    target_publishers = ['AUSSDA', 'CUHK Research Data Repository', 'Qualitative Data Repository']
    df_datacite_dedup = df_datacite_dedup[~((df_datacite_dedup['publisher'].isin(target_publishers)) & (df_datacite_dedup['doi'].str.count('/') > 1))]
    #handling blanket 'affiliation' of UT Austin for all DesignSafe deposits
    ##DesignSafe is a UT-managed repository and this step is unlikely to be signficant for other institutions; there should also be a metadata fix for this forthcoming
    if austin:
        df_datacite_dedup = df_datacite_dedup[~((df_datacite_dedup['publisher'] == 'Designsafe-CI') & (df_datacite_dedup['affiliation_permutation'] != 'University of Texas at Austin'))
        ]

    #handling Dataverse partial duplication (oversplitting of one manuscript's materials)
    if dataverse_duplicates:
        # Convert list columns to strings
        df_datacite_dedup['creators_names'] = df_datacite_dedup['creators_names'].apply(lambda x: ', '.join(x) if isinstance(x, list) else x)
        df_datacite_dedup['contributors_affiliations'] = df_datacite_dedup['contributors_affiliations'].apply(lambda x: ', '.join(x) if isinstance(x, list) else x)
        df_datacite_dedup['rights'] = df_datacite_dedup['rights'].apply(lambda x: ', '.join(x) if isinstance(x, list) else x)
        dataverse = df_datacite_dedup[df_datacite_dedup['publisher'].str.contains('Texas Data Repository|Harvard|Dataverse', case=True, na=False)]
        df_datacite_no_dataverse = df_datacite_dedup[~df_datacite_dedup['publisher'].str.contains('Texas Data Repository|Harvard|Dataverse', case=True, na=False)]
        group_variables = ['publisher', 'publication_date', 'creators_names', 'contributors_affiliations', 'type', 'rights']
        sum_columns = ['deposit_size', 'views', 'citations', 'downloads']

        #ensure list-type columns are hashable for grouping
        for col in dataverse.columns:
            if dataverse[col].apply(lambda x: isinstance(x, list)).any():
                dataverse[col] = dataverse[col].apply(lambda x: tuple(x) if isinstance(x, list) else x)

        #create column for partial duplicates
        dataverse['had_partial_duplicate'] = dataverse.duplicated(subset=group_variables, keep=False)

        #modified entry aggregation function
        def agg_func(column_name):
            if column_name in sum_columns:
                return 'sum'
            else:
                return lambda x: sorted(set(
                    str(item)
                    for sublist in x
                    for item in (list(sublist) if isinstance(sublist, (list, set)) else [sublist])
                ))

        agg_funcs = {
            col: agg_func(col)
            for col in dataverse.columns
            if col not in group_variables
        }
        for col in sum_columns:
            if col in dataverse.columns:
                dataverse[col] = pd.to_numeric(dataverse[col], errors='coerce')

        dataverse_combined = dataverse.groupby(group_variables).agg(agg_funcs).reset_index()

        #convert list-type columns to semicolon-separated strings
        for col in dataverse_combined.columns:
            if dataverse_combined[col].apply(lambda x: isinstance(x, list)).any():
                dataverse_combined[col] = dataverse_combined[col].apply(lambda x: '; '.join(map(str, x)))

        dataverse_deduplicated = dataverse_combined.drop_duplicates(subset=group_variables, keep='first')
        print(f'Number of deposits cut from {len(dataverse)} to {len(dataverse_combined)}')
        df_datacite_dedup = pd.concat([df_datacite_no_dataverse, dataverse_combined], ignore_index=True)

    #final sweeping dedpulication step, will catch a few odd edge cases that have been manually discovered
    ##mainly addresses hundreds of EMSL datasets that seem overly granularized (many deposits share all metadata other than DOI including detailed titles) - will not be relevant for all institutions
    df_sorted = df_datacite_dedup.sort_values(by='doi')
    df_datacite = df_sorted.drop_duplicates(subset=['title', 'first_author', 'relation_type', 'related_identifier', 'container_identifier'], keep='first')

    #the file exported here is intended only to be used to compare affiliation source fields; the fields will be dropped in later steps in the workflow
    df_datacite.to_csv(f'outputs/{today}_{resource_filename}_datacite-output-for-affiliation-source.csv', index=False, encoding='utf-8') 

    #additional metadata assessment steps, fields are also dropped in later steps
    df_datacite['file_format'] = df_datacite['formats'].apply(lambda formats: ('; '.join([format_map.get(fmt, fmt) for fmt in formats])if isinstance(formats, set) else formats))        
    ##look for software file formats
    df_datacite['contains_code'] = df_datacite['file_format'].apply(lambda x: any(part.strip() in software_formats for part in x.split(';')) if isinstance(x, str) else False)
    df_datacite['only_code'] = df_datacite['file_format'].apply(lambda x: all(part.strip() in software_formats for part in x.split(';')) if isinstance(x, str) else False)
    df_datacite['title_reformatted'] = df_datacite['title'].str.replace('_', ' ') #gets around text linked by underscores counting as 1 word
    df_datacite['title_reformatted'] = df_datacite['title_reformatted'].str.lower()
    df_datacite[['total_word_count_title', 'descriptive_word_count_title']] = (df_datacite['title_reformatted'].apply(lambda x: pd.Series(count_words(x, nondescriptive_words))))
    df_datacite['descriptive_word_count_title'] = df_datacite.apply(adjust_descriptive_count, axis=1)
    df_datacite['nondescriptive_word_count_title'] = df_datacite['total_word_count_title'] - df_datacite['descriptive_word_count_title']

    #standardizing licenses
    df_datacite['rights'] = df_datacite['rights'].apply(lambda x: ' '.join(x) if isinstance(x, list) else x).astype(str).str.strip('[]')
    df_datacite['rights_standardized'] = 'Rights unclear'  #default value
    df_datacite.loc[df_datacite['rights'].str.contains('Creative Commons Zero|CC0'), 'rights_standardized'] = 'CC0'
    df_datacite.loc[df_datacite['rights'].str.contains('Creative Commons Attribution Non Commercial Share Alike'), 'rights_standardized'] = 'CC BY-NC-SA'
    df_datacite.loc[df_datacite['rights'].str.contains('Creative Commons Attribution Non Commercial'), 'rights_standardized'] = 'CC BY-NC'
    df_datacite.loc[df_datacite['rights'].str.contains('Creative Commons Attribution 3.0|Creative Commons Attribution 4.0|Creative Commons Attribution-NonCommercial'), 'rights_standardized'] = 'CC BY'
    df_datacite.loc[df_datacite['rights'].str.contains('GNU General Public License'), 'rights_standardized'] = 'GNU GPL'
    df_datacite.loc[df_datacite['rights'].str.contains('Apache License'), 'rights_standardized'] = 'Apache'
    df_datacite.loc[df_datacite['rights'].str.contains('MIT License'), 'rights_standardized'] = 'MIT'
    df_datacite.loc[df_datacite['rights'].str.contains('BSD'), 'rights_standardized'] = 'BSD'
    df_datacite.loc[df_datacite['rights'].str.contains('ODC-BY'), 'rights_standardized'] = 'ODC-BY'
    df_datacite.loc[df_datacite['rights'].str.contains('Open Access'), 'rights_standardized'] = 'Rights unclear'
    df_datacite.loc[df_datacite['rights'].str.contains('Closed Access'), 'rights_standardized'] = 'Restricted access'
    df_datacite.loc[df_datacite['rights'].str.contains('Restricted Access'), 'rights_standardized'] = 'Restricted access'
    df_datacite.loc[df_datacite['rights'].str.contains('Databrary'), 'rights_standardized'] = 'Custom terms'
    df_datacite.loc[df_datacite['rights'].str.contains('UCAR'), 'rights_standardized'] = 'Custom terms'
    df_datacite.loc[df_datacite['rights'] == '', 'rights_standardized'] = 'Rights unclear'
 
    df_datacite.to_csv(f'outputs/{today}_{resource_filename}_datacite-output-for-metadata-assessment.csv', index=False, encoding='utf-8') 

    #subsetting dataframe
    # df_datacite_pruned = df_datacite[['publisher', 'doi', 'publication_year', 'title', 'first_author', 'first_affiliation', 'last_author', 'last_affiliation', 'source', 'type']]
    ##currently not pruning to maximize metadata retention for internal processes
    df_datacite_pruned = df_datacite

    #adding column for select high-volume repos
    repo_mapping = {
        'Dryad': 'Dryad',
        'Zenodo': 'Zenodo',
        'Texas Data Repository': 'Texas Data Repository'
    }

    df_datacite_pruned['repository2'] = df_datacite_pruned['publisher'].map(repo_mapping).fillna('Other')
    df_datacite_pruned['uni_lead'] = df_datacite_pruned.apply(lambda row: determine_affiliation(row, ut_variations), axis=1)

    #standardizing repositories with multiple versions of name in dataframe
    ##different institutions may need to add additional repositories; nothing will happen if you don't have any of the ones listed below and don't comment the lines out
    df_datacite_pruned['publisher'] = df_datacite_pruned['publisher'].fillna('None')
    df_datacite_pruned.loc[df_datacite_pruned['publisher'].str.contains('Digital Rocks', case=False), 'publisher'] = 'Digital Porous Media Portal'
    df_datacite_pruned.loc[df_datacite_pruned['publisher'].str.contains('Environmental System Science Data Infrastructure for a Virtual Ecosystem|Southeast Texas Urban Integrated Field Laboratory', case=False), 'publisher'] = 'ESS-DIVE'
    df_datacite_pruned.loc[df_datacite_pruned['publisher'].str.contains('Texas Data Repository|Texas Research Data Repository', case=False), 'publisher'] = 'Texas Data Repository'
    df_datacite_pruned.loc[df_datacite_pruned['publisher'].str.contains('ICPSR', case=True), 'publisher'] = 'ICPSR'
    df_datacite_pruned.loc[df_datacite_pruned['publisher'].str.contains('Environmental Molecular Sciences Laboratory', case=True), 'publisher'] = 'Environ Mol Sci Lab'
    df_datacite_pruned.loc[df_datacite_pruned['publisher'].str.contains('BCO-DMO|Biological and Chemical Oceanography Data', case=True), 'publisher'] = 'Biol Chem Ocean Data Mgmt Office'
    df_datacite_pruned.loc[df_datacite_pruned['publisher'].str.contains('BCO-DMO', case=True), 'publisher'] = 'Biol Chem Ocean Data Mgmt Office'
    df_datacite_pruned.loc[df_datacite_pruned['publisher'].str.contains('Taylor & Francis|SAGE|The Royal Society|SciELO journals', case=True), 'publisher'] = 'figshare'
    df_datacite_pruned.loc[df_datacite_pruned['publisher'].str.contains('Oak Ridge', case=True), 'publisher'] = 'Oak Ridge National Laboratory'
    df_datacite_pruned.loc[df_datacite_pruned['publisher'].str.contains('PARADIM', case=True), 'publisher'] = 'PARADIM'
    df_datacite_pruned.loc[df_datacite_pruned['publisher'].str.contains('4TU', case=True), 'publisher'] = '4TU.ResearchData'
    df_datacite_pruned.loc[df_datacite_pruned['publisher'].str.contains('Scratchpads|Biodiversity Collection|Algae', case=True), 'publisher'] = 'Global Biodiversity Information Facility (GBIF)'
    df_datacite_pruned.loc[df_datacite_pruned['publisher'].str.contains('NCAR', case=True), 'publisher'] = 'NSF NCAR Earth Observing Laboratory'
    df_datacite_pruned.loc[df_datacite_pruned['publisher'].str.contains('Consortium of Universities for the Advancement of Hydrologic Science, Inc', case=False), 'publisher'] = 'CUAHSI'
    df_datacite_pruned.loc[df_datacite_pruned['publisher'].str.contains('Bureau of Economic Geology (UT-BEG)', case=False), 'publisher'] = 'AmeriFlux'
    df_datacite_pruned.loc[df_datacite_pruned['doi'].str.contains('zenodo', case=False), 'publisher'] = 'Zenodo'


    #EDGE CASES, likely unnecessary for other universities, but you will need to find your own edge cases
    ##confusing metadata with UT Austin (but not Dataverse) listed as publisher; have to be manually adjusted over time
    if austin:
        df_datacite_pruned.loc[(df_datacite_pruned['doi'].str.contains('10.11578/dc')) & (df_datacite_pruned['publisher'].str.contains('University of Texas')), 'publisher'] = 'Department of Energy (DOE) CODE'
        ##other edge cases
        df_datacite_pruned.loc[df_datacite_pruned['doi'].str.contains('10.23729/547d8c47-3723-4396-8f84-322c02ccadd0'), 'publisher'] = 'Finnish Fairdata' #labeled publisher as author's name

    #adding categorization
    ##identifying institutional repositories that are not the Texas Data Repository
    df_datacite_pruned['non_TDR_IR'] = np.where(df_datacite_pruned['publisher'].str.contains('University|UCLA|UNC|Harvard|ASU Library|Dataverse|DaRUS', case=True), 'non-TDR institutional', 'not university or TDR')
    df_datacite_pruned['US_federal'] = np.where(df_datacite_pruned['publisher'].str.contains('NOAA|NIH|NSF|U.S.|DOE|DOD|DOI|National|Designsafe', case=True), 'Federal US repo', 'not federal US repo')
    df_datacite_pruned['GREI'] = np.where(df_datacite_pruned['publisher'].str.contains('Dryad|figshare|Harvard|Zenodo|Vivli|Mendeley|Open Science Framework', case=False), 'GREI member', 'not GREI member')
    generalist_keywords = 'Dryad|figshare|Zenodo|Mendeley|Open Science Framework|Science Data Bank'
    institutional_keywords = 'ASU Library|Boise State|Borealis|Caltech|CUHK|Dataverse|Oregon|Princeton|University|Wyoming|DaRUS|Texas|Institut Laue-Langevin|Jagiellonian|Hopkins|Purdue|Yale|GRO.data|DR-NTU|CUAHSI'

    df_datacite_pruned['scope'] = df_datacite_pruned['publisher'].apply(
        lambda x: (
            'Generalist' if pd.notnull(x) and re.search(generalist_keywords, x, re.IGNORECASE)
            else 'Institutional' if pd.notnull(x) and re.search(institutional_keywords, x, re.IGNORECASE)
            else 'Specialist'
        )
    )
    df_datacite_pruned = df_datacite_pruned.rename(columns={'publisher': 'repository'})

    #manually reclassifying certain resourceTypes
    conditions = [
        df_datacite_pruned['type'].isin(['Dataset', 'Image', 'PhysicalObject']),
        df_datacite_pruned['type'].isin(['Software', 'ComputationalNotebook']), 
        df_datacite_pruned['type'] == 'Collection'
    ]
    types = ['Dataset', 'Software', 'Collection']
    df_datacite_pruned['type_reclassified'] = np.select(conditions, types, default='Other')

    #isolating affiliated authors
    ##cloning df
    df_datacite_researchers = df_datacite_pruned
    df_datacite_researchers['affiliated_creators'] = df_datacite_researchers['creators_formatted'].apply(
    lambda creators: [creator for creator in creators if any(perm.lower() in creator.lower() for perm in ut_variations)] if isinstance(creators, list) else [])
    df_datacite_researchers['affiliated_contributors'] = df_datacite_researchers['contributors_formatted'].apply(
    lambda contributors: [contributor for contributor in contributors if any(perm.lower() in contributor.lower() for perm in ut_variations)] if isinstance(contributors, list)else [])
    df_datacite_researchers['affiliated_creators'] = df_datacite_researchers['affiliated_creators'].apply(lambda x: '; '.join(x))
    df_datacite_researchers['affiliated_contributors'] = df_datacite_researchers['affiliated_contributors'].apply(lambda x: '; '.join(x))

    df_datacite_researchers['affiliated_combined'] = df_datacite_researchers.apply(
    lambda row: list({researcher for col in ['creators_formatted', 'contributors_formatted'] if isinstance(row[col], list) for researcher in row[col] if any(perm.lower() in researcher.lower() for perm in ut_variations)}),axis=1)
    df_datacite_researchers['affiliated_combined'] = df_datacite_researchers['affiliated_combined'].apply(lambda x: '; '.join(x))
    df_datacite_researchers['affiliated_combined'] = df_datacite_researchers['affiliated_combined'].apply(
    lambda x: [i.strip() for i in x.split(';')] if pd.notnull(x) and x != '' else []
    )
    df_researchers = df_datacite_researchers.explode('affiliated_combined')
    df_researchers['researcher'] = df_researchers['affiliated_combined'].str.split('(').str[0].str.strip()

    df_researchers_pruned = df_researchers[['researcher','repository', 'doi', 'publication_year', 'type', 'affiliation_permutation', 'US_federal', 'scope']]

    unique_names = df_researchers_pruned['researcher'].unique()
    standardized_names = {}

    for name in unique_names:
        if standardized_names:
            result = process.extractOne(name, standardized_names.keys(), scorer=fuzz.token_sort_ratio)
            if result is not None:
                match, score, _ = result  # rapidfuzz returns (match, score, index)
                if score > 90:  # threshold between 0 to 100, with higher numbers being more stringent
                    standardized_names[name] = match
                else:
                    standardized_names[name] = name
            else:
                standardized_names[name] = name
        else:
            standardized_names[name] = name

    df_researchers_pruned['researcher_standardized'] = df_researchers_pruned['researcher'].map(standardized_names)

    df_researchers_unique = df_researchers_pruned.groupby('researcher_standardized').agg(lambda x: '; '.join(x.astype(str).unique())).reset_index()
    df_researchers_unique['dataset_count'] = df_researchers_unique['researcher_standardized'].map(df_researchers_pruned.groupby('researcher_standardized')['doi'].count())
    df_researchers_unique['name_count'] = df_researchers_unique['researcher_standardized'].map(df_researchers_pruned.groupby('researcher_standardized')['researcher'].nunique())
    df_researchers_unique['repository_count'] = df_researchers_unique['researcher_standardized'].map(df_researchers_pruned.groupby('researcher_standardized')['repository'].nunique())

    df_researchers_unique.to_csv(f'outputs/{today}_{resource_filename}_unique-affiliated-researchers.csv', index=False, encoding='utf-8')

    df_datacite_pruned.to_csv(f'outputs/{today}_{resource_filename}_full-concatenated-dataframe.csv', index=False, encoding='utf-8')

###### FIGSHARE WORKFLOW ######
#These sections are for cleaning up identified figshare deposits or identifying associated ones that lack affiliation metadata

if load_previous_data:
    #for reading in previously generated file of all associated datasets
    print('Reading in previous DataCite output file\n')
    directory = './outputs' 
    pattern = '_full-concatenated-dataframe.csv'

    files = os.listdir(directory)
    files.sort(reverse=True)
    latest_file = None
    for file in files:
        if pattern in file:
            latest_file = file
            break

    if latest_file:
        file_path = os.path.join(directory, latest_file)
        df_datacite_pruned = pd.read_csv(file_path)
        print(f'The most recent file "{latest_file}" has been loaded successfully.')
    else:
        print(f'No file with "{pattern}" was found in the directory "{directory}".')

### This codeblock will retrieve all figshare deposits with a listed journal/publisher as 'publisher,' extract related identifiers, retrieve all articles published by a certain publisher, cross-reference article DOIs against dataset related identifiers, and produce a match list. ###
if figshare_workflow_1:

    #figshare DOIs sometimes have a .v* for version number; this toggles whether to include them (True) or only include the parent (False)
    countVersions = config['TOGGLES']['figshare_versions']

    #pull in map of publisher names and OpenAlex codes
    publisher_mapping = config['FIGSHARE_PARTNERS']
    #create empty object to store results
    data_select_datacite = [] 
    data_select_openalex = []

    for publisher_name, openalex_code in publisher_mapping.items():
        try:
            #update both params for each publisher in map
            params_openalex = {
            'filter': f'authorships.institutions.ror:https://ror.org/00hj54h04,type:article,from_publication_date:2000-01-01,locations.source.host_organization:{openalex_code}',
            'per_page': config['VARIABLES']['PAGE_SIZES']['openalex'],
            'select': 'id,doi,title,authorships,publication_year,primary_location,type',
            'mailto': config['EMAIL']['user_email']
            }
            j = 0
            #define different number of pages to retrieve from OpenAlex API based on 'test' vs. 'prod' env
            page_limit_openalex = config['VARIABLES']['PAGE_LIMITS']['openalex_test'] if test else config['VARIABLES']['PAGE_LIMITS']['openalex_prod']
            #DataCite params (different from general affiliation-based retrieval params)
            ## !! Warning: if you do not set a resource_type in the query (recommended if you want to get broad coverage), this will be a very large retrieval. In the test env, there may not be enough records to find a match with a university-affiliated article !!
            
            #reset to default after large-scale general retrieval through DataCite
            page_limit_datacite = config['VARIABLES']['PAGE_LIMITS']['datacite_test'] if test else config['VARIABLES']['PAGE_LIMITS']['datacite_prod']
            page_start_datacite = config['VARIABLES']['PAGE_STARTS']['datacite']
            if figshare_resource_type_filter:
                params_datacite_figshare = {
                    # 'affiliation': 'true',
                    'query': f'(publisher:"{publisher_name}" AND types.resourceTypeGeneral:{figshare_datacite_resource_type})',
                    'page[size]': config['VARIABLES']['PAGE_SIZES']['datacite'],
                    'page[cursor]': 1,
                }
            else:
                params_datacite_figshare = {
                # 'affiliation': 'true',
                'query': f'(publisher:"{publisher_name}")',
                'page[size]': config['VARIABLES']['PAGE_SIZES']['datacite'],
                'page[cursor]': 1,
                }

            print(f'Starting DataCite retrieval for {publisher_name}.\n')
            data_datacite_figshare = retrieve_datacite(url_datacite, params_datacite_figshare, page_start_datacite, page_limit_datacite, per_page_datacite)
            print(f'Number of datasets associated with {publisher_name} found by DataCite API: {len(data_datacite_figshare)}\n')
            
            for item in data_datacite_figshare:
                if not isinstance(item, dict):
                    print(f'ERROR: item is not a dict! Type: {type(item)}, Value: {item}')
                    continue
                attributes = item.get('attributes', {})
                doi_dc = attributes.get('doi', None)
                state = attributes.get('state', None)
                publisher_dc = attributes.get('publisher', '')
                publisher_year_dc = attributes.get('publicationYear', '')
                registered = attributes.get('registered', '')
                if registered:
                    publisher_year_dc = datetime.fromisoformat(registered.rstrip('Z')).year
                    publisher_date_dc = datetime.fromisoformat(registered.rstrip('Z')).date()
                else:
                    publisher_year_dc = None
                    publisher_date_dc = None
                title_dc = attributes.get('titles', [{}])[0].get('title', '')
                creators_dc = attributes.get('creators', [{}])
                if not isinstance(creators_dc, list):
                    print(f'ERROR: creators_dc is not a list! Type: {type(creators_dc)}, Value: {creators_dc}')
                    creators_dc = [{}]
                creators_names = []
                contributors_affiliations = []
                for creator in creators_dc:
                    if not isinstance(creator, dict):
                        print(f'ERROR: creator is not a dict! Type: {type(creator)}, Value: {creator}')
                        continue
                    creators_names.append(creator.get('name', ''))
                    affiliations = creator.get('affiliation', [])
                    aff_names = []
                    if not isinstance(affiliations, list):
                        print(f'ERROR: affiliations is not a list! Type: {type(affiliations)}, Value: {affiliations}')
                        continue
                    for aff in affiliations:
                        if isinstance(aff, dict):
                            aff_names.append(aff.get('name', ''))
                        elif isinstance(aff, str):
                            aff_names.append(aff)
                        else:
                            print(f'ERROR: affiliation is not dict or str! Type: {type(aff)}, Value: {aff}')
                    contributors_affiliations.append('; '.join(aff_names) if aff_names else '')
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
                contributors_dc = attributes.get('contributors', [{}])
                if not isinstance(contributors_dc, list):
                    print(f'ERROR: contributors_dc is not a list! Type: {type(contributors_dc)}, Value: {contributors_dc}')
                    contributors_dc = [{}]
                contributors_names = []
                contributors_affiliations = []
                for contributor in contributors_dc:
                    if not isinstance(contributor, dict):
                        print(f'ERROR: creator is not a dict! Type: {type(contributor)}, Value: {contributor}')
                        continue
                    contributors_names.append(contributor.get('name', ''))
                    affiliations = contributor.get('affiliation', [])
                    aff_names = []
                    if not isinstance(affiliations, list):
                        print(f'ERROR: affiliations is not a list! Type: {type(affiliations)}, Value: {affiliations}')
                        continue
                    for aff in affiliations:
                        if isinstance(aff, dict):
                            aff_names.append(aff.get('name', ''))
                        elif isinstance(aff, str):
                            aff_names.append(aff)
                        else:
                            print(f'ERROR: affiliation is not dict or str! Type: {type(aff)}, Value: {aff}')
                    contributors_affiliations.append('; '.join(aff_names) if aff_names else '')
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
                related_identifiers = attributes.get('relatedIdentifiers', [])
                container_dc = attributes.get('container', {})
                container_identifier_dc = container_dc.get('identifier', None)
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

                for rel in related_identifiers: #'explodes' deposits with multiple relatedIdentifiers
                    data_select_datacite.append({
                        'doi': doi_dc,
                        'state': state,
                        'repository': publisher_dc,
                        'publisher_original': publisher_dc,
                        'publication_year': publisher_year_dc,
                        'publication_date': publisher_date_dc,
                        'title': title_dc,
                        'creators_names': creators_names,
                        'contributors_affiliations': contributors_affiliations,
                        'creators_formatted': creators_formatted,
                        'relation_type': rel.get('relationType'),
                        'related_identifier': rel.get('relatedIdentifier'),
                        'related_identifier_type': rel.get('relatedIdentifierType'),
                        'container_identifier': container_identifier_dc,
                        'type': resource_type,
                        'subjects': subjects_combined,
                        'deposit_size': total_size,
                        'formats': formats,
                        'file_count': file_count,
                        'rights': rights,
                        'rights_code': rights_code,
                        'views': views,
                        'downloads': downloads,
                        'citations': citations
                    })
            print(f'Starting OpenAlex retrieval for {publisher_name}.\n')
            openalex = retrieve_openalex(url_openalex, params_openalex, page_limit_openalex)
            if openalex:
                print(f'Number of articles associated with {publisher_name} found by OpenAlex API: {len(openalex)}\n')
            else:
                print('WARNING: DATA NOT RETRIEVED')
            for item in openalex:
                doi = item.get('doi')
                title = item.get('title')
                publication_year = item.get('publication_year')
                primary_location = item.get('primary_location')
                if primary_location and isinstance(primary_location, dict):
                    source = primary_location.get('source')
                    if source and isinstance(source, dict):
                        source_display_name = source.get('display_name')
                    else:
                        source_display_name = None
                else:
                    source_display_name = None
                for authorship in item.get('authorships', []):
                    if authorship.get('author_position') == 'first':
                        first_author = authorship.get('author', {}).get('display_name')
                        first_affiliation = [inst.get('display_name') for inst in authorship.get('institutions', [])]
                    if authorship.get('author_position') == 'last':
                        last_author = authorship.get('author', {}).get('display_name')
                        last_affiliation = [inst.get('display_name') for inst in authorship.get('institutions', [])]
                        
                        data_select_openalex.append({
                            'doi_article': doi,
                            'title_article': title,
                            'publication_year': publication_year,
                            'journal': source_display_name,
                            'first_author': first_author,
                            'first_affiliation': first_affiliation,
                            'last_author': last_author,
                            'last_affiliation': last_affiliation
                        })
        except Exception as e:
            print(f'An error occurred with the retrieval for {publisher_name}: {e}')
            continue 

    df_datacite_initial = pd.json_normalize(data_select_datacite)
    df_datacite_initial.to_csv(f'outputs/{today}_{figshare_resource_filename}_figshare-discovery-initial.csv', index=False, encoding='utf-8')

    if countVersions:
        ##These steps will count different versions as distinct datasets and remove the 'parent' (redundant with most recent version)
        df_datacite_initial['base'] = df_datacite_initial['doi'].apply(lambda x: x.split('.v')[0])
        df_datacite_initial['version'] = df_datacite_initial['doi'].apply(lambda x: int(x.split('.v')[1]) if '.v' in x else 0)
        max_versions = df_datacite_initial.groupby('base')['version'].max().reset_index()
        df_datacite_initial = df_datacite_initial.merge(max_versions, on='base', suffixes=('', '_max'))
        df_deduplicated = df_datacite_initial(subset='base')
    else:
        ##This step will remove all child deposits with a .v*  to retain only the 'parent'
        df_deduplicated = df_datacite_initial[~df_datacite_initial['doi'].str.contains(r'\.v\d+$')]

    df_datacite_supplement = df_deduplicated[df_deduplicated['relation_type'] == 'IsSupplementTo']
    #mediated workflow sometimes creates individual deposit for each file, want to treat as single dataset here
    df_datacite_supplement['had_partial_duplicate'] = df_datacite_supplement.duplicated(subset='related_identifier', keep='first')
    df_datacite_supplement_dedup = df_datacite_supplement.drop_duplicates(subset='related_identifier', keep='first')
    
    df_openalex = pd.json_normalize(data_select_openalex)
    df_openalex['related_identifier'] = df_openalex['doi_article'].str.replace('https://doi.org/', '')
    df_openalex = df_openalex.drop_duplicates(subset='doi_article', keep='first')
    df_openalex.to_csv(f'outputs/{today}_openalex-articles.csv', index=False, encoding='utf-8')

    #output all UT linked deposits, no deduplication (for Figshare validator workflow)
    df_openalex_datacite = pd.merge(df_openalex, df_datacite_supplement, on='related_identifier', how='left')
    df_openalex_datacite = df_openalex_datacite[df_openalex_datacite['doi'].notnull()]
    df_openalex_datacite.to_csv(f'outputs/{today}_{figshare_resource_filename}_figshare-discovery-affiliated.csv', index=False, encoding='utf-8')
    df_openalex_datacite = df_openalex_datacite.drop_duplicates(subset='related_identifier', keep='first')

    #working with deduplicated dataset for rest of process
    df_openalex_datacite_dedup = pd.merge(df_openalex, df_datacite_supplement_dedup, on='related_identifier', how='left')
    new_figshare = df_openalex_datacite_dedup[df_openalex_datacite_dedup['doi'].notnull()]
    new_figshare = new_figshare.drop_duplicates(subset='doi', keep='first')
    new_figshare.to_csv(f'outputs/{today}_{figshare_resource_filename}_figshare-discovery-affiliated-deduplicated.csv', index=False, encoding='utf-8')

    ##currently not pruning to maximize metadata retention for internal processes
    # new_figshare = new_figshare[['doi','publication_year','title', 'first_author', 'first_affiliation', 'last_author', 'last_affiliation', 'type']]

    new_figshare['title_reformatted'] = new_figshare['title'].str.replace('_', ' ') #gets around text linked by underscores counting as 1 word
    new_figshare['title_reformatted'] = new_figshare['title_reformatted'].str.lower()
    new_figshare[['total_word_count_title', 'descriptive_word_count_title']] = (new_figshare['title_reformatted'].apply(lambda x: pd.Series(count_words(x, nondescriptive_words))))
    new_figshare['descriptive_word_count_title'] = new_figshare.apply(adjust_descriptive_count, axis=1)
    new_figshare['nondescriptive_word_count_title'] = new_figshare['total_word_count_title'] - new_figshare['descriptive_word_count_title']

    #standardizing licenses
    new_figshare['rights'] = new_figshare['rights'].apply(lambda x: ' '.join(x) if isinstance(x, list) else x).astype(str).str.strip('[]')
    new_figshare['rights_standardized'] = 'Rights unclear'  #default value
    new_figshare.loc[new_figshare['rights'].str.contains('Creative Commons Zero|CC0'), 'rights_standardized'] = 'CC0'
    new_figshare.loc[new_figshare['rights'].str.contains('Creative Commons Attribution Non Commercial Share Alike'), 'rights_standardized'] = 'CC BY-NC-SA'
    new_figshare.loc[new_figshare['rights'].str.contains('Creative Commons Attribution Non Commercial'), 'rights_standardized'] = 'CC BY-NC'
    new_figshare.loc[new_figshare['rights'].str.contains('Creative Commons Attribution 3.0|Creative Commons Attribution 4.0|Creative Commons Attribution-NonCommercial'), 'rights_standardized'] = 'CC BY'
    new_figshare.loc[new_figshare['rights'].str.contains('GNU General Public License'), 'rights_standardized'] = 'GNU GPL'
    new_figshare.loc[new_figshare['rights'].str.contains('Apache License'), 'rights_standardized'] = 'Apache'
    new_figshare.loc[new_figshare['rights'].str.contains('MIT License'), 'rights_standardized'] = 'MIT'
    new_figshare.loc[new_figshare['rights'].str.contains('BSD'), 'rights_standardized'] = 'BSD'
    new_figshare.loc[new_figshare['rights'].str.contains('ODC-BY'), 'rights_standardized'] = 'ODC-BY'
    new_figshare.loc[new_figshare['rights'].str.contains('Open Access'), 'rights_standardized'] = 'Rights unclear'
    new_figshare.loc[new_figshare['rights'].str.contains('Closed Access'), 'rights_standardized'] = 'Restricted access'
    new_figshare.loc[new_figshare['rights'].str.contains('Restricted Access'), 'rights_standardized'] = 'Restricted access'
    new_figshare.loc[new_figshare['rights'].str.contains('Databrary'), 'rights_standardized'] = 'Custom terms'
    new_figshare.loc[new_figshare['rights'].str.contains('UCAR'), 'rights_standardized'] = 'Custom terms'
    new_figshare.loc[new_figshare['rights'] == '', 'rights_standardized'] = 'Rights unclear'

    #file formats (not presently returned for mediated deposits)
    new_figshare['file_format'] = new_figshare['formats'].apply(
    lambda formats: ('; '.join([format_map.get(fmt, fmt) for fmt in formats])if isinstance(formats, set) else formats))   
    # Assume software_formats is a set of friendly software format names
    new_figshare['contains_code'] = new_figshare['file_format'].apply(lambda x: any(part.strip() in software_formats for part in x.split(';')) if isinstance(x, str) else False)
    new_figshare['only_code'] = new_figshare['file_format'].apply(lambda x: all(part.strip() in software_formats for part in x.split(';')) if isinstance(x, str) else False)

    #adding in columns to reconcatenate with full dataset
    new_figshare['first_affiliation'] = new_figshare['first_affiliation'].apply(lambda x: ' '.join([str(item) for item in x if item is not None]) if isinstance(x, list) else x)
    new_figshare['last_affiliation'] = new_figshare['last_affiliation'].apply(lambda x: ' '.join([str(item) for item in x if item is not None]) if isinstance(x, list) else x)    
    new_figshare['uni_lead'] = new_figshare.apply(lambda row: determine_affiliation(row, ut_variations), axis=1)
    new_figshare['repository'] = 'figshare'
    new_figshare['source'] = 'DataCite+' #slight differentiation from records only retrieved from DataCite
    new_figshare['repository2'] = 'Other'
    new_figshare['non_TDR_IR'] = 'not university or TDR'
    new_figshare['US_federal'] = 'not federal US repo'
    new_figshare['GREI'] = 'GREI member'
    new_figshare['scope'] = 'Generalist'
    conditions = [
        new_figshare['type'].isin(['Dataset', 'Image', 'PhysicalObject']),
        new_figshare['type'].isin(['Software', 'ComputationalNotebook']), 
        new_figshare['type'] == 'Collection'
    ]
    types = ['Dataset', 'Software', 'Collection']
    new_figshare['type_reclassified'] = np.select(conditions, types, default='Other')
    new_figshare['affiliation_permutation'] = 'Not applicable' #filler to match original DataCite since affiliation only in linked article and not detected through DataCite
    new_figshare['affiliation_source'] = 'Not applicable' #filler to match original DataCite since affiliation only in linked article and not detected through DataCite

    df_datacite_plus = pd.concat([df_datacite_pruned, new_figshare], ignore_index=True)
    #de-duplicating in case some DOIs were caught twice (for the few publishers that do cross-walk affiliation metadata), you could use a sorting method to determine which one to 'keep'; the default will retain the ones returned from the main workflow
    df_datacite_plus_dedup = df_datacite_plus.drop_duplicates(subset='doi', keep='first')
    df_datacite_plus_dedup.to_csv(f'outputs/{today}_{resource_filename}_full-concatenated-dataframe-plus-figshare.csv', index=False, encoding='utf-8')

### This codeblock identifies publishers known to create figshare deposits (can be any object resource type) with a '.s00*' system, finds affiliated articles, constructs a hypothetical figshare DOI for them, and tests its existence ###
# !! Warning: Depending on the number of articles, this can be an extremely time-intensive process !! #

if figshare_workflow_2:
    #toggle to select which indexer to use: 'OpenAlex' or 'Crossref'
    indexer = config['TOGGLES']['figshare_workflow_2_indexer']

    #OpenAlex params
    j = 0
    if test:
        page_limit_openalex = config['VARIABLES']['PAGE_LIMITS']['openalex_test']
    else:
        page_limit_openalex = config['VARIABLES']['PAGE_LIMITS']['openalex_prod']

    #Crossref params
    k = 0
    if test:
        page_limit_crossref = config['VARIABLES']['PAGE_LIMITS']['crossref_test']
    else:
        page_limit_crossref = config['VARIABLES']['PAGE_LIMITS']['crossref_prod']
    params_crossref_journal = {
        'select': 'DOI,prefix,title,author,container-title,publisher,created',
        'filter': 'type:journal-article',
        'rows': config['VARIABLES']['PAGE_SIZES']['crossref'],
        'query': 'affiliation:University+Texas+Austin',
        'mailto': config['EMAIL']['user_email'],
        'cursor': '*',
    }

    params_openalex = {
        'filter': 'authorships.institutions.ror:https://ror.org/00hj54h04,locations.source.host_organization:https://openalex.org/P4310315706', #PLOS ID in OpenAlex
        'per-page': config['VARIABLES']['PAGE_SIZES']['openalex'],
        'select': 'id,doi,title,authorships,primary_location,type',
        'mailto': config['EMAIL']['user_email']
    }

    #JSON dictionary of journals for Crossref API query (PLOS in this example)
    with open('journal-list.json', 'r') as file:
        journal_list = json.load(file)

    if indexer == 'OpenAlex':
        openalex = retrieve_openalex(url_openalex, params_openalex, page_limit_openalex)
        df_openalex = pd.json_normalize(openalex)
        df_openalex['hypothetical_dataset'] = df_openalex['doi'] + '.s001'
        
        #Check if each DOI with suffix redirects to a real page and create a new column
        df_openalex['Valid'] = df_openalex['hypothetical_dataset'].apply(check_link)
        df_openalex.to_csv(f'outputs/{today}_openalex-articles-with-hypothetical-deposits.csv', index=False, encoding='utf-8')
        print(f'Number of valid datasets: {len(df_openalex)}.')
    else:
        crossref_data = retrieve_all_journals(url_crossref_issn, journal_list, params_crossref_journal, page_limit_crossref, retrieve_crossref)

        data_journals_select = []
        for item in crossref_data:
            publisher = item.get('publisher', None)
            journal = item.get('container-title', None)[0]
            doi = item.get('DOI', '')
            title_list = item.get('title', [])
            title = title_list[0] if title_list else None
            author = item.get('author', None)
            created = item.get('created', {})
            createdDate = created.get('date-time', None)
            
            data_journals_select.append({
                'publisher': publisher,
                'journal': journal, 
                'doi': doi,
                'author': author,
                'title': title,
                'published': createdDate,
        })

        df_crossref = pd.DataFrame(data_journals_select)
        df_crossref['doi_html'] = 'https://doi.org/' + df_crossref['doi']
        df_crossref['hypothetical_dataset'] = df_crossref['doi_html'] + '.s001'

        # Check if each DOI with suffix redirects to a real page and create a new column
        df_crossref['Valid'] = df_crossref['hypothetical_dataset'].apply(check_link)
        df_crossref.to_csv(f'outputs/{today}_crossref-articles-with-hypothetical-deposits.csv', index=False, encoding='utf-8')
        print(f'Number of valid datasets: {len(df_crossref)}.')

##### NCBI Bioproject #####
if ncbi_workflow:
    print('Starting NCBI process.\n')

    #set path for browser
    ##works differently for Jupyter vs. .py file
    try:
        #for .py file
        script_dir = os.path.dirname(os.path.abspath(__file__))
    except NameError:
        #for Jupyter
        script_dir = os.getcwd()
    if test:
        outputs_dir = os.path.join(script_dir, 'test/outputs')
    else:
        outputs_dir = os.path.join(script_dir, 'outputs')

    #check if previous output file exists
    directory = './outputs'
    pattern = 'bioproject_result'

    files = os.listdir(directory)
    for file in files:
        if pattern in file:
            existingOutput = True
            print(f'A previous "{pattern}" download was found in the directory "{directory}".')
            break
    else:
        existingOutput = False
        print(f'No file with "{pattern}" was found in the directory "{directory}".')

    #read in config file
    if not load_ncbi_data:
        institution_name = config['INSTITUTION']['name']
        #URL encode name
        encoded_institution_name = quote(institution_name)
        if not biopython:
            #set up temporary Firefox 'profile' to direct downloads (profile not saved outside of script)
            options = Options()
            options.set_preference('browser.download.folderList', 2)
            options.set_preference('browser.download.dir', outputs_dir)
            options.set_preference('browser.helperApps.neverAsk.saveToDisk', 'application/octet-stream')
            #blocking pop-up window to cancel download
            options.set_preference('browser.download.manager.showWhenStarting', False)
            options.set_preference('browser.download.manager.focusWhenStarting', False)
            options.set_preference('browser.download.useDownloadDir', True)
            options.set_preference('browser.download.manager.alertOnEXEOpen', False)
            options.set_preference('browser.download.manager.closeWhenDone', True)
            options.set_preference('browser.download.manager.showAlertOnComplete', False)
            options.set_preference('browser.download.manager.useWindow', False)
            options.set_preference('services.sync.prefs.sync.browser.download.manager.showWhenStarting', False)
            options.set_preference('browser.download.alwaysOpenPanel', False)  # Disable the download panel
            options.set_preference('browser.download.panel.shown', False)  # Ensure the download panel is not shown

            #initialize Selenium WebDriver
            driver = webdriver.Firefox(options=options)
            ##searches all fields; searching Submitter Organization specifically does not recover all results
            ncbi_url = f'https://www.ncbi.nlm.nih.gov/bioproject?term={encoded_institution_name}'
            driver.get(ncbi_url)

            try:
                #load page and find the 'Send to' dropdown
                send_to_link = WebDriverWait(driver, 20).until(EC.element_to_be_clickable((By.ID, 'sendto')))
                send_to_link.click()

                #load dropdown and select 'File' radio button
                file_option = WebDriverWait(driver, 20).until(EC.element_to_be_clickable((By.ID, 'dest_File')))
                file_option.click()

                #load 'Format' dropdown and select 'XML'
                format_dropdown = WebDriverWait(driver, 20).until(EC.element_to_be_clickable((By.ID, 'file_format')))
                format_dropdown.click()
                xml_option = WebDriverWait(driver, 20).until(EC.element_to_be_clickable((By.XPATH, "//option[@value='xml']")))
                xml_option.click()

                #click the 'Create File' button
                create_file_button = WebDriverWait(driver, 20).until(EC.element_to_be_clickable((By.XPATH, "//button[@cmd='File']")))
                create_file_button.click()

                print('Download complete, about to close window.\n')
                time.sleep(10)

                #overwrite any existing file with 'bioproject_result.xml' rather than continually creating new version with (*) appended in filename (e.g., bioproject_result(1).xml)
                ##will delete previous one and then rename the just-downloaded one with (*) appended
                if existingOutput:
                    downloaded_file = max([os.path.join(outputs_dir, f) for f in os.listdir(outputs_dir)], key=os.path.getctime)
                    target_file = os.path.join(outputs_dir, 'bioproject_result.xml')
                    if os.path.exists(target_file):
                        os.remove(target_file)
                        print(f'Deleted existing file: {target_file}')
                    os.rename(downloaded_file, target_file)
                    print(f'Renamed {downloaded_file} to {target_file}')

            except TimeoutException:
                print('Element not found or not clickable within the specified time.')

            finally:
                driver.quit()
        else:
            print('Starting biopython retrieval')
            #NCBI requires email to be provided
            Entrez.email = f'{email}'

            #if you get a free API key, increases rate limit from 3/sec to 10/sec
            #Entrez.api_key = 'YOUR_NCBI_API_KEY'

            search_term = config['INSTITUTION']['name'] #check that this string is the right one in the web interface
            handle = Entrez.esearch(db='bioproject', term=search_term, usehistory='y', retmax=1200) #currently at 955
            record = Entrez.read(handle)
            handle.close()

            webenv = record['WebEnv']
            query_key = record['QueryKey']

            handle = Entrez.efetch(db='bioproject', query_key=query_key, WebEnv=webenv, retmode='xml')
            xml_data = handle.read().decode('utf-8')
            handle.close()

            with open(f'{outputs_dir}/bioproject_result.xml', 'w', encoding='utf-8') as f:
                f.write(xml_data)

            print(f'Saved XML record to "{outputs_dir}/bioproject_result.xml"')

    #read in XML file (required regardless of whether you downloaded version in this run or not)
    print('Loading previously generated XML file.\n')
    with open(f'{outputs_dir}/bioproject_result.xml', 'r', encoding='utf-8') as file:
        data = file.read()

    #wrapping in a root element for parsing if from Selenium output
    if not data.strip().startswith('<?xml'):
        data = f'<root>{data}</root>'
    root = ET.fromstring(data)

    #select certain fields from XML
    def filter_ncbi(doc):
        data_select = {}
        project = doc.find('Project')
        if project is not None:
            project_id = project.find('ProjectID')
            if project_id is not None:
                archive_id = project_id.find('ArchiveID')
                if archive_id is not None:
                    data_select['doi'] = archive_id.get('accession') #this is not a DOI but will be aligned with DOI column in main dataframe
                    data_select['repository'] = archive_id.get('archive')
                    data_select['ID'] = archive_id.get('id')
                center_id = project_id.find('CenterID')
                if center_id is not None:
                    data_select['Center'] = center_id.get('center')
                    data_select['CenterName'] = center_id.text
            project_descr = project.find('ProjectDescr')
            if project_descr is not None:
                name = project_descr.find('Name')
                if name is not None:
                    data_select['Name'] = name.text
                title = project_descr.find('Title')
                if title is not None:
                    data_select['title'] = title.text
                description = project_descr.find('Description')
                if description is not None:
                    data_select['Description'] = description.text
        submission = doc.find('Submission')
        if submission is not None:
            data_select['LastUpdate'] = submission.get('last_update')
            data_select['SubmissionID'] = submission.get('submission_id')
            data_select['publication_date'] = submission.get('submitted')
            organization = submission.find('.//Organization/Name')
            if organization is not None:
                data_select['Affiliation'] = organization.text

        return data_select

    #cxtract data from each element and store in a list
    data_list = []
    for doc in root.findall('DocumentSummary'):
        data_list.append(filter_ncbi(doc))

    #dataframe conversion and standardization for alignment with main dataframe
    ncbi = pd.DataFrame(data_list)
    ncbi['publication_year'] = pd.to_datetime(ncbi['publication_date']).dt.year
    ##look for one of the permutation strings listed in config.json
    ncbi['first_affiliation'] = ncbi.apply(lambda row: next((perm for perm in ut_variations if perm in row['Affiliation']), None), axis=1)

    ##removing hits that have one of the keywords in a different field like the title
    ncbi_df_select = ncbi[ncbi['Affiliation'].str.contains(uni_identifier)]
    ncbi_df_select = ncbi_df_select[['publication_date', 'repository','doi', 'publication_year', 'title','first_affiliation']]   
    ##adding columns for alignment with main dataframe
    ncbi_df_select['first_author'] = 'Not specified' #filler to match DataCite processing, no equivalent field
    ncbi_df_select['first_affiliation'] = 'Not specified' #filler to match DataCite processing, no equivalent field
    ncbi_df_select['last_author'] = 'Not specified' #filler to match DataCite processing, no equivalent field
    ncbi_df_select['last_affiliation'] = 'Not specified' #filler to match DataCite processing, no equivalent field
    ncbi_df_select['creators_names'] = 'Not specified' #filler to match DataCite, no equivalent field
    ncbi_df_select['contributors_affiliations'] = 'Not specified' #filler to match DataCite, no equivalent field
    ncbi_df_select['creators_formatted'] = 'Not specified' #filler to match DataCite processing, no equivalent field
    ncbi_df_select['contributors_names'] = 'No equivalent field' #filler to match DataCite, no equivalent field
    ncbi_df_select['contributors_affiliations'] = 'No equivalent field' #filler to match DataCite, no equivalent field
    ncbi_df_select['contributors_formatted'] = 'Not applicable' #filler to match DataCite processing, no equivalent field
    ncbi_df_select['relation_type'] = 'No equivalent field' #filler to match DataCite, no equivalent field
    ncbi_df_select['related_identifier'] = 'No equivalent field' #filler to match DataCite, no equivalent field
    ncbi_df_select['container_identifier'] = 'No equivalent field' #filler to match DataCite, no equivalent field
    ncbi_df_select['type'] = 'Dataset'
    ncbi_df_select['subjects'] = 'No keyword information' #filler to match DataCite, no equivalent field
    ncbi_df_select['deposit_size'] = 'No file size information' #filler to match DataCite, no equivalent field
    ncbi_df_select['formats'] = 'No file size information' #filler to match DataCite, no equivalent field
    ncbi_df_select['file_count'] = 'No file size information' #filler to match DataCite processing, no equivalent field
    ncbi_df_select['rights'] = 'Rights unclear' #filler to match DataCite, no equivalent field
    ncbi_df_select['rights_code'] = 'Rights unclear' #filler to match DataCite, no equivalent field
    ncbi_df_select['views'] = 'No metrics information' #filler to match DataCite, no equivalent field
    ncbi_df_select['downloads'] = 'No metrics information' #filler to match DataCite, no equivalent field
    ncbi_df_select['citations'] = 'No metrics information' #filler to match DataCite, no equivalent field
    ncbi_df_select['source'] = 'NCBI'
    ncbi_df_select['affiliation_source'] = 'Not applicable' #filler to match DataCite processing, no equivalent field
    ncbi_df_select['affiliation_permutation'] = 'University of Texas at Austin' #standardized for search
    ncbi_df_select['had_partial_duplicate'] = 'Not applicable' #filler to match DataCite processing, no equivalent field
    ncbi_df_select['file_format'] = 'No file size information' #filler to match DataCite processing, no equivalent field
    ncbi_df_select['contains_code'] = 'No file size information' #filler to match DataCite processing, no equivalent field
    ncbi_df_select['only_code'] = 'No file size information' #filler to match DataCite processing, no equivalent field

    #select metadata assessment for titles
    ncbi_df_select['title_reformatted'] = ncbi_df_select['title'].str.replace('_', ' ') #gets around text linked by underscores counting as 1 word
    ncbi_df_select['title_reformatted'] = ncbi_df_select['title_reformatted'].str.lower()
    ncbi_df_select[['total_word_count_title', 'descriptive_word_count_title']] = (ncbi_df_select['title_reformatted'].apply(lambda x: pd.Series(count_words(x, nondescriptive_words))))
    ncbi_df_select['descriptive_word_count_title'] = ncbi_df_select.apply(adjust_descriptive_count, axis=1)
    ncbi_df_select['nondescriptive_word_count_title'] = ncbi_df_select['total_word_count_title'] - ncbi_df_select['descriptive_word_count_title']
    ncbi_df_select['rights_standardized'] = 'Rights unclear'
    ncbi_df_select['repository2'] = 'NCBI'
    ncbi_df_select['uni_lead'] = 'Affiliated (authorship unclear)'    
    ncbi_df_select['non_TDR_IR'] = 'not university or TDR'
    ncbi_df_select['US_federal'] = 'Federal US repo'
    ncbi_df_select['GREI'] = 'not GREI member'
    ncbi_df_select['scope'] = 'Specialist'
    ncbi_df_select['type_reclassified'] = 'Dataset'
    ncbi_df_select['doi_article'] = 'Not applicable' #filler to match Figshare workflow, no equivalent process or field
    ncbi_df_select['title_article'] = 'Not applicable' #filler to match Figshare workflow, no equivalent process or field
    ncbi_df_select['publication_year'] = 'Not applicable' #filler to match Figshare workflow, no equivalent process or field
    ncbi_df_select['journal'] = 'Not applicable' #filler to match Figshare workflow, no equivalent process or field
    ncbi_df_select['related_identifier_type'] = 'Not applicable' #filler to match Figshare workflow, no equivalent process or field

    if load_previous_data_plus:
        #for reading in previously generated file of all associated datasets
        print('Reading in existing DataCite+ output file\n')
        directory = './outputs' 
        pattern = '_full-concatenated-dataframe-plus-figshare.csv'

        files = os.listdir(directory)
        files.sort(reverse=True)
        latest_file = None
        for file in files:
            if pattern in file:
                latest_file = file
                break

        if latest_file:
            file_path = os.path.join(directory, latest_file)
            df_datacite_plus_dedup = pd.read_csv(file_path)
            print(f'The most recent file "{latest_file}" has been loaded successfully.')
        else:
            print(f'No file with "{pattern}" was found in the directory "{directory}".')

    df_datacite_plus_ncbi = pd.concat([df_datacite_plus_dedup, ncbi_df_select], ignore_index=True)
    df_datacite_plus_ncbi.to_csv(f'outputs/{today}_{resource_filename}_full-concatenated-dataframe-plus-figshare-ncbi.csv', index=False, encoding='utf-8')

    # ncbi_df_select.to_csv(f'outputs/{today}_NCBI-select-output-aligned.csv', index=False, encoding='utf-8')

#to load in externally queried Crossref data
if any([load_previous_data, load_previous_data_plus, load_previous_data_plus_ncbi]) and load_crossref:
    print('Reading in existing DataCite+ output file\n')
    directory = './outputs'
    if load_previous_data_plus_ncbi: 
        pattern = '_full-concatenated-dataframe-plus-figshare-ncbi.csv'
    elif load_previous_data_plus:
        pattern = '_full-concatenated-dataframe-plus.csv'
    elif load_previous_data:
        pattern = '_full-concatenated-dataframe.csv'

    files = os.listdir(directory)
    files.sort(reverse=True)
    latest_file = None
    for file in files:
        if pattern in file:
            latest_file = file
            break

    if latest_file:
        file_path = os.path.join(directory, latest_file)
        df_datacite_plus_ncbi = pd.read_csv(file_path)
        print(f'The most recent file "{latest_file}" has been loaded successfully.')
    else:
        print(f'No file with "{pattern}" was found in the directory "{directory}".')

    #set path for browser
    print('\nReading in existing Crossref output file\n')

    directory = './accessory-scripts/accessory-outputs'
    pattern = 'true-datasets'

    files = os.listdir(directory)
    files.sort(reverse=True)
    latest_file = None
    for file in files:
        if pattern in file:
            latest_file = file
            break

    if latest_file:
        file_path = os.path.join(directory, latest_file)
        crossref_true_datasets = pd.read_csv(file_path)
        print(f'The most recent file "{latest_file}" has been loaded successfully.')
    else:
        print(f'No file with "{pattern}" was found in the directory "{directory}".')

    if load_previous_data:
        df_datacite_plus_crossref = pd.concat([df_datacite_pruned, crossref_true_datasets], ignore_index=True)
        df_datacite_plus_crossref.to_csv(f'outputs/{today}_{resource_filename}_full-concatenated-dataframe-plus-crossref.csv', index=False, encoding='utf-8')
    elif load_previous_data_plus:
        df_datacite_plus_crossref = pd.concat([df_datacite_plus, crossref_true_datasets], ignore_index=True)
        df_datacite_plus_crossref.to_csv(f'outputs/{today}_{resource_filename}_full-concatenated-dataframe-plus-figshare-crossref.csv', index=False, encoding='utf-8')
    elif load_previous_data_plus_ncbi:
        df_datacite_plus_crossref = pd.concat([df_datacite_plus_ncbi, crossref_true_datasets], ignore_index=True)
        df_datacite_plus_crossref.to_csv(f'outputs/{today}_{resource_filename}_full-concatenated-dataframe-plus-figshare-ncbi-crossref.csv', index=False, encoding='utf-8')
    elif not load_previous_data and not load_previous_data and not figshare_workflow_1:
        df_datacite_plus_crossref = pd.concat([df_datacite_pruned, crossref_true_datasets], ignore_index=True)
        df_datacite_plus_crossref.to_csv(f'outputs/{today}_{resource_filename}_full-concatenated-dataframe-plus-crossref.csv', index=False, encoding='utf-8')
    elif not load_previous_data and not load_previous_data and figshare_workflow_1:
        df_datacite_plus_crossref = pd.concat([df_datacite_plus, crossref_true_datasets], ignore_index=True)
        df_datacite_plus_crossref.to_csv(f'outputs/{today}_{resource_filename}_full-concatenated-dataframe-plus-figshare-crossref.csv', index=False, encoding='utf-8')

if not any([load_previous_data, load_previous_data_plus, load_previous_data_plus_ncbi]) and load_crossref:
    print('\nReading in existing Crossref output file\n')
    directory = './accessory-scripts/accessory-outputs'
    pattern = 'true-datasets'

    files = os.listdir(directory)
    files.sort(reverse=True)
    latest_file = None
    for file in files:
        if pattern in file:
            latest_file = file
            break

    if latest_file:
        file_path = os.path.join(directory, latest_file)
        crossref_true_datasets = pd.read_csv(file_path)
        print(f'The most recent file "{latest_file}" has been loaded successfully.')
    else:
        print(f'No file with "{pattern}" was found in the directory "{directory}".')

    if not df_datacite_pruned.empty and df_datacite_plus_dedup.empty:
        df_datacite_plus_crossref = pd.concat([df_datacite_pruned, crossref_true_datasets], ignore_index=True)
        df_datacite_plus_crossref.to_csv(f'outputs/{today}_{resource_filename}_full-concatenated-dataframe-plus-crossref.csv', index=False, encoding='utf-8')
    elif not df_datacite_plus_dedup.empty and df_datacite_plus_ncbi.empty:
        df_datacite_plus_crossref = pd.concat([df_datacite_plus, crossref_true_datasets], ignore_index=True)
        df_datacite_plus_crossref.to_csv(f'outputs/{today}_{resource_filename}_full-concatenated-dataframe-plus-figshare-crossref.csv', index=False, encoding='utf-8')
    elif not df_datacite_plus_ncbi.empty:
        df_datacite_plus_crossref = pd.concat([df_datacite_plus_ncbi, crossref_true_datasets], ignore_index=True)
        df_datacite_plus_crossref.to_csv(f'outputs/{today}_{resource_filename}_full-concatenated-dataframe-plus-figshare-ncbi-crossref.csv', index=False, encoding='utf-8')

runtime = datetime.now() - start_time

print('Dataframe processing completed, beginning log writing.\n')

def log_selected_config(config, keys, file):
    for key in keys:
        value = config.get(key)
        if value is not None:
            if isinstance(value, dict):
                # Pretty-print the nested dict
                file.write(f'{key}:\n{pformat(value, indent=2, width=120)}\n\n')
            else:
                file.write(f'{key}: {value}\n\n')

#writes one file specific to this run:
unique_timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
with open(f'logs/{unique_timestamp}-log.txt', 'w') as resultssummaryfile:
    resultssummaryfile.write(f'Affiliated research object discovery for: {config['INSTITUTION']['name']}, run on {start_timezone_formatted} for {runtime} (hours:minutes:seconds.milliseconds).\n\n')
    resultssummaryfile.write(f'User: {config['EMAIL']['user_email']}\n\n')

    env = 'test' if config['TOGGLES']['test'] else 'production'
    cross = 'with cross-validation' if config['TOGGLES']['cross_validate'] else 'without cross-validation'
    dataverse = 'included the Dataverse API' if config['TOGGLES']['dataverse_duplicates'] else 'did not include the Dataverse API'
    dataversededup = 'with dataverse deduplication' if config['TOGGLES']['dataverse_duplicates'] else 'without dataverse deduplication'
    figshare1 = 'with the first secondary figshare workflow' if config['TOGGLES']['figshare_workflow_1'] else 'without the first secondary figshare workflow'
    figshare2 = 'with the second secondary figshare workflow' if config['TOGGLES']['figshare_workflow_2'] else 'without the second secondary figshare workflow'
    figshareVers = 'removing versions for multi-version deposits' if config['TOGGLES']['figshare_versions'] else 'retaining versions for multi-version deposits'
    ncbi = 'with the secondary NCBI workflow' if config['TOGGLES']['ncbi_workflow'] else 'without the secondary NCBI workflow'
    biopy = 'using the biopython module' if config['TOGGLES']['biopython'] else 'using the Selenium approach'
    loadPrev = 'previous primary output was loaded' if config['TOGGLES']['load_previous_data'] else 'previous primary output was not loaded'
    loadPrevPlus = 'previous primary output with secondary Figshare data was loaded' if config['TOGGLES']['load_previous_data_plus'] else 'previous primary output with secondary Figshare data was not loaded'
    loadPrevPlusNCBI = 'previous primary output with secondary Figshare and NCBI data was loaded' if config['TOGGLES']['load_previous_data_plus_ncbi'] else 'previous primary output with secondary Figshare and NCBI data was not loaded'
    loadCross = 'separate Crossref output was loaded' if config['TOGGLES']['load_crossref'] else 'separate Crossref output was not loaded'

    resultssummaryfile.write(f'Short summary: The script was run in {env} mode and applied a filter to search for {resource_filename} objects. The initial search was performed {cross}, which was itself performed {dataverse} and {dataversededup}. The script was run {figshare1}, with a filter to search for {figshare_resource_filename}; {figshare2}; and {ncbi}, {biopy}. The {loadPrev}; the {loadPrevPlus}; the {loadPrevPlusNCBI}; and the {loadCross}. \n\n')

    #writes select fields from the config.json file
    fields_to_log = ['TOGGLES', 'VARIABLES', 'PERMUTATIONS', 'FIGSHARE_PARTNERS']
    log_selected_config(config, fields_to_log, resultssummaryfile)
    resultssummaryfile.write('\n')

#writes to master CSV file
##ensuring it writes to the same file regardless of env
script_dir = os.path.dirname(os.path.abspath(__file__))
comp_log_dir = os.path.join(script_dir, 'logs')
comp_log_file = os.path.join(comp_log_dir, 'composite-log.csv')
file_exists = os.path.exists(comp_log_file)

log_entry = {
    'script_name': os.path.basename(__file__),
    'timestamp': start_timezone_formatted,
    'runtime': runtime,
    'institution': config['INSTITUTION']['name'],
    'user': config['EMAIL']['user_email'],
    'test_mode': config['TOGGLES']['test'],
    'cross-validation': config['TOGGLES']['cross_validate'],
    'dataverse_deduplication': config['TOGGLES']['dataverse_duplicates'],
    'figshare1': config['TOGGLES']['figshare_workflow_1'],
    'figshare2': config['TOGGLES']['figshare_workflow_2'],
    'figshare_versions': config['TOGGLES']['figshare_versions'],
    'ncbi': config['TOGGLES']['ncbi_workflow'],
    'ncbi_method':'Not applicable'
        if not config['TOGGLES']['ncbi_workflow']
        else 'BioPython'
        if config['TOGGLES']['biopython']
        else 'Selenium',
    'loadedPrevious': config['TOGGLES']['load_previous_data'],
    'loadedPreviousPlus': config['TOGGLES']['load_previous_data_plus'],
    'loadedPreviousPlusNCBI': config['TOGGLES']['load_previous_data_plus_ncbi'],
    'loadedCrossref': config['TOGGLES']['load_crossref']
}

try:
    df = pd.read_csv(comp_log_file)
    df = pd.concat([df, pd.DataFrame([log_entry])], ignore_index=True)
except FileNotFoundError:
    df = pd.DataFrame([log_entry])

df.to_csv(comp_log_file, index=False)

print('Logging completed. Script completed.\n')

print(f'Time to run: {datetime.now() - start_time}')
if test:
    print('**REMINDER: THIS IS A TEST RUN, AND ANY RESULTS ARE NOT COMPLETE!**')