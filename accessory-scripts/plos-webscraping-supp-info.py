from bs4 import BeautifulSoup
from datetime import datetime
import json
import math
import numpy as np
import os
import pandas as pd
import re
import requests

#operator for quick test runs
test = False
#setting timestamp to calculate run time
startTime = datetime.now() 
#creating variable with current date for appending to filenames
todayDate = datetime.now().strftime('%Y%m%d') 

#read in config file
with open('config.json', 'r') as file:
    config = json.load(file)

#pull in ROR ID
ror_id = config['INSTITUTION']['ror']

#creating directories
if test:
    if os.path.isdir("test"):
        print("test directory found - no need to recreate")
    else:
        os.mkdir("test")
        print("test directory has been created")
    os.chdir('test')
    if os.path.isdir("outputs"):
        print("test outputs directory found - no need to recreate")
    else:
        os.mkdir("outputs")
        print("test outputs directory has been created")
else:
    if os.path.isdir("outputs"):
        print("outputs directory found - no need to recreate")
    else:
        os.mkdir("outputs")
        print("outputs directory has been created")

url_openalex = 'https://api.openalex.org/works'

params_openalex = {
    'filter': f'authorships.institutions.ror:{ror_id},type:article,from_publication_date:2000-01-01,locations.source.host_organization:https://openalex.org/publishers/p4310315706',
    'per-page': config['VARIABLES']['PAGE_SIZES']['openalex'],
    'select': 'id,doi,title,authorships,primary_location,type',
    'mailto': config['EMAIL']['user_email']
    }
j = 0
#define different number of pages to retrieve from OpenAlex API based on 'test' vs. 'prod' env
page_limit_openalex = config['VARIABLES']['PAGE_LIMITS']['openalex_test'] if test else config['VARIABLES']['PAGE_LIMITS']['openalex_prod']

##retrieves single page of results
def retrieve_page_openalex(url, params=None):
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()  
        return response.json()
    except requests.RequestException as e:
        print(f"Error retrieveing page: {e}")
        return {'results': [], 'meta':{}}
##recursively retrieves pages
def retrieve_all_data_openalex(url, params):
    global j
    all_data_openalex = []
    data = retrieve_page_openalex(url, params)
    params = params_openalex.copy()
    params['cursor'] = '*'
    next_cursor = '*'
    previous_cursor = None
    
    if not data['results']:
        print("No data found.")
        return all_data_openalex

    all_data_openalex.extend(data['results'])
    
    total_count = data.get('meta', {}).get('count', None)
    per = data.get('meta', {}).get('per_page', None)
    total_pages = math.ceil(total_count/per) + 1

    print(f"Total: {total_count} entries over {total_pages} pages")
    print()

    while j < page_limit_openalex:
        j += 1
        print(f"Retrieving page {j} of {total_pages} from OpenAlex...")
        print()
        data = retrieve_page_openalex(url, params)
        next_cursor = data.get('meta', {}).get('next_cursor', None)

        if next_cursor == previous_cursor:
            print("Cursor did not change. Ending loop to avoid infinite loop.")
            break
        
        if not data['results']:
            print("End of OpenAlex response.")
            print()
            break
        
        all_data_openalex.extend(data['results'])
        
        previous_cursor = next_cursor
        params['cursor'] = next_cursor
    
    return all_data_openalex

openalex = retrieve_all_data_openalex(url_openalex, params_openalex)
data_select_openalex = []
for item in openalex:
    doi = item.get('doi')
    title = item.get('title')
    publication_year = item.get('publication_year')
    source_display_name = item.get('primary_location', {}).get('source', {}).get('display_name')
    
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

df_openalex = pd.json_normalize(data_select_openalex)
if test:
    df_openalex = df_openalex[:10]

# URL of the webpage to scrape
doi = "https://doi.org/10.1371/journal.pone.0297637"

# Initialize lists to store the extracted data
all_files = []
all_descriptions = []
all_urls = []
all_formats = []
all_articles = []

for doi in df_openalex['doi_article']:
    try:
        # Send a GET request to the webpage
        response = requests.get(doi)
        response.raise_for_status()
        print(f"Retrieving information for {doi}")
        
        # Parse the HTML content of the webpage
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Find all divs with the class "supplementary-material"
        supplementary_materials = soup.find_all('div', class_='supplementary-material')
        
        # Loop through each supplementary material div and extract the relevant information
        for material in supplementary_materials:
            # Extract file label (e.g., S1 File)
            file_label_tag = material.find('h3', class_='siTitle title-small')
            file_label = file_label_tag.text.strip() if file_label_tag else 'not found'
            all_files.append(file_label)
            
            # Extract description
            description_tag = material.find('p', class_='preSiDOI')
            description = description_tag.text.strip() if description_tag else 'not found'
            all_descriptions.append(description)
            
            # Extract URL
            url_tag = material.find('p', class_='siDoi').find('a') if material.find('p', class_='siDoi') else None
            url = url_tag['href'] if url_tag else 'not found'
            all_urls.append(url)
            
            # Extract format from URL (e.g., XLSX, DOCX)
            format_tag = material.find('p', class_='postSiDOI')
            format_ = format_tag.text.strip().strip('()') if format_tag else 'not found'
            ##some articles have the file size listed with the format in parentheses
            ###as far as I can tell, file sizes do not exceed MB scale
            format_clean = re.sub(r'\d+(\.\d+)?\s*(KB|MB|GB|TB)\s*', '', format_).strip()
            all_formats.append(format_clean)
            
            # Add the article DOI
            all_articles.append(doi)
    
    except requests.RequestException as e:
        print(f"Error retrieving page for DOI {doi}: {e}")
    except AttributeError as e:
        print(f"Error parsing HTML for DOI {doi}: {e}")

# Create a DataFrame with the extracted data
data = {
    "title": all_files,
    "description": all_descriptions,
    "doi": all_urls,
    "format": all_formats,
    "article": all_articles
}

df_supplementary_materials = pd.DataFrame(data)

def generic_classification(row):
    title_criteria = {
        'Figure': 'Figure',  
        'Fig.': 'Figure',  
        'Table': 'Table',  
        'Data': 'Dataset',
        'Dataset': 'Dataset'
    }

    #check if any of the words in title_criteria appear in 'title'
    for word, value in title_criteria.items():
        if word in row['title']:
            return value
    return 'Other'  #Return 'Other' if no match is found

df_supplementary_materials['genericResourceType'] = df_supplementary_materials.apply(generic_classification, axis=1)
# Save the DataFrame to a CSV file
df_supplementary_materials.to_csv(f"accessory-outputs/{todayDate}_plos_extracted_SI_metadata.csv", index=False)