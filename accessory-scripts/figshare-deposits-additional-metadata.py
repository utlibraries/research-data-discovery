from datetime import datetime
import json
import pandas as pd
import os
import requests

#read in config file
with open('config.json', 'r') as file:
    config = json.load(file)

#creating variable with current date for appending to filenames
todayDate = datetime.now().strftime('%Y%m%d') 

#API endpoint
url_figshare = 'https://api.figshare.com/v2/articles/{id}/files?page_size=10'

#for reading in previously generated file of all discovered datasets
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
#extracting deposit ID from DOI
figshare['id'] = figshare['doi'].str.extract(r'figshare\.(\d+)')

print('Retrieving additional Figshare metadata from Figshare API\n')
results = []
for id in figshare['id']:
    try:
        response = requests.get(url_figshare.format(id=id))
        if response.status_code == 200:
            print(f'Retrieving record #{id}\n')
            results.append({'id': id, 'data': response.json()})
        else:
            print(f'Error retrieving {id}: {response.status_code}, {response.text}')
    except requests.exceptions.RequestException as e:
        print(f'Timeout error on ID {id}: {e}')

data_figshare_select = []
for item in results:
    id = item.get('id')
    dataset = item.get('data', [])
    mimetypes_set = set()
    for file_info in dataset:
        mimetypes_set.add(file_info.get('mimetype'))
        data_figshare_select.append({
            'id': id,
            'name': file_info.get('name'),
            'size': file_info.get('size'),
            'mimeTypeSet': mimetypes_set,
            'mimeType': file_info.get('mimetype'),
        })

df_figshare_metadata = pd.DataFrame(data_figshare_select)
format_map = config['FORMAT_MAP']
df_figshare_metadata['fileFormat'] = df_figshare_metadata['mimeType'].apply(lambda x: format_map.get(x, x))
df_figshare_metadata['fileFormatsSet'] = df_figshare_metadata['mimeTypeSet'].apply(lambda x: '; '.join([format_map.get(fmt, fmt) for fmt in x]) if x != 'no files' else 'no files')

#working around vague or misclassified mimetypes in Figshare metadata
##may need to be expanded for other institutions
extension_criteria = {
'.csv': 'CSV',
'.doc': 'MS Word',
'.m': 'MATLAB Script',
'.ppt': 'MS PowerPoint',
'.R': 'R Script',
'.rds': 'R Data File',
'.xls': 'MS Excel'
}

# Apply criteria to replace fileFormat entry based on file extension and mimetype
def apply_extension_criteria(row):
    for ext, fmt in extension_criteria.items():
        if row['name'].endswith(ext) and row['mimeType'] == 'text/plain':
            return fmt
        if row['name'].endswith(ext) and row['mimeType'] == 'application/CDFV2':
            return fmt
        if row['name'].endswith(ext) and row['mimeType'] == 'application/x-xz':
            return fmt
    return row['fileFormat']

df_figshare_metadata['editedFileFormat'] = df_figshare_metadata.apply(apply_extension_criteria, axis=1)
# df_figshare_metadata.to_csv(f'accessory-outputs/{todayDate}_figshare-discovery-all-metadata.csv', index=False)

#combines all file types for one deposit ('id') into semi-colon-delimited string
df_figshare_metadata_unified = df_figshare_metadata.groupby('id')['editedFileFormat'].apply(lambda x: '; '.join(set(x))).reset_index()
#alphabetically orders file formats
df_figshare_metadata_unified['ordered_formats'] = df_figshare_metadata_unified['editedFileFormat'].apply(lambda x: '; '.join(sorted(x.split('; '))))

#basic assessment of 'dataset' classification
##list of strings for software formats to check for
software = ['MATLAB Script', 'R Script', 'Python', 'Shell Script']
##create two new columns for software detection
df_figshare_metadata_unified['onlySoftware'] = df_figshare_metadata_unified['ordered_formats'].apply(lambda x: x if x in software else '')
df_figshare_metadata_unified['containsSoftware'] = df_figshare_metadata_unified['ordered_formats'].apply(lambda x: any(s in x for s in software))
##list of formats that are less likely to be data to check for
notData = ['MS Word', 'PDF', 'MS Word; PDF']
# Create a new column with 'Suspect' values
df_figshare_metadata_unified['possiblyNotData'] = df_figshare_metadata_unified['ordered_formats'].apply(lambda x: 'Suspect' if x in notData else '')
df_figshare_metadata_combined = pd.merge(figshare, df_figshare_metadata_unified, on='id', how='left')
df_figshare_metadata_combined.to_csv(f'accessory-outputs/{todayDate}_figshare-discovery-all-metadata_combined.csv', index=False)