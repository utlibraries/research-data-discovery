import matplotlib.pyplot as plt
import numpy as np
import os
import pandas as pd
import shutil
from datetime import datetime

#creating variable with current date for appending to filenames
todayDate = datetime.now().strftime("%Y%m%d") 
#toggle for file format (prefer TIFF, if not, use PNG)
tiff = False
if tiff:
    plotFormat = 'tiff'
else:
    plotFormat = 'png'

script_dir = os.getcwd()
parent_dir = os.path.abspath(os.path.join(os.getcwd(), '..'))
outputs_dir = os.path.join(parent_dir, 'outputs')

#creating directory for older plots
if os.path.isdir("plots/old-plots"):
    print("old plots directory found - no need to recreate")
else:
    os.mkdir("plots/old-plots")
    print("old plots directory has been created")
#move plots not created today to that folder
for filename in os.listdir('plots'):
    if os.path.isfile(os.path.join('plots', filename)) and not filename.startswith(todayDate):
        shutil.move(os.path.join('plots', filename), os.path.join('plots/old-plots', filename))
print(f"Files not generated on {todayDate} have been moved to the old-plots subdirectory.")

#retrieve most recent output file
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

#patterns for output files from API queries
pattern1 = '_datacite-output-for-affiliation-source.csv'
df_datacite = load_most_recent_file(outputs_dir, pattern1)
if 'Software' in df_datacite['type'].values:
    df = df_datacite[df_datacite['type'] == 'Dataset']

pattern2 = '_full-concatenated-dataframe.csv'
df_all_repos = load_most_recent_file(outputs_dir, pattern2)
if 'Software' in df_all_repos['type'].values:
    df_all_repos = df_all_repos[df_all_repos['type'] == 'Dataset']

pattern3 = '_full-concatenated-dataframe-plus-figshare-ncbi-crossref.csv'
df_all_repos_plus = load_most_recent_file(outputs_dir, pattern3)
if 'Software' in df_all_repos_plus['type'].values:
    df_all_repos_plus = df_all_repos_plus[df_all_repos_plus['type'] == 'Dataset']
#subsetting for datasets where UT researcher is first/last/both
conditions = ['only lead', 'only senior', 'single author', 'both lead and senior']
df_all_repos_plus_ut_lead = df_all_repos_plus[df_all_repos_plus['uni_lead'].isin(conditions)]

pattern4 = '_figshare-discovery-deduplicated.csv'
df_extra_figshare = load_most_recent_file(outputs_dir, pattern4)
df_extra_figshare = df_extra_figshare.drop_duplicates(subset='relatedIdentifier', keep='first')

pattern5 = 'datacite-output-for-metadata-assessment'
df_metadata = load_most_recent_file(outputs_dir, pattern5)
if 'Software' in df_metadata['type'].values:
    df_metadata = df_metadata[df_metadata['type'] == 'Dataset']

plots_dir = os.path.join(script_dir, 'plots')
if os.path.isdir(plots_dir):
    print("plots directory found - no need to recreate")
else:
    os.mkdir(plots_dir)
    print("plots directory has been created")
#Renavigate back to the script directory
os.chdir(script_dir)

### Source of affiliation detection
if df_datacite is not None:
    plot_filename = f"{todayDate}_affiliation-source-counts.{plotFormat}"
    affiliation_source_counts = df_datacite['affiliation_source'].value_counts(ascending=True)
        
    fig, ax1 = plt.subplots(figsize=(10, 5))
    bars = ax1.barh(affiliation_source_counts.index, affiliation_source_counts.values, color='#00a9b7', edgecolor='black')
    ax1.set_xlabel("Dataset count", fontsize=15)
    ax1.set_ylabel("")
    ax1.set_title("Primary source of affiliation detection", fontsize=16)
    ax1.set_facecolor('#f7f7f7')
    ax1.grid(True, which='both', color='white', linestyle='-', linewidth=1.5)
    ax1.tick_params(axis='both', which='major', labelsize=14)
    ax1.set_axisbelow(True)
    plt.tight_layout()
    # plt.show()
    plot_path = os.path.join(plots_dir, plot_filename)
    plt.savefig(plot_path, format=plotFormat)

    print(f"{plot_filename} has been saved successfully at {plot_path}.\n")

    ### Affiliation variation
    plot_filename = f"{todayDate}_affiliation-permutation-counts.{plotFormat}"
    affiliation_source_counts = df_datacite['affiliation_source'].value_counts(ascending=True)
    affiliation_permutation_counts = df_datacite['affiliation_permutation'].value_counts(ascending=True)
        
    fig, ax1 = plt.subplots(figsize=(10, 7))
    bars = ax1.barh(affiliation_permutation_counts.index, affiliation_permutation_counts.values, color='#bf5700', edgecolor='black')
    ax1.set_xlabel("Dataset count", fontsize=15)
    ax1.set_ylabel("")
    ax1.set_title("Institutional permutation", fontsize=16)
    ax1.set_facecolor('#f7f7f7')
    ax1.grid(True, which='both', color='white', linestyle='-', linewidth=1.5)
    ax1.tick_params(axis='both', which='major', labelsize=14)
    ax1.set_axisbelow(True)
    plt.tight_layout()
    # plt.show()
    plot_path = os.path.join(plots_dir, plot_filename)
    plt.savefig(plot_path, format=plotFormat)
    print(f"{plot_filename} has been saved successfully at {plot_path}.\n")

### Repository count
####toggle for whether or not to include an aggregate 'Other' bar
includeOther = False
if df_all_repos_plus is not None:
    plot_filename = f"{todayDate}_repository-counts-30-plus.{plotFormat}"
    repo_counts = df_all_repos_plus['repository'].value_counts()
    df_all_repos_plus['collapsed_repository'] = df_all_repos_plus['repository'].apply(lambda x: x if repo_counts[x] >= 30 else 'Other')
    if includeOther:
        collapsed_repo_counts = df_all_repos_plus['collapsed_repository'].value_counts(ascending=True)
    else:
        df_all_repos_distinct = df_all_repos_plus[df_all_repos_plus['collapsed_repository'] != 'Other']
        collapsed_repo_counts = df_all_repos_distinct['collapsed_repository'].value_counts(ascending=True)

    fig, ax1 = plt.subplots(figsize=(10, 7))
    bars = ax1.barh(collapsed_repo_counts.index, collapsed_repo_counts.values, edgecolor='black')
    ax1.set_xlabel("Dataset count", fontsize=15)
    ax1.set_ylabel("")
    ax1.set_title("Most commonly identified repositories (30+ datasets)", fontsize=16)
    ax1.set_facecolor('#f7f7f7')
    ax1.grid(True, which='both', color='white', linestyle='-', linewidth=1.5)
    ax1.tick_params(axis='both', which='major', labelsize=14)
    ax1.set_axisbelow(True)
    plt.tight_layout()
    # plt.show()
    plot_path = os.path.join(plots_dir, plot_filename)
    plt.savefig(plot_path, format=plotFormat)
    print(f"{plot_filename} has been saved successfully at {plot_path}.\n")

    ### only for first/last author UT datasets
    plot_filename = f"{todayDate}_repository-counts-30-plus_UT-first-last.{plotFormat}"
    repo_counts = df_all_repos_plus_ut_lead['repository'].value_counts()
    df_all_repos_plus_ut_lead['collapsed_repository'] = df_all_repos_plus_ut_lead['repository'].apply(lambda x: x if repo_counts[x] >= 20 else 'Other')
    if includeOther:
        collapsed_repo_counts = df_all_repos_plus_ut_lead['collapsed_repository'].value_counts(ascending=True)
    else:
        df_all_repos_distinct = df_all_repos_plus_ut_lead[df_all_repos_plus_ut_lead['collapsed_repository'] != 'Other']
        collapsed_repo_counts = df_all_repos_distinct['collapsed_repository'].value_counts(ascending=True)

    fig, ax1 = plt.subplots(figsize=(10, 7))
    bars = ax1.barh(collapsed_repo_counts.index, collapsed_repo_counts.values, edgecolor='black')
    ax1.set_xlabel("Dataset count", fontsize=15)
    ax1.set_ylabel("")
    ax1.set_title("Most commonly identified repositories (20+ datasets)", fontsize=16)
    ax1.set_facecolor('#f7f7f7')
    ax1.grid(True, which='both', color='white', linestyle='-', linewidth=1.5)
    ax1.tick_params(axis='both', which='major', labelsize=14)
    ax1.set_axisbelow(True)
    plt.tight_layout()
    # plt.show()
    plot_path = os.path.join(plots_dir, plot_filename)
    plt.savefig(plot_path, format=plotFormat)
    print(f"{plot_filename} has been saved successfully at {plot_path}.\n")


### Discovered Figshare deposits
if df_extra_figshare is not None:
    plot_filename = f"{todayDate}_extra-figshare-counts.{plotFormat}"
    extra_publisher_counts = df_extra_figshare['repository'].value_counts(ascending=True)
        
    fig, ax1 = plt.subplots(figsize=(10, 7))
    bars = ax1.barh(extra_publisher_counts.index, extra_publisher_counts.values, color='#005f86', edgecolor='black')
    ax1.set_xlabel("Dataset count", fontsize=15)
    ax1.set_ylabel("")
    ax1.set_title("Count of extra Figshare deposits", fontsize=16)
    ax1.set_facecolor('#f7f7f7')
    ax1.grid(True, which='both', color='white', linestyle='-', linewidth=1.5)
    ax1.tick_params(axis='both', which='major', labelsize=14)
    ax1.set_axisbelow(True)
    plt.tight_layout()
    # plt.show()
    plot_path = os.path.join(plots_dir, plot_filename)
    plt.savefig(plot_path, format=plotFormat)
    print(f"{plot_filename} has been saved successfully at {plot_path}.\n")

### Repository deposit volume over time
if df_all_repos_plus is not None:
    plot_filename = f"{todayDate}_repositories-by-year.{plotFormat}"
    df_all_repos_2024 = df_all_repos_plus[df_all_repos_plus['publicationYear'] < 2025]
    repository_counts_by_year = df_all_repos_2024.groupby(['repository', 'publicationYear']).size().reset_index(name="counts")

    selected_repos = ['Dryad', 'Harvard Dataverse', 'Zenodo', 'Texas Data Repository', 'NCBI']
    df_filtered = repository_counts_by_year[repository_counts_by_year['repository'].isin(selected_repos)]
    color_map = {'Dryad': '#88AB75', 
                'NCBI': '#30638E', 
                'Zenodo': '#925E78', 
                'Texas Data Repository': '#bf5700', 
                'Harvard Dataverse': '#A51C30'}  

    #for comparison in a single plot
    # fig, ax = plt.subplots()
    # for i, repo in enumerate(df_filtered['repository'].unique()):
    #     subset = df_filtered[df_filtered['repository'] == repo]
    #     ax.plot(subset['publicationYear'], subset['counts'], label=repo, linewidth=2, color=color_map[repo])
    # # Customize the plot
    # ax.set_xlabel("")
    # ax.set_ylabel("Number of discovered datasets")
    # ax.legend(loc='lower center', title='', ncol=1)
    # ax.tick_params(axis='x', labelsize=12)
    # ax.tick_params(axis='y', labelsize=12)
    # ax.yaxis.label.set_size(14)
    # ax.legend(fontsize=12)
    # plt.subplots_adjust(left=0.1, right=0.9, top=0.9, bottom=0.1)
    # # Format x-axis to display years without decimal points
    # ax.xaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f'{int(x)}'))
    # plt.tight_layout()

    #for a gridded view with each repository in a different subplot
    fig, axs = plt.subplots(1, 5, figsize=(10, 5))
    for i, repo in enumerate(df_filtered['repository'].unique()):
        subset = df_filtered[df_filtered['repository'] == repo]
        # ax = axs[i // 3, i % 3]
        ax = axs[i]
        ax.plot(subset['publicationYear'], subset['counts'], label=repo, linewidth=3.5, color=color_map[repo])
        ax.set_title(repo, fontsize = 14)
        # ax.set_xlabel("Publication Year")
        # ax.set_ylabel("Number of discovered datasets")
        ax.tick_params(axis='x', labelsize=12)
        ax.tick_params(axis='y', labelsize=12)
        ax.set_facecolor('#f7f7f7')
        ax.grid(True, which='both', color='white', linestyle='-', linewidth=1.5)
        ax.tick_params(axis='both', which='major', labelsize=12)
        ax.set_axisbelow(True)
        ax.yaxis.label.set_size(12)
        #standardize axes
        ax.set_xlim(2014, 2024)
        ax.set_xticks(list(range(2014, 2024, 2)) + [2024])
        ax.set_ylim(0, 250)   
        ax.xaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f'{int(x)}'))
        plt.setp(ax.get_xticklabels(), rotation=45, ha='right')

        #only showing y-axis label on left-most graph
        if i != 0:
            ax.set_yticklabels([])
            ax.set_ylabel('')
            ax.tick_params(axis='y', which='both', left=False)

    # Remove the empty subplot (bottom right)
    # fig.delaxes(axs[1][2])
    plt.tight_layout()

    plot_path = os.path.join(plots_dir, plot_filename)
    plt.savefig(plot_path, format=plotFormat)
    print(f"{plot_filename} has been saved successfully at {plot_path}.\n")

    ### only for first/last author UT datasets
    plot_filename = f"{todayDate}_repositories-by-year_UT-first-last.{plotFormat}"
    df_all_repos_plus_ut_lead_2024 = df_all_repos_plus_ut_lead[df_all_repos_plus_ut_lead['publicationYear'] < 2025]
    repository_counts_by_year = df_all_repos_plus_ut_lead_2024.groupby(['repository', 'publicationYear']).size().reset_index(name="counts")

    selected_repos = ['Dryad', 'Harvard Dataverse', 'Zenodo', 'Texas Data Repository']
    df_filtered = repository_counts_by_year[repository_counts_by_year['repository'].isin(selected_repos)]
    color_map = {'Dryad': '#88AB75', 
                'Zenodo': '#925E78', 
                'Texas Data Repository': '#bf5700', 
                'Harvard Dataverse': '#A51C30'}  

    #for a gridded view with each repository in a different subplot
    fig, axs = plt.subplots(1, 4, figsize=(16, 6))
    for i, repo in enumerate(df_filtered['repository'].unique()):
        subset = df_filtered[df_filtered['repository'] == repo]
        # ax = axs[i // 3, i % 3]
        ax = axs[i]
        ax.plot(subset['publicationYear'], subset['counts'], label=repo, linewidth=3.5, color=color_map[repo])
        ax.set_title(repo, fontsize = 15)
        # ax.set_xlabel("Publication Year")
        # ax.set_ylabel("Number of discovered datasets")
        ax.tick_params(axis='x', labelsize=12)
        ax.tick_params(axis='y', labelsize=12)
        ax.set_facecolor('#f7f7f7')
        ax.grid(True, which='both', color='white', linestyle='-', linewidth=1.5)
        ax.tick_params(axis='both', which='major', labelsize=12)
        ax.set_axisbelow(True)
        ax.yaxis.label.set_size(12)
        #standardize axes
        ax.set_xlim(2014, 2024)
        ax.set_xticks(list(range(2014, 2024, 2)) + [2024])
        ax.set_ylim(0, 230)   
        ax.xaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f'{int(x)}'))
        plt.setp(ax.get_xticklabels(), rotation=45, ha='right')
    # Remove the empty subplot (bottom right)
    # fig.delaxes(axs[1][2])
    plt.tight_layout()

    plot_path = os.path.join(plots_dir, plot_filename)
    plt.savefig(plot_path, format=plotFormat)
    print(f"{plot_filename} has been saved successfully at {plot_path}.\n")

### Authorship position of UT researcher
if df_all_repos_plus is not None:
    plot_filename = f"{todayDate}_ut-author-position_all-repos.{plotFormat}"
    authorPositions = df_all_repos_plus['uni_lead'].value_counts(ascending=True)
    
    color_map = {
        'only lead': '#bf5700',
        'only senior': '#bf5700',
        'neither lead nor senior': "#76a0ee",
        'both lead and senior': '#bf5700',
        'single author': '#bf5700',
        'Affiliated (authorship unclear)': "#b4b3b2"
    }

    # Ensure the colors are in the same order as the columns
    colors = [color_map.get(position, '#cccccc') for position in authorPositions.index]

    fig, ax = plt.subplots(figsize=(10, 7))
    bars = ax.barh(authorPositions.index, authorPositions.values, color=colors, edgecolor='black')
    ax.set_xlabel("Dataset count", fontsize=15)
    ax.set_ylabel("")
    ax.set_title("Distribution of datasets by author position of UT researcher(s)", fontsize=16)
    ax.set_facecolor('#f7f7f7')
    ax.grid(True, which='both', color='white', linestyle='-', linewidth=1.5)
    ax.tick_params(axis='both', which='major', labelsize=14)
    ax.set_axisbelow(True)
    plt.tight_layout()
    plot_path = os.path.join(plots_dir, plot_filename)
    plt.savefig(plot_path, format=plotFormat)
    print(f"{plot_filename} has been saved successfully at {plot_path}.\n")

    plot_filename = f"{todayDate}_extra-figshare-counts_top-repos.{plotFormat}"
    topRepos = df_all_repos_plus[df_all_repos_plus['repository'].str.contains('Dryad|Texas Data Repository|Zenodo|Harvard Dataverse')]
    #create binary column
    topRepos['affiliated'] = topRepos['uni_lead'].apply(
        lambda x: 'first/last' if x != 'neither lead nor senior' else x
    )
    authorPositionsTopRepos = pd.crosstab(topRepos['repository'], topRepos['affiliated'])
    authorProportions = authorPositionsTopRepos.div(authorPositionsTopRepos.sum(axis=1), axis=0)

    color_map = {
            'first/last': '#bf5700',
            'neither lead nor senior': "#76a0ee"
        }

    colors = [color_map.get(col, '#cccccc') for col in authorProportions.columns]

    fig, ax = plt.subplots(figsize=(10, 7))
    authorProportions.plot(kind='barh', stacked=True, ax=ax, color=colors, edgecolor='black')
    ax.set_xlabel("Proportion", fontsize=15)
    ax.set_ylabel("")
    ax.set_title("Proportional distribution of author positions by repository", fontsize=16)
    ax.set_facecolor('#f7f7f7')
    ax.grid(True, which='both', color='white', linestyle='-', linewidth=1.5)
    ax.tick_params(axis='both', which='major', labelsize=14)
    ax.set_axisbelow(True)
    plt.legend(title='Author Position', title_fontsize='13', fontsize='12', ncol=2, loc='lower center', bbox_to_anchor=(0.5, -0.33))
    plt.tight_layout()
    plot_path = os.path.join(plots_dir, plot_filename)
    plt.savefig(plot_path, format=plotFormat)
    print(f"{plot_filename} has been saved successfully at {plot_path}.\\n")

###metadata assessments
####contains software
if df_metadata is not None:
    plot_filename = f"{todayDate}_contains-software.{plotFormat}"
    #making some df modifications
    ##will want to move into main script later
    # Convert 'TRUE'/'FALSE' strings to Boolean values
    df_metadata['containsCode'] = df_metadata['containsCode'].astype(str).str.upper().map({'TRUE': True, 'FALSE': False})
    df_metadata['onlyCode'] = df_metadata['onlyCode'].astype(str).str.upper().map({'TRUE': True, 'FALSE': False})

    # Apply your logic to create 'containsCodeAdjusted'
    df_metadata['containsCodeAdjusted'] = (
        df_metadata['onlyCode'].apply(lambda x: 'Only code' if x else None)
        .combine_first(
            df_metadata.apply(
                lambda row: (
                    'No file format information' if row['formats'] == 'No file information'
                    else 'No code' if not row['containsCode']
                    else 'Code and non-code files' if row['containsCode'] and not row['onlyCode']
                    else 'Only code' if row['onlyCode']
                    else 'Unknown'
                ),
                axis=1
            )
        )
    )
    softwareInclusions = df_metadata['containsCodeAdjusted'].value_counts(ascending=True)
    color_map = {
            'No file information': "#eaeaea",
            'Code and non-code files': "#d41159",
            'Only code': "#d41159",
            'No code': "#1a85ff"
        }
    colors = [color_map.get(type, '#cccccc') for type in softwareInclusions.index]
    fig, ax = plt.subplots(figsize=(10, 7))
    softwareInclusions.plot(kind='barh', stacked=False, ax=ax, color=colors, edgecolor='black')
    ax.set_xlabel("Count", fontsize=15)
    ax.set_ylabel("")
    ax.set_title("Count of datasets that contain at least one software format", fontsize=16)
    ax.set_facecolor('#f7f7f7')
    ax.grid(True, which='both', color='white', linestyle='-', linewidth=1.5)
    ax.tick_params(axis='both', which='major', labelsize=14)
    ax.set_axisbelow(True)
    plt.tight_layout()

    plot_path = os.path.join(plots_dir, plot_filename)
    plt.savefig(plot_path, format=plotFormat)
    print(f"{plot_filename} has been saved successfully at {plot_path}.\\n")

    # plot_filename = f"{todayDate}_only-software.{plotFormat}"
    # softwareOnly = df_metadata['onlyCode'].value_counts(ascending=True)

    # fig, ax = plt.subplots(figsize=(10, 7))
    # softwareOnly.plot(kind='barh', stacked=False, ax=ax, color="#51bed3", edgecolor='black')
    # ax.set_xlabel("Count", fontsize=15)
    # ax.set_ylabel("")
    # ax.set_title("Count of datasets that only contain software format(s)", fontsize=16)
    # ax.set_facecolor('#f7f7f7')
    # ax.grid(True, which='both', color='white', linestyle='-', linewidth=1.5)
    # ax.tick_params(axis='both', which='major', labelsize=14)
    # ax.set_axisbelow(True)
    # plt.tight_layout()

    # plot_path = os.path.join(plots_dir, plot_filename)
    # plt.savefig(plot_path, format=plotFormat)
    # print(f"{plot_filename} has been saved successfully at {plot_path}.\\n")

    ## file size
    plot_filename = f"{todayDate}_datasets-by-size-bin.{plotFormat}"
    df_metadata['depositSize'] = pd.to_numeric(df_metadata['depositSize'], errors='coerce')

    bins = [
        -1, # For 0 or negative/NaN
        10 * 1024,                   
        1 * 1024 * 1024,             
        100 * 1024 * 1024,           
        1 * 1024 * 1024 * 1024,      
        5 * 1024 * 1024 * 1024, 
        10 * 1024 * 1024 * 1024,     
        15 * 1024 * 1024 * 1024,     
        20 * 1024 * 1024 * 1024,    
        25 * 1024 * 1024 * 1024,     
        30 * 1024 * 1024 * 1024, 
        35 * 1024 * 1024 * 1024,     
        40 * 1024 * 1024 * 1024,
        45 * 1024 * 1024 * 1024,      
        50 * 1024 * 1024 * 1024,     
        float('inf') # >50 GB
    ]

    labels = [
        "0-10 kB",
        "10 kB-1 MB",
        "1-100 MB",
        "100 MB-1 GB",
        "1-5 GB",
        "5-10 GB",
        "10-15 GB",
        "15-20 GB",
        "20-25 GB",
        "25-30 GB",
        "30-35 GB",
        "35-40 GB",
        "40-45 GB",
        "45-50 GB",
        ">50 GB"
    ]

    df_metadata['size_bin'] = pd.cut(
        df_metadata['depositSize'],
        bins=bins,
        labels=labels
    )
    df_metadata['size_bin'] = df_metadata['size_bin'].cat.add_categories('Empty')
    df_metadata.loc[df_metadata['depositSize'].isna(), 'size_bin'] = 'Empty'
    df_metadata.loc[(df_metadata['depositSize'] <= 0), 'size_bin'] = 'Empty'
    plot_filename = f"{todayDate}_datasets-by-size-bin.{plotFormat}"
    
    datasets_size = df_metadata['size_bin'].value_counts(ascending=True).reset_index()
    datasets_size.columns = ['size_bin', 'count']
        
    fig, ax = plt.subplots(figsize=(10, 7))
    bars = ax.barh(datasets_size['size_bin'], datasets_size['count'], color='#3a71c9', edgecolor='black')
    ax.set_ylabel("Dataset count", fontsize=15)
    ax.set_xlabel("")
    ax.set_title("Distribution of published datasets by size bin", fontsize=16)
    ax.set_facecolor('#f7f7f7')
    ax.grid(True, which='both', color='white', linestyle='-', linewidth=1.5)
    ax.tick_params(axis='both', which='major', labelsize=14)
    ax.set_axisbelow(True)
    # ax.set_xticks(datasets_size['size_bin'])
    # ax.set_xticklabels(datasets_size['size_bin'])

    plot_path = os.path.join(plots_dir, plot_filename)
    plt.savefig(plot_path, format=plotFormat)
    print(f"{plot_filename} has been saved successfully at {plot_path}.\\n")

    ##licensing
    plot_filename = f"{todayDate}_datasets-by-licensing-all.{plotFormat}"
    licensing = df_metadata['rights_standardized'].value_counts()
    print(licensing)

    fig, ax = plt.subplots(figsize=(10, 7))
    bars = ax.barh(licensing.index, licensing.values, edgecolor='black')
    ax.set_xlabel("Dataset count", fontsize=15)
    ax.set_ylabel("")
    ax.set_title("Distribution of datasets by identified license", fontsize=16)
    ax.set_facecolor('#f7f7f7')
    ax.grid(True, which='both', color='white', linestyle='-', linewidth=1.5)
    ax.tick_params(axis='both', which='major', labelsize=14)
    ax.set_axisbelow(True)
    plt.tight_layout()

    plot_path = os.path.join(plots_dir, plot_filename)
    plt.savefig(plot_path, format=plotFormat)
    print(f"{plot_filename} has been saved successfully at {plot_path}.\\n")

    ###licensing minus Dryad and TDR (CC0 default or mandatory)
    plot_filename = f"{todayDate}_datasets-by-licensing-select.{plotFormat}"
    df_metadata_select = df_metadata[~df_metadata['publisher'].str.contains('Dryad|Texas', case=True, na=False)]
    licensing_select = df_metadata_select['rights_standardized'].value_counts()

    fig, ax = plt.subplots(figsize=(10, 7))
    bars = ax.barh(licensing_select.index, licensing_select.values, edgecolor='black')
    ax.set_xlabel("Dataset count", fontsize=15)
    ax.set_ylabel("")
    ax.set_title("Distribution of datasets by identified license (select repositories)", fontsize=16)
    ax.set_facecolor('#f7f7f7')
    ax.grid(True, which='both', color='white', linestyle='-', linewidth=1.5)
    ax.tick_params(axis='both', which='major', labelsize=14)
    ax.set_axisbelow(True)
    plt.tight_layout()

    plot_path = os.path.join(plots_dir, plot_filename)
    plt.savefig(plot_path, format=plotFormat)
    print(f"{plot_filename} has been saved successfully at {plot_path}.\\n")

    ###combined licensing
    plot_filename = f"{todayDate}_datasets-by-licensing-combined.{plotFormat}"
    ####convert to dfs
    # Convert Series to DataFrame and add 'Type' column
    licensing = licensing.reset_index()
    licensing.columns = ['License', 'Count']
    licensing_select = licensing_select.reset_index()
    licensing_select.columns = ['License', 'Count']
    licensing['Type'] = 'All datasets'
    licensing_select['Type'] = 'Removal of Dryad and TDR datasets'
    licensing_combo = pd.concat([licensing, licensing_select], ignore_index=True)
    df_pivot = licensing_combo.pivot(index='License', columns='Type', values='Count').fillna(0)
    df_pivot_sorted = df_pivot.sort_values(by='All datasets', ascending=True)

    y = np.arange(len(df_pivot_sorted.index))
    bar_height = 0.4
    fig, ax = plt.subplots(figsize=(10, 7))
    ax.barh(y - bar_height/2, df_pivot_sorted['All datasets'], height=bar_height, label='All datasets', color='#E1BE6A', edgecolor='black')
    ax.barh(y + bar_height/2, df_pivot_sorted['Removal of Dryad and TDR datasets'], height=bar_height, label='Removal of Dryad and TDR datasets', color='#40B0A6', edgecolor='black')
    ax.set_xlabel("Dataset count", fontsize=15)
    ax.set_yticks(y)
    ax.set_yticklabels(df_pivot_sorted.index, fontsize=14)
    ax.set_title("Distribution of datasets by identified license", fontsize=16)
    ax.set_facecolor('#f7f7f7')
    ax.grid(True, which='both', color='white', linestyle='-', linewidth=1.5)
    ax.tick_params(axis='x', which='major', labelsize=14)
    ax.set_axisbelow(True)
    ax.legend(loc='lower right')
    plt.tight_layout()
    plot_path = os.path.join(plots_dir, plot_filename)
    plt.savefig(plot_path, format=plotFormat)
    print(f"{plot_filename} has been saved successfully at {plot_path}.\\n")

    ##descriptive words
    plot_filename = f"{todayDate}_datasets-title-descriptive.{plotFormat}"
    descriptiveTitles = df_metadata['descriptive_word_count_title'].value_counts()

    fig, ax = plt.subplots(figsize=(10, 7))
    bars = ax.bar(descriptiveTitles.index, descriptiveTitles.values, edgecolor='black')
    ax.set_xlabel("Word count", fontsize=15)
    ax.set_ylabel("Dataset count")
    ax.set_title("Distribution of datasets by number of descriptive words in the title", fontsize=16)
    ax.set_facecolor('#f7f7f7')
    ax.grid(True, which='both', color='white', linestyle='-', linewidth=1.5)
    ax.tick_params(axis='both', which='major', labelsize=14)
    ax.set_axisbelow(True)
    plt.tight_layout()

    plot_path = os.path.join(plots_dir, plot_filename)
    plt.savefig(plot_path, format=plotFormat)
    print(f"{plot_filename} has been saved successfully at {plot_path}.\\n")

    ##non-descriptive words
    plot_filename = f"{todayDate}_datasets-title-nondescriptive.{plotFormat}"
    nondescriptiveTitles = df_metadata['nondescriptive_word_count_title'].value_counts()

    fig, ax = plt.subplots(figsize=(10, 7))
    bars = ax.bar(nondescriptiveTitles.index, nondescriptiveTitles.values, edgecolor='black')
    ax.set_xlabel("Word count", fontsize=15)
    ax.set_ylabel("Dataset count")
    ax.set_title("Distribution of datasets by number of nondescriptive words in the title", fontsize=16)
    ax.set_facecolor('#f7f7f7')
    ax.grid(True, which='both', color='white', linestyle='-', linewidth=1.5)
    ax.tick_params(axis='both', which='major', labelsize=14)
    ax.set_axisbelow(True)
    plt.tight_layout()

    plot_path = os.path.join(plots_dir, plot_filename)
    plt.savefig(plot_path, format=plotFormat)
    print(f"{plot_filename} has been saved successfully at {plot_path}.\\n")

##### FOR ACCESSING FILES IN ACCESSORY-OUTPUTS #####
script_dir = os.getcwd()
parent_dir = os.path.abspath(os.path.join(os.getcwd(), '..'))
outputs_dir = os.path.join(parent_dir, 'accessory-scripts/accessory-outputs')

#retrieve most recent output file
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

#patterns for output files from API queries
patternA = '_Dryad-into-DataCite_joint-all-dataframe.csv'
df_dryad = load_most_recent_file(outputs_dir, patternA)
df_dryad.rename(columns={'publicationYear_x': 'publicationYear (DataCite)', 'publicationYear_y': 'publicationYear (Dryad)'}, inplace=True)

patternB = '_Dryad-into-DataCite_joint-all-dataframe_ut-austin'
df_dryadUT = load_most_recent_file(outputs_dir, patternB)
df_dryadUT.rename(columns={'publicationYear_x': 'publicationYear (DataCite)', 'publicationYear_y': 'publicationYear (Dryad)'}, inplace=True)

# Filter: remove DOIs with two slashes unless they contain 'digitalcsic'
filtered_df = df_dryad[~df_dryad['doi'].str.contains(r'/.*/') | df_dryad['doi'].str.contains(r'/digitalcsic/')]

if filtered_df is not None:
    plot_filename = f"{todayDate}_dryad-timestamp-comparison.{plotFormat}"
    #columns to compare
    # year_columns = ['registeredYear', 'createdYear', 'issuedYear', 'availableYear', 'publicationYear (DataCite)', 'publicationYear (Dryad)']
    year_columns = ['availableYear', 'publicationYear (DataCite)', 'publicationYear (Dryad)']

    # Convert year columns to numeric, coercing errors to NaN
    for col in year_columns:
        filtered_df[col] = pd.to_numeric(filtered_df[col], errors='coerce')
        df_dryadUT[col] = pd.to_numeric(df_dryadUT[col], errors='coerce')

    #count the number of entries per year for each column
    yearly_counts = {col: filtered_df[col].value_counts().sort_index() for col in year_columns}
    yearly_counts_df = pd.DataFrame(yearly_counts)
    yearly_countsUT = {col: df_dryadUT[col].value_counts().sort_index() for col in year_columns}
    yearly_countsUT_df = pd.DataFrame(yearly_countsUT)
    # Convert wide format to long format
    long_format_df = yearly_counts_df.reset_index().melt(
        id_vars=['index'],
        var_name='year_column',
        value_name='count'
    )
    long_format_df.rename(columns={'index': 'year'}, inplace=True)
    long_formatUT_df = yearly_countsUT_df.reset_index().melt(
        id_vars=['index'],
        var_name='year_column',
        value_name='count'
    )
    long_formatUT_df.rename(columns={'index': 'year'}, inplace=True)
    #map column to source
    repo_mapping = {
        'registeredYear': 'DataCite',
        'createdYear': 'DataCite',
        'publicationYear (DataCite)': 'DataCite',
        'publicationYear (Dryad)': 'Dryad API',
        'issuedYear': 'DataCite',
        'availableYear': 'DataCite'
    }

    #Apply mapping
    long_format_df['source'] = long_format_df['year_column'].map(repo_mapping)
    long_formatUT_df['source'] = long_formatUT_df['year_column'].map(repo_mapping)

    # Plotting
    fig, axs = plt.subplots(2, 1, figsize=(10, 8))
    color_map = {
        'registeredYear': 'blue',
        'createdYear': 'brown',
        'publicationYear (DataCite)': '#D67AB1',
        'publicationYear (Dryad)': '#8DAB7F',
        'issuedYear': 'purple',
        'availableYear': '#EF8354'
    }

    # Full dataset plot
    for year_column in long_format_df['year_column'].unique():
        subset = long_format_df[long_format_df['year_column'] == year_column]
        axs[1].plot(subset['year'], subset['count'], linewidth=3.5, label=year_column, color=color_map.get(year_column, 'black'))
    axs[1].set_title('All Dryad datasets', fontsize=16)
    axs[1].set_xlabel("")
    axs[1].set_ylabel("Dataset count", fontsize=15)
    axs[1].tick_params(axis='x', labelsize=14)
    axs[1].tick_params(axis='y', labelsize=14)
    axs[1].set_facecolor('#f7f7f7')
    axs[1].grid(True, which='both', color='white', linestyle='-', linewidth=1.5)
    axs[1].set_axisbelow(True)
    axs[1].set_xlim(2011, 2025)
    axs[1].set_xticks(list(range(2011, 2025, 2)) + [2025])
    axs[1].xaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f'{int(x)}'))
    axs[1].legend()

    # Subset dataset plot
    for year_column in long_formatUT_df['year_column'].unique():
        subset = long_formatUT_df[long_formatUT_df['year_column'] == year_column]
        axs[0].plot(subset['year'], subset['count'], linewidth=3.5, label=year_column, color=color_map.get(year_column, 'black'))
    axs[0].set_title('UT Austin-affiliated Dryad datasets', fontsize=16)
    axs[0].set_xlabel("")
    axs[0].set_ylabel("Dataset count", fontsize=15)
    axs[0].tick_params(axis='x', labelsize=14)
    axs[0].tick_params(axis='y', labelsize=14)
    axs[0].set_facecolor('#f7f7f7')
    axs[0].grid(True, which='both', color='white', linestyle='-', linewidth=1.5)
    axs[0].set_axisbelow(True)
    axs[0].set_xlim(2011, 2025)
    axs[0].set_xticks(list(range(2011, 2025, 2)) + [2025])
    axs[0].set_ylim(0, long_formatUT_df['count'].max() + 10)
    axs[0].xaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f'{int(x)}'))
    axs[0].legend()
    
    # ax.set_title('Number of datasets by year', fontsize=15)
    # ax.set_xlabel("")
    # ax.set_ylabel("Number of Entries")
    # ax.tick_params(axis='x', labelsize=12)
    # ax.tick_params(axis='y', labelsize=12)
    # ax.set_facecolor('#f7f7f7')
    # ax.grid(True, which='both', color='white', linestyle='-', linewidth=1.5)
    # ax.set_axisbelow(True)
    # ax.set_xlim(2011, 2025)
    # ax.set_xticks(list(range(2011, 2025, 2)) + [2025])
    # ax.set_ylim(0, max([max(counts.values) for counts in yearly_counts.values()]) + 200)
    # ax.xaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f'{int(x)}'))
    # # plt.setp(ax.get_xticklabels(), rotation=45, ha='right')
    # ax.legend()
    plt.tight_layout()

    plot_path = os.path.join(plots_dir, plot_filename)
    plt.savefig(plot_path, format=plotFormat)
    print(f"{plot_filename} has been saved successfully at {plot_path}.\\n")

#RADS reanalysis (versions)
patternC = '_RADS-figshare-datasets-progressive-filtering-summary.csv'
df_rads = load_most_recent_file(outputs_dir, patternC)

patternD = 'figshare-associated-articles-merged.csv'
df_rads_figshare_articles = load_most_recent_file(outputs_dir, patternD)

# ###Journals linked to RADS Figshare deposits
# if df_rads_figshare_articles is not None:
#     plot_filename = f"{todayDate}_RADS-linked-figshare-articles.{plotFormat}"
#     journalCount = df_rads_figshare_articles['journal'].value_counts(ascending=True)
    
#     color_map = {
#         'Springer Nature': '#bf5700',
#         'CABI Publishing': "#76a0ee"
#     }

#     # Ensure the colors are in the same order as the columns
#     colors = [color_map.get(position, '#cccccc') for position in journalCount.index]

#     fig, ax = plt.subplots(figsize=(10, 7))
#     bars = ax.barh(journalCount.index, journalCount.values, color=colors, edgecolor='black')
#     ax.set_xlabel("Article count", fontsize=15)
#     ax.set_ylabel("")
#     ax.set_title("Distribution of linked Figshare articles", fontsize=16)
#     ax.set_facecolor('#f7f7f7')
#     ax.grid(True, which='both', color='white', linestyle='-', linewidth=1.5)
#     ax.tick_params(axis='both', which='major', labelsize=14)
#     ax.set_axisbelow(True)
#     plt.tight_layout()
#     plot_path = os.path.join(plots_dir, plot_filename)
#     plt.savefig(plot_path, format=plotFormat)
#     print(f"{plot_filename} has been saved successfully at {plot_path}.\n")

if df_rads is not None:
    #get proportions of each count
    original_counts = df_rads[df_rads['type'] == 'Original'][['institution', 'entry_count']]
    original_counts = original_counts.rename(columns={'entry_count': 'original_count'})
    df_rads = df_rads.merge(original_counts, on='institution')
    df_rads['proportion'] = df_rads['entry_count'] / df_rads['original_count']

    plot_filename = f"{todayDate}_RADS-version-reduction-comparison.{plotFormat}"

    # Define colors
    type_colors = {
        'Original': "#8e1804",
        'Versions removed': "#d35b5b",
        'Consolidation': "#ebb8b5"
    }

    # Create subplots
    institutions = df_rads['institution'].unique()
    fig, axs = plt.subplots(len(institutions), 1, figsize=(10, 10), sharex=True)

    # Plot each institution
    for i, institution in enumerate(institutions):
        subset = df_rads[df_rads['institution'] == institution].sort_values(by='proportion', ascending=True)
        ax = axs[i]
        for _, row in subset.iterrows():
            ax.barh(row['type'], row['proportion'], color=type_colors[row['type']])
        ax.set_title(institution, fontsize=15)
        ax.set_ylabel('')
        ax.set_xlim(0, 1)
        ax.set_xticks(np.arange(0.0, 1.1, 0.1))
        ax.set_xticklabels([f"{x:.1f}" for x in np.arange(0.0, 1.1, 0.1)], fontsize=12)
        ax.tick_params(axis='x', labelbottom=True)
        ax.tick_params(axis='y', labelsize=14)
        ax.grid(True, which='both', color='white', linestyle='-', linewidth=1.5)
        ax.set_facecolor('#f7f7f7')
        ax.set_axisbelow(True)
        if i == len(institutions) - 1:
            ax.set_xlabel('Proportion', fontsize=14)
        else:
            ax.set_xlabel('')

    plt.tight_layout()

    plot_path = os.path.join(plots_dir, plot_filename)
    plt.savefig(plot_path, format=plotFormat)
    print(f"{plot_filename} has been saved successfully at {plot_path}.\\n")