import matplotlib.pyplot as plt
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

pattern2 = '_full-concatenated-dataframe.csv'
df_all_repos = load_most_recent_file(outputs_dir, pattern2)

pattern3 = '_full-concatenated-dataframe-plus-figshare-ncbi.csv'
df_all_repos_plus = load_most_recent_file(outputs_dir, pattern3)

pattern4 = '_figshare-discovery.csv'
df_extra_figshare = load_most_recent_file(outputs_dir, pattern4)

plots_dir = os.path.join(script_dir, 'plots')
if os.path.isdir(plots_dir):
    print("plots directory found - no need to recreate")
else:
    os.mkdir(plots_dir)
    print("plots directory has been created")
#Renavigate back to the script directory
os.chdir(script_dir)

### Source of affiliation detection
plot_filename = f"{todayDate}_affiliation-source-counts.{plotFormat}"
affiliation_source_counts = df_datacite['affiliation_source'].value_counts(ascending=True)
    
fig, ax1 = plt.subplots(figsize=(10, 7))
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
if tiff:
    plot_filename = f"{todayDate}_affiliation-permutation-counts.{plotFormat}"
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

plot_filename = f"{todayDate}_repository-counts-30-plus.{plotFormat}"
repo_counts = df_all_repos['repository'].value_counts()
df_all_repos['collapsed_repository'] = df_all_repos['repository'].apply(lambda x: x if repo_counts[x] >= 30 else 'Other')
if includeOther:
    collapsed_repo_counts = df_all_repos['collapsed_repository'].value_counts(ascending=True)
else:
    df_all_repos_distinct = df_all_repos[df_all_repos['collapsed_repository'] != 'Other']
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

### Discovered Figshare deposits
plot_filename = f"{todayDate}_extra-figshare-counts.{plotFormat}"
extra_publisher_counts = df_extra_figshare['publisher'].value_counts(ascending=True)
    
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
fig, axs = plt.subplots(1, 5, figsize=(16, 6))
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