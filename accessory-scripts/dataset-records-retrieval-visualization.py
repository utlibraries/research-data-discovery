import matplotlib.pyplot as plt
import os
import pandas as pd
from datetime import datetime

#creating variable with current date for appending to filenames
todayDate = datetime.now().strftime("%Y%m%d") 

script_dir = os.getcwd()
parent_dir = os.path.abspath(os.path.join(os.getcwd(), '..'))
outputs_dir = os.path.join(parent_dir, 'outputs')

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

pattern1 = '_datacite-output-for-affiliation-source.csv'
df_datacite = load_most_recent_file(outputs_dir, pattern1)

pattern2 = '_full-concatenated-dataframe.csv'
df_all_repos = load_most_recent_file(outputs_dir, pattern2)

pattern3 = '_full-concatenated-dataframe-plus.csv'
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
plot_filename = f"{todayDate}_affiliation-source-counts.tiff"
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
plt.savefig(plot_path, format='tiff')

print(f"{plot_filename} has been saved successfully at {plot_path}.\n")

### Affiliation variation

plot_filename = f"{todayDate}_affiliation-permutation-counts.tiff"
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
plt.savefig(plot_path, format='tiff')

print(f"{plot_filename} has been saved successfully at {plot_path}.\n")

### Repository count

plot_filename = f"{todayDate}_repository-counts-30-plus.tiff"
repo_counts = df_all_repos['repository'].value_counts()
df_all_repos['collapsed_repository'] = df_all_repos['repository'].apply(lambda x: x if repo_counts[x] >= 30 else 'Other')
collapsed_repo_counts = df_all_repos['collapsed_repository'].value_counts(ascending=True)

fig, ax1 = plt.subplots(figsize=(10, 7))
bars = ax1.barh(collapsed_repo_counts.index, collapsed_repo_counts.values, edgecolor='black')
ax1.set_xlabel("Dataset count", fontsize=15)
ax1.set_ylabel("")
ax1.set_title("Most commonly identified repositories (30+ datasets)", color="#ffd600", fontsize=16)
ax1.set_facecolor('#f7f7f7')
ax1.grid(True, which='both', color='white', linestyle='-', linewidth=1.5)
ax1.tick_params(axis='both', which='major', labelsize=14)
ax1.set_axisbelow(True)
plt.tight_layout()
# plt.show()
plot_path = os.path.join(plots_dir, plot_filename)
plt.savefig(plot_path, format='tiff')

print(f"{plot_filename} has been saved successfully at {plot_path}.\n")

### Discovered Figshare deposits

plot_filename = f"{todayDate}_extra-figshare-counts.tiff"
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
plt.savefig(plot_path, format='tiff')

print(f"{plot_filename} has been saved successfully at {plot_path}.\n")