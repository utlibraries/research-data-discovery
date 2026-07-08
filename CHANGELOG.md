# Changelog

The current version scheme follows a MAJOR.MINOR.PATCH format, with a 'major' change involving added functionality or significant revisions to the workflow; a 'minor' change involving addition of accessory files or minor revisions to the workflow (e.g., refactoring); and a 'patch' is a bug fix.

## 2.3.0
Implements *uv* for package management, switches the name of the `config.json` file to `env.json`, adds in some additional specifications for encoding of CSV outputs to preserve uncommon punctuation, makes some minor tweaks to account for changes in API structure/output, and begins restructuring the README.

## 2.2.0
Makes minor, mostly non-functional syntax changes for code standardization across nearly all scripts. This primarily includes more consistent casing of variables and the externalization of common functions into a `utils.py` file. Certain functions that are unique to a script are retained internally. All scripts have been tested with at least a 'test' run to ensure that the call to this utilities file is working correctly. Some additional cleaning steps specific to entries retrieved for UT Austin have been added to the primary script (standardizing repository listings). Minor edits to more functional code have been applied to handle minor changes to fields in the Figshare API and to the structure of the PLOS OSI dataset in the most recent version. The corresponding Jupyter notebook for the primary workflow has also been updated. The README now links to the published JeSLIB article.

## 2.1.0
Makes minor, mostly non-functional syntax changes for code standardization in the core script and the data visualization script. Some minor bugs (not all found in the previous version) were fixed. Some metadata fields were added or modified in the output files, and additional cleaning steps have been added for UT-specific records. Logging capacity has now been introduced.

## 2.0.0
Identical to version **1.2.1** and is only labeled as such for alignment with Zenodo releases (i.e. it is considered a major change from the initial release that was ported to Zenodo).

## 1.2.1
Involves refactoring of some code and some new functionality. The `dataset-records-retrieval.py` has been modified to handle when the cross-validation process is enabled but the retrieval from one specific repository's API fails (this seems to happen not infrequently with Zenodo's) to allow the script to continue. A bug fix has also been applied related to querying of the OpenAlex API as part of the secondary Figshare workflow (failed retrieval of Taylor & Francis records, failure to retrieve publication year, de-duplication of initial records). New functionality has also been added here to generate a dataframe of all unique author names, with select columns combined in order to get, for example, a total count of datasets. Note that this relies on exact matching of author names, so common names are more likely to represent multiple individuals, and the same individual may be represented by multiple entries (e.g., Last, First format vs. First Last; inclusion of middle initial). Additional functionality has also been added to begin logging script runs in two forms, a text file unique to each run and a composite CSV file that appends all logs. Both record the same information (e.g., timestamp, runtime, certain parameters from the env file). Additional work on logging is planned.

## 1.1.1
Makes a few minor modifications to accessory scripts and some more sizable modifications to the primary `dataset-records-retrieval.py` script. The `crossref-query.py` script was updated to add additional fields for concatenation with the DataCite output (most are 'filler' uniform values as no equivalent field exists); the script was also modified to have a toggle for enabling or disabling a filter for a particular resource type (see [issue #82](https://github.com/utlibraries/research-data-discovery/issues/82)). The `datacite-ror-query.py` script was updated to fix a bug in which some Figshare deposits were inadvertently being removed from the dataframe after consolidation. The `dataset-records-retrieval-visualization.py` script was updated in response to feedback from peer review of the manuscript; several plots have been slightly altered or rearranged. The `config-template.json` file has been updated to reflect the externalization of toggles for the primary `dataset-records-retrieval.py` script and the addition of a list of compressed file formats in order to identify datasets with such files (if file information is provided). The changes to the primary `dataset-records-retrieval.py` script include:
* externalizing toggles to the `config.json` file
* fixing a bug in which certain metadata assessments were contingent on running certain (optional) steps
* increasing robusticity to handle author information retrieved from the DataCite API
* setting all CSVs to be exported in UTF-8 format specifically
* increasing metadata subsetting from the API response and exporting in the most common dfs to improve efficiency of internal reporting for UT Austin (this will likely lead some files to be deprecated in the future)
* harmonizing metadata fields between different dfs for concatenation

## 1.0.1
Adds some minor functionality to more clearly indicate what resource type(s) were queried in a DataCite call to output filenames and adds a generalist/specialist categorization based on the repository a deposit is in. It also fixes some issues with toggles to enable/disable certain parts of the workflow.

## 1.0.0
The first release that is also synced to Zenodo. Makes a few minor adjustments for refactoring the primary workflow and some of the accessory workflows. The most substantive changes are made to the `dataset-records-retrieval-visualization.py` and the `accessory-scripts/preprint-rads-dataset-reanalysis.py` files in preparation for the preprint. The `accessory-scripts/plos-webscraping-supp-info.py` file is newly added; see file list for description. Some very minor edits are made to a few files: (1) the license is updated to indicate the copyright holder is the UT Board of Regents (https://www.utsystem.edu/board-of-regents/rules/90101-intellectual-property); (2) the `accessory-scripts/crossref-query.py` file is updated to account for changes to Authorea deposit metadata; (3) some annotations are updated in the `accessory-scripts/preprint-dryad-date-comparison.py` file; and (4) some additional file export lines are added to the `dataset-records-retrieval.py` (and Jupyter) files. The versioning is reset to synchronize with planned periodic release on Zenodo.

---

*Previous version history notes are available in older README files with the older version system before it was reset.*