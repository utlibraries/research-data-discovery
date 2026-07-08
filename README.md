[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](https://opensource.org/licenses/MIT) [![DOI](https://zenodo.org/badge/858925551.svg)](https://doi.org/10.5281/zenodo.18037530) [![arXiv](https://img.shields.io/badge/arXiv-2507.01228-b31b1b.svg?style=flat)](https://arxiv.org/abs/2507.01228) [![JeSLIB article](https://img.shields.io/badge/JeSLIB-10.7191/jeslib.1170-B79E1F.svg?style=flat)](https://doi.org/10.7191/jeslib.1170)

# Scripted process for retrieving metadata on institutional-affiliated research dataset publications

## Metadata
* *Version*: 2.3.1 (**not released to Zenodo**)
* *Released*: 2026/07/08
* *Author(s)*: Bryan Gee (UT Libraries, University of Texas at Austin; bryan.gee@austin.utexas.edu; ORCID: [0000-0003-4517-3290](https://orcid.org/0000-0003-4517-3290))
* *Contributor(s)*: None
* *License*: [MIT](https://opensource.org/license/mit)
* *README last updated*: 2026/07/08

## Table of Contents
1. [Purpose](#purpose)
2. [Setup](#setup)
3. [Organization & file list](#organization--file-list)
4. [Overview](#overview)
5. [Outputs](#outputs)
6. [Planned development](#planned-development)
7. [Version notes](#version--notes)
8. [Disclaimer](#disclaimer)

## Purpose
This repository contains Python code that is designed to gather and organize metadata from a number of individual research data repository/platform APIs in order to analyze and summarize research dataset publications that are affiliated with at least one researcher from a particular institution. This code is being developed in the specific context of retrieving data for the University of Texas at Austin but can be readily adapted for use at other institutions.

## Setup
This project uses [uv](https://docs.astral.sh/uv/) for package management. To get started:

1. Install uv following the [official instructions](https://docs.astral.sh/uv/getting-started/installation/).
2. Clone the repository and navigate to the repo root in a terminal.
3. Run `uv sync` to create a virtual environment and install all dependencies.
4. Open the repo root folder in your IDE (e.g., **File → Open Folder** in VS Code) to ensure the environment is detected automatically (alternatively use the terminal to change directory).

### env file
API keys, numerical API query parameters (e.g., records per page, page limit), and other lists/dictionaries are defined in `env.json`; the file is JSON-formatted to allow for nesting of the many parameters. The file included in this repository called `env-template.json` should be populated with API keys (see below) and renamed to `env.json` before running any scripts. If running this for another institution, you will need to do some additional customization for your institution.

### Third-party API access
Users will need to create accounts for [Dataverse](https://guides.dataverse.org/en/latest/api/auth.html) and [Zenodo](https://developers.zenodo.org/) in order to obtain personalized API keys, add those to `env-template.json`, and rename it as `env.json`. If you wish to query multiple Dataverse installations (e.g., a non-Harvard institutional dataverse and Harvard Dataverse), you will need to get a key for each installation. Crossref, DataCite, Dryad, Figshare, and OpenAlex do not require API keys for standard access. Some APIs impose rate limiting (e.g., [Dryad](https://datadryad.org/api); [OpenAlex](https://help.openalex.org/hc/en-us/articles/24397762024087-Pricing); [Zenodo](https://developers.zenodo.org/#rate-limiting)). Zenodo also restricts the total number of records that can be retrieved with one query to 10,000. Dataverse installations may or may not have rate limits.

### Adaptation to another institution
To use the workflow in its current state, but for another institution, users should do the following:
1. Modify institution-specific information in `env.json`. This is mainly under *INSTITUTIONS*, *PERMUTATIONS*, AND *PERMUTATIONS_IDENTIFIED*. *INSTITUTIONS* contains several fields; the only one that needs to be use a controlled vocabulary is the ROR field. You can short-hand/represent the others as you wish. *PERMUTATIONS* should contain as many permutations as you can think of that would reasonably occur. As DataCite has a limit on how many can be queried in one call (something like 36, I think), you should still to realistic ones and avoid highly granular ones (e.g., with departmental information). The wild-carding implemented in December 2025 has somewhat reduced the need for comprehensive permutations, but abbreviations are still important (e.g., 'UT Austin' and 'University of Texas at Austin'). *PERMUTATIONS_IDENTIFIED* should be the official institution name.
2. Provide user-specific information in `env.json`. This includes at least your email (for making polite API calls). Get your own API tokens if you will be doing cross-validation (this is recommended for a first run in order to identify additional permutations of the institution's name). If you want to cross-validate with a Dataverse repository, you will need to change *url_dataverse* to the target one. For any Dataverse that is NOT multi-institutional, the *subtree* parameter can be removed as well.
3. Run and refine. You will probably want to run in the test env first just to make sure things are working as expected (see below). Then you would want to do a production run with *cross_validate* set to 'true' to identify more permutations and check the outputs for institution-specific things like repository names that should be standardized (this is *REPOSITORY_MAPPING* in `env.json`).

### Test environment
A Boolean variable called *test*, located in the env file, can be used to create a 'test environment.' If set to *true*, the script retrieves only a few pages from the DataCite or Crossref APIs (the largest sources of metadata). 

### Estimated runtime
Typically, a run of the primary workflow without cross-validation or any of the secondary Figshare workflows should complete in under 20 minutes. If cross-validation is employed, a run should complete in under 25 minutes. The test environment without cross-validation should complete in about 1 minute; if cross-validation is employed, it should complete in about 8-9 minutes. Adding one of the Figshare workflows can significantly increase the runtime; significant variation between institutions is expected. Typical runtime of the combined main workflow, first Figshare workflow (DataCite + OpenAlex), and NCBI step is around 40-55 minutes at present. The NCBI workflow itself should complete in 30-40 seconds.

## Organization & file list

### Root directory

| File | Description |
|------|-------------|
| `dataset-records-retrieval.py` | Primary script for records retrieval via the DataCite API. Includes functionality for using other APIs to identify Figshare deposits without affiliation metadata that can be connected to an article with an author from the focal institution. |
| `env-template.json` | env file with most parameters. Should be populated with institution-specific information and renamed as `env.json`. For some fields, the UT Austin-specific information is included in the template as a model of how information should be formatted. |
| `journal-list.json` | Contains the official journal names and ISSNs to be queried as part of one of the possible Figshare workflows (construction of a hypothetical SI DOI and testing its existence). This file contains all PLOS titles as an example but could be expanded to any other journal that appends '.s00x' to the article DOI for linked Figshare deposits. |
| `data-dictionary.csv` | Describes the columns that are contained in each output and accessory-output file. Export of some files is commented out at present, and columns for those files are not defined here. |

### accessory-scripts/

| File | Description |
|------|-------------|
| `dataset-records-retrieval-visualization.py` | Generates visuals for conference presentations and manuscripts. |
| `plos-osi-search.py` | Retrieves the latest version of the [PLOS Open Science Indicators (OSI) Dataset](https://plos.Figshare.com/articles/dataset/PLOS_Open_Science_Indicators/21687686), identifies articles that list data as having been shared in part or in whole through Supplemental Information (mediated Figshare deposit for all PLOS titles), retrieves a list of PLOS articles with at least one author from a focal institution, and searches for DOI matches. It does the same for affiliated articles that link to NCBI deposit. |
| `plos-webscraping-supp-info.py` | Retrieves a list of affiliated PLOS articles and scrapes their webpages, targeting the consistently-formatted SI section that contains metadata on the files that are hosted on Figshare. As of this release, this is permissible under the [PLOS text scraping policy](https://api.plos.org/text-and-data-mining.html), but re-users should ensure that these conditions have not changed. |
| `datacite-ror-query.py` | Contains a trimmed version of the main workflow and uses the ROR identifier (specified in `env.json`) to search for affiliated datasets. The only purpose for this script is to quantify the degree to which a ROR-based query will result in an incomplete retrieval due to lack of widespread adoption of ROR. |
| `datacite-figshare-partner-query.py` | Adaptation of one of the secondary Figshare workflows that identifies journal-mediated, DataCite-minted deposits without affiliation metadata and attempts to connect them to articles that were (co)authored by a researcher at a focal institution. The main workflow's version loops through publishers and searches for specific resource types. In this version, only one publisher is queried but for every resource type; this is restricted to the past few years to keep the scale of the retrieval manageable. The script runs the same cross-matching of DataCite-minted deposits against a list of affiliated articles retrieved through OpenAlex. The purpose of this workflow is to explore whether some objects not labeled as 'dataset' might contain data and whether some objects labeled as 'dataset' might not be data. **The use of file formats to attempt to predict the 'data' nature of an object is still very preliminary.** |
| `datacite-figshare-partner-query_metadata-only.py` | Retrieve the automated metadata summary facets that can be returned from the DataCite API (e.g., comparison of resource type counts). Useful for rapid summaries when retrieving all pages of data would be an intensive query process due to the number of records. |
| `crossref-query.py` | Conducts a general institution-based query to the Crossref REST API. It is separated from the primary workflow based on the results for UT Austin (100,000's of records, most of which have nothing to do with UT Austin), which indicate that this does not need to be run as frequently as a DataCite query and could instead have a recently generated output file pulled in to concatenate with the primary workflow's output. |
| `figshare-deposits-linked-articles.py` | Takes a dataframe of Figshare deposits and queries each one in the DataCite API to look for any object where the dataset is listed as being 'IsSupplementTo'. The presumed related article DOIs are passed into the Crossref API to identify which journals/publishers are associated with these deposits. This process applies to both datasets listed with 'Figshare' as the publisher in the DataCite metadata and datasets listed with a publisher partner like 'Taylor & Francis.' |
| `figshare-deposits-additional-metadata.py` | Takes a dataframe of Figshare deposits and queries each one in the Figshare API to obtain additional metadata that is not crosswalked to DataCite. |
| `accessory-data/20250310-mediated-figshare-metadata-summary.csv` | Contains a manually compiled summary of select metadata for Figshare deposits mediated through [publisher partners](https://info.Figshare.com/working-with/)(filter on 'Publishers'); it is intended to provide insight into possible filter parameters that may permit their programmatic retrieval. This is a static file created on 2025/03/10, and partners/metadata may change in the future (e.g., SciELO journals was listed the last time I examined this in October 2024). Briefly, I accessed each publisher's Figshare collection through the web interface and selected 10 random deposits, with preference given to recent deposits. A few listed publishers are not recorded in the CSV file: JACC and SAGE redirect to the publishers' homepage, not a Figshare collection; Human Genome Variation is a database; and IEEE Standards, Medical Affairs Professional Society, Optica Open, and Physiome appeared to contain out-of-scope topic (e.g., only preprints in Optica Open). I recorded which indexer (DataCite vs. Crossref) was used to mint the DOI; what the listed publisher name is (*listed_publisher*); the *client-id* and *provider-id* if minted through DataCite; up to 10 DOIs that were examined; whether the DOIs contain the string 'Figshare' (*doi_figshare*); and how the DOIs were constructed (*doi_construction*). |

## Overview
### Primary workflow
`dataset-records-retrieval.py` uses four REST APIs for a large-scale initial sweep for university-affiliated datasets based on a set of permutations of the institutional name: [DataCite](https://support.datacite.org/docs/api); [Dataverse](https://guides.dataverse.org/en/latest/api/index.html); [Dryad](https://datadryad.org/stash/api); and [Zenodo](https://developers.zenodo.org/). The Dataverse code is configured for the [Texas Data Repository (TDR)'s](https://dataverse.tdl.org/) instance. Other APIs have been incorporated into secondary workflows or accessory scripts: [Crossref](https://www.crossref.org/documentation/retrieve-metadata/rest-api/); [Figshare](https://docs.Figshare.com/#figshare_documentation_api_description_searching_filtering_and_pagination); and [OpenAlex](https://docs.openalex.org/how-to-use-the-api/api-overview). Finally, some APIs have been explored but are not currently incorporated: [Mendeley Data](https://data.mendeley.com/api/docs/); and [Open Science Framework (OSF)](https://developer.osf.io/#tag/Filtering). 

The use of many APIs, search terms (e.g., institutional permutations), and target fields (e.g., creator vs. contributor) is designed to maximize the retrieval scope. However, institution-/repository-specific exploration will always be necessary in order to identify unusual metadata and their crosswalks.

The primary script consists of four major components: 
1) API query construction and calls;
2) Filtering of the JSON response and conversion to a pandas dataframe;
3) Cross-validation checks of the responses from individual repositories' API against their equivalent output as retrieved from the DataCite API; and
4) Concatenation and de-duplication, with the 'original' (specific repository API) source preferred when a dataset was returned by both the repository API and the DataCite API.

The cross-validation step is optional and can be enabled/disabled with a Boolean variable in `env.json`; if disabled, DataCite will be the exclusive source of retrieved information. De-duplication is necessary regardless of whether cross-validation is implemented or not due to variable granularity of DOI assignment between repositories. The current process also handles 'double-minting' of DOIs for one deposit (e.g., Zenodo) and includes a toggle to de-duplicate Dataverse deposits that have the same list of authors and affiliations, the same publication date, and the same rights/licensing. The latter accounts for oversplit materials from one manuscript between multiple DOI-backed datasets, all nested under a non-DOI-backed dataverse (see [ticket](https://github.com/utlibraries/research-data-discovery/issues/7)). 

### Secondary workflows
Early testing led to development of targeted secondary workflows that attempt to fill gaps (e.g., paucity of Figshare deposits). Most secondary workflows currently target Figshare and are contained in external accessory scripts. `dataset-records-retrieval.py` contains two secondary Figshare workflows that can be toggled on or off with Boolean variables in `env.json`. 

1. The first workflow (*figshareWorkflow1*) takes advantage of the fact that for many partner journals, mediated Figshare deposits are listed with the publisher in the 'publisher' metadata field, rather than 'Figshare.' This workflow retrieves all datasets with a publisher listing like 'Taylor & Francis' from DataCite, retrieves university-affiliated articles published by that same publisher from OpenAlex, and looks for matches. Not all mediated Figshare objects labeled as 'dataset' are datasets, and not all objects containing 'data proper' will be labeled as 'dataset'; the resource type assignment is usually system-generated. 

2. The second workflow (*figshareWorkflow2*) takes advantage of a different configuration in certain journals in which mediated Figshare deposits are minted through Crossref with a DOI that appends '.s00x' (or sometimes '.t00x') to the end of the associated article DOI where 'x' is a sequential number. This workflow retrieves all university-affiliated articles from a publisher that does this (e.g., PLOS) via `journal-list.json`, constructs a hypothetical Figshare DOI by adding '.s001' to the article DOI, and tests whether that link exists. This only establishes that there is a Figshare deposit - this may not be classified as a 'dataset'. 

`dataset-records-retrieval.py` also contains a secondary workflow for NCBI, which does not use digital PIDs, instead issuing collection/accession/project IDs that while persistent, do not have a persistent-resolving URL. There is no API specifically designed for institutional records retrieval, but the Entrez system can be queried through various modules by searching for an affiliation string. This workflow specifically uses the *biopython* module and looks for BioProjects, which are considered the most equivalent to a 'dataset'-level object.

## Outputs
See [output-dictionary.csv](output-dictionary.csv) for a description of output files.
See [data-dictionary.csv](data-dictionary.csv) for a description of column headers.

## Planned development
Product development ideas/plans are listed as '[Issues](https://github.com/utlibraries/research-data-discovery/issues)'. The projected timelines are listed in a linked [Project](https://github.com/orgs/utlibraries/projects/3). 

### Questions / comments
(Potential) re-users should feel free to create an issue if there is a bug or feature request. These can alternatively be directed to the UT Libraries Research Data Services team that developed this tool by sending an email to utl-rds@austin.utexas.edu. For conceptual understanding of the process, please refer to the preprint or published article.

## Version notes

See [CHANGELOG.md](CHANGELOG.md) for full version history.

## Disclaimer
This workflow is, and likely will always be, perpetually under development. Because of the marked heterogeneity in how datasets are shared (e.g., lack of persistent identifiers; use of identifiers other than DOIs; variation in affiliation metadata), it is practically assured that not all datasets will be captured by this workflow or any other and that substantial gaps may exist for certain platforms/avenues for data sharing. Reusers should be cognizant of these limitations in determining how data gained from this workflow may inform decision-making. The creator(s) and contributor(s) of this repository and any entities to which they are affiliated are not responsible for any decisions, policies, or other actions that are made on the basis of obtained data.