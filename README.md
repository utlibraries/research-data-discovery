# Scripted process for retrieving metadata on institutional-affiliated research dataset publications

## Metadata
* *Version*: 1.0.0.
* *Released*: 2025/02/06
* *Author(s)*: Bryan Gee (UT Libraries, University of Texas at Austin; bryan.gee@austin.utexas.edu; ORCID: [0000-0003-4517-3290](https://orcid.org/0000-0003-4517-3290))
* *Contributor(s)*: None

## Purpose
This repository contains Python code that is designed to gather and organize metadata from a number of individual research data repository/platform APIs in order to analyze and summarize research dataset publications that are affiliated with at least one researcher from a particular institution. This code is being developed in the specific context of retrieving data for the University of Texas at Austin and is intended to be eventually be used in tandem with [separate but related work](https://github.com/UT-OSPO/institutional-innovation-grapher) that searches for UT-Austin-affiliated GitHub repositories in order to build a more comprehensive understanding of how researchers on campus are sharing their research outputs. However, this code has been constructed to be stand-alone and can be adapted for use at other institutions.

## Overview

The present script makes use of four REST APIs: [DataCite](https://support.datacite.org/docs/api); [Dataverse](https://guides.dataverse.org/en/latest/api/index.html); [Dryad](https://datadryad.org/stash/api); and [Zenodo](https://developers.zenodo.org/). Note that the Dataverse code is configured specifically for the [Texas Data Repository (TDR)'s](https://dataverse.tdl.org/) instance. Other APIs have been cursorily explored but are not incorporated here: [Crossref](https://www.crossref.org/documentation/retrieve-metadata/rest-api/); [figshare](https://docs.figshare.com/#figshare_documentation_api_description_searching_filtering_and_pagination); [Mendeley Data](https://data.mendeley.com/api/docs/); [OpenAlex](https://docs.openalex.org/how-to-use-the-api/api-overview); and [Open Science Framework (OSF)](https://developer.osf.io/#tag/Filtering). Finally, two semi-static resources have been explored, the [PLOS Open Science Indicators (OSI](https://plos.figshare.com/articles/dataset/PLOS_Open_Science_Indicators/21687686) and the [DataCite Citation Corpus](https://support.datacite.org/docs/data-citation-corpus); neither is incorporated into the workflow provided in the initial release.

The code is designed to maximize the potential retrieval scope of a given API query, specifically as it relates to the fields in which an affiliation may be found (which is not always the 'affiliation' field) and the various permutations for UT Austin specifically (e.g., 'University of Texas at Austin' versus 'University of Texas, Austin'). Even though the three repositories that are integrated into the script (Dataverse, Dryad, Zenodo) all mint their DOIs through DataCite and should thus be discoverable collectively through the DataCite API, the individual repository APIs were queried as both a cross-validation process and an exploration of whether there might be some important variability in metadata cross-walks; an initial inability to perfectly cross-validate all three repositories records in the early stages of this code's development facilitated refinement of the workflow and identified edge-case scenarios. An additional benefit of exploring repository-specific APIs is the potential to identify additional metadata that are not cross-walked to DataCite (possibly because they are not supported in the present schema), such as certain controlled vocabularies.

The present script consists of four major components: 
1) API query construction and calls;
2) Filtering of the JSON response and conversion to a pandas dataframe;
3) Cross-validation checks of the responses from individual repositories' API against their equivalent output as retrieved from the DataCite API; and
4) Concatenation and de-duplication, with the 'original' (specific repository API) source preferred when a dataset was returned by both the repository API and the DataCite API.

The cross-validation step is optional and can be enabled/disabled with a single Boolean variable; if disabled, DataCite will be the exclusive source of retrieved information. De-duplication is necessary regardless of whether cross-validation is implemented or not, primarily due to variable granularity of DOI assignment between repositories.

## Important caveats
The present script collects any item that is labeled as a 'Dataset' in the [DataCite metadata schema](https://datacite-metadata-schema.readthedocs.io/en/4.6/introduction/about-schema/) (for some repositories, this is the only allowable object type). It is a given that not all of these meet the criteria for 'data' proper, in part or in whole, and may constitute other materials like appendices or software; the present script does not attempt to make inferences on the precise nature of content. Conversely, some deposits that do constitute 'data' proper are labeled as another object type (e.g., 'Component,' 'Text'), and these are not presently detected. Retrieving objects through the DataCite API requires downstream filtering, as some objects that labeled as 'datasets' are either individual files within a DOI-backed deposit (common to Dataverse installations) or are versions of the same deposit ([Zenodo, which mints a parent DOI and then a separate DOI for each version](https://zenodo.org/help/versioning)). This script omits individual files that are part of a larger project and restricts the Zenodo deposits to a single record per 'lineage' of deposits. 

There are additional considerations to keep in mind related to how research organize materials within a single project. In some instances, distinct deposits with separate DOIs may in fact be part of the same project (e.g., associated with a single manuscript), and some calculations might wish to further consolidate these to attempt to capture the number of 'unique projects' with at least one dataset. For example, Dataverse has the relatively unique '[dataverse](https://guides.dataverse.org/en/latest/user/dataverse-management.html)' object, a non-DOI-backed structure in which other dataverses and DOI-backed datasets can be nested. For this reason, some researchers will separate the materials for a single manuscript along some logical delineation (e.g., by data format; data vs. software) into multiple DOI-backed deposits that are housed within a single dataverse ([example in TDR](https://dataverse.tdl.org/dataverse/DMD-MLA-01)), whereas if those materials had been deposited in a different repository without an equivalent higher-level structure, they might have been deposited together in one PID-backed deposit. Consolidation along these lines can be done by deduplicating along a stricter combination of attributes on the assumption that related deposits likely have nearly identical metadata (e.g., publication date, author list); it may also be possible to use relations to other objects, if provided (this is more likely to be exclusively recorded in a repository-specific API). The theoretical concept of consolidation that is given above for Dataverse could be accomplished with the Dataverse API since the dataverse in which a dataset is housed can be retrieved, but this would not be possible through the DataCite API since this information does not cross-walk (likely because dataverses do not receive DOIs and an equivalent structure is otherwise rare in other repositories). The present version of the script does not currently consolidate deposits, but given the potential use of the Harvard Dataverse by a large number of non-Harvard researchers, this functionality may be developed in the future.

## Re-use
This script can be freely re-used, re-distributed, and modified in line with the associated [MIT license](https://opensource.org/license/mit). If a re-user is only seeking to replicate a UT-Austin-specific output or to retrieve an equivalent output for a different institution, the script will require very little modification - essentially only the defining of affiliation parameters will be necessary. For other Dataverse-based platforms that have significantly altered the metadata framework, it is possible that additional edits to the API call and subsetting of the response will be necessary. If additional fields or processing of the output are desired, the script will require more substantive modification and knowledge of the specific structure of a given API response. 

### Config file
API keys and numerical API query parameters (e.g., records per page, page limit) are defined in a *config.json* file. The file included in this repository called *config-template.json* should be populated with API keys (if necessary) and renamed. 

### Third-party API access
Users will need to create accounts for [Dataverse](https://guides.dataverse.org/en/latest/api/auth.html) and [Zenodo](https://developers.zenodo.org/) in order to obtain personalized API keys, add those to the *template.env* file, and rename it as *.env*. Note that if you wish to query multiple Dataverse installations (e.g., a non-Harvard institutional dataverse and Harvard Dataverse), you will need to create an account and get a separate API key for each installation. DataCite and Dryad do not require API keys for standard access. 

### Constructing API query parameters
If users need to modify the existing API queries, they should refer to the previously linked API documentation for specific APIs. For targeting a different institution (or set of institutions), users will need to identify a list of possible permutations of the institutional name; the use of of [ROR identifiers](https://ror.org/) in either the DataCite API or most repositories' specific API will fail to retrieve most related deposits because most repositories have not implemented ROR into their platforms given its relatively recent added support in the DataCite schema (Dryad is a notable example as an early adopter of ROR). It may also not be feasible for platforms to retroactively ROR identifiers for all previously published deposits in an efficient programmatic fashion without potentially introducing errors. The optional cross-validation step can facilitate identification of some permutations if querying an API that does not require an exact string match for retrieval based on affiliation. Another approach is to compile a list of known affiliated deposits within and across different repositories and then to examine their metadata in the DataCite API; testing this on some of my own datasets led to the discovery of a lack of recording of affiliation in figshare metadata, for example. A third approach would be to survey affiliated scholarly articles, books, and preprints (e.g., through the Crossref API, which does not require an exact string match for affiliation).

### Test environment
A Boolean variable called *test*, located immediately after the importing of packages, can be used to create a 'test environment.' If this setting is set to TRUE, the script is set to only retrieve 5 pages from the DataCite API (currently a full run requires more than 77 pages with page size of 1,000 records for UT Austin). Currently, the number of records retrieved from the three other APIs utilized here (Dataverse, Dryad, Zenodo) is significantly smaller, so different page limits for a test run are not defined for these (but could be added). 

### Cross-validation
Similar to the test environment, a Boolean value called *crossValidate* (located immediately after *test*) can be used to toggle the cross-validation component on and off (TRUE will retrieve records from other APIs and cross-validate against DataCite). A future version of the script will allow for toggling of the use of the Dataverse API in the cross-validation process.

### Rate limiting
Dataverse installations can impose unique rate limits; for users attempting to retrieve data from the Texas Data Repository, there are currently no rate limits, although a to-be-determined limit is planned in the near future. Harvard Dataverse does have a rate limit, and depending on the volume that you intend to retrieve, you may need to add manual rate limiting.

### Predicted runtime
Exact runtime will vary on local internet speed and external server traffic. Typically, a run of the script without cross-validation (only retrieving from DataCite) should complete in under 12 minutes for UT Austin or an institution of predicted similar research output. If cross-validation is employed, a run should complete in under 20 minutes for UT Austin or an institution of predicted similar research output. The test environment without cross-validation should complete in about 1 minute; if cross-validation is employed, it should complete in about 8-9 minutes. 


## Planned development
The output of the present script represents the results of accessing what are considered to be the most accessible APIs (publicly accessible, permits filtering by institution name). Other repositories that host research datasets, some with appreciable volumes of deposits, are known to exist for which records are not presently returned by DataCite. The most notable are figshare deposits that are automatically generated by certain manuscript submission/publication systems when materials are uploaded as 'supplemental materials' in the submission portal (see [list of partner journals](https://info.figshare.com/working-with/)). Beyond the fact that not all of these materials are 'data' proper, the metadata of deposits created through this mediated process is variable between publishers/journals, they are almost never indexed as 'datasets' (usually as the more generic 'component'), are variably indexed in DataCite or Crossref depending on publishing partner, and almost never no affiliation metadata (a broader characteristic of figshare deposits). Identifying these associated materials will require a customized approach of identifying UT-affiliated articles in partner journals and then attempting to match those against a large corpus of figshare deposits that are retrieved from Crossref and DataCite. The PLOS OSI dataset has been used to begin developing the framework for such an approach. Similar issues in object type labeling of deposits were identified for OSF by [Johnston et al. (2024; *PLOS ONE*)](https://doi.org/10.1371/journal.pone.0302426), and retrieving deposits from this platform will also require a custom approach. Mendeley Data is a third widely used generalist repository that is under-represented in the present workflow's output because it also does not cross-walk affiliation metadata to DataCite. This repository has a more closed API that requires a formal request for access, and it instead recommends the use of [OAI-PMH](https://data.mendeley.com/oai?verb=Identify) for harvesting, which is guaranteed to be comprehensive but inefficient because of the lack of extensive filters, including affiliation. The use of OAI-PMH protocols and large "data dumps" (like the 200 GB [Crossref public data file](https://www.crossref.org/learning/public-data-file/) are under consideration for future incorporation, but a secondary objective of this workflow is to employ code and data sources that are both accessible and computationally tractable for a wide range of potential users who may not have access to above-average storage or computing capacities. Finally, a number of other relatively well-known specialist repositories that were not detected thus far (e.g., Qualitative Data Repository) will be examined in order to determine how best to ensure their retrieval.
