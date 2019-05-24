# OCLC Hadoop Development Project
created by Hugh Jamieson, Dec 9 2018
&copy; 2018 OCLC.org

## Project Purpose
The intended use of this project is to have a set of tasks that can be measured against on-prem _vs_ cloud instances of HBase.

## Project Outline
1. Create a table to hold the data
1. Generate some data
1. write the data to the table
1. run a full-table-scan on the data producing some metrics
1. random edit of records in the table
1. record metrics on editing

## Data Design
* data entities (DE) are synthetic bib entries with accompanying simulated holdings.  This data is purely fictitious and randomly generated.  The DE uses a JSON representation when being transported, but is stored in HBase as columns.
* DE should have title, author/contributor, publication date, and a collection of holdings.  
* The holdings have an institution symbol and a date when the holding was established