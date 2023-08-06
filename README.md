# rule4
Code snippets on the topic of Codd's Rule 4

SQLite extensions used by the project
=====================================

Use https://github.com/nalgeon/sqlean as the SQLite CLI as it has a bunch of extensions built in.

use https://github.com/asg017/sqlite-http for HTTP access


https://github.com/phrrngtn/sqlite-template-inja for templating. This is one that I wrote and I have not made any binaries available for download so you have to build it yourself. This might be cumbersome.

https://github.com/phrrngtn/sqlite-embedded-odbc for scraping catalog definitions of anything for which you have an ODBC driver and knowledge of the catalog layout.

What does/will this do?
=======================

Use SQLite as a flexi-database for storing relational representation of database structure in such a way as to support:
 * search by name
 * versioning: see how the structure evolves over time. Maybe generate change scripts
 * data-type detection: mostly numeric but with funny representation of nulls? 
 * candidate key identification
 * detect normalization violations
 * data thumbprint: get an idea of what the data looks like by using histograms
 * sampling/downloading/transformation
 * search by shape of data: if you have a sample of data, see if you can find sources that are similar in structure.

 We use SQLite fts5 (for search) and trigger-maintained backlogs (for versioning); 
  * http0 for HTTP requests
  * fileio extension for file-system reading/writing/listing
  * ODBC for accessing remote databases

The 'rule4' part comes in through seeing just how far we can push this idea of doing stuff in a relational manner. Since most database *definition* stuff is done via Data Definition *Language*, there is a lot of trickery going on
to give a metadata-centric view ... think of it as akin to *insertable catalog tables*. However, rather than 
work at the facade level (e.g. by INSTEAD OF triggers), a lot of the intermediate steps are shown explicitly e.g. 
insert data into a regular table and then use a virtual table to access that table for code-generation purposes. 
This is why the Inja templating is such an integral part of the project. Rather than come up with a strict relational schema for everything up front, we make a lot of use of JSON to tunnel tabular values as scalars (SQLite does not support table-valued parameters but it is trivial to pack them into a JSON value using JSON_OBJECT and JSON_ARRAY_AGG). The internal schema of the rule4 support tables are not at all normalized.

Socrata
=======
There is basic support for:
 * domains and resource-counts
 * relational schema for metadata for resource, view and column information
 * generating URLs to retrieve resources