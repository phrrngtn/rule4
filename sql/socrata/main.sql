
-- meant to be processed via the sqlean shell (as it has a bunch of extensions built in)
-- together with a -init file that loads in the http0 and inja extensions.

PRAGMA foreign_keys=ON;

-- https://stackoverflow.com/a/76344213/40387
PRAGMA trusted_schema=1;

-- https://www.sqlite.org/cli.html#sql_parameters
.parameter init
-- this is the root of the location in the file-system to which Socrata 
-- artifacts will be downloaded.
.parameter set @socrata_data_root "/data/socrata"

-- see documentation on https://github.com/asg017/sqlite-http
.parameter set @http_rate_limit 100
.parameter set @http_timeout 100000

-- give ample time for each HTTP request
-- I am trying to keep the "policy" information in the sqlite "driver" file


select http_timeout_set(@http_timeout) as "" LIMIT 1;
select http_rate_limit(@http_rate_limit) as "" LIMIT 1;

.read templates.sql

.read resource.sql

.read initialize_seed_data.sql

-- this makes a HTTP request to Socrata to get the list of domains together with a resource-count
-- for each of the domains
.read update_socrata_domain_list.sql
.read upsert_socrata_resources.sql
.read fs_upsert_socrata_resource_view_column.sql
.read fs_upsert_socrata_resource_all_views.sql

-- this is a workaround for a message from the define extension.
-- it may be better to just ignore the diagnostic that comes if this line is *omitted* because
-- if we try and run code that uses define, *after* calling this function, then the define-based code will 
-- fail.
select define_free() as "";
