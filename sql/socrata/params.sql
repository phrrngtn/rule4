
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
