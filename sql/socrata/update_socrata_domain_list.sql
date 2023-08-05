-- we copy in the table structure but none of the data
--ATTACH DATABASE ':memory:' as socrata_tempdb;
-- XXXX: we seem to have major badness with response_body if
-- we don't write it out to the filesystem?!!! 
-- wonder if the blobbiness of the column is signficant 
-- and it is being lost in the creation of the temp table?
CREATE TEMP TABLE IF NOT EXISTS temp_http_request AS
SELECT *
FROM http_request
where 1 = 0;


PRAGMA trusted_schema = 1;

-- give ample time for each HTTP request
select http_timeout_set(25000);
select http_rate_limit(100);
/*
 
 This should not need to be run all that often. It may be useful to have an estimate of 
 refresh intervals for each of the the query templates and to record the timestamp of the last update (over
 and above what is available from the http timings fields)
 */
WITH U(url, url_template_family, url_template_name) AS (
    -- this looks a bit weird as their is no actual templating going
    -- on as the Socrata URL to get all domains and their resource count, is not 
    -- actually parameterized. However, to make the CTE look like the other queries
    -- we query from the url_template
    SELECT url_template as url, -- see note above.
    family as url_template_family,
    name as url_template_name
    FROM url_template
    WHERE family = 'SOCRATA'
        and name = 'all_domains'
) -- so we do the HTTP request which retrieves the contents and saves the response and
   --  all of the headers in the database.
INSERT INTO temp_http_request(
        request_url,
        request_method,
        request_headers,
        request_cookies,
        request_body,
        response_status,
        response_status_code,
        response_headers,
        response_cookies,
        response_body,
        remote_address,
        timings,
        meta,
        url_template_family,
        url_template_name
    )
SELECT H.request_url,
    H.request_method,
    H.request_headers,
    H.request_cookies,
    H.request_body,
    H.response_status,
    H.response_status_code,
    H.response_headers,
    H.response_cookies,
    H.response_body,
    H.remote_address,
    H.timings,
    H.meta,
    U.url_template_family,
    U.url_template_name
FROM http_get(U.url) AS H,
    U;



-- socrata_tempdb.http_request should be a single-row table
WITH T(domain, reported_resource_count) AS (
    select E.value->>'$.domain' as domain,
        E.value->>'$.count' as reported_resource_count
    FROM temp_http_request as b,
        JSON_EACH(b.response_body, '$.results') as E
)
-- the 'domain' table has a trigger-maintaine temporal backlog
-- associated with it so we don't have to do anything explicit
-- here in this query other than try to reduce/eliminate value-equivalent
-- updated (i.e. where you update a row with the same value as it already had
-- and thus spam the backlog). Note that there is an assumption that 
-- resource_count will always go up. It may be the case that if a domain had
-- n resources and then deleted 1 and added one then it would look to this sample
-- as if the number of resources was unchanged and thus no chage would be made to the
-- domain table so we would not bother refreshing the resource and resource_column lists
insert into domain(domain, resource_count)
select domain,
    reported_resource_count as resource_count
from T
WHERE true ON CONFLICT(domain) DO -- https://www.sqlite.org/lang_upsert.html
UPDATE
SET resource_count = excluded.resource_count
WHERE COALESCE(resource_count, '') != COALESCE(excluded.resource_count,'');