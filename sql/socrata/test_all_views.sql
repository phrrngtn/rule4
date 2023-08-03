


-- TODO: generate FTS and backlog

/*


Can generate a CSV of the dataset by rollowing the steps below. The CSV format should work with 
Google sheet import

C:\work\rule4\sql\socrata>\tools\sqlean.exe --csv c:\data\soc.db3
SQLite version 3.42.0 2023-05-16 12:36:15
Enter ".help" for usage hints.
sqlean> .headers on
sqlean> .output resource_all_views.csv
sqlean> select rav.domain, rav.resource_id, rav.name, rav.asset_type, rav.category, rav.provenance,rav.display_type, rav.description, rav.publication_date FROM resource_all_views as rav;
sqlean> .output

*/


PRAGMA trusted_schema = 1;

CREATE TEMP TABLE IF NOT EXISTS temp_http_request AS
SELECT *
FROM http_request
where 1 = 0;

-- we may do this differently it we want requests to stay around for 
-- debugging purposes.
delete from temp_http_request;

-- give ample time for each HTTP request

select http_timeout_set(100000) as "" LIMIT 0;

select http_rate_limit(100) as "" LIMIT 0;


WITH T(url_template_family, url_template_name, socrata_template, local_path_template) AS (
     SELECT s.family as url_template_family,
            s.name   as url_template_name,
            s.url_template as socrata_template,
            s1.url_template as local_path_template
         FROM url_template  as s
         JOIN url_template as s1
         ON (s.family='SOCRATA' and s1.family = 'SOCRATA_PATH'
             and s1.name = s.name)
         WHERE s.name = 'all_views'
    ), U AS (
    SELECT d.domain,
           T.url_template_family,
           T.url_template_name,
           template_render(T.socrata_template,
                           JSON_object('domain', d.domain)) as url,
           template_render(T.local_path_template,
                           json_object('workspace_root', '/data/socrata', 'domain', d.domain)) as path
    FROM domain as d
    LEFT OUTER JOIN socrata_domain_of_interest as doi
    ON (doi.domain = d.domain),
       T
    where d.resource_count < 20000 -- seems to be unreliable over this number
    and d.resource_count > 0
    ),MOST_RECENT AS (
            -- the _td_bl_domain table is the temporal (td) backlog (bl) trigger-maintained
            -- table for the domain table. We find the max timestamp from there for each domain
            -- we will use that as a check against our local file-sytem copy of the 
            SELECT U.domain,
                   max(bl.ts) as ts
            FROM U JOIN _td_bl_domain as bl
              ON (u.domain = bl.domain)
              WHERE operation <> 'D'
              GROUP BY U.domain
    ), STALE AS (
        SELECT U.*, mr.ts,cache_stat.mtime
        FROM U
        -- this JOIN means that there *must* be an existing backlog record
        -- consider if we can use a LOJ
        JOIN MOST_RECENT as mr 
          ON (U.domain = mr.domain)
        -- the LOJ is needed.
        LEFT OUTER JOIN  lsdir(U.path) as cache_stat
        WHERE mr.ts > COALESCE(cache_stat.mtime,0)
        ORDER BY random(), mr.ts ASC
        LIMIT 5    -- this may need to be run several times if starting cold
    )  

--    Select * FROM STALE;
--select domain, ts, mtime FROM STALE;

INSERT INTO temp_http_request(
        local_path_response_body,
        response_body_bytes_written,
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
SELECT 
    S.path,
    fileio_write(S.path, H.response_body),
    -- https://github.com/asg017/sqlite-http/issues/27
    -- can't use the response_body multiple times.
    -- XXXX: this seems important to avoid multiple, parallel HTTP connections
    H.request_url,
    H.request_method,
    H.request_headers,
    H.request_cookies,
    H.request_body,
    H.response_status,
    H.response_status_code,
    H.response_headers,
    H.response_cookies,
    NULL,
    -- XXX: this may be "waving dead chickens". Trying to avoid problems with 30 or so parallel HTTP connections
    H.remote_address,
    H.timings,
    H.meta,
    S.url_template_family,
    S.url_template_name
FROM STALE AS S
    CROSS JOIN  http_get(S.url) AS H
WHERE H.response_status_code  between 200 and 299;


WITH T AS (
    select 
        E.value->>'$.domainName' as domain,
        E.value->>'$.id' as resource_id,
        E.value->>'$.name' as [name],
        E.value->>'$.assetType' as [asset_type],
        E.value->>'$.category' as category,
        E.value->>'$.description' as [description],
        E.value->>'$.displayType' as [display_type],
        E.value->>'$.provenance' as [provenance],
        E.value->>'$.publicationDate' as publication_date,
        E.value->>'$.viewLastModified' as view_last_modified,
        E.value  as [resource]
        FROM temp_http_request as b, -- maybe should read the list of files from the fs?
        -- socrata_tempdb.http_request as b,
        JSON_EACH(
            fileio_read(b.local_path_response_body)
        ) as E 
)
INSERT INTO resource_all_views(
    domain,
resource_id,
name,
asset_type,
category,
description,
display_type,
provenance,
publication_date,
view_last_modified,
resource
)
select  T.domain,
    T.resource_id,
    T.name,
    T.asset_type,
    T.category,
    T.description,
    T.display_type,
    T.provenance,
    T.publication_date,
    T.view_last_modified,
    T.resource
FROM T
WHERE true ON CONFLICT(resource_id) DO
UPDATE
SET [domain]=excluded.[domain],
    [name] = excluded.name,
    [description]=excluded.[description],
    [asset_type]=excluded.[asset_type],
    [category]=excluded.[category],
    [provenance]=excluded.[provenance],
    [publication_date]=excluded.[publication_date],
    [view_last_modified]=excluded.[view_last_modified],
    [resource]=excluded.[resource]
WHERE NOT 
    (
        COALESCE(domain, '') = COALESCE(excluded.domain,'')
    AND COALESCE(name,'') = COALESCE(excluded.name,'')
    AND COALESCE(description, '') = COALESCE(excluded.description,'')
    AND COALESCE(asset_type,'') = COALESCE(excluded.asset_type,'')
    AND COALESCE(category, '') = COALESCE(excluded.category, '')
    AND COALESCE(provenance, '') = COALESCE(excluded.provenance,'')
    AND COALESCE(publication_date,'') = COALESCE(excluded.publication_date, '')
    AND COALESCE(view_last_modified,'') = COALESCE(excluded.view_last_modified, '')
    );
