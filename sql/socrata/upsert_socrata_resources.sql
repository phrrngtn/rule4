

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

-- we may do this differently it we want requests to stay around for 
-- debugging purposes.
delete from temp_http_request;

-- give ample time for each HTTP request
-- we cannot use LIMIT 0 as it seems to supress the function call in the first place?
select http_timeout_set(@http_timeout) as "" LIMIT 1;
select http_rate_limit(@http_rate_limit) as "" LIMIT 1;



-- select 'hi', time_t_ms();

WITH T(url_template_family, url_template_name, socrata_template, local_path_template) AS (
    -- This is a common pattern: we want a URL and a PATH template paired up (since we are going
    -- to retrieve a URL and write the contents locally to a file). This code seems a bit cumbersome
    -- and I think it could be factored out a bit better, perhaps as a virtual table/tvf
     SELECT s.family as url_template_family,
            s.name   as url_template_name,
            s.url_template as socrata_template,
            s1.url_template as local_path_template
         FROM url_template  as s
         JOIN url_template as s1
         ON (s.family='SOCRATA' and s1.family = 'SOCRATA_PATH'
             and s1.name = s.name)
         WHERE s.name = 'resources'
    ), U AS (
    -- likewise, this is where we fill out the templates with some data from a local table.
    -- we could factor this out into a virtual table and perhaps take in 'socrata_domain_of_interest' as 
    -- a JSON parameter (in the spirit of separating out the stuff you make up from the stuff you can't 
    -- make up)
    SELECT d.domain,
           T.url_template_family,
           T.url_template_name,
           template_render(T.socrata_template,
                           JSON_object('domain', d.domain, 'resource_count', d.resource_count)
                           ) as url,
           template_render(T.local_path_template,
                           json_object('workspace_root', @socrata_data_root, 'domain', d.domain)
                            ) as path
    FROM domain as d
    JOIN socrata_domain_of_interest as doi
    ON (doi.domain = d.domain),
       T
    where d.resource_count < 20000 -- seems to be unreliable over this number
                                    -- although that may have been due to problems with timeouts and 
                                    -- http0 misconfiguration on my part.
    and d.resource_count > 0 -- definitely do not want to attempt to process bogus domains.
    ),MOST_RECENT AS (
            -- the _td_bl_domain table is the temporal (td) backlog (bl) trigger-maintained
            -- table for the domain table. We find the max timestamp from there for each domain
            -- we will use that as a check against our local file-system copy of the resource blob
            -- 
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
        LEFT OUTER JOIN  fileio_ls(U.path) as cache_stat
        WHERE mr.ts > COALESCE(cache_stat.mtime,0)
        ORDER BY  mr.ts ASC
        LIMIT 100 -- this may need to be run several times if starting cold
    )  

--select * FROM STALE;
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
FROM STALE AS S  -- not sure if putting this first helps avoid problem with poor performance
    LEFT OUTER JOIN http_get(S.url) AS H;

    -- TODO: check HTTP status codes
    -- TODO: maybe roll some of the HTTP requests from the temp table to a more durable one (for auditing/performance
    -- purposes)
    

-- select 'bye', time_t_ms();


select 'DONE with retrieval of resources' as "";
WITH T AS (
    select json_extract(
            E.value,
            '$.metadata.domain',
            '$.resource.id',
            '$.resource.name',
            '$.resource.description',
            '$.permalink',
            '$.link'
        ) as flat_row,
        E.value ->> '$.resource.updatedAt' as updated_at,
        E.value ->> '$.resource.createdAt' as created_at,
        E.value ->> '$.resource.metadata_updated_at' as metadata_updated_at,
        E.value ->> '$.resource.data_updated_at' as data_updated_at,
        E.value->'$.metadata'       as metadata,
        E.value->'$.owner'          as [owner],
        E.value->'$.creator'        as [creator],
        E.value->'$.classification' as classification,
        E.value->'$.resource'       as [resource]
    FROM temp_http_request as b, -- maybe should read the list of files from the fs?
        -- socrata_tempdb.http_request as b,
        JSON_EACH(
            fileio_read(b.local_path_response_body),
            '$.results'
        ) as E --WHERE b.request_url like 'https://api.us.socrata.com/api/catalog/v1%'
),
_RESOURCE AS (
    SELECT T.flat_row->>0 as domain,
        T.flat_row->>1 as resource_id,
        T.flat_row->>2 as [name],
        T.flat_row->>3 as [description],
        T.flat_row->>4 as permalink,
        T.updated_at            as updated_at,
        T.created_at            as created_at,
        T.metadata_updated_at   as metadata_updated_at,
        T.data_updated_at       as data_updated_at,
        T.metadata as metadata,
        T.owner as [owner],
        T.creator as creator,
        T.classification as classification,
        T.resource as [resource]
    FROM T
)
INSERT INTO [resource_tabular] (
        domain,
        resource_id,
        [name],
        [description],
        permalink,
        updated_at,
        created_at,
        metadata_updated_at,
        data_updated_at,
        metadata,
        [owner],
        creator,
        classification,
        [resource]
    )
SELECT [domain],
    [resource_id],
    [name],
    [description],
    [permalink],    
    COALESCE(unixepoch(datetime(updated_at)), updated_at) as updated_at,
    COALESCE(unixepoch(datetime(created_at)), created_at) as created_at,
    COALESCE(unixepoch(datetime(metadata_updated_at)), metadata_updated_at) as metadata_updated_at,
    COALESCE(unixepoch(datetime(data_updated_at)), data_updated_at) as data_updated_at,
    [metadata],
    [owner],
    [creator],
    [classification],
    [resource]
FROM _RESOURCE
WHERE true ON CONFLICT(resource_id) DO
UPDATE
SET [domain]=excluded.[domain],
    [name] = excluded.name,
    [description]=excluded.[description],
    [permalink]=excluded.[permalink],
    [metadata]=excluded.[metadata],
    [created_at]=excluded.[created_at],
    [updated_at]=excluded.[updated_at],
    [metadata_updated_at]=excluded.[metadata_updated_at],
    [data_updated_at]=excluded.[data_updated_at],
    [owner]=excluded.[owner],
    [creator]=excluded.[creator],
    [classification]=excluded.[classification],
    [resource]=excluded.[resource]
WHERE NOT 
    (
        COALESCE(domain, '') = COALESCE(excluded.domain,'')
    AND COALESCE(name,'') = COALESCE(excluded.name,'')
    AND COALESCE(description, '') = COALESCE(excluded.description,'')
    AND COALESCE(permalink,'') = COALESCE(excluded.permalink,'')
    AND COALESCE(metadata, '') = COALESCE(excluded.metadata, '')
    AND COALESCE([updated_at], '') = COALESCE(excluded.[updated_at], '')
    AND COALESCE([created_at], '') = COALESCE(excluded.[created_at], '')
    AND COALESCE([metadata_updated_at], '') = COALESCE(excluded.[metadata_updated_at], '')
    AND COALESCE([data_updated_at], '') = COALESCE(excluded.[data_updated_at], '')
    AND COALESCE(owner, '') = COALESCE(excluded.owner,'')
    AND COALESCE(creator,'') = COALESCE(excluded.creator, '')
    AND COALESCE(resource, '') = COALESCE(excluded.resource,'')
    );
    




select format('Done with resource_tabular') as "";
-- now do the columns; likewise, another need for an UPSERT.

-- Not sure why we have to do *all* columns each time? Looks like it could
-- be n^2 runtime (note the indexing by i)
WITH T AS (
    select r.resource_id,
        i + 1 as field_number,
        -- the JSON is zero-based but we want the fields to be 1-based
        r.resource->'$.columns_field_name' ->>i AS field_name,
        r.resource->'$.columns_datatype'   ->>i AS data_type,
        r.resource->'$.columns_name'       ->>i AS [name],
        r.resource->'$.columns_description'->>i AS [description]
    FROM resource_tabular as r -- this contains the resource blobs as shredded from the catalog blob for a domain
        JOIN nums ON (
            -- note the < .. nums is zero-based
            nums.i < json_array_length(r.resource, '$.columns_name')
        )
    where json_array_length(r.resource, '$.columns_name') <> 0 -- want to pick out the resources that have columns
        -- we might be able to use $.lens_view_type = 'tabular'
)
INSERT INTO resource_column(
        resource_id,
        field_number,
        field_name,
        data_type,
        [name],
        [description]
    )
SELECT resource_id,
    field_number,
    field_name,
    data_type,
    [name],
    [description]
FROM T
WHERE true ON CONFLICT(resource_id, field_number) DO -- https://www.sqlite.org/lang_upsert.html
UPDATE
SET field_name = excluded.field_name,
    data_type=excluded.data_type,
    [name] = excluded.[name], 
    [description] = excluded.[description]
WHERE NOT (    COALESCE(field_name, '') = COALESCE(excluded.field_name,'')
          AND COALESCE(data_type,'') = COALESCE(excluded.data_type,'')
          AND COALESCE([name],'') = COALESCE(excluded.[name],'')
          AND COALESCE([description],'')=COALESCE(excluded.[description],'')
);
  
select format('done with columns') as "";


DELETE FROM temp_http_request;
