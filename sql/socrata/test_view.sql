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



DROP TABLE IF EXISTS resource_view_column;

CREATE TABLE resource_view_column (
    resource_id         VARCHAR(9) NOT NULL,
    id                  integer not null,
    field_number        INTEGER NOT NULL,
    data_type           VARCHAR NOT NULL,
    render_type         varchar NOT null,
    _count              int NULL,
    _cardinality        INT NULL,
    non_null_count      int NULL,
    smallest_value      varchar NULL,
    largest_value       varchar NULL,
    null_count          int NULL,
    [name]              VARCHAR NOT NULL,
    field_name          VARCHAR,    
    [description]       VARCHAR NULL,    
    PRIMARY KEY (id, field_number)
    /*,
    FOREIGN KEY(resource_id) REFERENCES resource_view (resource_id) */
);


-- TODO: put in FTS index and backlog
-- TODO: get paths from table and/or JSON function
--    use sqlean define function to give a calling mechanism that is easy to interface to from
--    a spreadsheet

WITH F AS (
    select name AS path
    FROM fileio_ls('/data/socrata/data.cityofnewyork.us')
    where name like '%SOCRATA_views.json'
), B AS (
    select path, json(fileio_read(path)) AS contents
    FROM F
),
    T AS (
 select B.path, 
        B.contents->>'$.id'                         AS resource_id,
        C.value   ->>'$.id'                         AS id,
        C.value   ->>'$.position'                   AS position,
        C.value   ->>'$.dataTypeName'               AS data_type,
        C.value   ->>'$.renderTypeName'             AS render_type,
        C.value   ->>'$.cachedContents.non_null'    AS [non_null_count],
        C.value   ->>'$.cachedContents.null'        AS null_count,   
        C.value   ->>'$.cachedContents.count'       AS _count,
        C.value   ->>'$.cachedContents.cardinality' AS _cardinality,             
        C.value   ->>'$.cachedContents.smallest'    AS smallest_value, 
        C.value   ->>'$.cachedContents.largest'     AS [largest_value],
        C.value   ->>'$.name'                       AS [name],
        C.value   ->>'$.fieldName'                  AS field_name,
        C.value   ->>'$.description'                AS [description],        
    /*
    Stash away the JSON itself in case we want to get some other fields (e.g. histograms) later on
    */
        C.value                                  AS [resource]
    FROM B, 
        JSON_EACH(B.contents, '$.columns') AS C
    WHERE json_valid(B.contents) = 1
), W AS (
select  'data.cityofnewyork.us' AS domain,
    T.path,
    T.resource_id,
    T.id,
    T.position AS field_number,
    T.data_type,
    T.render_type,
    T.non_null_count,
    T.null_count,
    T._count,
    T._cardinality,
    T.smallest_value,
    T.largest_value,
    T.name,
    T.field_name,
    T.description
FROM T
)

 INSERT INTO resource_view_column(
    resource_id,
    id,
    field_number,
    data_type,
    render_type,
    _count,
    _cardinality,
    non_null_count,
    null_count,
    smallest_value,
    largest_value,
    name,
    field_name,
    [description]
 )
 SELECT
    resource_id,
    id,
    field_number,
    data_type,
    render_type,
    _count,
    _cardinality,
    non_null_count,
    null_count,
    smallest_value,
    largest_value,
    [name],
    field_name,
    [description]
FROM W;

-- TODO: put in usable ON CONFLICT clause to avoid trashing the backlog

-- WHERE true ON CONFLICT(resource_id) DO
-- UPDATE
-- SET [domain]=excluded.[domain],
--     [name] = excluded.name,
--     [description]=excluded.[description],
--     [asset_type]=excluded.[asset_type],
--     [category]=excluded.[category],
--     [provenance]=excluded.[provenance],
--     [publication_date]=excluded.[publication_date],
--     [view_last_modified]=excluded.[view_last_modified],
--     [resource]=excluded.[resource]
-- WHERE NOT 
--     (
--         COALESCE(domain, '') = COALESCE(excluded.domain,'')
--     AND COALESCE(name,'') = COALESCE(excluded.name,'')
--     AND COALESCE(description, '') = COALESCE(excluded.description,'')
--     AND COALESCE(asset_type,'') = COALESCE(excluded.asset_type,'')
--     AND COALESCE(category, '') = COALESCE(excluded.category, '')
--     AND COALESCE(provenance, '') = COALESCE(excluded.provenance,'')
--     AND COALESCE(publication_date,'') = COALESCE(excluded.publication_date, '')
--     AND COALESCE(view_last_modified,'') = COALESCE(excluded.view_last_modified, '')
--     );
