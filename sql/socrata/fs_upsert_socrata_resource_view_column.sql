PRAGMA trusted_schema = 1;

CREATE TEMP TABLE IF NOT EXISTS temp_http_request AS
SELECT *
FROM http_request
where 1 = 0;

-- we may do this differently it we want requests to stay around for 
-- debugging purposes.
delete from temp_http_request;


-- DONE: put in FTS index and backlog 
-- DONE: get paths from table and/or JSON function
-- TODO:  use sqlean define function to give a calling mechanism that is easy to interface to from
--    a spreadsheet

-- This is a brute force solution: read in all of the downloaded view JSON files and upsert
WITH F AS (
    select name AS path
    FROM fileio_ls(@socrata_data_root,1) -- recursive
    where name like '%SOCRATA_views.json' -- file name pattern
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
select 
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
    T.description,
    T.resource
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
    [description],
    [resource]
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
    [description],
    [resource]
FROM W
WHERE true ON CONFLICT(id, field_number) DO
UPDATE
set data_type=excluded.data_type,
    render_type=excluded.render_type,
    _count=excluded._count,
    _cardinality=excluded._cardinality,
    non_null_count=excluded.non_null_count,
    null_count=excluded.null_count,
    smallest_value=excluded.smallest_value,
    largest_value=excluded.largest_value,
    [name]=excluded.[name],
    field_name=excluded.field_name,
    description=excluded.description
WHERE NOT (
        COALESCE(data_type, '') = COALESCE(excluded.data_type, '')
    AND COALESCE(render_type, '') = COALESCE(excluded.render_type, '')
    AND COALESCE(_count, '') = COALESCE(excluded._count, '')
    AND COALESCE(_cardinality, '') = COALESCE(excluded._cardinality, '')
    AND COALESCE(non_null_count, '') = COALESCE(excluded.non_null_count, '')
    AND COALESCE(null_count, '') = COALESCE(excluded.null_count, '')    
    AND COALESCE(smallest_value, '') = COALESCE(excluded.smallest_value, '')
    AND COALESCE(largest_value, '') = COALESCE(excluded.largest_value, '')
    AND COALESCE(name, '') = COALESCE(excluded.name, '')
    AND COALESCE(field_name, '') = COALESCE(excluded.field_name, '')
    AND COALESCE(description, '') = COALESCE(excluded.description, '')
);


