


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




WITH T AS (
    select 
        E.value->>'$.id' as resource_id,
        E.value->>'$.name' as [name],
        E.value->>'$.assetType' as [asset_type],
        E.value->>'$.category' as category,
        E.value->>'$.description' as [description],
        E.value->>'$.displayType' as [display_type],
        E.value->>'$.provenance' as [provenance],
        E.value->>'$.createdAt' as created_at,
        E.value->>'$.publicationDate' as publication_date,
        E.value->>'$.viewLastModified' as view_last_modified,
        E.value->>'$.rowsUpdatedAt'    as rows_updated_at,
        E.value  as [resource]
        FROM fileio_ls(@socrata_data_root,1) as ls, -- xref: https://www.sqlite.org/cli.html#sql_parameters
        JSON_EACH(
            fileio_read(ls.name)
        ) as E 
        where ls.name like '%all_views%.json'
        and ls.size > 10000 -- weed out some bogus JSON
        and json_valid(fileio_read(ls.name))=1
)
INSERT INTO resource_all_views(
resource_id,
name,
asset_type,
category,
description,
display_type,
provenance,
created_at,
publication_date,
view_last_modified,
rows_updated_at,
resource
)
select 
    T.resource_id,
    T.name,
    T.asset_type,
    T.category,
    T.description,
    T.display_type,
    T.provenance,
    COALESCE(unixepoch(datetime(T.created_at)), T.created_at) as created_at,
    COALESCE(unixepoch(datetime(T.publication_date)), T.publication_date) as publication_date,
    COALESCE(unixepoch(datetime(T.view_last_modified)), T.view_last_modified) as view_last_modified,
    T.rows_updated_at,
    T.resource
FROM T
WHERE true ON CONFLICT(resource_id) DO
UPDATE
SET
    [name] = excluded.name,
    [description]=excluded.[description],
    [asset_type]=excluded.[asset_type],
    [category]=excluded.[category],
    [provenance]=excluded.[provenance],
    [created_at]=excluded.[created_at],    
    [publication_date]=excluded.[publication_date],
    [view_last_modified]=excluded.[view_last_modified],
    [rows_updated_at]=excluded.[rows_updated_at],
    [resource]=excluded.[resource]
WHERE NOT 
    (
        COALESCE(name,'') = COALESCE(excluded.name,'.')
    AND COALESCE(description, '') = COALESCE(excluded.description,'.')
    AND COALESCE(asset_type,'') = COALESCE(excluded.asset_type,'.')
    AND COALESCE(category, '') = COALESCE(excluded.category, '.')
    AND COALESCE(provenance, '') = COALESCE(excluded.provenance,'.')
    AND COALESCE(publication_date,'') = COALESCE(excluded.publication_date, '.')
    AND COALESCE(created_at,'') = COALESCE(excluded.created_at, '.')
    AND COALESCE(view_last_modified,'') = COALESCE(excluded.view_last_modified, '.')
    AND COALESCE(rows_updated_at,'') = COALESCE(excluded.rows_updated_at, '.')
    );
