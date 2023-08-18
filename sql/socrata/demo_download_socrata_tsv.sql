-- download all tabular datasets (first 1000 rows) in the 'infrastructure'
-- and 'housing & development' categories.


-- look into using API key for downloads https://socrataapikeys.docs.apiary.io/#reference

-- see about passing in a JSON parameter blob to JOIN on the various tables
-- think of a google sheet as a frontend passing in a blob of JSON generated
-- a key-value map between named ranges and their contents.
WITH T AS (SELECT 
            s.family        as url_template_family,
            s.name          as url_template_name,
            s.url_template  as socrata_template,
            s1.url_template as local_path_template
         FROM url_template  as s
         JOIN url_template  as s1
         ON (s.family='SOCRATA' and s1.family = 'SOCRATA_PATH'
             and s1.name = s.name)
         WHERE s.name = 'tsv'
), INFRASTRUCTURE AS (
    select rtc.*, 
        rt.domain, 
        rt.name 
    FROM resource_tabular_category as rtc 
    LEFT OUTER JOIN resource_tabular as rt 
    ON rtc.resource_id = rt.resource_id
     where category IN ('infrastructure', 'housing & development') 
     --and domain = 'data.cityofnewyork.us'
), T1 AS (
    -- I think the {{resource_id}}.tsv downloads just the first 1000 rows
    -- so we would need to pull in row_count information from resource_view_column to get
    -- counts. 
    -- see docs on system fields https://dev.socrata.com/docs/system-fields.html to get
    -- some pointers on how to do incremental scrape of a resource.
    SELECT template_render(T.socrata_template,
                            JSON_object('domain', I.domain, 'resource_id', I.resource_id)) as url,
            template_render(T.local_path_template,
                            json_object('workspace_root', @socrata_data_root, 'domain', I.domain, 'resource_id', I.resource_id)) as path
    FROM T, INFRASTRUCTURE AS I
)
-- be super careful with the details of doing the HTTP get and writing out the response
-- we should really be logging the response headers, status-codes, timing information etc.
-- xref https://github.com/asg017/sqlite-http/issues/29

SELECT fileio_write(T1.path, H.response_body)
FROM T1 LEFT OUTER JOIN http_get(T1.url) AS H;


-- create the VSV virtual tables in a throwaway database because this is all
-- metadata book-keeping and quite cheap relative to accessing the data.
ATTACH ':memory:' as tsv;

-- map in all the .tsv files underneath the @socrata_data_root directory
-- Ideally this would be driven off a table with a log of the HTTP requests
-- so that we are not mucking around with string manipulation on the file paths to
-- come up with resource-ids

 WITH J AS (
    select JSON_OBJECT('database', 'tsv', 
                       'vsv_table_name', LEFT(RIGHT(name, 13), 9), 
                       'vsv_file_name', name) as jo 
    FROM fileio_ls(@socrata_data_root, 1)
    where name like '%.tsv' 
    and instr(name, '_') = 0 
     and size > 200
     ), DDL AS (
            SELECT template_render(T.template, J.jo) as ddl 
            FROM J 
            JOIN codegen_template as T 
            ON (T.family='SOCRATA' and T.name = 'create_vsv_table')
    ) 
-- The DDL is idempotent in the sense that you can call it multiple times and it
-- will not crash if there is already a VSV of the same name but it will not 'refresh'
-- anything

 SELECT eval(DDL.ddl) FROM DDL;

 --- once we have the TSV files downloaded and the VSV wrappers around them, the data in the
 -- flat files can be queried like it was already in a database. This is handy for us for
 -- doing stuff like data-profiling and consistency checking of datatypes prior to importing
 -- the data into a higher ceremony database such as SQL Server, PostgreSQL or duckdb.
 -- One of the things that is nice about the duck is that we could use this metadata-database
 -- for the codegen of duckdb statements to COPY the .tsv to Parquet, for example. The type-system
 -- is much more forgiving on SQLite so we could test out if the data would load cleanly into
 -- a particular schema.