-- download all tabular datasets (first 1000 rows) in the 'infrastructure'
-- and 'housing & development' categories.


-- look into using API key for downloads https://socrataapikeys.docs.apiary.io/#reference

WITH T AS (SELECT 
            s.family as url_template_family,
            s.name   as url_template_name,
            s.url_template as socrata_template,
            s1.url_template as local_path_template
         FROM url_template  as s
         JOIN url_template as s1
         ON (s.family='SOCRATA' and s1.family = 'SOCRATA_PATH'
             and s1.name = s.name)
         WHERE s.name = 'tsv'
), INFRASTRUCTURE AS (
    select rtc.*, rt.domain, rt.name 
    FROM resource_tabular_category as rtc 
    LEFT OUTER JOIN resource_tabular as rt 
    ON rtc.resource_id = rt.resource_id
     where category IN ('infrastructure', 'housing & development') 
     --and domain = 'data.cityofnewyork.us'
), T1 AS (
    -- I think the {{resource_id}}.tsv downloads just the first 1000 rows
    -- so we would need to pull in row_count information from resource_view_column to get
    -- counts.
SELECT template_render(T.socrata_template,
                           JSON_object('domain', I.domain, 'resource_id', I.resource_id)) as url,
           template_render(T.local_path_template,
                           json_object('workspace_root', @socrata_data_root, 'domain', I.domain, 'resource_id', I.resource_id)) as path
FROM T, INFRASTRUCTURE AS I
)
SELECT fileio_write(T1.path, H.response_body)
FROM T1 LEFT OUTER JOIN http_get(T1.url) AS H;


ATTACH ':memory:' as tsv;

-- map in all the .tsv files underneath the @socrata_data_root directory
-- Ideally this would be driven off a table with a log of the HTTP requests
-- so that we are not mucking around with string manipulation on the file paths to
-- come up with resource-ids

 WITH J AS (
    select JSON_OBJECT('database', 'tsv', 
                       'vsv_table_name', LEFT(RIGHT(name, 13), 9), 
                       'vsv_file_name', name) as jo 
    FROM fileio_ls('/data/socrata', 1) 
    where name like '%.tsv' 
    and instr(name, '_') = 0 
     and size > 200
     ), DDL AS (
            SELECT template_render(T.template, J.jo) as ddl 
            FROM J 
            JOIN codegen_template as T 
            ON (T.family='SOCRATA' and T.name = 'create_vsv_table')
    ) 
 SELECT eval(DDL.ddl) FROM DDL;