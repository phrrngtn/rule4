
-- these are used to generate URLs and file-paths for Socrata-related activities.
-- family = 'SOCRATA' is for Socrata URLs
-- family = 'SOCRATA_PATH' is for local files which contain the downloaded contents of the HTTP requests
-- we want to avoid overwhelming Socrata with HTTP calls so use the local file-system as a 
-- loosely structured cache. The {{workspace_root}} placeholder will usually be filled in by the
-- @socrata_data_root SQLite parameter.

INSERT OR REPLACE INTO url_template(family,name, url_template)
VALUES (
        'SOCRATA',
        'views',
        'https://{{domain}}/api/views/{{resource_id}}'
    ),(
        'SOCRATA_PATH',
        'views',
        '{{workspace_root}}/{{domain}}/{{domain}}_SOCRATA_views.json'
    ),(
        'SOCRATA',
        'tsv',
        'https://{{domain}}/resource/{{resource_id}}.tsv'
    ),
    (
        'SOCRATA',
        'resource',
        'https://{{domain}}/resource/{{resource_id}}'
    ),
    (
        'SOCRATA',
        'OData',
        'https://{{domain}}/api/odata/v4/{{resource_id}}'
    ),
    (
        'SOCRATA_PATH',
        'tsv',
        '{{workspace_root}}/{{domain}}/{{resource_id}}.tsv'
    ),(
        'SOCRATA',
        'tsv_sample',
        'https://{{domain}}/resource/{{resource_id}}.tsv?$limit={{limit}}&offset=0'
    ),(
        'SOCRATA_PATH',
        'tsv_sample',
        '{{workspace_root}}/{{domain}}/{{resource_id}}_0_{{limit}}.tsv'
    ),(
        'SOCRATA',
        'resources',
        'https://api.us.socrata.com/api/catalog/v1?domains={{domain}}&offset=0&limit={{resource_count}}'
    ),(
        'SOCRATA_PATH',
        'resources',
        '{{workspace_root}}/{{domain}}/resources_{{domain}}.json'
    ),(
        'SOCRATA',
        'all_domains',
        'https://api.us.socrata.com/api/catalog/v1/domains'
    ),(
        'SOCRATA',
        'all_views',
        'https://{{domain}}/api/views'
    ),(
        'SOCRATA_PATH',
        'all_views',
        '{{workspace_root}}/{{domain}}/all_views_{{domain}}.json'
    );


select printf("done with templates.sql");
