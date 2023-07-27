
INSERT OR REPLACE INTO url_template
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
    ),(
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

