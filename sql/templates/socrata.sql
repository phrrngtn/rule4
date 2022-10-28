
INSERT INTO url_template
VALUES(
        'SOCRATA',
        'views',
        'https://{{domain}}/api/views/{{resource_id}}'
    );
INSERT INTO url_template
VALUES(
        'SOCRATA',
        'tsv',
        'https://{{domain}}/resource/{{resource_id}}.tsv'
    );
INSERT INTO url_template
VALUES(
        'SOCRATA',
        'tsv_sample',
        'https://{{domain}}/resource/{{resource_id}}.tsv?$limit={{limit}}&offset=0'
    );
INSERT INTO url_template
VALUES(
        'SOCRATA',
        'resources',
        'https://api.us.socrata.com/api/catalog/v1?domains={{domain}}&offset=0&limit={{resource_count}}'
    );
INSERT INTO url_template
VALUES(
        'SOCRATA',
        'all_domains',
        'https://api.us.socrata.com/api/catalog/v1/domains'
    );
INSERT INTO url_template
VALUES(
        'SOCRATA',
        'all_views',
        'https://{{domain}}/api/views'
    );

