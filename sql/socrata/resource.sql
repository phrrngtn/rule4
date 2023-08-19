
PRAGMA foreign_keys = ON;

-- This is the DDL for the table that contains the column definitions
-- for tabular resources.

-- xref https://socratadiscovery.docs.apiary.io/#   

-- [domain]
-- https://api.us.socrata.com/api/catalog/v1/domains
--
-- A list of all the domains managed by Socrata and the resource-count. This is scrapable by 
-- a single API call and gives us a quick way of finding out when a new domain has been added
-- or a new resource has been added to a domain (since deletions appear to be rare)
-- There is a temporal backlog on the table and a "suppress value equivalent update" poll and
-- populate query (update_socrata_domain_list.sql) so we can call the endpoint once or twice a day
-- and then look at the backlog to see what has changed. 



-- [socrata_domain_of_interest]
-- Because the overall metadata volumes are large, we can narrow the focus of a number of operations
-- to the 'socrata_domain_of_interest'. If one wants to study a domain in more depth, the first
-- step is to add the domain to this table.

-- [resource]
-- https://api.us.socrata.com/api/catalog/v1?domains={{domain}}&offset=0&limit={{resource_count}}
--
-- Note how we can get all the resource definitions for a single domain by including the resource_count
-- (which we get from the [domain] table)

-- [resource_all_views]
-- https://{{domain}}/api/views
--
-- resource_view
-- resource_tabular
-- resource_column
-- resource_view_column
-- resource_tabular_category



DROP TABLE IF EXISTS socrata_blob;

CREATE TABLE socrata_blob([path] primary key, mtime, [blob], blob_checksum);

-- this is a simple table of socrata domains with the reported number of resources
-- hosted by that domain. We get this from the 'https://api.us.socrata.com/api/catalog/v1/domains'
-- API call

DROP TABLE IF EXISTS domain;

CREATE TABLE domain(
    domain varchar(512) primary key,
    resource_count int NOT NULL
);

DROP TABLE IF EXISTS resource;

CREATE TABLE resource(
    domain        VARCHAR(512),
    resource_id   VARCHAR(9) NOT NULL,
    [name]        VARCHAR,
    [description] VARCHAR,
    permalink     VARCHAR,
    metadata      JSON,
    [resource]    JSON NOT NULL,
    PRIMARY KEY (resource_id),
    FOREIGN KEY(domain) REFERENCES domain (domain)
);

-- we have a resource table that has metadata for each resource pluse
-- the blob of JSON that contains the definition of the resource. 
-- Note the foreign

DROP TABLE IF EXISTS resource_tabular;

CREATE TABLE resource_tabular(
    domain VARCHAR(512),
    resource_id VARCHAR(9) NOT NULL,
    [name] VARCHAR,
    [description] VARCHAR,
    permalink VARCHAR,
    updated_at datetime,
    created_at datetime,
    metadata_updated_at datetime,
    data_updated_at datetime,    
    metadata JSON,
    classification JSON,
    [owner] JSON,
    [creator] JSON,
    [resource] JSON NOT NULL,
    PRIMARY KEY (resource_id),
    FOREIGN KEY(domain) REFERENCES domain (domain)
);


DROP TABLE IF EXISTS resource_all_views;

CREATE TABLE resource_all_views(
    domain VARCHAR(512),
    resource_id VARCHAR(9) NOT NULL,
    [name] VARCHAR,
    asset_type varchar,
    category varchar,
    [description] VARCHAR,
    display_type varchar,
    provenance varchar,
    created_at datetime NULL,
    publication_date datetime NULL,
    view_last_modified datetime NULL,
    rows_updated_at datetime NULL,
    [resource] JSON NOT NULL,
    PRIMARY KEY (resource_id),
    FOREIGN KEY(domain) REFERENCES domain (domain)
);


DROP TABLE IF EXISTS resource_view;
CREATE TABLE resource_view(
    domain VARCHAR(512),
    resource_id VARCHAR(9) NOT NULL,
    [name] VARCHAR,
    [description] VARCHAR,
    asset_type VARCHAR,
    view_type varchar,
    display_type varchar,
    [resource] JSON NOT NULL,
    PRIMARY KEY (resource_id),
    FOREIGN KEY(domain) REFERENCES domain (domain)
);

DROP TABLE IF EXISTS resource_view_column;

CREATE TABLE resource_view_column (
    resource_id VARCHAR(9) NOT NULL,
    id integer not null,
    field_number INTEGER NOT NULL,
    data_type VARCHAR NULL,
    render_type varchar not null,
    _count    int NULL,
    _cardinality INT NULL,
    non_null_count int NULL,
    smallest_value varchar NULL,
    largest_value varchar NULL,
    null_count int NULL,
    [name] VARCHAR NOT NULL,
    field_name VARCHAR,    
    [description] VARCHAR NULL,
    [resource] JSON NULL, 
    PRIMARY KEY (id, field_number)
    /*,
    FOREIGN KEY(resource_id) REFERENCES resource_view (resource_id) */
);



CREATE VIEW IF NOT EXISTS resource_tabular_category(resource_id, category_ordinal, category)
AS
select r.resource_id,
    CAST(j.[key] as smallint) as category_ordinal,
    CAST(j.[value] as text) as category
FROM resource_tabular as r,
    JSON_EACH(r.classification, '$.categories') as j;

DROP TABLE IF EXISTS resource_column;

CREATE TABLE resource_column (
    resource_id VARCHAR(9) NOT NULL,
    field_number INTEGER NOT NULL,
    field_name VARCHAR,
    data_type VARCHAR NOT NULL,
    [name] VARCHAR NOT NULL,
    [description] VARCHAR NOT NULL,
    PRIMARY KEY (resource_id, field_number),
    FOREIGN KEY(resource_id) REFERENCES resource_tabular (resource_id)
);


-- STUDY: perhaps add a 'facet'/grouping for domains.
-- this is a way of reducing the volume of metadata and data presented to the 
-- end user
DROP TABLE IF EXISTS socrata_domain_of_interest;

CREATE TABLE socrata_domain_of_interest(
    domain varchar PRIMARY KEY,
    notes varchar NULL
);

-- these rely on https://github.com/phrrngtn/sqlite-template-inja
-- and sqlean define
-- create_fts_index_t is a virtual table that is a wrapper on top of expansion of 
-- the 'create_fts_index' template.

-- TODO: drive this off a 'systems' catalog so that we can do it with one query. Don't
-- worry about 1NF violation and just store the stuff 'as is' as JSON.
-- it seems that we need a physical table with a schema that matches up with the schema of
-- the formal parameters of the various virtual table-based functions. That reads a bit awkwardly
-- but I hope the intent is clear.

-- STUDY: should the triggers be incorporated into a single template? Is there any utility
-- in having them factored out separately like this?



INSERT OR REPLACE INTO rule4_fts([object_name], [fts], [indexed_columns])
VALUES ('resource', 'resource_fts', json_array('name', 'description')),
       ('resource_column', 'resource_column_fts', json_array('field_name','name', 'description'))
       /*,('resource_view_column', 'resource_view_column_fts', json_array('field_name','name', 'description'))       */
       
        ;


INSERT OR REPLACE INTO rule4_temporal_backlog([object_name], [backlog_name],[primary_key_columns],[columns])
    VALUES (
        'resource',
        '_td_bl_resource',
        json_array('resource_id'),
        json_array('domain', 'name', 'description', 'permalink')
    ),(
        'resource_all_views',
        '_td_bl_resource_all_views',
        json_array('resource_id'),
        json_array('domain', 'name', 'description', 'asset_type','display_type',
        'provenance', 'publication_date', 'view_last_modified')
    ),(
        'resource_column',
        '_td_bl_resource_column',
        json_array('resource_id', 'field_number'),
        json_array('field_name', 'data_type', 'name', 'description')
    ),(
        'resource_view_column',
        '_td_bl_resource_view_column',
        json_array('id', 'field_number'),
        json_array('field_name', 'data_type', 'name', 'description')
    ),(
        'socrata_domain_of_interest',
        '_td_bl_socrata_domain_of_interest',
        json_array('domain'),
        json_array('notes')        
    ),(
        'domain',
        '_td_bl_domain',
        json_array('domain'),
        json_array('resource_count')
    );

SELECT eval(ddl) 
FROM create_fts_index_t(r4.[object_name], r4.[fts], r4.[indexed_columns]),
    rule4_fts as r4;


SELECT eval(ddl) 
FROM create_fts_triggers_t(r4.[object_name], r4.[fts], r4.[indexed_columns]),
    rule4_fts as r4;



SELECT eval(ddl) 
FROM create_temporal_backlog_t(r4.[object_name],
                               r4.[backlog_name],
                               r4.[primary_key_columns],
                               r4.[columns]),
                               rule4_temporal_backlog as r4;
                               


DROP TABLE IF EXISTS socrata_resource_http_request;

CREATE TABLE socrata_resource_http_request(
    request_url TEXT,
    request_method TEXT,
    request_headers TEXT,
    request_cookies TEXT,
    request_body BLOB,
    response_status TEXT,
    response_status_code INT,
    response_headers TEXT,
    response_cookies TEXT,
    response_body BLOB,
    remote_address TEXT,
    -- IP address of responding server
    timings TEXT,
    -- JSON of various event timestamps
    meta TEXT, -- Metadata of request
    socrata_domain varchar NOT NULL,
    resource_id varchar NULL, -- may be null unless we are doing something specific to a resource
    ts datetime NOT NULL,
    local_path_response_body varchar,
    response_body_bytes_written int,
    url_template_family varchar NULL,
    url_template_name varchar NULL    
);

CREATE INDEX ix_ts_socrata_resource_http_request ON socrata_resource_http_request(ts);
CREATE INDEX ix_socrata_domain_ts_socrata_resource_http_request ON socrata_resource_http_request(socrata_domain,ts);


select printf("done with resource.sql");