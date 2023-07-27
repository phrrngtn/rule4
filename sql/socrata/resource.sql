
PRAGMA foreign_keys = ON;

-- This is the DDL for the table that contains the column definitions
-- for tabular resources.

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
    metadata JSON,
    classification JSON,
    [owner] JSON,
    [creator] JSON,
    [resource] JSON NOT NULL,
    PRIMARY KEY (resource_id),
    FOREIGN KEY(domain) REFERENCES domain (domain)
);

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
       ;


INSERT OR REPLACE INTO rule4_temporal_backlog([object_name], [backlog_name],[primary_key_columns],[columns])
    VALUES (
        'resource',
        '_td_bl_resource',
        json_array('resource_id'),
        json_array('domain', 'name', 'description', 'permalink')
    ),(
        'resource_column',
        '_td_bl_resource_column',
        json_array('resource_id', 'field_number'),
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

