-- This is the DDL for the table that contains the column definitions
-- for tabular resources.
CREATE TABLE socrata_blob([path] primary key, mtime, [blob], blob_checksum);

CREATE TABLE domain(domain varchar(512) primary key);

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


-- these rely on https://github.com/phrrngtn/sqlite-template-inja
-- and sqlean define
-- create_fts_index_t is a virtual table that is a wrapper on top of expansion of 
-- the 'create_fts_index' template.
SELECT eval(ddl) 
FROM create_fts_index_t('resource', 'resource_fts', json_array('name', 'description'));

SELECT eval(ddl)
FROM create_fts_triggers_t('resource', 'resource_fts', json_array('name', 'description'));

-- create trigger-maintained temporal backlog on resource
SELECT eval(ddl) FROM create_temporal_backlog_t('resource',
        '_td_bl_resource',
        json_array('resource_id'),
        json_array('domain', 'resource_id', 'name', 'description', 'permalink', 'metadata', 'resource')
);