
INSERT OR REPLACE INTO codegen_template(family, name, template)
VALUES('FTS', 'create_fts_index',
'
-- see https://www.sqlite.org/fts5.html for general background on FTS
-- we want to have a very high performance index that is suitable for use within interactive
-- environments like Excel.
CREATE VIRTUAL TABLE {{fts}} USING fts5(
{% for c in indexed_columns%} [{{c}}], {% endfor%}
    content = {{object_name}}
);
');

INSERT OR REPLACE INTO codegen_template(family, name, template)
VALUES('FTS', 'create_fts_triggers',
'
-- this is some boilerplate adapted from https://kimsereylam.com/sqlite/2020/03/06/full-text-search-with-sqlite.html
-- for auto-maintaining the full-text indexes in the face of data modification (DML) on the tables.
CREATE TRIGGER {{object_name}}_after_insert
AFTER
INSERT
    ON {{object_name}} BEGIN
INSERT INTO
    {{fts}} (
        rowid,
        {% for c in indexed_columns%} [{{c}}]{% if loop.is_last%}{%else %}, {%endif%}{% endfor%}
    )
VALUES
    (
        new.rowid,
        {% for c in indexed_columns%} new.[{{c}}]{% if loop.is_last%}{%else %}, {%endif%}{% endfor%}
    );
END;
CREATE TRIGGER {{object_name}}_after_delete
AFTER
    DELETE ON {{object_name}} BEGIN
INSERT INTO
    {{fts}} (
        {{fts}},
        rowid,
        {% for c in indexed_columns%} [{{c}}]{% if loop.is_last%}{%else %}, {%endif%}{% endfor%}
    )
VALUES
    (
        ''delete'',
        old.rowid,
        {% for c in indexed_columns%} old.[{{c}}]{% if loop.is_last%}{%else %}, {%endif%}{% endfor%}
    );
END;
CREATE TRIGGER {{object_name}}_after_update
AFTER
UPDATE
    ON {{object_name}} BEGIN
INSERT INTO
    {{fts}} (
        {{fts}},
        rowid,
        {% for c in indexed_columns%} [{{c}}]{% if loop.is_last%}{%else %}, {%endif%}{% endfor%}
    )
VALUES
    (
        ''delete'',
        old.rowid,
        {% for c in indexed_columns%} old.[{{c}}]{% if loop.is_last%}{%else %}, {%endif%}{% endfor%}
    );
INSERT INTO
    {{fts}} (
        rowid,
        {% for c in indexed_columns%} [{{c}}]{% if loop.is_last%}{%else %}, {%endif%}{% endfor%}
        )
VALUES
    (
        new.rowid,
        {% for c in indexed_columns%} old.[{{c}}]{% if loop.is_last%}{%else %}, {%endif%}{% endfor%}
    );
END;
');


-- this is a template that generates a virtual table that expands out
-- FTS-related codegens (at the moment the FTS table and the triggers)
INSERT OR REPLACE INTO codegen_template(family, name, template)
VALUES('FTS', 'meta_create_fts_t',
'
CREATE VIRTUAL TABLE {{virtual_table_name}} USING define(
    (WITH DDL AS (
        SELECT template_render(template,
               json_object(''object_name'', :name,
                           ''fts'' , :fts,
                           ''indexed_columns'', json(:indexed_columns)
                           )
                ) AS ddl
         FROM codegen_template
         WHERE NAME=''{{template_name}}''
         ) 
     SELECT ddl FROM DDL
     )
);
');


-- not working yet because of the difficulties of quoting.
-- it may be a very bad idea in any case as it is difficult to debug
INSERT OR REPLACE INTO codegen_template(family, name, template)
VALUES('FTS', 'meta_create_fts_f',
'SELECT define(''{{function_name}}'', ''(WITH DDL AS (
    SELECT template_render(template,json_object("object_name", :name, "fts" , :fts, "indexed_columns", json(:indexed_columns)) AS ddl
    FROM codegen_template
    WHERE name=''''{{template_name}}'''')
    SELECT ddl FROM DDL)'')'
);


-- it may be a pattern to have  a virtual table codegen interface and a 
-- function interface.
select eval(template_render(template, json_object('virtual_table_name', 'create_fts_index_t', 'template_name', 'create_fts_index')))
 from codegen_template where name = 'meta_create_fts_t';

select eval(template_render(template, json_object('virtual_table_name', 'create_fts_triggers_t', 'template_name', 'create_fts_triggers')))
 from codegen_template where name = 'meta_create_fts_t';

