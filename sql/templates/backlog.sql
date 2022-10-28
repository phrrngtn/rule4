

-- TODO: deal with computed columns
-- deal with ROWID columns
-- ensure we are dealing with tables and not virtual tables or views


INSERT OR REPLACE INTO codegen_template(family, [name], template)
VALUES('TEMPORAL', 'create_temporal_backlog','

CREATE TABLE {{backlog}} (
    ts timestamp NOT NULL,
    operation char(1) NOT NULL,
    {% for c in columns%} [{{c}}]{% if loop.is_last%}{%else %}, {%endif%}{% endfor%}
);

CREATE INDEX ts_ix_{{backlog}} ON {{backlog}}(ts);
CREATE INDEX reconstruction_ix_{{backlog}} ON
 {{backlog}}({% for c in primary_key_columns%} [{{c}}]{% if loop.is_last%}{%else %}, {%endif%}{% endfor%}, ts);



DROP TRIGGER IF EXISTS {{object_name}}_td_bl_after_insert;

CREATE TRIGGER {{object_name}}_td_bl_after_insert
AFTER INSERT ON  {{object_name}}
BEGIN INSERT INTO {{backlog}}(
    ts, 
    operation, 
    {% for c in columns%} [{{c}}]{% if loop.is_last%}{%else %}, {%endif%}{% endfor%}
    ) VALUES (
        time_t_ms(),
        ''I'',
        {% for c in columns%} new.[{{c}}]{% if loop.is_last%}{%else %}, {%endif%}{% endfor%}
    );
END;

DROP TRIGGER IF EXISTS {{object_name}}_td_bl_after_update;

CREATE TRIGGER {{object_name}}_td_bl_after_update 
AFTER UPDATE ON  {{object_name}}
BEGIN INSERT INTO {{backlog}}(
    ts, 
    operation, 
    {% for c in columns%} [{{c}}]{% if loop.is_last%}{%else %}, {%endif%}{% endfor%}
    ) VALUES (
        time_t_ms(),
        ''U'',
        {% for c in columns%} new.[{{c}}]{% if loop.is_last%}{%else %}, {%endif%}{% endfor%}
    );
END;

DROP TRIGGER IF EXISTS {{object_name}}_td_bl_after_delete;

CREATE TRIGGER {{object_name}}_td_bl_after_delete
AFTER DELETE ON  {{object_name}}
BEGIN INSERT INTO {{backlog}}(
    ts, 
    operation, 
    {% for c in columns%} [{{c}}]{% if loop.is_last%}{%else %}, {%endif%}{% endfor%}
    ) VALUES (
        time_t_ms(),
        ''D'',
        {% for c in columns%} old.[{{c}}]{% if loop.is_last%}{%else %}, {%endif%}{% endfor%}
    );
END;
');



/*
WITH T AS (
    select s.name as object_name,
        s.type, '_td_bl_' || s.name as backlog,
        json_group_array(ti.name) FILTER (WHERE ti.pk=1) as primary_key_columns,
        json_group_array(ti.name) as columns
    FROM sqlite_schema as s,
        pragma_table_info(s.name) as ti
    WHERE like('_td_bl_%', s.name) = 0 -- can't seem to get escape to work?
    GROUP BY s.name, s.type
), TABLES_WITH_PK AS (
    SELECT object_name, backlog, primary_key_columns, columns
    FROM T 
    WHERE json_array_length(primary_key_columns)>0
)
SELECT ddl FROM create_temporal_backlog_t(tpk.object_name,
    tpk.backlog,
    tpk.primary_key_columns,
    tpk.columns
    ), TABLES_WITH_PK as tpk;
*/



DROP TABLE IF EXISTS create_temporal_backlog_t;

CREATE VIRTUAL TABLE create_temporal_backlog_t USING define(
    (WITH DDL AS (
        SELECT template_render(template,
               json_object('object_name', :name,
                           'backlog' , :fts,
                           'primary_key_columns', json(:primary_key_columns),
                           'columns', json(:columns)
                           )
                ) AS ddl
         FROM codegen_template
         WHERE NAME='create_temporal_backlog'
         ) 
     SELECT ddl FROM DDL
     )
);