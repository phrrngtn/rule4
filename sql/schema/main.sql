
-- check that we have define and http extensions

PRAGMA foreign_keys=ON;

SELECT 'need the "define" extension from https://github.com/nalgeon/sqlean'
    WHERE NOT EXISTS (SELECT * FROM pragma_module_list where name = 'define');

SELECT 'need the "sqlite-http" extension from https://github.com/asg017/sqlite-http'
    WHERE NOT EXISTS (SELECT * FROM pragma_module_list where name = 'http_get');

SELECT 'need the "template" extension from https://github.com/phrrngtn/sqlite-template-inja'
    WHERE NOT EXISTS (SELECT * FROM pragma_function_list where name = 'template_render');

.read base.sql

.read fts.sql

.read backlog.sql

.read http_request.sql

select define_free();