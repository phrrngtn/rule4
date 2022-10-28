

-- needs the following two extensions
-- https://github.com/nalgeon/sqlean/blob/main/docs/define.md
-- https://github.com/phrrngtn/sqlite-template-inja


-- we can use define to create useful functions that we can then refer to either
-- directly or in the codegen templates.
SELECT define("time_t_ms", 'format("%d.%d", strftime("%s","now"),substr(strftime("%f","now"),4))');
SELECT define("time_t", 'format("%d", strftime("%s","now"))');


-- template as a means to overcome the fact that you can't do DML on system catalogs
-- but instead have to manipulate the model via DDL. The templates may be a way around 
-- this by converting Rule4 relational data to DDL
CREATE TABLE codegen_template (
    family varchar NOT NULL,
    name varchar not null,
    template varchar NOT NULL,
    PRIMARY KEY (family, name)
);

-- might be obviated somewhat by https://github.com/asg017/sqlite-url
CREATE TABLE url_template (
    family varchar NOT NULL,
    name varchar not null,
    url_template varchar NOT NULL,
    PRIMARY KEY (family, name)
);

