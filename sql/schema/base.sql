

-- needs the following two extensions
-- https://github.com/nalgeon/sqlean/blob/main/docs/define.md
-- https://github.com/phrrngtn/sqlite-template-inja


-- we can use define to create useful functions that we can then refer to either
-- directly or in the codegen templates.


-- see comment in  https://stackoverflow.com/a/76344213/40387
-- PRAGMA trusted_schema=1;


SELECT define("time_t_ms", 'format("%d.%d", strftime("%s","now"),substr(strftime("%f","now"),4))');
SELECT define("time_t", 'format("%d", strftime("%s","now"))');


create table nums (i integer primary key);

-- From https://stackoverflow.com/a/24662818
WITH RECURSIVE cte(x) AS (
    SELECT
        random()
    UNION
    ALL
    SELECT
        random()
    FROM
        cte
    LIMIT
        10000 -- should be sufficient for general purposes
)
INSERT INTO
    nums(i)
select
    ROW_NUMBER() OVER (
        ORDER BY
            x
    ) - 1 -- we want ours to start at 0 ... can't exactly remember why but there was a valid use-case 
    -- recently.
FROM
    cte;


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


-- https://www.sqlite.org/sqlar.html
CREATE TABLE sqlar(
  name TEXT PRIMARY KEY,  -- name of the file
  mode INT,               -- access permissions
  mtime INT,              -- last modification time
  sz INT,                 -- original file size
  data BLOB               -- compressed content
);


