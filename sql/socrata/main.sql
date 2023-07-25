PRAGMA foreign_keys=ON;

-- https://stackoverflow.com/a/76344213/40387

PRAGMA trusted_schema=1;


.read templates.sql

.read resource.sql

.read initialize_seed_data.sql

select define_free();
