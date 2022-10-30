PRAGMA foreign_keys=ON;

.read templates.sql

.read resource.sql

.read initialize_seed_data.sql

select define_free();
