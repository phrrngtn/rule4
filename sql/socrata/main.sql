
-- meant to be processed via the sqlean shell (as it has a bunch of extensions built in)
-- together with a -init file that loads in the http0 and inja extensions.

.read params.sql 

.read templates.sql

.read resource.sql

.read initialize_seed_data.sql

-- this makes a HTTP request to Socrata to get the list of domains together with a resource-count
-- for each of the domains
.read update_socrata_domain_list.sql

-- We use the resource-count information from the previous step to make URLs to 
-- download *all* the resource definitions for a given domain. If we have N domains, then there will be N
-- HTTP requests.
.read upsert_socrata_resources.sql



.read fs_upsert_socrata_resource_view_column.sql
.read fs_upsert_socrata_resource_all_views.sql

-- this is a workaround for a message from the define extension.
-- it may be better to just ignore the diagnostic that comes if this line is *omitted* because
-- if we try and run code that uses define, *after* calling this function, then the define-based code will 
-- fail.
select define_free() as "";
