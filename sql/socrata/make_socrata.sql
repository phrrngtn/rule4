-- driver script to create Socrata database from seed data and refresh via HTTP
.cd ../schema
.read main.sql
.cd ../socrata
.read main.sql
.read upsert_socrata_resources.sql 
.read update_socrata_domain_list.sql                                            
.read test_all_views.sql
