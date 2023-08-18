@echo off
rem simple DOS script to load in the geonames data into a database specified on the command-line as the first argument
rem e.g. "lgn foo.db3" would load the geonames data (assumed to be in flat files in the current directory)
rem into foo.db3


cd ..\schema
sqlean -init c:\tools\extensions.sqlite %1 ".read main.sql"                      
cd ..\socrata                                                                                       
sqlean -init c:\tools\extensions.sqlite %1 ".read main.sql"                      
sqlean -init c:\tools\extensions.sqlite %1  ".read update_socrata_domain_list.sql"
sqlean -init c:\tools\extensions.sqlite %1 ".read t_local_fs.sql"                                                   
sqlean -init c:\tools\extensions.sqlite %1 ".read test_view.sql"                 
                                     