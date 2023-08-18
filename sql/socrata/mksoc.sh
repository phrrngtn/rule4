db=shift

cd ../schema
/home/sqlean/sqlean -init /home/sqlean/init.sqlite ${db} ".read main.sql"                      
cd ../socrata                                                                                       
/home/sqlean/sqlean -init /home/sqlean/init.sqlite ${db} ".read make_socrata.sql"                      
