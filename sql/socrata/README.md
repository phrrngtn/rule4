Edit the [params.sql](params.sql) file paying attention to this parameter in particular. It controls where the system downloads files and other Socrata artifacts.
```
.parameter set @socrata_data_root "/data/socrata"
```

Create a sample Socrata database by running this command, after modifying the path/name of the SQLite database you want to create. It does not have to be called `socrata.db3`: you can call it
anything you like and place it wherever you like as long as there is sufficient space to hold the relational representation of the downloaded metadata. Similarly, `extensions.sqlite` should be in the same directory as the `sqlean` binary
and the binaries for the `http0` and `inja` extensions. The body of the extensions.sqlite file should be:
```sql
.load http0
.load inja
```
I will write up more detailed instructions at some point.

```
sqlean -init extensions.sqlite make_socrata.sql /path/to/wherever/you/want/to/put/socrata.db3
```
