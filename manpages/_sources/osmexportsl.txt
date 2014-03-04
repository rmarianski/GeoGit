
.. _geogit-osm-export-sl:

geogit-osm-export-ls documentation
###################################



SYNOPSIS
********

geogit osm export-sl [options] --mapping <mapping_file>


DESCRIPTION
***********

Exports OSM data in the current working tree to a Spatialite database. Data is not exported in its canonical representation, but mapped instead before exporting.


If the table already exists, it will not be overwritten, unless the ``-o`` modifier is used.

OPTIONS
*******

--mapping <mapping_file> 	The file that contains the mapping file to use. The mapping must contain a single rule. Additional rules will be ignored. The rule defines the name of the output table.

-o, --overwrite 			Overwrite the output table if it already exists.

--host          			Machine name or IP address to connect to. Default: localhost

--port          			Port number to connect to.  Default: 5432

--schema        			The database schema to access.  Default: public

--database      			The databse to connect to.  Default: database

--user          			User name.  Default: postgres

--password      			Password.  Default: <no password>

SEE ALSO
********

:ref:`geogit-osm-export-pg`

:ref:`geogit-osm-export-shp`

:ref:`geogit-osm-export-sl`


BUGS
****

Discussion is still open.

