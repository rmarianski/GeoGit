
.. _geogit-osm-export-shp:

geogit-osm-export-shp documentation
####################################



SYNOPSIS
********
geogit osm export-shp <shapefile>  --mapping <mapping_file> [--overwrite]


DESCRIPTION
***********

Exports OSM data in the current working tree to a shapefile. Data is not exported in its canonical representation, but mapped instead before exporting.

OPTIONS
*******

--mapping <mapping_file> 	The file that contains the mapping file to use. The mapping must contain a single rule. Additional rules will be ignored.
    
--overwrite, -o 			Overwrites the specified outut file if it already exists.

SEE ALSO
********

:ref:`geogit-osm-export-pg`

:ref:`geogit-osm-export-sl`

BUGS
****

Discussion is still open.

