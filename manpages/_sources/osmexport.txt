
.. _geogit-export:

geogit-osmexport documentation
###############################



SYNOPSIS
********
geogit osm export <file> [commitish] [--bbox] [--overwrite]


DESCRIPTION
***********

Exports a given commit to a file using an OSM export format. Only features in the canonical OSM trees (``way`` and ``node``) are exported.

If the output filename has the ``pbf`` extension, the pbf format will be used. Otherwise, the OSM XML format will be 

OPTIONS
*******

[commitish]							A reference that resolves to the commit to export from. If not provided, the current HEAD is used.
    
-b <S W N E>, --bbox <S W N E>		Exports only the features within a given bounding box, defined by its 4 boundary values.

--overwrite, -o 	 				Overwrites the specified outut file if it already exists.

SEE ALSO
********

:ref:`geogit-osm-export-pg`

:ref:`geogit-osm-export-shp`

:ref:`geogit-osm-export-sl`


BUGS
****

Discussion is still open.

