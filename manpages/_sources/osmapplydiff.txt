
.. _geogit-osm-apply-diff:

geogit-osm-apply-diff documentation
###################################


SYNOPSIS
********
geogit osm apply-diff <diff_file>


DESCRIPTION
***********

Applies an OSM diff file to the OSM data currently stored in the repository	

Diff files should use the `OsmChange format <http://wiki.openstreetmap.org/wiki/OsmChange>`_

If a change in the diff cannot apply (such as, for instance, a modification of a feature that does not exist in the repo), it will be skipped. The command output will provide information about the number of changes that could not be applied.

SEE ALSO
********

:ref:`geogit-osm-createchangeset`

BUGS
****


