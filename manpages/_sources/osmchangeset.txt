
.. _geogit-osm-createchangeset:

geogit-osm-createchangeset documentation
#########################################



SYNOPSIS
********
geogit osm create-changeset [<commit> [<commit>]] -f <changesets_file>


DESCRIPTION
***********

Saves the differences between two snapshots as an OSM changeset. It's syantax is similar to the ``diff`` command, but the output uses OSM changeset format and is always written to an output file.

Two commits can be specified for comparison. If no commit is specified, the working tree and index will be compared. If only one commit is specified, it will compare the working tree with the given commit

Only the ``node`` and ``way`` trees are compared to find the differences between the specified commits. Changes in other trees will be ignored, and no changeset entries will be created based on them.

OPTIONS
*******

-f <filename>			The file to write the changesets to.


SEE ALSO
********

:ref:`geogit-osm-download`

:ref:`geogit-osm-map`

:ref:`geogit-osm-unmap`

BUGS
****

Discussion is still open.

