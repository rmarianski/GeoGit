
.. _geogit-sl-import:

geogit-sl-import documentation
##############################



SYNOPSIS
********
geogit sl import [options] [--all|-t <table>]


DESCRIPTION
***********

This command imports one or more tables from a SpatiaLite database into the GeoGit working tree.  Either ``-t`` or ``--all`` must be set for the import process to commence.  To see a list of available tables, use ``geogit sl list``.

OPTIONS
*******    

-t, --table     The table to import.

--all           Import all tables.

--database      The database to connect to.  Default: database

--user          User name.  Default: user

SEE ALSO
********

:ref:`geogit-sl-list`

BUGS
****

Discussion is still open.

