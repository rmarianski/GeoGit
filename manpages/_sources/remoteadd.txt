
.. _geogit-remote-add:

geogit-remote-add documentation
###############################



SYNOPSIS
********
geogit remote add [-t <branch>] <name> <url>


DESCRIPTION
***********

Adds a remote for the repository with the given name and URL.

With ``-t <branch>`` option, instead of the default global refspec for the remote to track all branches under the refs/remotes/<name>/ namespace, a refspec to track only <branch> is created.

OPTIONS
*******    

-t <branch>, --track <branch>    Remote branch to track.

SEE ALSO
********

:ref:`geogit-remote-list`

:ref:`geogit-remote-remove`

BUGS
****

Discussion is still open.

