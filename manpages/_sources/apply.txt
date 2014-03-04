
.. _geogit-apply:

geogit-apply documentation
#############################


SYNOPSIS
********
geogit apply [<patch_file>] [--reject] [--summary] [--check]


DESCRIPTION
***********

Applies a patch or part of it on the current working tree.


OPTIONS
*******    


--check		Do not apply, just check that the specified patch can be safely applied

--reject	apply only those changes that can be safely applied and create a ``.rej`` patch file with those other changes that were rejected and could not be safely applied

--summary	Do not apply, just show content of patch.


SEE ALSO
********

:ref:`geogit-format-patch`

BUGS
****


