
.. _geogit-diff:

geogit-diff documentation
###########################



SYNOPSIS
********
geogit diff [<commit> [<commit>]] [-- <path>...] [--cached] [--summary] [--nogeom]


DESCRIPTION
***********

Shows changes between two commits, a commit and the working tree or a commit and the index.

If no commits are specified, it will compare the working tree and index. If only a single commit is passed, it will compare the working tree and the specified commit.

Comparison can be restricted to a given path, by using ``-- <path>``.

By default, a full detailed report of changes is shown. By using the ``--nogeom`` and ``--summary`` switches, a less detailed output can be obtained.

OPTIONS
*******

-cached				Use index instead of working tree for comparison. If no commit is specified, it compares index and HEAD commit

--nogeom			Do not show detailed changes in coordinates, but just a summary of altered points in each modified geometry

--summary			List only summary of changes. It will only show which features have changed, but not give details about the changes in each of them.

SEE ALSO
********

:ref:`geogit-format-patch`

BUGS
****

Discussion is still open.

