
.. _geogit-pull:

geogit-pull documentation
#########################



SYNOPSIS
********
geogit pull [options] [<repository> [<refspec>...]]


DESCRIPTION
***********

Incorporates changes from a remote repository into the current branch.

More precisely, geogit pull runs geogit fetch with the given parameters and calls geogit merge to merge the retrieved branch heads into the current branch. With --rebase, it runs geogit rebase instead of geogit merge.

Refspecs can be used to gain more control over the pull process.  The format of a pull refspec is [+]<remoteref>[:<localref>] where <remoteref> is the remote branch to pull and <localref> is the local branch to pull to.  If the local branch is not present, it will be created.  If <localref> is not specified, it will pull the remote branch into a local branch with a matching name.

OPTIONS
*******

--all       			Fetch all remotes.

--rebase    			Rebase the current branch on top of the upstream branch after fetching.

--depth <depth>			In the case of a shallow clone, it fetches history only up to the specified depth

--fulldepth 			In the case of a shallow clone, fetch the full history from the repository. This will turn the repository into a full clone.


SEE ALSO
********

:ref:`geogit-fetch`

:ref:`geogit-clone`

:ref:`geogit-rebase`

BUGS
****

Discussion is still open.

