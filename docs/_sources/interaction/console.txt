The GeoGit Console
====================

GeoGit is a command-line application that is run by calling commands in the form ``geogit <command> [options]``. Each time you call it with a sentence like that, GeoGit has to be initialized. If your session involves running several commands, a better option is to run the geogit console, which lets you run a set of commands, initializing GeoGit just once at the begining of the session.

To start the GeoGit console, type ``geogit-console``

The prompt will show you the current folder, and in case it is a GeoGit folder, the name of the current branch (or the current commit pointed by HEAD, in case you are in a detached HEAD state).

::
	
	(geogit):my/current/folder (master) $

You can enter commands just like you do on a normal console when calling GeoGit, but without having to add ``geogit`` at the beginning of each sentence. For instance, to get the history of your repository, just type ``log`` (instead of ``geogit log``).

Console commands (like  ``ls`` if you are on Linux or ``dir`` if you are running Windows) are not available in the GeoGit console.

The GeoGit console has autocompletion, which is available pressing the tab key.

When you finish working with GeoGit and want to go back to you shell environment, type ``exit``.


Running GeoGit console in batch mode
------------------------------------

If you need to run several GeoGit operations in a batch script, you will be initializing GeoSgit as many times as commands you execute. Instead, you can pass the whole set of commands to the GeoGit console, and reduce the number of initializations to just one.

To do so, create a text file with all the GeoGit commands that you want to run, and run the ``geogit-console`` option followed by the path to that text file.

::

	$ geogit-console myscript.txt

As when using the GeoGit console in its interactive mode, the command calls in the text file should not start with the ``geogit`` command name, but with the operation name instead. Here is an example of a simple batch file.

::

	import shp myfile.shp
	add
	commit -m "First commit"

.. note:: If you use GeoGit on Windows and you create a batch file to call several GeoGit commands using the normal ``geogit`` script (not ``geogit-console``), notice that the ``geogit`` command is itself a batch process. To be able to run more than a single GeoGit command, make each call in your batch file using the ``call`` command. For instance, this will not work (it will only execute the first line):

	::	
		geogit import shp myfile.shp
		geogit add
		geogit commit -m "First commit"

	Instead, use this:

	::	
		call geogit import shp myfile.shp
		call geogit add
		call geogit commit -m "First commit"
