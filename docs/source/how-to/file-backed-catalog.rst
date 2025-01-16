How to create a new Catalog backed by files
===========================================

*I want to quickly set up a small Databroker Catalog.*

Why backed by files and not a database?
---------------------------------------

Pro: You don't have to run a database to try it.

Cons: It only scales to about 100 Runs, it will be a bit slower, and you lose
*some* of the search capability, such as full text search.

Databroker works best when backed by a proper database, but for tiny
deployments of up to about 100 Runs, Databroker can run on top of ordinary
files on disk. This can be convenient in these situations:

* Just trying things out
* Testing
* Tutorials and demos
* Sending users home with a "portable databroker" without setting up a
  database, as long as they don't have a large number of Runs

Temporary Catalog
-----------------

If you are in the "just trying things" phase, you might start by creating a
*temporary* Catalog backed by file in your system's temp directory. It will
be hard to find again after you exit Python, and it will be permanently deleted
whenever you system next reboots, so do not put anything important (or
especially large) there.

.. code:: python

   from databroker.v2 import temp
   catalog = temp()
   # That's it!

See :doc:`store-data-from-run-engine` or
:doc:`store-analysis-results` to put some actual data in there, and see
the tutorials for how to get it back out.

Persistent Catalog
------------------

Taking the next step, let's make a persistent Catalog.

#. Find where Databroker looks for Catalog configuration files on your system.
   It varies by OS and environment because Databroker does its best to be a
   polite guest and place configuration files where the local conventions
   dictate. Run this snippet to find the list of paths where it looks
   on your system.

   .. code:: bash

      python3 -c "import databroker; print(databroker.catalog_search_path())"

#. Compose a configuration file like this. The filename of the configuration
   file is unimportant, but using ``CATALOG_NAME.yml`` is conventional. The
   file should be placed in any one of the directories listed by the previous
   step.

   .. code:: yaml
   
      sources:
        CATALOG_NAME:
          driver: "bluesky-msgpack-catalog"
          args:
            paths:
              - "DESTINATION_DIRECTORY/*.msgpack"
   
   where ``CATALOG_NAME`` is a name of the entry that will appear in
   ``databroker.catalog``, and ``DESTINATION_DIRECTORY`` is where the data
   will be stored.
   
   Note that the value of ``paths`` is a list. Multiple data directories may be
   grouped into one "source".

#. Now ``CATALOG_NAME`` should appear in

   .. code:: python

      import databroker

      # List catalog names.
      list(databroker.catalog)

   If it does not appear, call ``databroker.catalog.force_reload()`` and retry.
   The catalog may be accessed like

   .. code:: python

      import databroker

      # List catalog names.
      list(databroker.catalog)

   using the ``CATALOG_NAME`` in the text of the configuration file. (Again,
   the *filename* of the configuration file is not relevant.)

See :doc:`store-data-from-run-engine` or
:doc:`store-analysis-results` to put some actual data in there, and see
the tutorials for how to get it back out.
