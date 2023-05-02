.. Bookmark manager (a learning project) documentation master file, created by
   sphinx-quickstart on Tue Mar  7 13:50:24 2023.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Welcome to Bookmark manager's documentation!
=================================================================

Description
-----------
A bookmark manager is created and refactored during the process of Python studying.
Editor creates a tree of nodes, each node is a folder or URL.
User can create a database from scratch or make a copy of Google Chrome bookmarks from Bookmarks.json file.
Bookmarks can be viewed, added, updated and deleted.
MVP pattern was implemented. Interface protocols were added to separate the Presenter and View, Model parts.

Notes
-----
- Release 3.1 uses standalone version Mongo DB.
- A database tree stores normalized folder data and embedded URL nodes to achieve atomic write operations.
- Data integrity is provided by JSON schema validation of mongodb server.
- User interface was implemented by CLI.

.. toctree::
   :maxdepth: 2
   :caption: Contents:

   modules



Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
