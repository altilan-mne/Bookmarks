# Bookmarks
Bookmark manager, a training project

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