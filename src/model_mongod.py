"""A Model part of Bookmark Manager (BM) implemented with Mongo DB (document db model).
There are 2 types of nodes: folders and urls (leaf node).
Version 3.1 has a new db data structure optimized for atomic (only one document at once) write operation.
This is preparation for the transition to the distributed model of BM.
Url node objects placed to the children array of the parent folder node (data denormalization).
Folder nodes are still documents of the collection, as they were in the first version.

There are common fields and there are specific fields of nodes.
Common fields:
    _id: GUID (UUID4) of the node as 128-bit binary value (not as a string)
    name: str - name of the node, root of tree names 'roots'
    date_added: Date - timestamp when the node was created, internal Mongo format (N of ms from Unix epoch)
    id_no: int - legacy from Chrome bookmarks

Folder specific fields:
    parent_guid: GUID of the parent node, 'roots' has Null(None for Python) value
    children: array of objects - list of child url nodes, which are embedded documents
    date_modified: Date - timestamp when the folder was modified, internal Mongo format

Url specific fields:
    url: str - URL reference of the bookmark node
    icon: str - icon data of the WEB page for the referenced URL
    keywords: array of strings - list of keywords (tags) of the referenced URL

Parent guid was excluded from url fields to avoid duplication with _id of the parent folder.

JSON schema validation performs dynamic verification of the database documents.

Version 3.1 is prepared for a local Mongo instance (mongod) for testing purposes.
"""

from pymongo import MongoClient, database, collection
from datetime import datetime
import uuid
import typing as t

from time_convert import stamp_to_object, stamp_to_string
from time_convert import stamp_to_object
import exceptions
from common import URL_FIELDS, FOLDER_FIELDS  # fields enabled to update
from schema_mongo import folder_json_schema  # JSON schema of validation

LOCAL_URI = 'localhost:27017'  # local connection for the mongod
DB_NAME = 'bookmarks'  # database name by default
COLLECTION_NAME = 'bm'

class ModelMongod:
    """Implementation of a Model module with standalone MongoDB instance.

    """
    def __init__(self, connect_timeout=200):
        """Construction method.

        :param connect_timeout: 'serverSelectionTimeoutMS' value, 200ms instead 30000ms by default from pymongo
        """

        # create a client and connect to the running MongoDB standalone server (mongod)
        self.client: MongoClient = MongoClient(LOCAL_URI, uuidRepresentation='standard',
                                               serverSelectionTimeoutMS=connect_timeout)
        self.db: database.Database = None  # type: ignore # database name
        self.bm: collection.Collection = None  # type: ignore # collection name
        self.cwd = ''  # for compatibility with the Model interface

    # ---- nodes section ----
    def _get_child_folders(self, folder_id: str) -> list[str]:
        """Return a list of child folders for the given folder.
        Search in the collection for the folders on their 'parent_guid'.

        :param folder_id:
        :return:
        """
        children = self.bm.find({'parent_guid': folder_id},  # search condition
                           {'_id': False, 'name': True}  # fetch names only
        )
        return [child['name'] for child in children]  # get a list from database cursor

    def get_children(self, node_name: str) -> tuple[bool, tuple[str, ...]]:
        """Get a list of child names of the node.
        Duplicate partially the code of the get_node() for querying children information only.
        It reduces server traffic.

        :exceptions: NodeNotExists if node_name does not exist

        :param node_name: name of a node
        :return: True/False, tuple of child's names/empty tuple
        """
        # search the folder 'node_name' in the collection
        node_content = self.bm.find_one({'name': node_name},  # search condition
                         {'_id': True, 'children': True}  # get _id and children list only
        )
        if node_content is not None:
            # folder 'name' found
            # make a list comprehension for the names of child url objects
            children = [child['name'] for child in node_content['children']]
            # 'children' has child url objects but has no child folder names
            children.extend(self._get_child_folders(node_content['_id']))  # add names of child folders
            return True, tuple(children)  # return folder flag True end children list
        else:
            # node 'name' is an url or does not exist
            # search 'name' among urls
            res = self.bm.find_one({'children.name': node_name},
                                   {'_id': True}  # no value needed, return something small to reduce traffic
            )
            if res is None:
                # this node does not exist
                raise exceptions.NodeNotExists(node_name)  # error exception
            # url 'node_name' found, it has no children
            return False, ()  # return url flag False and empty tuple

    def add_node(self, attr_dict: dict, node_type: bool):
        """Add a folder or url to the collection.
        Update 'date_modified' field of the parent folder if an url will be added

        :param attr_dict: dictionary with initial node attributes
        :param node_type: True for folder adding, False for url
        :return: nothing
        """
        add_doc = {}  # additional fields for the new node? folder or url
        # find 'parent_guid' from parent_name
        # search within folder docs only!
        res = self.bm.find_one({'name': attr_dict['parent_name']},
                                                                {'_id': True})  # return a {key, value} pair
        # get _id value explicitly because res type is ambiguous for mypy
        parent_guid = res['_id']  # type: ignore
        dates = datetime.utcnow()  # date_added and date_modified for nodes
        if 'id_no' in attr_dict:
            id_no = attr_dict['id_no']  # copy if exists
        else:
            id_no = 0
        common_doc = {'_id': uuid.uuid4(),
                      'name': attr_dict['name'],
                      'date_added': dates,
                      'id_no': id_no}  # common dictionary for mongo doc
        if node_type:
            # folder, create add_doc for the new folder
            add_doc = {'date_modified': dates,
                       'parent_guid': parent_guid,
                       'children': []}
        else:
            # url, create add_doc for the new url: copy or empty string
            if 'url' in attr_dict:
                add_doc['url'] = attr_dict['url']
            else:
                add_doc['url'] = ''
            if 'icon' in attr_dict:
                add_doc['icon'] = attr_dict['icon']
            else:
                add_doc['icon'] = ''
            if 'keywords' in attr_dict:
                add_doc['keywords'] = attr_dict['keywords']
            else:
                add_doc['keywords'] = ''

        # concatenate common and additional docs and insert the new node
        full_doc = common_doc | add_doc
        if node_type:
            # insert folder node to the collection
            result = self.bm.insert_one(full_doc)  # insert a folder is atomic (one doc operation)
        else:
            # add child url doc to the parent children list
            self.bm.update_one({'_id': parent_guid},
                               {'$set': {'date_modified': dates},  # update modify timestamp
                                '$push': {'children': full_doc}  # add new url object
                               }
            )  # insert an url is also atomic (one doc operation)

    def update_node(self, name: str, attr_dict: dict):
        """Update a folder or url of the bookmarks' database.
        Update operation is applied for one document only tp preserve atomic condition.
        User may update the following fields:
            - for folders: name
            - for urls: name, url, icon, keywords

        Other fields can be updated by internal routines only
        Field 'date_modified' of the parent folder updates automatically, if any child url was updated.
        Field 'children' can be modified only within this Model Mongo DB module.
        Field '_id' can not be updated.
        Full validation of the database schema was not implemented in this version.

        :param name: updating node name
        :param attr_dict: dictionary with the updating fields
        :return: nothing
        """
        # check if the node exists
        node_type, children = self.get_children(name)  # raise NodeNotExists if the node does not exist
        # if we are here then the node exists
        if node_type:
            # this is a folder
            update_dict = {k: v for (k, v) in attr_dict.items() if k in FOLDER_FIELDS }  # folder fields only
            if not update_dict:
                return  # return if all fields were shaven
            # update the document e.g. folder, date_modified of the parent folder remains unchanged
            res = self.bm.update_one(
                {'name': name},  # find condition
                {'$set': update_dict}  # updated fields
            )  # one single-document write operation is atomic
        else:
            # this is an url, name, url, icon, keywords allowed only
            update_dict = {'children.$.' + k: v for (k, v) in attr_dict.items() if k in URL_FIELDS}  # url fields only
            if not update_dict:
                return  # return if all fields were shaven
            update_dict['date_modified'] = datetime.utcnow()  # include new 'date_modified' to the mongodb expression
            # update operator
            res = self.bm.update_one(
                {'children.name': name},  # find condition
                # expression in the dict: name, url, icon, keywords fields of embedded url as 'children.$.key': value
                # and 'date_modified: new current timestamp for parent folder
                {'$set': update_dict}
            )  # one single-document write operation is atomic

    def delete_node(self, name: str):
        """Delete the node 'name' from the collection.
        Delete only one document, this is an atomic operation.
        Delete un url by removing its object from the children list of the parent folder.
        Update 'date_modified' of the parent folder after url deleting.
        Delete un folder (non-empty) by removing the folder document from the collection.
        After the folder is deleted, the parent date_modified field remains unchanged.

        :raises NodeNotExists: if node_name does not exist
        :raises FolderNotEmpty: if node_name folder is not empty

        :param name: node name to delete
        :return: nothing
        """
        # check if the node exists
        node_type, children = self.get_children(name)  # raise NodeNotExists if the node does not exist
        # if we are here then the node exists
        if node_type:
            # this is a folder, check it is empty or not
            if children:
                # non-empty folder
                raise exceptions.FolderNotEmpty(name)  # error
            # delete empty folder
            res = self.bm.delete_one({'name': name})  # one single-document write operation is atomic
        else:
            # this is an url, delete it from children list and set a new 'date_modified'
            result = self.bm.update_one(
                {'children.name': name},  # find condition
                {'$set': {'date_modified': datetime.utcnow()},
                 '$pull': {'children':
                               {'name': name}}  # children.name (dot notation) not allowed!!!
                }
            )  # one single-document write operation is atomic

    def get_node(self, name: str) -> dict:
        """Get all fields of the node.
        For folders replace children urls in the list with their names.

        :param name: node name
        :return: dictionary {field_name: field_value} of the node
        """
        # try to find folder 'name' from collection if it exists
        node_content = self.bm.find_one({'name': name})
        if node_content:
            # folder 'name' found
            # make a list comprehension for the names of child url objects
            children = [child['name'] for child in node_content['children']]
            # 'children' has child url objects but has no child folder names
            children.extend(self._get_child_folders(node_content['_id']))  # add names of child folders
            node_content['children'] = children  # set a list of names instead of objects
            node_content['date_modified'] = datetime.isoformat(node_content['date_modified'],
                                                               timespec='seconds')
            node_content['parent_guid'] = str(node_content['parent_guid'])

        else:
            # node 'name' is an url or does not exist
            # search 'name' among urls
            res = self.bm.find_one({'children.name': name},  # search in embedded urls by name
                                   {'_id': True,
                                    'children.$': True}  # get the found object from the parent children list
            )
            if res is None:
                # this node does not exist
                raise exceptions.NodeNotExists(name)  # error exception
            # url 'name' found, trim the key
            node_content = res['children'][0]  # get the clear dict without the dict key
            node_content['parent_guid'] = str(res['_id'])  # add 'parent_guid' field from folder '_id'
        # convert other common fields
        node_content['guid'] = str(node_content.pop('_id'))
        node_content['date_added'] = datetime.isoformat(node_content['date_added'],
                                                        timespec='seconds')
        return node_content



    # ---- database section ----
    def create_database(self, name: str):
        """Create a database and fill 'roots' document on the Mongo server.
        Connection established.

        :exceptions: FileExistsError if given filename exists

        :param name: name of the new database
        :return: nothing
        """
        # check if database 'name' already exists on the connected server
        if name in self.client.list_database_names():
            raise FileExistsError(name)  # database 'name' exists on the server

        self.db = self.client[name]  # set the new current database on server
        # create a collection (table)
        self.bm = self.db.create_collection(COLLECTION_NAME,
                                            validator=folder_json_schema)  # set collection for bookmarks storage
        # create a roots document
        guid = uuid.uuid4()  # guid for roots
        roots_date = datetime.utcnow()  # date_added and date_modified for roots
        roots_doc = {'_id': guid, 'name': 'roots', 'parent_guid': None,
                     'date_added': roots_date, 'id_no': 0,
                     'date_modified': roots_date, 'children': []}
        result = self.bm.insert_one(roots_doc)


    def open_database(self, name: str):
        """Open a database, read and extract it into a bookmark tree.

        :exception: FileNotFoundError if the filename does not exist

        :param name: name and filename of the deleting database
        :return: nothing
        """
        # check if database 'name' exists on the connected server
        if name not in self.client.list_database_names():
            raise FileNotFoundError(name)  # database 'name' does not exist on the server
        # create connection and open collection
        self.db = self.client[name]  # database
        self.bm = self.db[COLLECTION_NAME]  # collection

    def delete_database(self, name):
        """Delete the database file.

        :exception: FileNotFoundError if the filename does not exist

        :param name: name and filename of the deleting database
        :return: nothing
        """
        # check if database 'name' exists on the connected server
        if name not in self.client.list_database_names():
            raise FileNotFoundError(name)  # database 'name' does not exist on the server
        self.client.drop_database(name)  # delete database 'name' from the server
        self.db = None  # type: ignore # database name is undefined
        self.bm = None  # type: ignore # collection name is undefined




def main():
    """Test Mongo DB interface.

    """
    # create a client and connect to the running MongoDB server (mongod)
    client = MongoClient(LOCAL_URI, uuidRepresentation='standard')  # type: ignore
    db = client.testing  # connect to the existing database
    coll = db.my_tree  # connect to the existing collection  (aka table for SQL)

    # document presents in Python as a dictionary: by key, value pairs
    # document is an item aka row in SLQ
    doc1 = {'name': 'folder', 'type_id': True, 'children': [
        {'name': 'node1', '_id': 1}, {'name': 'node2', '_id': 2}
    ]}  # a document

    # insert a document into collection
    # into existing collection
    # insert_one() returns an instance of InsertOneResult
    # InsertOneResult().inserted_id returns an id of the inserted doc
    # InsertOneResult().acknowledged returns if writing was acknowledged (when concerning about writing result)
    doc1_id = coll.insert_one(doc1) # return an instance
    print(doc1_id, doc1_id.inserted_id, doc1_id.acknowledged)

    print(coll.find_one())  # fetch one result directly

    for d in coll.find({}):  # fetch all documents by cursor, not documents
        print(d)

    # ---- timestamp conversions ----
    # insert current UTC time
    # result = coll.insert_one(
    #     {'date_added': datetime.utcnow()}
    # )

    # result = coll.find_one({'date_added': {'$exists': True}})
    # Mongo Data BSON field represents any 64-bit number of millisecond from the Unix epoch
    # datetime.datetime object - within the range allowed by min and max attrs of the datetime module
    # Bookmarks manager truncates a time to seconds
    # bson.datetime_ms.DatetimeMS object has extended representation of Unix time then datetime limits

    epoch_type = 'Google'
    timestamp = 13097921382951728  # test value
    res = stamp_to_object(timestamp, epoch_type)
    res_str = stamp_to_string(timestamp, epoch_type)
    print(res_str)
    # insert the external timestamp
    result = coll.insert_one(
        {'date_added': res}
    )

    res1 = coll.find_one({'date_added': {'$exists': True}})
    print(res1)

    # ---- GUID (UUID) conversion ----
    guid = uuid.uuid4()  # get a random uuid in the native format
    # insert a native uuid
    result = coll.insert_one(
        {'guid': guid, 'name': 'test node'}
    )
    print(guid, str(guid))
    # fetch a native uuid
    res2 = coll.find_one({'guid': {'$exists': True}})
    print(res2)
    # convert guid from string
    str_guid = str(guid)  # string guid
    copy_guid = uuid.UUID(hex=str_guid)  # get the native guid from hex 32-digit string
    result = coll.insert_one(
        {'guid': copy_guid, 'name': 'test node'}
    )

if __name__ == '__main__':
    main()