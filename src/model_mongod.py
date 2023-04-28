"""A Model part of Bookmark Manager implemented with Mongo DB (document type).
There are 2 types of nodes: folder and url (as leaf node).
Datastorage schema: tree with parent field and children list for folder.
One document for each node, there are common fields and there are specific fields.
Common fields:
    _id: GUID (UUID4) of the node as 128-bit value (not as a string)
    name: str - name of the node, root of tree names 'roots'
    parent_guid: GUID of the parent node, 'roots' has Null(None for Python) value
    date_added: Date - timestamp when the node was created, internal Mongo format (N of ms from Unix epoch)
    id_no: int32 - legacy from Chrome bookmarks
Folder specific fields:
    children: array of objects - list of children nodes, [{name: 'name', _id: 'GUID'},,,]
    date_modified: Date - timestamp when the folder was modified, internal Mongo format
Url specific fields:
    url: str - URL reference of the bookmark node
    icon: str - icon data of the WEB page for the referenced URL
    keywords: array of strings - list of keywords (tags) of the referenced URL
Version 1 is prepared for a local Mongo instance (mongod) for testing purposes.
The database schema assumes the use of a multi-server application in the following implementation.



"""

from pymongo import MongoClient
import pymongo.errors as me
from datetime import datetime
import uuid
import typing as t

from time_convert import stamp_to_object, stamp_to_string
from time_convert import stamp_to_object
import exceptions

LOCAL_URI = 'localhost:27017'  # local connection for the mongod
DB_NAME = 'bookmarks'  # database name by default
COLLECTION_NAME = 'bm'

class ModelMongod:
    """Implementation of a Model module with standalone MongoDB instance.

    """
    def __init__(self):
        """Construction method.
        """

        # create a client and connect to the running MongoDB standalone server (mongod)
        self.client = MongoClient(LOCAL_URI, uuidRepresentation='standard')
        self.db = None  # name of the database
        self.bm = None  # name of the collection
        self.cwd = None # for compatibility with the Model interface

    # ---- nodes section ----
    def get_children(self, node_name: str) -> tuple[bool, tuple[str, ...]]:
        """Get a list of child names of the node.

        :exceptions: NodeNotExists if node_name does not exist

        :param node_name: name of a node
        :return: True/False, tuple of child's names/empty tuple
        """
        node_content = self.get_node(node_name)  # get node's fields as a dict
        if 'children' not in node_content:
            # this is an url
            return False, ()
        # this is a folder, return a list of child names, which can be empty
        return True, tuple(node_content['children'])

    def add_node(self, attr_dict: dict, node_type: bool):
        """Add a folder or url to the collection.

        :param attr_dict: dictionary with initial node attributes
        :param node_type: True for folder adding, False for url
        :return: nothing
        """
        add_doc = {}  # additional fields for the new node? folder or url
        # find 'parent_guid' from parent_name
        parent_guid = self.bm.find_one({'name': attr_dict['parent_name']},
                                       {'_id': True})  # return a dictionary
        dates = datetime.utcnow()  # date_added and date_modified for nodes
        if 'id_no' in attr_dict:
            id_no = attr_dict['id_no']  # copy if exists
        else:
            id_no = 0
        common_doc = {'_id': uuid.uuid4(),
                      'name': attr_dict['name'],
                      'parent_guid': parent_guid['_id'],
                      'date_added': dates,
                      'id_no': id_no}  # common dictionary for mongo doc
        if node_type:
            # folder, create add_doc for the new folder
            add_doc = {'date_modified': dates, 'children': []}
        else:
            # url, create add_doc for the new url
            add_doc['url'] = attr_dict['url']
            add_doc['icon'] = attr_dict['icon']
            add_doc['keywords'] = attr_dict['keywords']

        # concatenate common and additional docs and insert the new node
        full_doc = common_doc | add_doc
        result = self.bm.insert_one(full_doc)

        # add {name: _id} of the child to the parent children list
        self.bm.update_one(parent_guid,
                            {'$push':
                                {'children':
                                    {common_doc['name']: common_doc['_id']}
                                }
                            }
                           )

    def update_node(self, name: str, attr_dict: dict):
        """Update a folder or url of the collection.
        User may update the following fields:
            - for folders: name
            - for urls: name, url, icon, keywords
        Other fields can be updated by internal routines.
        Field 'date_modified' for the parent folder updates automatically, if any child was updates.
        Name of the child object in the children list of the parent node will update if child name was changed.
        Field 'children' can be modified only within this Model Mongo DB module.
        Field '_id' can not be updated.
        Full validation of incoming fields and database schema was not implemented in this version.

        :param name: updating node name
        :param attr_dict: dictionary with the updating fields
        :return: nothing
        """
        if not attr_dict:
            return  # return if empty attr_dict

        # check if the node exists
        res, children = self.get_children(name)  # raise NodeNotExists if the node does not exist
        # if we are here that node is in the collection, trim invalid fields if they present
        if res:
            # this is a folder, it is possible to change 'name' only
            update_dict = {k: v for (k, v) in attr_dict.items() if k == 'name' }  # only 'name'
        else:
            # this is an url, name, url, icon, keywords allowed only
            update_dict = {k: v for (k, v) in attr_dict.items()
                           if k in ('name', 'url', 'icon', 'keywords')}
        # update the document, return _id, old name and parent guid for the updated node
        old_fields = self.bm.find_one_and_update(
                    {'name': name},  # find condition
                    {'$set': update_dict},  # updated fields
                    {'name': True, '_id': True, 'parent_guid': True}  # projection of the returned doc
        )
        new_date = datetime.utcnow()  # new date_modified of parent folder
        # sync the parent folder of the updated document: date_modified and probably children list
        if 'name' in update_dict:
            # name was changed, update 'date_modified' and children list in one operation
            self.bm.update_one(
                {'_id': old_fields['parent_guid']},  # filter pos arg, find parent node by its _id
                {'$set': {  # update pos arg, update 2 items, k: v pairs separated by comma
                            'date_modified': new_date,  # a new date_modified of parent folder
                            'children.$[elem]':
                            {update_dict['name']: old_fields['_id']}  # put {'new name': _id} pair to the children array
                         }
                },
                array_filters=[{'elem':  # key args with array filter to find what we change
                                    {old_fields['name']: old_fields['_id']}  # filter by old child object
                              }]
            )
        else:
            # name is unchanged, update parent 'date_modified' only
            self.bm.update_one(
                {'_id': old_fields['parent_guid']},  # filter pos arg, find parent node by its _id
                {'$set': {'date_modified': new_date}}  # a new date_modified of parent folder
            )

    def delete_node(self, name: str):
        """Delete the node 'name' from the collection.
         Delete object from children list of the parent node.

        :raises NodeNotExists: if node_name does not exist
        :raises FolderNotEmpty: if node_name folder is not empty

        :param name: node name to delete
        :return: nothing
        """
        # check if the node exists
        res, children = self.get_children(name)  # raise NodeNotExists if the node does not exist
        # if we are here that node is in the collection, trim invalid fields if they present
        if res and children:
            # non-empty folder can not be deleted
            raise exceptions.FolderNotEmpty(name)  # error
        # an url or empty folder found, delete the node from the collection
        del_fields = self.bm.find_one_and_delete(
                    {'name': name},  # find condition
                    {'name': True, 'parent_guid': True}  # returned projection
        )
        # delete node's object from parent children list
        self.bm.update_one(
                        {'_id': del_fields['parent_guid']},  # find parent node
                        {'$pull':
                            {'children': {'$eq':
                                {del_fields['name']: del_fields['_id']}
                            }}
                        }
        )
    def get_node(self, name: str) -> dict:
        """Get a node content.
        Replace children objects with their names for folder children list

        :param name: node name
        :return: dictionary {field_name: field_value} of the node
        """
        # fetch node's fields from collection if it exist
        node_content = self.bm.find_one({'name': name})
        if node_content is None:
            raise exceptions.NodeNotExists(name)  # error exception
        # ok, the node exists
        # convert common fields first
        node_content['guid'] = str(node_content.pop('_id'))
        node_content['parent_guid'] = str(node_content['parent_guid'])
        node_content['date_added'] = datetime.isoformat(node_content['date_added'],
                                                        timespec='seconds')
        # convert folder's fields
        if 'children' in node_content:
            node_content['date_modified'] = datetime.isoformat(node_content['date_modified'],
                                                               timespec='seconds')

            # make a list comprehension for the first keys (=names) of child node objects
            children = [next(iter(child)) for child in node_content['children']]
            node_content['children'] = children  # return a list of names, not objects

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

        # a new database created but invisible until the first record to it
        self.db = self.client[name]  # set the new current database on server
        # create a collection (table)
        self.bm = self.db[COLLECTION_NAME]  # set collection for bookmarks storage
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
        self.db = None  # database name is undefined
        self.bm = None  # collection name is undefined




def main():
    """Test Mongo DB interface.

    """
    client = MongoClient(LOCAL_URI, uuidRepresentation='standard')  # create a client and connect to the running MongoDB server (mongod)
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

    result = coll.find_one({'date_added': {'$exists': True}})
    print(result)

    # ---- GUID (UUID) conversion ----
    guid = uuid.uuid4()  # get a random uuid in the native format
    # insert a native uuid
    result = coll.insert_one(
        {'guid': guid, 'name': 'test node'}
    )
    print(guid, str(guid))
    # fetch a native uuid
    result = coll.find_one({'guid': {'$exists': True}})
    print(result)
    # convert guid from string
    str_guid = str(guid)  # string guid
    copy_guid = uuid.UUID(hex=str_guid)  # get the native guid from hex 32-digit string
    result = coll.insert_one(
        {'guid': copy_guid, 'name': 'test node'}
    )

if __name__ == '__main__':
    main()