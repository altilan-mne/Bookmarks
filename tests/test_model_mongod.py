"""Test module for the Model module of Bookmark Manager, local standalone mongod implementation.
Uses fixture freeze_uuids from pytest_frozen_uuids plugin: https://pypi.org/project/pytest-frozen-uuids/
Uses fixture freezer from pytest-freezer plugin: https://github.com/pytest-dev/pytest-freezer

"""
import typing

import pytest
import uuid
import sys, time
from datetime import datetime

from pymongo import MongoClient, database, collection
import pymongo.errors as pme



from model_mongod import ModelMongod
from model_mongod import LOCAL_URI, COLLECTION_NAME
from model_mongod import URL_FIELDS, FOLDER_FIELDS
import exceptions as e

CONNECT_TIMEOUT = 200  # 200 ms for connection to the local mongod server
DB_NAME = 'test_db'  # name of the database for tests
TEST_COLLECTION = 'test_collection'


@pytest.fixture(scope='session')
def mongo_client():
    """Create a connection to a standalone mongod, expose it outside and close the connection at the end of the test.
    Test if mongod is responding to a ping request.

    """
    # setup
    client = MongoClient(LOCAL_URI, uuidRepresentation='standard',
                                               serverSelectionTimeoutMS=CONNECT_TIMEOUT)
    time.sleep (0.1)  # timeout to establish the connection
    try:
        # The ping command is cheap and does not require auth.
        client.admin.command('ping')
    except pme.ConnectionFailure:
        pytest.fail("MongoDB server not available")
    yield client
    # teardown
    client.close()

@pytest.fixture(scope='session')
def model_mongod():
    """Creqte a new instance of ModelModgod class, expose it and close after tests.
    Test of the ModelMongod instance creation should be already passed.

    """
    # setup
    md = ModelMongod()  # create a new model instance with a connection to mongod
    time.sleep(0.1)  # timeout to establish the connection
    try:
        # The ping command is cheap and does not require auth.
        md.client.admin.command('ping')
    except pme.ConnectionFailure:
        pytest.fail("MongoDB server not available")
    yield md
    # teardown
    md.client.close()


@pytest.fixture(scope='function')
def mongo_db(model_mongod):
    """Create database 'test_db' and collection 'test_collection', expose it and delete database at the end of the test.
    Test of the database creation should be already passed.

    """
    # setup, connection has been established by model_mongod instance
    model_mongod.create_database(DB_NAME)  # create a test database of the model instance
    yield model_mongod
    # teardown
    model_mongod.client.drop_database(DB_NAME)

@pytest.fixture(scope='function')
def folder1(mongo_db):
    """Create Folder1 with roots parent node, expose and delete db at the end of the test

    :param mongo_db: ModelMongod instance and and test db with roots document
    :return: db instance with Folder1
    """
    # setup
    attr_dict = {'name': 'Folder1', 'parent_name': 'roots'}  # min args set, other fields by default
    mongo_db.add_node(attr_dict, True)  # add Folder1 to roots
    return mongo_db  # return updated instance


@pytest.fixture(scope='function')
def folder1_url11(folder1):
    """Create Folder1 and child of Folder1 node Url11, expose and delete.

    :param folder1: ModelMongod instance and and test db with roots and Folder1

    """
    attr_dict = {'name': 'Url11', 'parent_name': 'Folder1'}  # min args set, other fields by default
    folder1.add_node(attr_dict, False)  # add Folder1 to roots
    return folder1  # return updated instance


def _make_fivefolders_foururls(model_mongod):
    """Create a test set of documents from 5 folders and 4 urls, see below:
             roots
      /        |      |    |
    Folder1 Folder2 Url1 Url2
      |        |
    Url11   Folder3
               |      |
            Folder4  Url31

    """
    # setup, connection has been established by model_mongod instance
    attr_dict = {'name': 'Folder1', 'parent_name': 'roots'}  # min args set, other fields by default
    model_mongod.add_node(attr_dict, True)  # Folder2 to roots
    attr_dict = {'name': 'Url11', 'parent_name': 'Folder1'}  # min args set, other fields by default
    model_mongod.add_node(attr_dict, False)  # add Folder1 to roots
    attr_dict = {'name': 'Folder2', 'parent_name': 'roots'}  # min args set, other fields by default
    model_mongod.add_node(attr_dict, True)  # Folder2 to roots
    attr_dict = {'name': 'Url1', 'parent_name': 'roots'}  # min args set, other fields by default
    model_mongod.add_node(attr_dict, False)  # add Url1 to roots
    attr_dict = {'name': 'Url2', 'parent_name': 'roots'}  # min args set, other fields by default
    model_mongod.add_node(attr_dict, False)  # add Url2 to roots
    attr_dict = {'name': 'Folder3', 'parent_name': 'Folder2'}  # min args set, other fields by default
    model_mongod.add_node(attr_dict, True)  # add Folder3 to Folder2
    attr_dict = {'name': 'Folder4', 'parent_name': 'Folder3'}  # min args set, other fields by default
    model_mongod.add_node(attr_dict, True)  # add Folder4 to Folder3
    attr_dict = {'name': 'Url31', 'parent_name': 'Folder3'}  # min args set, other fields by default
    model_mongod.add_node(attr_dict, False)  # add Url31 to Folder3

@pytest.fixture(scope='class')
def class_5f_4u(model_mongod):
    """Return 5 folders 4 urls with class scope."""
    # setup
    model_mongod.create_database(DB_NAME)  # create a test database of the model instance
    _make_fivefolders_foururls(model_mongod)  # fill db
    yield model_mongod
    # teardown
    model_mongod.client.drop_database(DB_NAME)

@pytest.fixture(scope='function')
def function_5f_4u(model_mongod):
    """Return 5 folders 4 urls with function scope."""
    # setup
    model_mongod.create_database(DB_NAME)  # create a test database of the model instance
    _make_fivefolders_foururls(model_mongod)  # fill db
    yield model_mongod
    # teardown
    model_mongod.client.drop_database(DB_NAME)

def test_init_instance():
    """Test of a new ModelMongod instance creation.

    """
    md = ModelMongod()  # create a new model instance with a connection to mongod
    time.sleep(0.1)  # timeout to establish the connection
    try:
        # The ping command is cheap and does not require auth.
        md.client.admin.command('ping')
    except pme.ConnectionFailure:
        pytest.fail("MongoDB server is not available")
    addr_port = LOCAL_URI.split(":", 2)  # list [addr, port] og the connection
    address = (addr_port[0], int(addr_port[1]))  # tuple with address and port of connection
    assert md.client.address == address
    assert md.client.topology_description.topology_type_name == 'Single'
    # get the ServerDescription object
    serv_descr = md.client.topology_description.server_descriptions()[address]
    assert serv_descr.server_type_name == 'Standalone'
    assert md.db is None
    assert md.bm is None
    md.client.close()

class TestCreateDB:
    """Class of database creation tests.
    Test of the ModelMongod instance creation should be already passed.

    """
    def test_create_db(self, model_mongod):
        """Test of create_database method, main path.

       :param model_mongod: fixture of the ModelMongod instance
        """
        model_mongod.create_database(DB_NAME)  # create database
        assert model_mongod.db.name == DB_NAME  # database name
        assert model_mongod.bm.name == 'bm'  # collection name
        model_mongod.client.drop_database(DB_NAME)  # delete test db

    def test_dbname_exists(self, mongo_db):
        """Test to create duplicate database name.

        :param mongo_db: fixture of the test database instance
        """
        with pytest.raises(FileExistsError, match=DB_NAME):
            mongo_db.create_database(DB_NAME)

    def test_create_roots(self, model_mongod, freeze_uuids, freezer):
        """Roots record's creation test.

        :param model_mongod: fixture of the ModelMongod instance
        :param freeze_uuids: fixture of a fake uuid
        :param freezer: fixture of a fake datetime

        """
        now = datetime.utcnow()  # get fake datetime
        milliseconds = now.microsecond // 1000 * 1000  # trim to milliseconds
        now = now.replace(microsecond=milliseconds)  # mongodb keeps milliseconds only
        model_mongod.create_database(DB_NAME)  # create test db
        result = model_mongod.bm.find_one()  # get roots document
        assert result['name'] == 'roots'
        assert str(result['_id']) == "00000000-0000-0000-0000-000000000001"  # WHY? must be 0, side effect from freezer
        assert result['date_added'] == now
        assert result['date_modified'] == now
        assert result['parent_guid'] is None
        assert result['id_no'] == 0
        assert result['children'] == []
        model_mongod.client.drop_database(DB_NAME)  # delete the test db

class TestDeleteDB:
    """Class of database deleting tests."""

    def test_delete_db(self, mongo_db):
        """Test of delete_database method, main path.

       :param mongo_db: fixture of the database and connection
        """
        mongo_db.delete_database(DB_NAME)  # test delete function, database exists
        assert DB_NAME not in mongo_db.client.list_database_names()  # test_db has been deleted
        assert mongo_db.db is None  # database name reset
        assert mongo_db.bm is None  # collection name reset

    def test_delete_nonedb(self, model_mongod):
        """Try to delete non-existing database.

        :param model_mongod: fixture of the connection to mongodb instance
        """
        with pytest.raises(FileNotFoundError, match=DB_NAME):
            model_mongod.delete_database(DB_NAME)

class TestOpenDB:
    """Class of database opening tests."""

    def test_open_db(self, mongo_db):
        """Test of open_database method, main path.

        :param mongo_db: fixture of the connection and database
        """
        # database DB_NAME has been created by fixture
        md = ModelMongod()  # get a new ModelMongod instance, create a second connection
        time.sleep(0.1)  # timeout to establish the connection

        md.open_database(DB_NAME)  # open the existing db
        assert md.db.name == DB_NAME  # check db name
        assert md.bm.name == COLLECTION_NAME  # check collection name
        md.client.close()  # close the second connection

    def test_open_nonedb(self, model_mongod):
        """Try to open non-existing database.

        :param model_mongod: fixture of the connection to mongodb instance
        """
        with pytest.raises(FileNotFoundError, match=DB_NAME):
            model_mongod.open_database(DB_NAME)


class TestAddNode:
    """Class of node adding tests."""

    def test_add_folder_default(self, mongo_db, freeze_uuids, freezer):
        """Folder creation test.

        :param mongo_db: fixture of the empty db with roots folder
        :param freeze_uuids: fixture of a fake uuid
        :param freezer: fixture of a fake datetime
        """
        node_type = True  # folder flag set
        attr_dict = {'name': 'Folder1', 'parent_name': 'roots'}  # min args set, other fields by default
        parent_guid = mongo_db.bm.find_one({'name': attr_dict['parent_name']},
                                           {'_id': True})  # get a parent guid

        now = datetime.utcnow()  # get fake datetime
        milliseconds = now.microsecond // 1000 * 1000  # trim to milliseconds
        now = now.replace(microsecond=milliseconds)  # mongodb keeps milliseconds only

        mongo_db.add_node(attr_dict, node_type)  # add Folder1 to roots
        result = mongo_db.bm.find_one({'name': 'Folder1'})  # get Folder1 document
        assert result['name'] == 'Folder1'
        assert str(result['_id']) == "00000000-0000-0000-0000-000000000001"  # WHY? must be 0, side effect from freezer
        assert result['date_added'] == now
        assert result['date_modified'] == now
        assert result['parent_guid'] == parent_guid['_id']
        assert result['id_no'] == 0
        assert result['children'] == []

    def test_add_url_default(self, mongo_db, freeze_uuids, freezer):
        """Url creation test.

        :param mongo_db: fixture of the empty db with roots folder
        :param freeze_uuids: fixture of a fake uuid
        :param freezer: fixture of a fake datetime

        """
        node_type = False  # url flag set
        attr_dict = {'name': 'Url1', 'parent_name': 'roots'}  # min args set, other fields by default
        parent_guid = mongo_db.bm.find_one({'name': attr_dict['parent_name']},
                                           {'_id': True})  # get a parent guid
        old_date_modified = mongo_db.bm.find_one(parent_guid,
                                                 {'_id': False,
                                                  'date_modified': True})  # get initial date_modified field of the parent

        now = datetime.utcnow()  # get fake datetime
        milliseconds = now.microsecond // 1000 * 1000  # trim to milliseconds
        now = now.replace(microsecond=milliseconds)  # mongodb keeps milliseconds only

        mongo_db.add_node(attr_dict, node_type)  # add Url1 to roots
        res = mongo_db.bm.find_one({'children.name': attr_dict['name']},
                                   {'_id': True, 'children.$': True}  # get embedded url from roots children
        )

        result = res['children'][0]  # get the clear fields dict, check this dict
        assert result['name'] == 'Url1'
        assert str(result['_id']) == "00000000-0000-0000-0000-000000000001"  # WHY? must be 0, side effect from freezer
        assert result['date_added'] == now
        assert result['id_no'] == 0
        assert result['url'] == ''
        assert result['icon'] == ''
        assert result['keywords'] == ''

        date_modified = mongo_db.bm.find_one(parent_guid,
                            {'_id': False, 'date_modified': True})  # get date_modified field of the parent folder
        assert date_modified['date_modified'] == now
        assert date_modified['date_modified'] > old_date_modified['date_modified']  # check date_modified increment

    # ---- optional fields setting tests ----
    folder_fields = {
        'id_no': 777,
    }  # optional fields possible when adding a folder

    @pytest.mark.parametrize('key, value', folder_fields.items())  # parameters for the test

    def test_add_folder_options(self, mongo_db, key, value):
        """Test to add folder fields with optional values.

        :param mongo_db: fixture of the empty db with roots folder
        :param key, value: marker with optional folder fields
        :return:
        """
        node_type = True  # folder flag set
        attr_dict = {'name': 'Folder1', 'parent_name': 'roots'}  # min args set, other fields by default
        attr_dict[key] = value  # add optional field
        mongo_db.add_node(attr_dict, node_type)  # add Folder1 with an optional field to roots
        result = mongo_db.bm.find_one({'name': 'Folder1'})  # get Folder1 document
        assert result[key] == value

    url_fields = {
        'id_no': 666,
        'url': 'http://www.google.com',
        'icon': 'ICON field',
        'keywords': 'bla bla bla'
    }  # optional fields possible when adding a folder
    @pytest.mark.parametrize('key, value', url_fields.items())  # parameters for the test

    def test_add_url_options(self, mongo_db, key, value):
        """Test to add url fields with optional values.

        :param mongo_db: fixture of the empty db with roots folder
        :param key, value: marker with optional url fields
        :return:
        """
        node_type = False  # url flag set
        attr_dict = {'name': 'Url1', 'parent_name': 'roots'}  # min args set, other fields by default
        attr_dict[key] = value  # add optional field
        mongo_db.add_node(attr_dict, node_type)  # add Url1 with an optional field to roots
        res = mongo_db.bm.find_one({'children.name': attr_dict['name']},
                                   {'_id': True, 'children.$': True})  # get embedded url from roots children
        result = res['children'][0]  # get the clear fields dict, check this dict
        assert result[key] == value


class TestGetChildren:
    """Class of get_children and _get_child_folders methods testing with parametrization.
    Create a test set of documents from 5 folders and 4 urls, see below:
             roots
      /        |      |    |
    Folder1 Folder2 Url1 Url2
      |        |
    Url11   Folder3
               |      |
            Folder4  Url31

    """
    @pytest.mark.parametrize('folder, children',
                             [
                                 ('roots', ['Folder1', 'Folder2']),
                                 ('Folder1', []),
                                 ('Folder2', ['Folder3']),
                                 ('Folder3', ['Folder4']),
                                 ('Folder4', []),
                             ])
    def test_get_child_folders(self, class_5f_4u, folder, children):
        """Testing of _get_child_folders method.

        :param class_5f_4u: fixture db with 5 folders and 4 urls
        :param folder: names of the testing folders
        :param children: lists of the folder's children

        """
        folder_id = class_5f_4u.bm.find_one({'name': folder})  # get folder guid
        assert set(class_5f_4u._get_child_folders(folder_id['_id'])) == set(children)  # compare w/o order

    @pytest.mark.parametrize('node, children',
                             [
                                 ('roots', (True, {'Folder1', 'Folder2', 'Url1', 'Url2'})),
                                 ('Folder1', (True, {'Url11'})),
                                 ('Folder2', (True, {'Folder3'})),
                                 ('Folder3', (True, {'Folder4', 'Url31'})),
                                 ('Folder4', (True, set())),
                                 ('Url1', (False, set())),
                                 ('Url2', (False, set())),
                                 ('Url11', (False, set())),
                                 ('Url31', (False, set())),
                             ])

    def test_get_children(self, class_5f_4u, node, children):
        """Testing of get_children method, without errors.

        :param class_5f_4u: fixture db with 5 folders and 4 urls
        :param node: names of the testing nodes
        :param children: lists of the node's children

        """
        res, actual_children = class_5f_4u.get_children(node)
        assert (res, set(actual_children)) == children  # compare w/o order

    def test_get_children_no_node(self, class_5f_4u):
        """Try to get children from non-existing node.

        :param class_5f_4u: fixture db with 5 folders and 4 urls

        """
        with pytest.raises(e.NodeNotExists, match='unknown'):
            class_5f_4u.get_children('unknown')

class TestDeleteNode:
    """Class of node deleting tests."""

    def test_delete_folder(self, folder1):
        """Test of the folder delete, without errors.

        :param folder1: fixture with roots and Folder1
        :return:
        """
        old_date_modified = folder1.bm.find_one({'name': 'roots'},
                                                 {'_id': False,
                                                  'date_modified': True})  # get initial date_modified of the parent
        folder1.delete_node('Folder1')  # delete Folder1
        res = folder1.bm.find_one({'name': 'Folder1'})
        assert res is None
        new_date_modified = folder1.bm.find_one({'name': 'roots'},
                                                 {'_id': False,
                                                  'date_modified': True})  # get final date_modified of the parent
        assert new_date_modified['date_modified'] == old_date_modified['date_modified']  # date_modified unchanged?

    def test_delete_url(self, folder1_url11):
        """Test of the url delete, without errors.

        :param folder1_url11: fixture with roots, Folder1 and its child Url11
        """
        old_date_modified = folder1_url11.bm.find_one({'name': 'Folder1'},
                                                {'_id': False,
                                                 'date_modified': True})  # get initial date_modified of the parent
        res1 = folder1_url11.bm.find_one({'children.name': 'Url11'})
        assert res1 is not None  # child Url11 exists

        folder1_url11.delete_node('Url11')  # delete Url11

        res2 = folder1_url11.bm.find_one({'children.name': 'Url11'})
        assert res2 is None  # Url11 disappeared

        new_date_modified = folder1_url11.bm.find_one({'name': 'Folder1'},
                                                {'_id': False,
                                                 'date_modified': True})  # get final date_modified of the parent
        assert new_date_modified['date_modified'] > old_date_modified['date_modified']  # date_modified increment


    def test_delete_nonempty_folder(self, folder1_url11):
        """Try to delete non-empty folder.

        :param folder1_url11: fixture with roots, Folder1 and its child Url11
        """
        with pytest.raises(e.FolderNotEmpty, match='Folder1'):  # a node with Folder2 name does not exist
            folder1_url11.delete_node('Folder1')

    def test_delete_no_node(self, folder1):
        """Test of the non-existing node delete.

        :param folder1: fixture with roots and Folder1
        :return:
        """
        with pytest.raises(e.NodeNotExists, match='Folder2'):  # a node with Folder2 name does not exist
            folder1.delete_node('Folder2')

class TestGetNode:
    """Class of the tests of get_node method."""

    def test_get_folder(self, mongo_db, freeze_uuids, freezer):
        """Test of the get_node method for folder.

        :param mongo_db: db fixture of db instance with roots
        :param freeze_uuids: fixture to freeze randon UUID generation
        :param freezer: fixture to freeze datetime
        :return:
        """
        node_type = True  # folder flag set
        attr_dict = {'name': 'Folder1', 'parent_name': 'roots',
                     'id_no': 555,
                     }  # args set, other fields by default
        parent_guid = mongo_db.bm.find_one({'name': attr_dict['parent_name']},
                                           {'_id': True})  # get a parent guid

        now = datetime.utcnow()  # get fake datetime
        milliseconds = now.microsecond // 1000 * 1000  # trim to milliseconds
        now = now.replace(microsecond=milliseconds)  # for mongodb format od datetime
        mongo_db.add_node(attr_dict, node_type)  # add Folder1 to roots
        result = mongo_db.get_node('Folder1')
        assert result['name'] == 'Folder1'
        assert result['guid'] == "00000000-0000-0000-0000-000000000001"  # guid is returning as a string
        assert result['date_added'] == datetime.isoformat(now, timespec='seconds')  # format of the get_node
        assert result['date_modified'] == datetime.isoformat(now, timespec='seconds')  # format of the get_node
        assert result['parent_guid'] == str(parent_guid['_id'])  # format of the get_node is a string
        assert result['id_no'] == 555
        assert result['children'] == []

    def test_get_url(self, mongo_db, freeze_uuids, freezer):
        """Test of the get_node method for url.

        :param mongo_db: db fixture of db instance with roots
        :param freeze_uuids: fixture to freeze randon UUID generation
        :param freezer: fixture to freeze datetime
        :return:
        """
        node_type = False  # folder flag set
        attr_dict = {'name': 'Url1', 'parent_name': 'roots',
                     'id_no': 999,
                     'url': 'http://www.microsoft.com',
                     'icon': 'ICON',
                     'keywords': 'ti ta ta'
                     }  # args set, other fields by default
        parent_guid = mongo_db.bm.find_one({'name': attr_dict['parent_name']},
                                           {'_id': True})  # get a parent guid

        now = datetime.utcnow()  # get fake datetime
        milliseconds = now.microsecond // 1000 * 1000  # trim to milliseconds
        now = now.replace(microsecond=milliseconds)  # for mongodb format od datetime
        mongo_db.add_node(attr_dict, node_type)  # add Url1 to roots
        result = mongo_db.get_node('Url1')
        assert result['name'] == 'Url1'
        assert result['guid'] == "00000000-0000-0000-0000-000000000001"  # guid is returning as a string
        assert result['date_added'] == datetime.isoformat(now, timespec='seconds')  # format of the get_node
        assert result['parent_guid'] == str(parent_guid['_id'])  # format of the get_node is a string
        assert result['id_no'] == attr_dict['id_no']
        assert result['url'] == attr_dict['url']
        assert result['icon'] == attr_dict['icon']
        assert result['keywords'] == attr_dict['keywords']

    def test_get_no_node(self, mongo_db):
        """Try to get fields of the non-existing node.

        :param mongo_db: db fixture of db instance with roots
        :return:
        """
        with pytest.raises(e.NodeNotExists, match='unknown'):  # a node with Folder2 name does not exist
            mongo_db.get_node('unknown')

class TestUpdateNode:
    """Tests of the update_node method.
    Create a test set of documents from 5 folders and 4 urls, see below:
             roots
      /        |      |    |
    Folder1 Folder2 Url1 Url2
      |        |
    Url11   Folder3
               |      |
            Folder4  Url31
    """
    @pytest.fixture(params=FOLDER_FIELDS)
    def folder_field(self, request):
        """Fixture to return a dictionary with updating fields, parametrize in the test method."""
        key = request.param
        attr_dict = {key: 'updated_value'}
        yield attr_dict  # expose a dict

    def test_update_folder(self, function_5f_4u, folder_field):
        """Test of a folder update, w/o errors.

        :param function_5f_4u: fixture db with 5 folders and 4 urls
        :return:
        """
        folder_name = 'Folder1'  # manipulate with Folder1
        old_parent_date_modified = function_5f_4u.bm.find_one({'name': 'roots'},
                                                              {'_id': False, 'date_modified': True})  # get old value
        field_key = next(iter(folder_field))  # get the first key of an input dict, i.e. field name
        function_5f_4u.update_node(folder_name, folder_field)  # update Folder1
        if field_key == 'name':
            folder_name = folder_field[field_key]  # field 'name' was changed then search the folder with the new name
        result = function_5f_4u.bm.find_one({'name': folder_name},
                                                  {'_id': False, field_key: True})  # get changed field only
        assert result == folder_field  # compare as dictionary
        new_parent_date_modified = function_5f_4u.bm.find_one({'name': 'roots'},
                                                              {'_id': False, 'date_modified': True})  # get new value
        assert new_parent_date_modified == old_parent_date_modified  # date_modified is not changed, compare dicts

    def test_update_frozen_folder(self, function_5f_4u):
        """ Try to update frozen or non-existing folder fields.
        Fields not included into FOLDER_FIELDS must be shaved by the method.

        :param function_5f_4u: fixture db with 5 folders and 4 urls
        :return:
        """
        folder_name = 'Folder1'
        now = datetime.utcnow()  # get datetime
        attr_dict = {'_id': uuid.uuid4(), 'parent_guid': uuid.uuid4(),'date_added': now, 'date_modified': now,
                     'id_no': 2048, 'children': [],
                     'non_existing_field': None,
                     }  # dict with frozen and non-existing fields
        old_folder = function_5f_4u.bm.find_one({'name': 'Folder1'})  # get the whole old document of Folder1
        function_5f_4u.update_node(folder_name, attr_dict)  # try to update the folder
        new_folder = function_5f_4u.bm.find_one({'name': 'Folder1'})  # get the whole new document of Folder1
        assert new_folder == old_folder  # compare whole dicts


    @pytest.fixture(params=URL_FIELDS)
    def url_field(self, request):
        """Fixture to return a dictionary with updating fields, parametrize in the test method."""
        key = request.param
        attr_dict = {key: 'updated_value'}
        yield attr_dict  # expose a dict

    def test_update_url(self, function_5f_4u, url_field):
        """Test of a url update, w/o errors.

        :param function_5f_4u: fixture db with 5 folders and 4 urls
        :return:
        """
        url_name = 'Url1'  # manipulate with Url1
        old_parent_date_modified = function_5f_4u.bm.find_one({'children.name': url_name},
                                                              {'_id': False, 'date_modified': True})  # get old value
        field_key = next(iter(url_field))  # get the first key of an input dict, i.e. a field name
        function_5f_4u.update_node(url_name, url_field)  # update Folder1
        if field_key == 'name':
            url_name = url_field[field_key]  # if field 'name' was changed then search the url with the new name
        res = function_5f_4u.bm.find_one({'children.name': url_name},
                                                  {'_id': False, 'children.$': True})  # get the whole changed url
        result = res['children'][0]  # get the clear fields dict, check this dict
        assert result[field_key] == url_field[field_key]  # compare changed field as values

        new_parent_date_modified = function_5f_4u.bm.find_one({'children.name': url_name},
                                                          {'_id': False, 'date_modified': True})  # get new value
        assert new_parent_date_modified['date_modified'] > old_parent_date_modified['date_modified']  # new > old

    def test_update_frozen_url(self, function_5f_4u):
        """ Try to update frozen or non-existing url fields.
        Fields not included into URL_FIELDS must be shaved by the method.

        :param function_5f_4u: fixture db with 5 folders and 4 urls
        :return:
        """
        url_name = 'Url1'
        now = datetime.utcnow()  # get datetime
        attr_dict = {'_id': uuid.uuid4(), 'parent_guid': uuid.uuid4(), 'date_added': now, 'date_modified': now,
                     'id_no': 2048, 'children': [],
                     'non_existing_field': None,
                     }  # dict with frozen and non-existing fields
        old_parent_date_modified = function_5f_4u.bm.find_one({'children.name': url_name},
                                                              {'_id': False, 'date_modified': True})  # get old value
        old_folder = function_5f_4u.bm.find_one({'children.name': 'Url1'},
                                                {'_id': False, 'children.$': True},
        )  # get the whole old document of Url1
        function_5f_4u.update_node(url_name, attr_dict)  # try to update the url
        new_folder = function_5f_4u.bm.find_one({'children.name': 'Url1'},
                                                {'_id': False, 'children.$': True},
        )  # get the whole new document of Url1
        assert new_folder == old_folder  # compare whole dicts
        new_parent_date_modified = function_5f_4u.bm.find_one({'children.name': url_name},
                                                              {'_id': False, 'date_modified': True})  # get new value
        assert new_parent_date_modified['date_modified'] == old_parent_date_modified['date_modified']  # new eq old

    def test_update_no_node(self, mongo_db):
        """Try to update fields of the non-existing node.

        :param mongo_db: db fixture of db instance with roots
        :return:
        """
        attr_dict = {'name': 'unknown'}  # non-empty dict for update_node method
        with pytest.raises(e.NodeNotExists, match='unknown'):  # a node with this name does not exist
            mongo_db.update_node('unknown', attr_dict)


class TestSchemaValidation:
    """Tests of JSON schema validation.
    Validation schema presents in the schema_mongo.py file

    """
    folder_dict = {'_id': uuid.uuid4(),
                   'name': 'new folder',
                   'date_added': datetime.utcnow(),
                   'id_no': 1,
                   'parent_guid': uuid.uuid4(),
                   'date_modified': datetime.utcnow(),
                   'children': [],
    }  # this is a good folder document

    url_dict = {'_id': uuid.uuid4(),
                'name': 'new url',
                'date_added': datetime.utcnow(),
                'id_no': 1,
                'url': 'http://www.test.com',
                'icon': 'ICON',
                'keywords': 'bla bla',
    }  # this is a good url document

    @pytest.mark.parametrize('no_folder_field', folder_dict.keys())
    def test_folder_no_field(self, mongo_db, no_folder_field):
        """ Try to insert a folder without required field.

        :param mongo_db: fixture with ModelMongo() instance and roots
        :param no_folder_field: fright folder document
        :return:
        """
        wrong_dict = self.folder_dict.copy()
        del wrong_dict[no_folder_field]
        with pytest.raises(pme.WriteError, match='Document failed validation*') as e:
            mongo_db.bm.insert_one(wrong_dict)

        mes2 = e.value.details['errInfo']['details']['schemaRulesNotSatisfied'][0]  # the dict with error text
        if no_folder_field == '_id':  # mongodb tries insert an own ObjectID instead of UUID, so it is a type error
            assert mes2['propertiesNotSatisfied'][0]['propertyName'] == no_folder_field
        else:  # missing properties error occurs for the other fields
            assert mes2['missingProperties'][0] == no_folder_field

    @pytest.mark.parametrize('no_folder_field', folder_dict.keys())
    def test_folder_wrong_type(self, mongo_db, no_folder_field):
        """ Try to insert a folder with bad type of the field.

        :param mongo_db: fixture with ModelMongo() instance and roots
        :param no_folder_field: right folder document
        :return:
        """
        wrong_dict = self.folder_dict.copy()
        wrong_dict[no_folder_field] = 2.0  # float is a wrong type for all fields
        with pytest.raises(pme.WriteError, match='Document failed validation*') as e:
            mongo_db.bm.insert_one(wrong_dict)

        mes2 = e.value.details['errInfo']['details']['schemaRulesNotSatisfied'][0]  # the dict with error text
        assert mes2['propertiesNotSatisfied'][0]['propertyName'] == no_folder_field

    def test_folder_extra_field(self, mongo_db):
        """ Try to insert a folder with an excessive field.

        :param mongo_db: fixture with ModelMongo() instance and roots
        :param no_folder_field: right folder document
        :return:
        """
        wrong_dict = self.folder_dict.copy()
        wrong_dict['excessive'] = 'intruder'  # add an extra field to the dict
        with pytest.raises(pme.WriteError, match='Document failed validation*') as e:
            mongo_db.bm.insert_one(wrong_dict)

        mes2 = e.value.details['errInfo']['details']['schemaRulesNotSatisfied'][0]  # the dict with error text
        assert mes2['additionalProperties'][0] == 'excessive'

    @pytest.mark.parametrize('no_url_field', url_dict.keys())
    def test_url_no_field(self, mongo_db, no_url_field):
        """ Try to insert an url without required field.

        :param mongo_db: fixture with ModelMongo() instance and roots
        :param no_url_field: right url document
        :return:
        """
        wrong_dict = self.url_dict.copy()
        del wrong_dict[no_url_field]
        with pytest.raises(pme.WriteError, match='Document failed validation*') as e:
            mongo_db.bm.update_one({'name': 'roots'},
                                   {'$push': {'children': wrong_dict}})

        mes1 = e.value.details['errInfo']['details']['schemaRulesNotSatisfied'][0]  # the dict with error text
        mes2 = mes1['propertiesNotSatisfied'][0]['details'][0]['details'][0]
        assert mes2['missingProperties'][0] == no_url_field

    @pytest.mark.parametrize('no_url_field', url_dict.keys())
    def test_url_wrong_type(self, mongo_db, no_url_field):
        """ Try to insert an url with wrong type of the  field.

        :param mongo_db: fixture with ModelMongo() instance and roots
        :param no_url_field: right url document
        :return:
        """
        wrong_dict = self.url_dict.copy()
        wrong_dict[no_url_field]  = 2.0  # float is wrong for all fields
        with pytest.raises(pme.WriteError, match='Document failed validation*') as e:
            mongo_db.bm.update_one({'name': 'roots'},
                                   {'$push': {'children': wrong_dict}})

        mes1 = e.value.details['errInfo']['details']['schemaRulesNotSatisfied'][0]  # the dict with error text
        mes2 = mes1['propertiesNotSatisfied'][0]['details'][0]['details'][0]
        assert mes2['propertiesNotSatisfied'][0]['propertyName'] == no_url_field
    def test_url_extra_field(self, mongo_db):
        """ Try to insert an url with an excessive field.

        :param mongo_db: fixture with ModelMongo() instance and roots
        :param no_folder_field: right folder document
        :return:
        """
        wrong_dict = self.url_dict.copy()
        wrong_dict['excessive'] = 'intruder'  # add an extra field to the dict
        with pytest.raises(pme.WriteError, match='Document failed validation*') as e:
            mongo_db.bm.update_one({'name': 'roots'},
                                   {'$push': {'children': wrong_dict}})
        mes1 = e.value.details['errInfo']['details']['schemaRulesNotSatisfied'][0]  # the dict with error text
        mes2 = mes1['propertiesNotSatisfied'][0]['details'][0]['details'][0]
        assert mes2['additionalProperties'][0] == 'excessive'

    def test_insert_double_url(self, mongo_db):
        """Try to double urls with the same id.

        :param mongo_db: fixture with ModelMongo() instance and roots
        :param no_folder_field: right folder document
        :return:
        """
        right_dict = self.url_dict
        mongo_db.bm.update_one({'name': 'roots'},
                               {'$push': {'children': right_dict}})  # insert 1st url
        with pytest.raises(pme.WriteError, match='Document failed validation*') as e:
            mongo_db.bm.update_one({'name': 'roots'},
                                   {'$push': {'children': right_dict}})
        mes1 = e.value.details['errInfo']['details']['schemaRulesNotSatisfied'][0]  # the dict with error text
        assert mes1['propertiesNotSatisfied'][0]['details'][0]['reason'] == 'found a duplicate item'
