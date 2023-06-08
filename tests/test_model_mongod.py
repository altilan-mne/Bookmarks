"""Test module for the Model module of Bookmark Manager, local standalone mongod implementation.
Uses fixture freeze_uuids from pytest_frozen_uuids plugin: https://pypi.org/project/pytest-frozen-uuids/
Uses fixture freezer from pytest-freezer plugin: https://github.com/pytest-dev/pytest-freezer

"""
import pytest
import uuid
import sys, time
from datetime import datetime

from pymongo import MongoClient, database, collection
import pymongo.errors as pme



from model_mongod import ModelMongod
from model_mongod import LOCAL_URI, COLLECTION_NAME
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


@pytest.fixture(scope='class')
def fivefolders_foururls(model_mongod):
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
    model_mongod.create_database(DB_NAME)  # create a test database of the model instance
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


class TestGetChildren:
    """Class of get_children and _get_child_folders methods testing with parametrization.
    Create a test set of documents from 5 folders and 5 urls, see below:
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
    def test_get_child_folders(self, fivefolders_foururls, folder, children):
        """Testing of _get_child_folders method.

        :param fivefolders_foururls: fixture db with 5 folders and 4 urls
        :param folder: names of the testing folders
        :param children: lists of the folder's children

        """
        folder_id = fivefolders_foururls.bm.find_one({'name': folder})  # get folder guid
        assert set(fivefolders_foururls._get_child_folders(folder_id['_id'])) == set(children)  # compare w/o order

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

    def test_get_children(self, fivefolders_foururls, node, children):
        """Testing of get_children method, without errors.

        :param fivefolders_foururls: fixture db with 5 folders and 4 urls
        :param node: names of the testing nodes
        :param children: lists of the node's children

        """
        res, actual_children = fivefolders_foururls.get_children(node)
        assert (res, set(actual_children)) == children  # compare w/o order

    def test_get_children_no_node(self, fivefolders_foururls):
        """Try to get children from non-existing node.

        :param fivefolders_foururls: fixture db with 5 folders and 4 urls

        """
        with pytest.raises(e.NodeNotExists, match='unknown'):
            fivefolders_foururls.get_children('unknown')

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


